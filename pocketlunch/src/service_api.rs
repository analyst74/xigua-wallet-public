use anyhow::{Context, Result};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use time::Month;
use tokio::net::TcpListener;

use crate::config::AppConfig;
use crate::lm_v2_client::LmV2Client;
use crate::models::CategorizeDecision;
use crate::store::Store;
use crate::sync_engine::SyncEngine;

#[derive(Clone)]
struct ApiState {
    store: Store,
    sync_engine: Option<SyncEngine>,
}

pub async fn run_http_server(
    config: AppConfig,
    store: Store,
    host: String,
    port: u16,
) -> Result<()> {
    let sync_engine = config.api_token.as_ref().map(|token| {
        let client = LmV2Client::new(config.base_url.clone(), token.clone());
        SyncEngine::new(config.clone(), store.clone(), client)
    });

    let state = ApiState { store, sync_engine };
    let app = build_app(state);

    let bind_addr = format!("{host}:{port}");
    let listener = TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("failed binding API listener on {bind_addr}"))?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("api server exited with error")?;

    Ok(())
}

fn build_app(state: ApiState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/sync/pull", post(sync_pull))
        .route(
            "/v1/sync/pull-non-transactions",
            post(sync_pull_non_transactions),
        )
        .route("/v1/sync/push", post(sync_push))
        .route("/v1/sync/all", post(sync_all))
        .route("/v1/outbox", get(list_outbox))
        .route("/v1/outbox/requeue", post(outbox_requeue))
        .route("/v1/audit/events", get(audit_events))
        .route("/v1/:entity", get(list_entity).post(create_entity))
        .route(
            "/v1/:entity/:entity_id",
            get(get_entity).put(update_entity).delete(delete_entity),
        )
        .with_state(state)
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

type ApiResponse = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn bad_request(message: impl Into<String>) -> (StatusCode, Json<Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": message.into() })),
    )
}

fn method_not_allowed(message: impl Into<String>) -> (StatusCode, Json<Value>) {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        Json(json!({ "error": message.into() })),
    )
}

fn internal_error(err: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": err.to_string() })),
    )
}

fn not_found(message: impl Into<String>) -> (StatusCode, Json<Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": message.into() })),
    )
}

fn missing_token_error() -> (StatusCode, Json<Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({
            "error": "LUNCH_MONEY_API_TOKEN is required for sync endpoints"
        })),
    )
}

fn entity_error(err: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    let message = err.to_string();
    if message.contains("unsupported entity") {
        return not_found(message);
    }
    if message.contains("local write operations are not supported")
        || message.contains("local delete operations are not supported")
    {
        return method_not_allowed(message);
    }
    if message.contains("invalid id") || message.contains("requires start_date") {
        return bad_request(message);
    }
    internal_error(message)
}

async fn healthz() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn sync_pull(State(state): State<ApiState>) -> ApiResponse {
    let Some(engine) = state.sync_engine.as_ref() else {
        return Err(missing_token_error());
    };
    let run_id = engine.sync_pull().await.map_err(internal_error)?;
    Ok(Json(json!({ "run_id": run_id })))
}

async fn sync_pull_non_transactions(State(state): State<ApiState>) -> ApiResponse {
    let Some(engine) = state.sync_engine.as_ref() else {
        return Err(missing_token_error());
    };
    let run_id = engine
        .sync_pull_non_transactions()
        .await
        .map_err(internal_error)?;
    Ok(Json(json!({ "run_id": run_id })))
}

async fn sync_push(State(state): State<ApiState>) -> ApiResponse {
    let Some(engine) = state.sync_engine.as_ref() else {
        return Err(missing_token_error());
    };
    let run_id = engine.sync_push().await.map_err(internal_error)?;
    Ok(Json(json!({ "run_id": run_id })))
}

async fn sync_all(State(state): State<ApiState>) -> ApiResponse {
    let Some(engine) = state.sync_engine.as_ref() else {
        return Err(missing_token_error());
    };
    let run_id = engine.sync_all().await.map_err(internal_error)?;
    Ok(Json(json!({ "run_id": run_id })))
}

#[derive(Deserialize)]
struct RequeueRequest {
    change_id: String,
}

async fn outbox_requeue(
    State(state): State<ApiState>,
    Json(body): Json<RequeueRequest>,
) -> ApiResponse {
    state
        .store
        .requeue_outbox_change(&body.change_id)
        .await
        .map_err(internal_error)?;

    Ok(Json(json!({ "requeued": body.change_id })))
}

#[derive(Deserialize)]
struct OutboxListQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

async fn list_outbox(
    State(state): State<ApiState>,
    Query(query): Query<OutboxListQuery>,
) -> ApiResponse {
    let limit = query.limit.unwrap_or(200).clamp(1, 1000);
    let offset = query.offset.unwrap_or(0).max(0);
    let (rows, total) = state
        .store
        .list_outbox_changes(limit, offset)
        .await
        .map_err(internal_error)?;

    let items = rows
        .into_iter()
        .map(|row| {
            json!({
                "change_id": row.change_id,
                "entity_type": row.entity_type,
                "entity_id": row.entity_id,
                "op": row.op,
                "payload_json": parse_json_string(row.payload_json),
                "idempotency_key": row.idempotency_key,
                "status": row.status,
                "attempt_count": row.attempt_count,
                "last_error": row.last_error,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            })
        })
        .collect::<Vec<_>>();

    Ok(Json(json!({
        "count": items.len(),
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": items
    })))
}

#[derive(Deserialize)]
struct AuditQuery {
    run_id: Option<String>,
    entity: Option<String>,
    entity_id: Option<String>,
    limit: Option<i64>,
}

async fn audit_events(
    State(state): State<ApiState>,
    Query(query): Query<AuditQuery>,
) -> ApiResponse {
    let limit = query.limit.unwrap_or(200).clamp(1, 1000);
    let rows = state
        .store
        .list_events(
            query.run_id.as_deref(),
            query.entity.as_deref(),
            query.entity_id.as_deref(),
            limit,
        )
        .await
        .map_err(internal_error)?;

    let events = rows
        .into_iter()
        .map(|row| {
            json!({
                "event_id": row.event_id,
                "run_id": row.run_id,
                "source": row.source,
                "action": row.action,
                "entity_type": row.entity_type,
                "entity_id": row.entity_id,
                "before_json": parse_optional_json(row.before_json),
                "after_json": parse_optional_json(row.after_json),
                "meta_json": parse_json_string(row.meta_json),
                "occurred_at": row.occurred_at,
            })
        })
        .collect::<Vec<_>>();

    Ok(Json(json!({
        "count": events.len(),
        "events": events
    })))
}

#[derive(Deserialize)]
struct EntityListQuery {
    start_date: Option<String>,
    end_date: Option<String>,
    category_id: Option<i64>,
    limit: Option<i64>,
    offset: Option<i64>,
}

async fn list_entity(
    State(state): State<ApiState>,
    Path(entity): Path<String>,
    Query(query): Query<EntityListQuery>,
) -> ApiResponse {
    let normalized_entity = entity.to_ascii_lowercase().replace('-', "_");
    let limit = query.limit.unwrap_or(200).clamp(1, 1000);
    let offset = query.offset.unwrap_or(0).max(0);

    if matches!(normalized_entity.as_str(), "transaction" | "transactions") {
        let start_date = parse_date_filter(query.start_date, "start_date")?;
        let end_date = parse_date_filter(query.end_date, "end_date")?;
        if let (Some(start), Some(end)) = (&start_date, &end_date) {
            if start > end {
                return Err(bad_request("start_date must be <= end_date"));
            }
        }

        let items = state
            .store
            .list_transactions_filtered(
                start_date.as_deref(),
                end_date.as_deref(),
                query.category_id,
                limit,
                offset,
            )
            .await
            .map_err(entity_error)?;
        return Ok(Json(json!({
            "entity": entity,
            "count": items.len(),
            "items": items
        })));
    }

    let items = state
        .store
        .list_entity_records(&entity, limit)
        .await
        .map_err(entity_error)?;
    Ok(Json(json!({
        "entity": entity,
        "count": items.len(),
        "items": items
    })))
}

async fn get_entity(
    State(state): State<ApiState>,
    Path((entity, entity_id)): Path<(String, String)>,
) -> ApiResponse {
    let item = state
        .store
        .get_entity_record(&entity, &entity_id)
        .await
        .map_err(entity_error)?;

    let Some(item) = item else {
        return Err(not_found(format!(
            "entity record not found: {entity}/{entity_id}"
        )));
    };

    Ok(Json(json!({
        "entity": entity,
        "id": entity_id,
        "item": item
    })))
}

async fn create_entity(
    State(state): State<ApiState>,
    Path(entity): Path<String>,
    Json(body): Json<Value>,
) -> ApiResponse {
    let upserted = state
        .store
        .upsert_entity_record(None, "local", "create", &entity, &body)
        .await
        .map_err(entity_error)?;
    Ok(Json(json!({
        "entity": entity,
        "upserted": upserted
    })))
}

async fn update_entity(
    State(state): State<ApiState>,
    Path((entity, entity_id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> ApiResponse {
    let normalized_entity = entity.to_ascii_lowercase().replace('-', "_");
    if matches!(normalized_entity.as_str(), "transaction" | "transactions") {
        return update_transaction_entity(state, entity, entity_id, body).await;
    }

    let mut record = body;
    let object = record
        .as_object_mut()
        .ok_or_else(|| bad_request("request body must be a JSON object"))?;

    if matches!(
        normalized_entity.as_str(),
        "summary" | "summary_period" | "summary_periods"
    ) {
        object.insert("period_key".to_string(), Value::String(entity_id.clone()));
        if let Some((start_date, end_date)) = entity_id.split_once(':') {
            object
                .entry("start_date".to_string())
                .or_insert_with(|| Value::String(start_date.to_string()));
            object
                .entry("end_date".to_string())
                .or_insert_with(|| Value::String(end_date.to_string()));
        }
    } else {
        let parsed_id = entity_id
            .parse::<i64>()
            .map_err(|_| bad_request("entity id must be an integer"))?;
        object.insert("id".to_string(), Value::from(parsed_id));
    }

    let upserted = state
        .store
        .upsert_entity_record(None, "local", "update", &entity, &record)
        .await
        .map_err(entity_error)?;

    Ok(Json(json!({
        "entity": entity,
        "id": entity_id,
        "upserted": upserted
    })))
}

async fn delete_entity(
    State(state): State<ApiState>,
    Path((entity, entity_id)): Path<(String, String)>,
    Query(query): Query<CategoryDeleteQuery>,
) -> ApiResponse {
    let normalized_entity = entity.to_ascii_lowercase().replace('-', "_");
    if matches!(normalized_entity.as_str(), "category" | "categories") {
        let category_id = entity_id
            .parse::<i64>()
            .map_err(|_| bad_request("entity id must be an integer"))?;
        let change_id = state
            .store
            .apply_local_category_delete(None, category_id, query.force, query.reason.as_deref())
            .await
            .map_err(entity_error)?;
        return Ok(Json(json!({
            "entity": entity,
            "id": entity_id,
            "archived": true,
            "change_id": change_id,
            "force": query.force,
        })));
    }

    let deleted = state
        .store
        .delete_entity_record(None, "local", &entity, &entity_id)
        .await
        .map_err(entity_error)?;

    if !deleted {
        return Err(not_found(format!(
            "entity record not found: {entity}/{entity_id}"
        )));
    }

    Ok(Json(json!({
        "entity": entity,
        "id": entity_id,
        "deleted": true
    })))
}

#[derive(Deserialize)]
struct CategoryDeleteQuery {
    #[serde(default)]
    force: bool,
    reason: Option<String>,
}

fn parse_optional_json(input: Option<String>) -> Option<Value> {
    input.map(parse_json_string)
}

fn parse_json_string(input: String) -> Value {
    serde_json::from_str(&input).unwrap_or_else(|_| Value::String(input))
}

fn parse_date_filter(
    value: Option<String>,
    field_name: &str,
) -> Result<Option<String>, (StatusCode, Json<Value>)> {
    let Some(value) = value else {
        return Ok(None);
    };

    if !is_valid_yyyy_mm_dd(&value) {
        return Err(bad_request(format!(
            "invalid {field_name}; expected YYYY-MM-DD"
        )));
    }
    Ok(Some(value))
}

fn is_valid_yyyy_mm_dd(value: &str) -> bool {
    if value.len() != 10 {
        return false;
    }

    let bytes = value.as_bytes();
    if bytes[4] != b'-' || bytes[7] != b'-' {
        return false;
    }
    if !bytes
        .iter()
        .enumerate()
        .all(|(idx, b)| (idx == 4 || idx == 7) || b.is_ascii_digit())
    {
        return false;
    }

    let year = match value[0..4].parse::<i32>() {
        Ok(year) => year,
        Err(_) => return false,
    };
    let month = match value[5..7].parse::<u8>() {
        Ok(month) => month,
        Err(_) => return false,
    };
    let day = match value[8..10].parse::<u8>() {
        Ok(day) => day,
        Err(_) => return false,
    };

    let month = match Month::try_from(month) {
        Ok(month) => month,
        Err(_) => return false,
    };

    time::Date::from_calendar_date(year, month, day).is_ok()
}

fn int_field(value: &Value, key: &str) -> Option<i64> {
    let field = value.get(key)?;
    if let Some(v) = field.as_i64() {
        return Some(v);
    }
    if let Some(v) = field.as_u64() {
        return i64::try_from(v).ok();
    }
    field.as_str()?.parse::<i64>().ok()
}

async fn update_transaction_entity(
    state: ApiState,
    entity: String,
    entity_id: String,
    body: Value,
) -> ApiResponse {
    let transaction_id = entity_id
        .parse::<i64>()
        .map_err(|_| bad_request("entity id must be an integer"))?;
    let patch = body
        .as_object()
        .ok_or_else(|| bad_request("request body must be a JSON object"))?
        .clone();

    let current = state
        .store
        .get_entity_record(&entity, &entity_id)
        .await
        .map_err(entity_error)?;
    let Some(mut merged_record) = current else {
        return Err(not_found(format!(
            "entity record not found: {entity}/{entity_id}"
        )));
    };

    let previous_category_id = int_field(&merged_record, "category_id");
    {
        let object = merged_record
            .as_object_mut()
            .ok_or_else(|| bad_request("stored transaction must be a JSON object"))?;
        for (key, value) in patch {
            object.insert(key, value);
        }
        object.insert("id".to_string(), Value::from(transaction_id));
    }

    let requested_category_id = int_field(&merged_record, "category_id");
    let mut upsert_record = merged_record.clone();
    let mut change_id: Option<String> = None;

    if requested_category_id != previous_category_id {
        if let Some(object) = upsert_record.as_object_mut() {
            match previous_category_id {
                Some(category_id) => {
                    object.insert("category_id".to_string(), Value::from(category_id));
                }
                None => {
                    object.insert("category_id".to_string(), Value::Null);
                }
            }
        }
    }

    let upserted = state
        .store
        .upsert_entity_record(None, "local", "update", &entity, &upsert_record)
        .await
        .map_err(entity_error)?;

    if requested_category_id != previous_category_id {
        if let Some(category_id) = requested_category_id {
            change_id = Some(
                state
                    .store
                    .apply_local_category_decision(
                        None,
                        &CategorizeDecision {
                            transaction_id,
                            category_id,
                            reason: None,
                        },
                    )
                    .await
                    .map_err(entity_error)?,
            );
        }
    }

    Ok(Json(json!({
        "entity": entity,
        "id": entity_id,
        "upserted": upserted,
        "change_id": change_id,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::{Method, Request, StatusCode};
    use sqlx::postgres::PgPoolOptions;
    use tower::ServiceExt;
    use uuid::Uuid;

    fn test_database_base_url() -> Option<String> {
        std::env::var("TEST_DATABASE_URL")
            .ok()
            .or_else(|| std::env::var("DATABASE_URL").ok())
    }

    fn database_url_with_database(base_url: &str, database: &str) -> Result<String> {
        let (head, query) = match base_url.split_once('?') {
            Some((head, query)) => (head, Some(query)),
            None => (base_url, None),
        };
        let (prefix, _) = head
            .rsplit_once('/')
            .ok_or_else(|| anyhow::anyhow!("invalid Postgres URL, missing database path"))?;

        let mut url = format!("{prefix}/{database}");
        if let Some(query) = query {
            url.push('?');
            url.push_str(query);
        }
        Ok(url)
    }

    async fn provision_test_database_url() -> Result<Option<String>> {
        let Some(base_url) = test_database_base_url() else {
            return Ok(None);
        };

        let database_name = format!("t_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&base_url)
            .await?;

        let create_database = sqlx::query(&format!("CREATE DATABASE {database_name}"))
            .execute(&admin_pool)
            .await;
        if create_database.is_ok() {
            drop(admin_pool);
            return Ok(Some(database_url_with_database(&base_url, &database_name)?));
        }

        let schema = format!("t_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
            .execute(&admin_pool)
            .await?;
        drop(admin_pool);

        let separator = if base_url.contains('?') { '&' } else { '?' };
        Ok(Some(format!(
            "{base_url}{separator}options=-c%20search_path%3D{schema}"
        )))
    }

    async fn setup_app() -> Result<Option<Router>> {
        let Some(database_url) = provision_test_database_url().await? else {
            return Ok(None);
        };

        let config = AppConfig {
            database_url,
            base_url: "http://localhost".to_string(),
            api_token: None,
            default_lookback_seconds: 300,
            pull_page_size: 100,
            outbox_batch_size: 100,
            transaction_update_batch_size: 50,
            max_push_attempts: 8,
        };

        let store = Store::new(&config).await?;
        store.bootstrap().await?;

        Ok(Some(build_app(ApiState {
            store,
            sync_engine: None,
        })))
    }

    async fn json_body(response: axum::response::Response) -> Value {
        let bytes = to_bytes(response.into_body(), 4 * 1024 * 1024)
            .await
            .expect("body bytes");
        serde_json::from_slice(&bytes).expect("valid json response")
    }

    fn transaction_payload(id: i64, date: &str, category_id: i64, updated_at: &str) -> Value {
        json!({
            "id": id,
            "date": date,
            "amount": "12.34",
            "currency": "USD",
            "payee": "Shop",
            "notes": "note",
            "category_id": category_id,
            "asset_id": 1,
            "status": "cleared",
            "is_pending": false,
            "external_id": format!("ext-{id}"),
            "updated_at": updated_at
        })
    }

    #[tokio::test]
    async fn categories_crud_endpoints_work() -> Result<()> {
        let Some(app) = setup_app().await? else {
            return Ok(());
        };

        let create = Request::builder()
            .method(Method::POST)
            .uri("/v1/categories")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "id": 4242,
                    "name": "Test Category",
                    "is_group": false,
                    "archived": false,
                    "updated_at": "2026-03-05T00:00:00Z"
                })
                .to_string(),
            ))?;
        let create_resp = app.clone().oneshot(create).await?;
        assert_eq!(create_resp.status(), StatusCode::OK);

        let list_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/categories")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(list_resp.status(), StatusCode::OK);
        let list_json = json_body(list_resp).await;
        assert!(list_json["count"].as_i64().unwrap_or_default() >= 1);

        let get_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/categories/4242")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(get_resp.status(), StatusCode::OK);
        let get_json = json_body(get_resp).await;
        assert_eq!(get_json["item"]["id"], 4242);

        let update_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/v1/categories/4242")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "name": "Updated Category",
                            "is_group": false,
                            "archived": false,
                            "updated_at": "2026-03-05T00:01:00Z"
                        })
                        .to_string(),
                    ))?,
            )
            .await?;
        assert_eq!(update_resp.status(), StatusCode::OK);

        let delete_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/v1/categories/4242?reason=test-cleanup")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(delete_resp.status(), StatusCode::OK);
        let delete_json = json_body(delete_resp).await;
        assert_eq!(delete_json["archived"], true);
        assert!(delete_json["change_id"].as_str().is_some());

        let get_archived_resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/categories/4242")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(get_archived_resp.status(), StatusCode::OK);
        let get_archived_json = json_body(get_archived_resp).await;
        assert_eq!(get_archived_json["item"]["archived"], true);

        Ok(())
    }

    #[tokio::test]
    async fn replicated_read_models_reject_local_writes() -> Result<()> {
        let Some(app) = setup_app().await? else {
            return Ok(());
        };

        let cases = [
            (
                Method::POST,
                "/v1/assets",
                Some(json!({
                    "id": 7,
                    "name": "Cash",
                    "currency": "USD",
                    "balance": "10.00",
                    "updated_at": "2026-03-05T00:00:00Z"
                })),
            ),
            (
                Method::PUT,
                "/v1/assets/7",
                Some(json!({
                    "name": "Cash"
                })),
            ),
            (Method::DELETE, "/v1/assets/7", None),
            (
                Method::POST,
                "/v1/me",
                Some(json!({
                    "id": 1,
                    "budget_name": "Mine",
                    "updated_at": "2026-03-05T00:00:00Z"
                })),
            ),
            (
                Method::POST,
                "/v1/plaid_accounts",
                Some(json!({
                    "id": 9,
                    "name": "Checking",
                    "updated_at": "2026-03-05T00:00:00Z"
                })),
            ),
            (
                Method::POST,
                "/v1/manual_accounts",
                Some(json!({
                    "id": 10,
                    "name": "Wallet",
                    "updated_at": "2026-03-05T00:00:00Z"
                })),
            ),
            (
                Method::POST,
                "/v1/tags",
                Some(json!({
                    "id": 11,
                    "name": "Trip",
                    "updated_at": "2026-03-05T00:00:00Z"
                })),
            ),
            (
                Method::POST,
                "/v1/recurring_items",
                Some(json!({
                    "id": 12,
                    "payee": "Rent",
                    "updated_at": "2026-03-05T00:00:00Z"
                })),
            ),
            (
                Method::POST,
                "/v1/summary_periods",
                Some(json!({
                    "period_key": "2026-03-01:2026-03-31",
                    "start_date": "2026-03-01",
                    "end_date": "2026-03-31",
                    "aligned": true,
                    "categories": []
                })),
            ),
            (
                Method::PUT,
                "/v1/summary_periods/2026-03-01:2026-03-31",
                Some(json!({
                    "aligned": false
                })),
            ),
            (
                Method::DELETE,
                "/v1/summary_periods/2026-03-01:2026-03-31",
                None,
            ),
        ];

        for (method, uri, body) in cases {
            let mut request = Request::builder().method(method.clone()).uri(uri);
            let body = if let Some(body) = body {
                request = request.header("content-type", "application/json");
                Body::from(body.to_string())
            } else {
                Body::empty()
            };

            let resp = app.clone().oneshot(request.body(body)?).await?;
            assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED, "{method} {uri}");
            let resp_json = json_body(resp).await;
            assert!(resp_json["error"].as_str().unwrap_or_default().contains("local"));
        }

        Ok(())
    }

    #[tokio::test]
    async fn unknown_entity_returns_not_found() -> Result<()> {
        let Some(app) = setup_app().await? else {
            return Ok(());
        };

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/not_real_entity")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        Ok(())
    }

    #[tokio::test]
    async fn transactions_query_supports_date_range_category_and_pagination() -> Result<()> {
        let Some(app) = setup_app().await? else {
            return Ok(());
        };

        for payload in [
            transaction_payload(1, "2026-01-03", 1, "2026-01-03T00:00:00Z"),
            transaction_payload(2, "2026-01-03", 2, "2026-01-03T00:01:00Z"),
            transaction_payload(3, "2026-01-02", 1, "2026-01-02T00:00:00Z"),
            transaction_payload(4, "2026-01-01", 1, "2026-01-01T00:00:00Z"),
        ] {
            let resp = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri("/v1/transactions")
                        .header("content-type", "application/json")
                        .body(Body::from(payload.to_string()))?,
                )
                .await?;
            assert_eq!(resp.status(), StatusCode::OK);
        }

        let single_date = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/transactions?start_date=2026-01-03&end_date=2026-01-03")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(single_date.status(), StatusCode::OK);
        let single_date_json = json_body(single_date).await;
        assert_eq!(single_date_json["count"], 2);
        assert_eq!(single_date_json["items"][0]["id"], 2);
        assert_eq!(single_date_json["items"][1]["id"], 1);

        let ranged = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/transactions?start_date=2026-01-02&end_date=2026-01-03&category_id=1")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(ranged.status(), StatusCode::OK);
        let ranged_json = json_body(ranged).await;
        assert_eq!(ranged_json["count"], 2);
        assert_eq!(ranged_json["items"][0]["id"], 1);
        assert_eq!(ranged_json["items"][1]["id"], 3);

        let paged = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/transactions?limit=1&offset=1")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(paged.status(), StatusCode::OK);
        let paged_json = json_body(paged).await;
        assert_eq!(paged_json["count"], 1);
        assert_eq!(paged_json["items"][0]["id"], 1);

        Ok(())
    }

    #[tokio::test]
    async fn transactions_query_rejects_invalid_date() -> Result<()> {
        let Some(app) = setup_app().await? else {
            return Ok(());
        };

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/transactions?start_date=2026-13-01")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = json_body(resp).await;
        assert!(body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("invalid start_date"));

        Ok(())
    }

    #[tokio::test]
    async fn outbox_query_supports_pagination() -> Result<()> {
        let Some(app) = setup_app().await? else {
            return Ok(());
        };

        for category_id in [1_i64, 2_i64, 3_i64] {
            let create_category = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri("/v1/categories")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            json!({
                                "id": category_id,
                                "name": format!("Category {category_id}"),
                                "is_group": false,
                                "archived": false,
                                "updated_at": format!("2026-01-0{category_id}T00:00:00Z")
                            })
                            .to_string(),
                        ))?,
                )
                .await?;
            assert_eq!(create_category.status(), StatusCode::OK);

            let delete_category = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::DELETE)
                        .uri(&format!("/v1/categories/{category_id}?reason=cleanup%20{category_id}"))
                        .body(Body::empty())?,
                )
                .await?;
            assert_eq!(delete_category.status(), StatusCode::OK);
        }

        let paged = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/outbox?limit=1&offset=1")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(paged.status(), StatusCode::OK);
        let paged_json = json_body(paged).await;
        assert_eq!(paged_json["count"], 1);
        assert_eq!(paged_json["total"], 3);
        assert_eq!(paged_json["limit"], 1);
        assert_eq!(paged_json["offset"], 1);
        assert_eq!(paged_json["items"][0]["entity_type"], "category");
        assert_eq!(paged_json["items"][0]["entity_id"], "2");
        assert_eq!(paged_json["items"][0]["op"], "delete");
        assert_eq!(paged_json["items"][0]["status"], "pending");
        assert_eq!(
            paged_json["items"][0]["payload_json"]["reason"],
            "cleanup 2"
        );

        Ok(())
    }

    #[tokio::test]
    async fn put_transaction_category_change_enqueues_outbox_change() -> Result<()> {
        let Some(app) = setup_app().await? else {
            return Ok(());
        };

        for category_id in [1_i64, 2_i64] {
            let create_category = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri("/v1/categories")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            json!({
                                "id": category_id,
                                "name": format!("Category {category_id}"),
                                "is_group": false,
                                "archived": false,
                                "updated_at": "2026-01-01T00:00:00Z"
                            })
                            .to_string(),
                        ))?,
                )
                .await?;
            assert_eq!(create_category.status(), StatusCode::OK);
        }

        let create_txn = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/transactions")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        transaction_payload(100, "2026-01-03", 1, "2026-01-03T00:00:00Z")
                            .to_string(),
                    ))?,
            )
            .await?;
        assert_eq!(create_txn.status(), StatusCode::OK);

        let update_txn = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/v1/transactions/100")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "category_id": 2 }).to_string()))?,
            )
            .await?;
        assert_eq!(update_txn.status(), StatusCode::OK);
        let update_json = json_body(update_txn).await;
        assert!(update_json["change_id"].as_str().is_some());

        let audit_resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/audit/events?entity=transaction&entity_id=100&limit=20")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(audit_resp.status(), StatusCode::OK);
        let audit_json = json_body(audit_resp).await;
        let has_local_enqueue = audit_json["events"].as_array().is_some_and(|events| {
            events.iter().any(|event| {
                event["source"] == "local"
                    && event["action"] == "enqueue"
                    && event["entity_type"] == "transaction"
            })
        });
        assert!(has_local_enqueue);

        Ok(())
    }

    #[tokio::test]
    async fn post_existing_transaction_category_change_enqueues_outbox_change() -> Result<()> {
        let Some(app) = setup_app().await? else {
            return Ok(());
        };

        for category_id in [1_i64, 2_i64] {
            let create_category = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri("/v1/categories")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            json!({
                                "id": category_id,
                                "name": format!("Category {category_id}"),
                                "is_group": false,
                                "archived": false,
                                "updated_at": "2026-01-01T00:00:00Z"
                            })
                            .to_string(),
                        ))?,
                )
                .await?;
            assert_eq!(create_category.status(), StatusCode::OK);
        }

        let create_txn = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/transactions")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        transaction_payload(101, "2026-01-03", 1, "2026-01-03T00:00:00Z")
                            .to_string(),
                    ))?,
            )
            .await?;
        assert_eq!(create_txn.status(), StatusCode::OK);

        let update_txn = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/transactions")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        transaction_payload(101, "2026-01-03", 2, "2026-01-03T00:10:00Z")
                            .to_string(),
                    ))?,
            )
            .await?;
        assert_eq!(update_txn.status(), StatusCode::OK);

        let audit_resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/audit/events?entity=transaction&entity_id=101&limit=20")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(audit_resp.status(), StatusCode::OK);
        let audit_json = json_body(audit_resp).await;
        let has_local_enqueue = audit_json["events"].as_array().is_some_and(|events| {
            events.iter().any(|event| {
                event["source"] == "local"
                    && event["action"] == "enqueue"
                    && event["entity_type"] == "transaction"
            })
        });
        assert!(has_local_enqueue);

        Ok(())
    }

    #[tokio::test]
    async fn generic_category_delete_archives_and_enqueues() -> Result<()> {
        let Some(app) = setup_app().await? else {
            return Ok(());
        };

        let create_category = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/categories")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "id": 4242,
                            "name": "Disposable",
                            "is_group": false,
                            "archived": false,
                            "updated_at": "2026-01-01T00:00:00Z"
                        })
                        .to_string(),
                    ))?,
            )
            .await?;
        assert_eq!(create_category.status(), StatusCode::OK);

        let delete_category = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/v1/categories/4242?force=true&reason=cleanup")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(delete_category.status(), StatusCode::OK);
        let delete_json = json_body(delete_category).await;
        assert_eq!(delete_json["archived"], true);
        assert_eq!(delete_json["force"], true);
        assert!(delete_json["change_id"].as_str().is_some());

        let audit_resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/audit/events?entity=category&entity_id=4242&limit=20")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(audit_resp.status(), StatusCode::OK);
        let audit_json = json_body(audit_resp).await;
        let has_local_enqueue = audit_json["events"].as_array().is_some_and(|events| {
            events.iter().any(|event| {
                event["source"] == "local"
                    && event["action"] == "enqueue"
                    && event["entity_type"] == "category"
            })
        });
        assert!(has_local_enqueue);

        Ok(())
    }
}
