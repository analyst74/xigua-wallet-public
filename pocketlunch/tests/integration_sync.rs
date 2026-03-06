use pocketlunch::config::AppConfig;
use pocketlunch::lm_v2_client::LmV2Client;
use pocketlunch::models::CategorizeDecision;
use pocketlunch::store::Store;
use pocketlunch::sync_engine::SyncEngine;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use uuid::Uuid;
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn test_config(base_url: String, database_url: String) -> AppConfig {
    AppConfig {
        database_url,
        base_url,
        api_token: Some("test-token".to_string()),
        default_lookback_seconds: 300,
        pull_page_size: 100,
        outbox_batch_size: 100,
        transaction_update_batch_size: 50,
        max_push_attempts: 8,
    }
}

fn test_database_base_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("DATABASE_URL").ok())
}

fn has_test_database() -> bool {
    test_database_base_url().is_some()
}

fn database_url_with_database(base_url: &str, database: &str) -> anyhow::Result<String> {
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

async fn provision_test_database_url() -> anyhow::Result<Option<String>> {
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
    let url = format!("{base_url}{separator}options=-c%20search_path%3D{schema}");
    Ok(Some(url))
}

async fn setup_engine(server: &MockServer) -> anyhow::Result<(Store, SyncEngine)> {
    let Some(database_url) = provision_test_database_url().await? else {
        anyhow::bail!("missing TEST_DATABASE_URL or DATABASE_URL");
    };
    let config = test_config(server.uri(), database_url);
    setup_engine_with_config(config).await
}

async fn setup_engine_with_config(config: AppConfig) -> anyhow::Result<(Store, SyncEngine)> {
    let store = Store::new(&config).await?;
    store.bootstrap().await?;

    let client = LmV2Client::new(config.base_url.clone(), "test-token");
    let engine = SyncEngine::new(config, store.clone(), client);
    Ok((store, engine))
}

fn sample_category(id: i64, name: &str) -> serde_json::Value {
    json!({
        "id": id,
        "name": name,
        "is_group": false,
        "archived": false,
        "updated_at": "2026-01-01T00:00:00Z"
    })
}

fn sample_asset(id: i64, name: &str) -> serde_json::Value {
    json!({
        "id": id,
        "name": name,
        "type_name": "cash",
        "updated_at": "2026-01-01T00:00:00Z"
    })
}

fn sample_transaction_with_id(id: i64, category_id: i64, updated_at: &str) -> serde_json::Value {
    json!({
        "id": id,
        "date": "2026-01-01",
        "amount": "12.34",
        "currency": "USD",
        "payee": "Coffee Shop",
        "notes": "test",
        "category_id": category_id,
        "asset_id": 201,
        "status": "cleared",
        "is_pending": false,
        "external_id": "abc",
        "updated_at": updated_at
    })
}

fn sample_transaction(category_id: i64, updated_at: &str) -> serde_json::Value {
    sample_transaction_with_id(101, category_id, updated_at)
}

#[tokio::test]
async fn sync_all_is_idempotent_for_same_remote_data() -> anyhow::Result<()> {
    if !has_test_database() {
        return Ok(());
    }

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/categories"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([sample_category(1, "Food")])))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/assets"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!([sample_asset(201, "Checking")])),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/transactions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!([sample_transaction(1, "2026-01-01T00:10:00Z")])),
        )
        .mount(&server)
        .await;

    let (store, engine) = setup_engine(&server).await?;

    engine.sync_all().await?;
    engine.sync_all().await?;

    let txn_count: (i64,) = sqlx::query_as("SELECT COUNT(1) FROM lm_transactions")
        .fetch_one(store.pool())
        .await?;
    assert_eq!(txn_count.0, 1);

    let pull_event_count: (i64,) =
        sqlx::query_as("SELECT COUNT(1) FROM event_log WHERE source = 'pull'")
            .fetch_one(store.pool())
            .await?;
    assert_eq!(pull_event_count.0, 3);

    Ok(())
}

#[tokio::test]
async fn pull_stats_include_fetch_modes() -> anyhow::Result<()> {
    if !has_test_database() {
        return Ok(());
    }

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/categories"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([sample_category(1, "Food")])))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/assets"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!([sample_asset(201, "Checking")])),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/transactions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!([sample_transaction(1, "2026-01-01T00:10:00Z")])),
        )
        .mount(&server)
        .await;

    let (store, engine) = setup_engine(&server).await?;

    let run_id = engine.sync_pull().await?;
    let row = sqlx::query("SELECT stats_json FROM sync_runs WHERE run_id = $1")
        .bind(run_id)
        .fetch_one(store.pool())
        .await?;
    let stats_json: String = row.try_get("stats_json")?;
    let stats: serde_json::Value = serde_json::from_str(&stats_json)?;

    assert_eq!(stats["fetch_modes"]["transactions"], "incremental");
    assert_eq!(stats["fetch_modes"]["categories"], "full");
    assert_eq!(stats["fetch_modes"]["summary"], "full");

    Ok(())
}

#[tokio::test]
async fn local_categorize_apply_then_push_marks_outbox_applied() -> anyhow::Result<()> {
    if !has_test_database() {
        return Ok(());
    }

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/categories"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            sample_category(1, "Food"),
            sample_category(2, "Travel")
        ])))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/assets"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!([sample_asset(201, "Checking")])),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/transactions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!([sample_transaction(1, "2026-01-01T00:10:00Z")])),
        )
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path("/v2/transactions"))
        .and(body_json(json!({
            "transactions": [
                {
                    "id": 101,
                    "category_id": 2
                }
            ]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "transactions": [sample_transaction(2, "2026-01-01T00:15:00Z")]
        })))
        .mount(&server)
        .await;

    let (store, engine) = setup_engine(&server).await?;

    engine.sync_pull().await?;

    store
        .apply_local_category_decision(
            None,
            &CategorizeDecision {
                transaction_id: 101,
                category_id: 2,
                reason: Some("model decision".to_string()),
            },
        )
        .await?;

    engine.sync_push().await?;

    let row = sqlx::query("SELECT category_id FROM lm_transactions WHERE id = 101")
        .fetch_one(store.pool())
        .await?;
    let category_id: i64 = row.try_get("category_id")?;
    assert_eq!(category_id, 2);

    let status_row =
        sqlx::query("SELECT status FROM outbox_changes ORDER BY created_at DESC LIMIT 1")
            .fetch_one(store.pool())
            .await?;
    let status: String = status_row.try_get("status")?;
    assert_eq!(status, "applied");

    let local_enqueue_events: (i64,) = sqlx::query_as(
        "SELECT COUNT(1) FROM event_log WHERE source='local' AND action='enqueue' AND entity_type='transaction' AND entity_id='101'",
    )
    .fetch_one(store.pool())
    .await?;
    assert!(local_enqueue_events.0 >= 1);

    let push_update_events: (i64,) = sqlx::query_as(
        "SELECT COUNT(1) FROM event_log WHERE source='push' AND action='update' AND entity_type='transaction' AND entity_id='101'",
    )
    .fetch_one(store.pool())
    .await?;
    assert!(push_update_events.0 >= 1);

    Ok(())
}

#[tokio::test]
async fn push_failure_retries_then_applies() -> anyhow::Result<()> {
    if !has_test_database() {
        return Ok(());
    }

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/categories"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            sample_category(1, "Food"),
            sample_category(2, "Travel")
        ])))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/assets"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!([sample_asset(201, "Checking")])),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/transactions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!([sample_transaction(1, "2026-01-01T00:10:00Z")])),
        )
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path("/v2/transactions"))
        .and(body_json(json!({
            "transactions": [
                {
                    "id": 101,
                    "category_id": 2
                }
            ]
        })))
        .respond_with(ResponseTemplate::new(500).set_body_string("server error"))
        .expect(1)
        .mount(&server)
        .await;

    let (store, engine) = setup_engine(&server).await?;

    engine.sync_pull().await?;

    store
        .apply_local_category_decision(
            None,
            &CategorizeDecision {
                transaction_id: 101,
                category_id: 2,
                reason: None,
            },
        )
        .await?;

    engine.sync_push().await?;

    let failed_row = sqlx::query(
        "SELECT status, attempt_count FROM outbox_changes ORDER BY created_at DESC LIMIT 1",
    )
    .fetch_one(store.pool())
    .await?;
    let failed_status: String = failed_row.try_get("status")?;
    let failed_attempts: i64 = failed_row.try_get("attempt_count")?;
    assert_eq!(failed_status, "failed");
    assert_eq!(failed_attempts, 1);

    let change_row =
        sqlx::query("SELECT change_id FROM outbox_changes ORDER BY created_at DESC LIMIT 1")
            .fetch_one(store.pool())
            .await?;
    let change_id: String = change_row.try_get("change_id")?;
    store.requeue_outbox_change(&change_id).await?;

    server.reset().await;

    Mock::given(method("PUT"))
        .and(path("/v2/transactions"))
        .and(body_json(json!({
            "transactions": [
                {
                    "id": 101,
                    "category_id": 2
                }
            ]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "transactions": [sample_transaction(2, "2026-01-01T00:20:00Z")]
        })))
        .mount(&server)
        .await;

    engine.sync_push().await?;

    let applied_row = sqlx::query(
        "SELECT status, attempt_count FROM outbox_changes ORDER BY created_at DESC LIMIT 1",
    )
    .fetch_one(store.pool())
    .await?;
    let applied_status: String = applied_row.try_get("status")?;
    let applied_attempts: i64 = applied_row.try_get("attempt_count")?;
    assert_eq!(applied_status, "applied");
    assert_eq!(applied_attempts, 1);

    Ok(())
}

#[tokio::test]
async fn local_category_delete_then_push_marks_outbox_applied() -> anyhow::Result<()> {
    if !has_test_database() {
        return Ok(());
    }

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/categories"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            sample_category(1, "Food"),
            sample_category(2, "Obsolete")
        ])))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/assets"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/transactions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .mount(&server)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/v2/categories/2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "deleted": true })))
        .mount(&server)
        .await;

    let (store, engine) = setup_engine(&server).await?;

    engine.sync_pull().await?;
    store
        .apply_local_category_delete(None, 2, false, Some("obsolete"))
        .await?;

    engine.sync_push().await?;

    let category_row = sqlx::query("SELECT archived FROM lm_categories WHERE id = 2")
        .fetch_one(store.pool())
        .await?;
    let archived: i64 = category_row.try_get("archived")?;
    assert_eq!(archived, 1);

    let outbox_row =
        sqlx::query("SELECT status FROM outbox_changes ORDER BY created_at DESC LIMIT 1")
            .fetch_one(store.pool())
            .await?;
    let status: String = outbox_row.try_get("status")?;
    assert_eq!(status, "applied");

    let push_delete_events: (i64,) = sqlx::query_as(
        "SELECT COUNT(1) FROM event_log WHERE source='push' AND action='delete' AND entity_type='category' AND entity_id='2'",
    )
    .fetch_one(store.pool())
    .await?;
    assert_eq!(push_delete_events.0, 1);

    Ok(())
}

#[tokio::test]
async fn local_category_create_then_push_remaps_local_category_id() -> anyhow::Result<()> {
    if !has_test_database() {
        return Ok(());
    }

    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v2/categories"))
        .and(body_json(json!({
            "name": "Income",
            "is_income": true,
            "exclude_from_budget": false,
            "exclude_from_totals": false,
            "is_group": false
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": 2507001,
            "name": "Income",
            "is_income": true,
            "exclude_from_budget": false,
            "exclude_from_totals": false,
            "is_group": false,
            "group_id": null,
            "archived": false,
            "updated_at": "2026-01-01T00:20:00Z"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let (store, engine) = setup_engine(&server).await?;

    store
        .upsert_category(
            None,
            "local",
            "create",
            &json!({
                "id": 9901001,
                "name": "Income",
                "is_income": true,
                "is_group": false,
                "exclude_from_budget": false,
                "exclude_from_totals": false,
                "archived": false,
                "updated_at": "2026-01-01T00:10:00Z"
            }),
        )
        .await?;

    store
        .upsert_transaction(
            None,
            "pull",
            "upsert",
            &sample_transaction_with_id(101, 9901001, "2026-01-01T00:11:00Z"),
        )
        .await?;

    engine.sync_push().await?;

    let outbox_status_row = sqlx::query(
        "SELECT status FROM outbox_changes WHERE entity_type='category' AND op='create' ORDER BY created_at DESC LIMIT 1",
    )
    .fetch_one(store.pool())
    .await?;
    let outbox_status: String = outbox_status_row.try_get("status")?;
    assert_eq!(outbox_status, "applied");

    let old_category_exists: (i64,) =
        sqlx::query_as("SELECT COUNT(1) FROM lm_categories WHERE id = 9901001")
            .fetch_one(store.pool())
            .await?;
    assert_eq!(old_category_exists.0, 0);

    let new_category_exists: (i64,) =
        sqlx::query_as("SELECT COUNT(1) FROM lm_categories WHERE id = 2507001")
            .fetch_one(store.pool())
            .await?;
    assert_eq!(new_category_exists.0, 1);

    let txn_row = sqlx::query("SELECT category_id FROM lm_transactions WHERE id = 101")
        .fetch_one(store.pool())
        .await?;
    let txn_category_id: i64 = txn_row.try_get("category_id")?;
    assert_eq!(txn_category_id, 2507001);

    Ok(())
}

#[tokio::test]
async fn unsupported_outbox_row_is_audited_as_push_failure() -> anyhow::Result<()> {
    if !has_test_database() {
        return Ok(());
    }

    let server = MockServer::start().await;
    let (store, engine) = setup_engine(&server).await?;

    sqlx::query(
        r#"
        INSERT INTO outbox_changes (
            change_id, entity_type, entity_id, op, payload_json, idempotency_key,
            status, attempt_count, last_error, created_at, updated_at
        )
        VALUES ('bad-1', 'unknown_entity', 'x1', 'bad_op', '{}', 'bad-key-1', 'pending', 0, NULL, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
        "#,
    )
    .execute(store.pool())
    .await?;

    engine.sync_push().await?;

    let status_row =
        sqlx::query("SELECT status, attempt_count FROM outbox_changes WHERE change_id='bad-1'")
            .fetch_one(store.pool())
            .await?;
    let status: String = status_row.try_get("status")?;
    let attempts: i64 = status_row.try_get("attempt_count")?;
    assert_eq!(status, "dead_letter");
    assert_eq!(attempts, 1);

    let event_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(1) FROM event_log WHERE source='push' AND action='fail' AND entity_type='unknown_entity' AND entity_id='x1' AND meta_json LIKE '%\"change_id\":\"bad-1\"%'",
    )
    .fetch_one(store.pool())
    .await?;
    assert_eq!(event_count.0, 1);

    Ok(())
}

#[tokio::test]
async fn incremental_pull_applies_late_arriving_change_with_same_timestamp() -> anyhow::Result<()> {
    if !has_test_database() {
        return Ok(());
    }

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/categories"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            sample_category(1, "Food"),
            sample_category(2, "Travel")
        ])))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/assets"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!([sample_asset(201, "Checking")])),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/transactions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!([sample_transaction(1, "2026-01-01T00:10:00Z")])),
        )
        .mount(&server)
        .await;

    let (store, engine) = setup_engine(&server).await?;
    engine.sync_pull().await?;

    server.reset().await;

    Mock::given(method("GET"))
        .and(path("/v2/categories"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            sample_category(1, "Food"),
            sample_category(2, "Travel")
        ])))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/assets"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!([sample_asset(201, "Checking")])),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/transactions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!([sample_transaction(2, "2026-01-01T00:10:00Z")])),
        )
        .mount(&server)
        .await;

    engine.sync_pull().await?;

    let row = sqlx::query("SELECT category_id FROM lm_transactions WHERE id = 101")
        .fetch_one(store.pool())
        .await?;
    let category_id: i64 = row.try_get("category_id")?;
    assert_eq!(category_id, 2);

    Ok(())
}

#[tokio::test]
async fn push_conflict_marks_outbox_failed_conflict_and_logs_event() -> anyhow::Result<()> {
    if !has_test_database() {
        return Ok(());
    }

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/categories"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            sample_category(1, "Food"),
            sample_category(2, "Travel")
        ])))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/assets"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!([sample_asset(201, "Checking")])),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/transactions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!([sample_transaction(1, "2026-01-01T00:10:00Z")])),
        )
        .mount(&server)
        .await;

    let (store, engine) = setup_engine(&server).await?;
    engine.sync_pull().await?;

    store
        .apply_local_category_decision(
            None,
            &CategorizeDecision {
                transaction_id: 101,
                category_id: 2,
                reason: None,
            },
        )
        .await?;

    store
        .upsert_transaction(
            Some("remote-advance"),
            "pull",
            "upsert",
            &sample_transaction(1, "2026-01-01T00:20:00Z"),
        )
        .await?;

    engine.sync_push().await?;

    let row = sqlx::query("SELECT status, attempt_count FROM outbox_changes LIMIT 1")
        .fetch_one(store.pool())
        .await?;
    let status: String = row.try_get("status")?;
    let attempts: i64 = row.try_get("attempt_count")?;
    assert_eq!(status, "failed_conflict");
    assert_eq!(attempts, 1);

    let conflict_events: (i64,) = sqlx::query_as(
        "SELECT COUNT(1) FROM event_log WHERE source='push' AND action='conflict' AND meta_json LIKE '%\"reason\":\"conflict\"%'",
    )
    .fetch_one(store.pool())
    .await?;
    assert_eq!(conflict_events.0, 1);

    Ok(())
}

#[tokio::test]
async fn push_terminal_http_failure_moves_to_dead_letter() -> anyhow::Result<()> {
    if !has_test_database() {
        return Ok(());
    }

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/categories"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            sample_category(1, "Food"),
            sample_category(2, "Travel")
        ])))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/assets"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!([sample_asset(201, "Checking")])),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/transactions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!([sample_transaction(1, "2026-01-01T00:10:00Z")])),
        )
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path("/v2/transactions"))
        .and(body_json(json!({
            "transactions": [
                {
                    "id": 101,
                    "category_id": 2
                }
            ]
        })))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
        .mount(&server)
        .await;

    let (store, engine) = setup_engine(&server).await?;
    engine.sync_pull().await?;

    store
        .apply_local_category_decision(
            None,
            &CategorizeDecision {
                transaction_id: 101,
                category_id: 2,
                reason: None,
            },
        )
        .await?;

    engine.sync_push().await?;

    let row = sqlx::query(
        "SELECT status, attempt_count FROM outbox_changes ORDER BY created_at DESC LIMIT 1",
    )
    .fetch_one(store.pool())
    .await?;
    let status: String = row.try_get("status")?;
    let attempts: i64 = row.try_get("attempt_count")?;
    assert_eq!(status, "dead_letter");
    assert_eq!(attempts, 1);

    Ok(())
}

#[tokio::test]
async fn push_chunks_transaction_updates_by_dedicated_batch_size() -> anyhow::Result<()> {
    if !has_test_database() {
        return Ok(());
    }

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/categories"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            sample_category(1, "Food"),
            sample_category(2, "Travel"),
            sample_category(3, "Bills")
        ])))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/assets"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!([sample_asset(201, "Checking")])),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/transactions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            sample_transaction_with_id(101, 1, "2026-01-01T00:10:00Z"),
            sample_transaction_with_id(102, 1, "2026-01-01T00:11:00Z")
        ])))
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path("/v2/transactions"))
        .and(body_json(json!({
            "transactions": [{ "id": 101, "category_id": 2 }]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "transactions": [sample_transaction_with_id(101, 2, "2026-01-01T00:20:00Z")]
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path("/v2/transactions"))
        .and(body_json(json!({
            "transactions": [{ "id": 102, "category_id": 3 }]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "transactions": [sample_transaction_with_id(102, 3, "2026-01-01T00:21:00Z")]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let Some(database_url) = provision_test_database_url().await? else {
        anyhow::bail!("missing TEST_DATABASE_URL or DATABASE_URL");
    };
    let mut config = test_config(server.uri(), database_url);
    config.transaction_update_batch_size = 1;
    let (store, engine) = setup_engine_with_config(config).await?;
    engine.sync_pull().await?;

    store
        .apply_local_category_decision(
            None,
            &CategorizeDecision {
                transaction_id: 101,
                category_id: 2,
                reason: None,
            },
        )
        .await?;

    store
        .apply_local_category_decision(
            None,
            &CategorizeDecision {
                transaction_id: 102,
                category_id: 3,
                reason: None,
            },
        )
        .await?;

    engine.sync_push().await?;

    let applied_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(1) FROM outbox_changes WHERE status='applied' AND entity_type='transaction'",
    )
    .fetch_one(store.pool())
    .await?;
    assert_eq!(applied_count.0, 2);

    Ok(())
}

#[tokio::test]
async fn push_splits_bad_request_batch_to_isolate_invalid_transaction() -> anyhow::Result<()> {
    if !has_test_database() {
        return Ok(());
    }

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/categories"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            sample_category(1, "Food"),
            sample_category(2, "Travel"),
            sample_category(3, "Bills")
        ])))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/assets"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!([sample_asset(201, "Checking")])),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/transactions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            sample_transaction_with_id(101, 1, "2026-01-01T00:10:00Z"),
            sample_transaction_with_id(102, 1, "2026-01-01T00:11:00Z")
        ])))
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path("/v2/transactions"))
        .and(body_json(json!({
            "transactions": [
                { "id": 101, "category_id": 2 },
                { "id": 102, "category_id": 3 }
            ]
        })))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path("/v2/transactions"))
        .and(body_json(json!({
            "transactions": [{ "id": 101, "category_id": 2 }]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "transactions": [sample_transaction_with_id(101, 2, "2026-01-01T00:20:00Z")]
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path("/v2/transactions"))
        .and(body_json(json!({
            "transactions": [{ "id": 102, "category_id": 3 }]
        })))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
        .expect(1)
        .mount(&server)
        .await;

    let (store, engine) = setup_engine(&server).await?;
    engine.sync_pull().await?;

    store
        .apply_local_category_decision(
            None,
            &CategorizeDecision {
                transaction_id: 101,
                category_id: 2,
                reason: None,
            },
        )
        .await?;

    store
        .apply_local_category_decision(
            None,
            &CategorizeDecision {
                transaction_id: 102,
                category_id: 3,
                reason: None,
            },
        )
        .await?;

    engine.sync_push().await?;

    let applied_row =
        sqlx::query("SELECT status, attempt_count FROM outbox_changes WHERE entity_id = '101'")
            .fetch_one(store.pool())
            .await?;
    let applied_status: String = applied_row.try_get("status")?;
    let applied_attempts: i64 = applied_row.try_get("attempt_count")?;
    assert_eq!(applied_status, "applied");
    assert_eq!(applied_attempts, 0);

    let failed_row =
        sqlx::query("SELECT status, attempt_count FROM outbox_changes WHERE entity_id = '102'")
            .fetch_one(store.pool())
            .await?;
    let failed_status: String = failed_row.try_get("status")?;
    let failed_attempts: i64 = failed_row.try_get("attempt_count")?;
    assert_eq!(failed_status, "dead_letter");
    assert_eq!(failed_attempts, 1);

    Ok(())
}

#[tokio::test]
async fn push_retries_rate_limited_transaction_batch_after_retry_after() -> anyhow::Result<()> {
    if !has_test_database() {
        return Ok(());
    }

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/categories"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            sample_category(1, "Food"),
            sample_category(2, "Travel")
        ])))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/assets"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!([sample_asset(201, "Checking")])),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/transactions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!([sample_transaction(1, "2026-01-01T00:10:00Z")])),
        )
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path("/v2/transactions"))
        .and(body_json(json!({
            "transactions": [{ "id": 101, "category_id": 2 }]
        })))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("Retry-After", "0")
                .set_body_string("rate limit"),
        )
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path("/v2/transactions"))
        .and(body_json(json!({
            "transactions": [{ "id": 101, "category_id": 2 }]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "transactions": [sample_transaction(2, "2026-01-01T00:20:00Z")]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let (store, engine) = setup_engine(&server).await?;
    engine.sync_pull().await?;

    store
        .apply_local_category_decision(
            None,
            &CategorizeDecision {
                transaction_id: 101,
                category_id: 2,
                reason: None,
            },
        )
        .await?;

    engine.sync_push().await?;

    let row = sqlx::query(
        "SELECT status, attempt_count FROM outbox_changes ORDER BY created_at DESC LIMIT 1",
    )
    .fetch_one(store.pool())
    .await?;
    let status: String = row.try_get("status")?;
    let attempts: i64 = row.try_get("attempt_count")?;
    assert_eq!(status, "applied");
    assert_eq!(attempts, 0);

    Ok(())
}

#[tokio::test]
async fn push_batches_multiple_transaction_updates_in_one_request() -> anyhow::Result<()> {
    if !has_test_database() {
        return Ok(());
    }

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/categories"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            sample_category(1, "Food"),
            sample_category(2, "Travel"),
            sample_category(3, "Bills")
        ])))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/assets"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!([sample_asset(201, "Checking")])),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/transactions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            sample_transaction_with_id(101, 1, "2026-01-01T00:10:00Z"),
            sample_transaction_with_id(102, 1, "2026-01-01T00:11:00Z")
        ])))
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path("/v2/transactions"))
        .and(body_json(json!({
            "transactions": [
                { "id": 101, "category_id": 2 },
                { "id": 102, "category_id": 3 }
            ]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "transactions": [
                sample_transaction_with_id(101, 2, "2026-01-01T00:20:00Z"),
                sample_transaction_with_id(102, 3, "2026-01-01T00:21:00Z")
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let (store, engine) = setup_engine(&server).await?;
    engine.sync_pull().await?;

    store
        .apply_local_category_decision(
            None,
            &CategorizeDecision {
                transaction_id: 101,
                category_id: 2,
                reason: None,
            },
        )
        .await?;

    store
        .apply_local_category_decision(
            None,
            &CategorizeDecision {
                transaction_id: 102,
                category_id: 3,
                reason: None,
            },
        )
        .await?;

    engine.sync_push().await?;

    let applied_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(1) FROM outbox_changes WHERE status='applied' AND entity_type='transaction'",
    )
    .fetch_one(store.pool())
    .await?;
    assert_eq!(applied_count.0, 2);

    let row_101 = sqlx::query("SELECT category_id FROM lm_transactions WHERE id = 101")
        .fetch_one(store.pool())
        .await?;
    let row_102 = sqlx::query("SELECT category_id FROM lm_transactions WHERE id = 102")
        .fetch_one(store.pool())
        .await?;
    let category_101: i64 = row_101.try_get("category_id")?;
    let category_102: i64 = row_102.try_get("category_id")?;
    assert_eq!(category_101, 2);
    assert_eq!(category_102, 3);

    Ok(())
}
