use pocketlunch::config::AppConfig;
use pocketlunch::models::CategorizeDecision;
use pocketlunch::store::Store;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use uuid::Uuid;

fn test_config(database_url: String) -> AppConfig {
    AppConfig::new(
        database_url,
        "https://api.lunchmoney.dev".to_string(),
        Some("test-token".to_string()),
        300,
        100,
        100,
        50,
        8,
    )
    .unwrap()
}

fn test_database_base_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("DATABASE_URL").ok())
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

async fn setup_store() -> anyhow::Result<Option<Store>> {
    let Some(database_url) = provision_test_database_url().await? else {
        return Ok(None);
    };
    let config = test_config(database_url);
    let store = Store::new(&config).await?;
    store.bootstrap().await?;
    Ok(Some(store))
}

fn category_json(id: i64, updated_at: &str) -> serde_json::Value {
    json!({
        "id": id,
        "name": format!("Category {id}"),
        "is_group": false,
        "archived": false,
        "updated_at": updated_at
    })
}

fn transaction_json(id: i64, category_id: i64, updated_at: &str) -> serde_json::Value {
    json!({
        "id": id,
        "date": "2026-01-01",
        "amount": "12.34",
        "currency": "USD",
        "payee": "Merchant",
        "notes": "test",
        "category_id": category_id,
        "asset_id": 1,
        "status": "cleared",
        "is_pending": false,
        "external_id": "abc",
        "updated_at": updated_at
    })
}

#[tokio::test]
async fn bootstrap_and_cursor_update_work() -> anyhow::Result<()> {
    let Some(store) = setup_store().await? else {
        return Ok(());
    };

    let state = store.get_sync_state("transactions").await?;
    assert_eq!(state.last_remote_updated_at, "1970-01-01T00:00:00Z");
    assert_eq!(state.lookback_seconds, 300);

    store
        .update_sync_cursor("transactions", "2026-01-01T00:10:00Z", Some("run-1"))
        .await?;

    let updated = store.get_sync_state("transactions").await?;
    assert_eq!(updated.last_remote_updated_at, "2026-01-01T00:10:00Z");
    assert_eq!(updated.last_success_run_id.as_deref(), Some("run-1"));

    Ok(())
}

#[tokio::test]
async fn upsert_transaction_handles_older_identical_and_changed_rows() -> anyhow::Result<()> {
    let Some(store) = setup_store().await? else {
        return Ok(());
    };

    let inserted = store
        .upsert_transaction(
            Some("run-a"),
            "pull",
            "upsert",
            &transaction_json(100, 1, "2026-01-01T00:10:00Z"),
        )
        .await?;
    assert!(inserted);

    let older = store
        .upsert_transaction(
            Some("run-a"),
            "pull",
            "upsert",
            &transaction_json(100, 2, "2026-01-01T00:09:00Z"),
        )
        .await?;
    assert!(!older);

    let identical = store
        .upsert_transaction(
            Some("run-a"),
            "pull",
            "upsert",
            &transaction_json(100, 1, "2026-01-01T00:10:00Z"),
        )
        .await?;
    assert!(!identical);

    let changed_same_ts = store
        .upsert_transaction(
            Some("run-a"),
            "pull",
            "upsert",
            &transaction_json(100, 3, "2026-01-01T00:10:00Z"),
        )
        .await?;
    assert!(changed_same_ts);

    let txn_row = sqlx::query("SELECT category_id FROM lm_transactions WHERE id = 100")
        .fetch_one(store.pool())
        .await?;
    let category_id: i64 = txn_row.try_get("category_id")?;
    assert_eq!(category_id, 3);

    let event_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(1) FROM event_log WHERE source = 'pull' AND entity_type = 'transaction'",
    )
    .fetch_one(store.pool())
    .await?;
    assert_eq!(event_count.0, 2);

    let insert_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(1) FROM event_log WHERE source = 'pull' AND entity_type = 'transaction' AND action = 'insert'",
    )
    .fetch_one(store.pool())
    .await?;
    let update_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(1) FROM event_log WHERE source = 'pull' AND entity_type = 'transaction' AND action = 'update'",
    )
    .fetch_one(store.pool())
    .await?;
    assert_eq!(insert_count.0, 1);
    assert_eq!(update_count.0, 1);

    Ok(())
}

#[tokio::test]
async fn apply_local_decision_dedupes_outbox_by_idempotency_key() -> anyhow::Result<()> {
    let Some(store) = setup_store().await? else {
        return Ok(());
    };

    store
        .upsert_category(
            Some("run-c"),
            "pull",
            "upsert",
            &category_json(1, "2026-01-01T00:00:00Z"),
        )
        .await?;
    store
        .upsert_category(
            Some("run-c"),
            "pull",
            "upsert",
            &category_json(2, "2026-01-01T00:00:00Z"),
        )
        .await?;

    store
        .upsert_transaction(
            Some("run-c"),
            "pull",
            "upsert",
            &transaction_json(777, 1, "2026-01-01T00:10:00Z"),
        )
        .await?;

    let decision = CategorizeDecision {
        transaction_id: 777,
        category_id: 2,
        reason: Some("model".to_string()),
    };

    let first_change_id = store.apply_local_category_decision(None, &decision).await?;
    let second_change_id = store.apply_local_category_decision(None, &decision).await?;
    assert_eq!(second_change_id, first_change_id);

    let outbox_count: (i64,) = sqlx::query_as("SELECT COUNT(1) FROM outbox_changes")
        .fetch_one(store.pool())
        .await?;
    assert_eq!(outbox_count.0, 1);

    let enqueue_events: (i64,) = sqlx::query_as(
        "SELECT COUNT(1) FROM event_log WHERE source='local' AND action='enqueue' AND entity_type='transaction' AND entity_id='777'",
    )
    .fetch_one(store.pool())
    .await?;
    assert_eq!(enqueue_events.0, 1);

    let pending_rows = store.fetch_outbox_batch(10).await?;
    assert_eq!(pending_rows.len(), 1);

    let exists = store.category_exists(2).await?;
    assert!(exists);

    let remote = store.get_transaction_remote_updated_at(777).await?;
    assert_eq!(remote.as_deref(), Some("2026-01-01T00:10:00Z"));

    Ok(())
}

#[tokio::test]
async fn local_transaction_upsert_category_change_enqueues_outbox_change() -> anyhow::Result<()> {
    let Some(store) = setup_store().await? else {
        return Ok(());
    };

    store
        .upsert_category(
            Some("run-u"),
            "pull",
            "upsert",
            &category_json(1, "2026-01-01T00:00:00Z"),
        )
        .await?;
    store
        .upsert_category(
            Some("run-u"),
            "pull",
            "upsert",
            &category_json(2, "2026-01-01T00:00:00Z"),
        )
        .await?;
    store
        .upsert_transaction(
            Some("run-u"),
            "pull",
            "upsert",
            &transaction_json(901, 1, "2026-01-01T00:10:00Z"),
        )
        .await?;

    let updated = store
        .upsert_entity_record(
            None,
            "local",
            "update",
            "transactions",
            &transaction_json(901, 2, "2026-01-01T00:20:00Z"),
        )
        .await?;
    assert!(updated);

    let txn_row = sqlx::query("SELECT category_id FROM lm_transactions WHERE id = 901")
        .fetch_one(store.pool())
        .await?;
    let category_id: i64 = txn_row.try_get("category_id")?;
    assert_eq!(category_id, 2);

    let outbox_row = sqlx::query(
        "SELECT status, payload_json FROM outbox_changes WHERE entity_type='transaction' AND entity_id='901' ORDER BY created_at DESC LIMIT 1",
    )
    .fetch_one(store.pool())
    .await?;
    let status: String = outbox_row.try_get("status")?;
    let payload_json: String = outbox_row.try_get("payload_json")?;
    assert_eq!(status, "pending");
    assert!(payload_json.contains("\"category_id\":2"));
    assert!(payload_json.contains("\"base_remote_updated_at\":\"2026-01-01T00:10:00Z\""));

    let enqueue_events: (i64,) = sqlx::query_as(
        "SELECT COUNT(1) FROM event_log WHERE source='local' AND action='enqueue' AND entity_type='transaction' AND entity_id='901'",
    )
    .fetch_one(store.pool())
    .await?;
    assert_eq!(enqueue_events.0, 1);

    Ok(())
}

#[tokio::test]
async fn local_transaction_upsert_duplicate_enqueue_reuses_existing_outbox_row() -> anyhow::Result<()> {
    let Some(store) = setup_store().await? else {
        return Ok(());
    };

    store
        .upsert_category(
            Some("run-u2"),
            "pull",
            "upsert",
            &category_json(1, "2026-01-01T00:00:00Z"),
        )
        .await?;
    store
        .upsert_category(
            Some("run-u2"),
            "pull",
            "upsert",
            &category_json(2, "2026-01-01T00:00:00Z"),
        )
        .await?;
    store
        .upsert_transaction(
            Some("run-u2"),
            "pull",
            "upsert",
            &transaction_json(901, 1, "2026-01-01T00:10:00Z"),
        )
        .await?;

    sqlx::query(
        r#"
        INSERT INTO outbox_changes (
            change_id, entity_type, entity_id, op, payload_json, idempotency_key,
            status, attempt_count, last_error, created_at, updated_at
        )
        VALUES (
            'existing-change',
            'transaction',
            '901',
            'update',
            '{"category_id":2,"base_remote_updated_at":"2026-01-01T00:10:00Z","reason":null}',
            'txn:901:cat:2:base:2026-01-01T00:10:00Z',
            'pending',
            0,
            NULL,
            '2026-01-01T00:11:00Z',
            '2026-01-01T00:11:00Z'
        )
        "#,
    )
    .execute(store.pool())
    .await?;

    let updated = store
        .upsert_entity_record(
            None,
            "local",
            "update",
            "transactions",
            &transaction_json(901, 2, "2026-01-01T00:20:00Z"),
        )
        .await?;
    assert!(updated);

    let outbox_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(1) FROM outbox_changes WHERE entity_type='transaction' AND entity_id='901'",
    )
    .fetch_one(store.pool())
    .await?;
    assert_eq!(outbox_count.0, 1);

    let outbox_row = sqlx::query(
        "SELECT change_id FROM outbox_changes WHERE entity_type='transaction' AND entity_id='901'",
    )
    .fetch_one(store.pool())
    .await?;
    let change_id: String = outbox_row.try_get("change_id")?;
    assert_eq!(change_id, "existing-change");

    let enqueue_events: (i64,) = sqlx::query_as(
        "SELECT COUNT(1) FROM event_log WHERE source='local' AND action='enqueue' AND entity_type='transaction' AND entity_id='901'",
    )
    .fetch_one(store.pool())
    .await?;
    assert_eq!(enqueue_events.0, 0);

    Ok(())
}

#[tokio::test]
async fn outbox_state_transitions_and_batch_filtering_work() -> anyhow::Result<()> {
    let Some(store) = setup_store().await? else {
        return Ok(());
    };

    let now = "2026-01-01T00:00:00Z";
    sqlx::query(
        r#"
        INSERT INTO outbox_changes (
            change_id, entity_type, entity_id, op, payload_json, idempotency_key,
            status, attempt_count, last_error, created_at, updated_at
        ) VALUES
            ('c1','transaction','1','update','{}','k1','pending',0,NULL,$1,$1),
            ('c2','transaction','2','update','{}','k2','failed',1,'err',$1,$1),
            ('c3','transaction','3','update','{}','k3','dead_letter',8,'err',$1,$1)
        "#,
    )
    .bind(now)
    .execute(store.pool())
    .await?;

    let batch = store.fetch_outbox_batch(10).await?;
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].change_id, "c1");

    store.mark_outbox_processing("c1").await?;
    store.mark_outbox_failure("c1", true, 2, "retry me").await?;

    let failed_row =
        sqlx::query("SELECT status, attempt_count FROM outbox_changes WHERE change_id='c1'")
            .fetch_one(store.pool())
            .await?;
    let failed_status: String = failed_row.try_get("status")?;
    let failed_attempts: i64 = failed_row.try_get("attempt_count")?;
    assert_eq!(failed_status, "failed");
    assert_eq!(failed_attempts, 1);

    store
        .mark_outbox_failure("c1", true, 2, "retry me again")
        .await?;
    let dead_row =
        sqlx::query("SELECT status, attempt_count FROM outbox_changes WHERE change_id='c1'")
            .fetch_one(store.pool())
            .await?;
    let dead_status: String = dead_row.try_get("status")?;
    let dead_attempts: i64 = dead_row.try_get("attempt_count")?;
    assert_eq!(dead_status, "dead_letter");
    assert_eq!(dead_attempts, 2);

    store.mark_outbox_conflict("c2", "conflict").await?;
    let conflict_row = sqlx::query("SELECT status FROM outbox_changes WHERE change_id='c2'")
        .fetch_one(store.pool())
        .await?;
    let conflict_status: String = conflict_row.try_get("status")?;
    assert_eq!(conflict_status, "failed_conflict");

    store.requeue_outbox_change("c2").await?;
    let requeued = store.fetch_outbox_batch(10).await?;
    assert_eq!(requeued.len(), 1);
    assert_eq!(requeued[0].change_id, "c2");

    let event_row = sqlx::query(
        "SELECT source, action, entity_type, entity_id, meta_json FROM event_log WHERE source='local' AND entity_type='outbox' ORDER BY occurred_at DESC LIMIT 1",
    )
    .fetch_one(store.pool())
    .await?;
    let source: String = event_row.try_get("source")?;
    let action: String = event_row.try_get("action")?;
    let entity_type: String = event_row.try_get("entity_type")?;
    let entity_id: String = event_row.try_get("entity_id")?;
    let meta_json: String = event_row.try_get("meta_json")?;
    assert_eq!(source, "local");
    assert_eq!(action, "enqueue");
    assert_eq!(entity_type, "outbox");
    assert_eq!(entity_id, "c2");
    assert!(meta_json.contains("\"reason\":\"requeue\""));

    Ok(())
}

#[tokio::test]
async fn list_transactions_filtered_supports_date_range_category_and_pagination(
) -> anyhow::Result<()> {
    let Some(store) = setup_store().await? else {
        return Ok(());
    };

    store
        .upsert_transaction(
            Some("run-f"),
            "pull",
            "upsert",
            &json!({
                "id": 1, "date": "2026-01-03", "amount": "1", "currency": "USD", "category_id": 1, "updated_at": "2026-01-03T00:00:00Z"
            }),
        )
        .await?;
    store
        .upsert_transaction(
            Some("run-f"),
            "pull",
            "upsert",
            &json!({
                "id": 2, "date": "2026-01-03", "amount": "1", "currency": "USD", "category_id": 2, "updated_at": "2026-01-03T00:00:01Z"
            }),
        )
        .await?;
    store
        .upsert_transaction(
            Some("run-f"),
            "pull",
            "upsert",
            &json!({
                "id": 3, "date": "2026-01-02", "amount": "1", "currency": "USD", "category_id": 1, "updated_at": "2026-01-02T00:00:00Z"
            }),
        )
        .await?;
    store
        .upsert_transaction(
            Some("run-f"),
            "pull",
            "upsert",
            &json!({
                "id": 4, "date": "2026-01-01", "amount": "1", "currency": "USD", "category_id": 1, "updated_at": "2026-01-01T00:00:00Z"
            }),
        )
        .await?;

    let single_day = store
        .list_transactions_filtered(Some("2026-01-03"), Some("2026-01-03"), None, 100, 0)
        .await?;
    let ids = single_day
        .iter()
        .filter_map(|v| v.get("id").and_then(|v| v.as_i64()))
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![2, 1]);

    let ranged = store
        .list_transactions_filtered(Some("2026-01-02"), Some("2026-01-03"), Some(1), 100, 0)
        .await?;
    let ranged_ids = ranged
        .iter()
        .filter_map(|v| v.get("id").and_then(|v| v.as_i64()))
        .collect::<Vec<_>>();
    assert_eq!(ranged_ids, vec![1, 3]);

    let paged = store
        .list_transactions_filtered(None, None, None, 1, 1)
        .await?;
    let paged_id = paged
        .first()
        .and_then(|v| v.get("id"))
        .and_then(|v| v.as_i64());
    assert_eq!(paged_id, Some(1));

    Ok(())
}

#[tokio::test]
async fn list_outbox_changes_supports_pagination() -> anyhow::Result<()> {
    let Some(store) = setup_store().await? else {
        return Ok(());
    };

    sqlx::query(
        r#"
        INSERT INTO outbox_changes (
            change_id, entity_type, entity_id, op, payload_json, idempotency_key,
            status, attempt_count, last_error, created_at, updated_at
        )
        VALUES
            ('c1','transaction','1','update','{"category_id":1}','k1','pending',0,NULL,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'),
            ('c2','transaction','2','update','{"category_id":2}','k2','failed',1,'oops','2026-01-02T00:00:00Z','2026-01-02T00:00:00Z'),
            ('c3','category','3','delete','{"force":false}','k3','applied',1,NULL,'2026-01-03T00:00:00Z','2026-01-03T00:00:00Z')
        "#,
    )
    .execute(store.pool())
    .await?;

    let (paged, total) = store.list_outbox_changes(1, 1).await?;
    assert_eq!(total, 3);
    assert_eq!(paged.len(), 1);
    assert_eq!(paged[0].change_id, "c2");
    assert_eq!(paged[0].status, "failed");

    Ok(())
}

#[tokio::test]
async fn apply_local_category_delete_dedupes_outbox_by_idempotency_key() -> anyhow::Result<()> {
    let Some(store) = setup_store().await? else {
        return Ok(());
    };

    store
        .upsert_category(
            Some("run-d"),
            "pull",
            "upsert",
            &category_json(11, "2026-01-01T00:00:00Z"),
        )
        .await?;

    let first_change_id = store
        .apply_local_category_delete(None, 11, false, Some("obsolete"))
        .await?;
    let second_change_id = store
        .apply_local_category_delete(None, 11, false, Some("obsolete"))
        .await?;
    assert_eq!(second_change_id, first_change_id);

    let outbox_count: (i64,) = sqlx::query_as("SELECT COUNT(1) FROM outbox_changes")
        .fetch_one(store.pool())
        .await?;
    assert_eq!(outbox_count.0, 1);

    let enqueue_events: (i64,) = sqlx::query_as(
        "SELECT COUNT(1) FROM event_log WHERE source='local' AND action='enqueue' AND entity_type='category' AND entity_id='11'",
    )
    .fetch_one(store.pool())
    .await?;
    assert_eq!(enqueue_events.0, 1);

    let row = sqlx::query("SELECT archived FROM lm_categories WHERE id = 11")
        .fetch_one(store.pool())
        .await?;
    let archived: i64 = row.try_get("archived")?;
    assert_eq!(archived, 1);

    Ok(())
}

#[tokio::test]
async fn list_events_filters_by_run_and_entity() -> anyhow::Result<()> {
    let Some(store) = setup_store().await? else {
        return Ok(());
    };

    store
        .append_event(
            Some("run-1"),
            "pull",
            "upsert",
            "transaction",
            "10",
            None,
            Some("{\"id\":10}"),
            json!({"n": 1}),
        )
        .await?;

    store
        .append_event(
            Some("run-2"),
            "local",
            "enqueue",
            "transaction",
            "11",
            None,
            Some("{\"id\":11}"),
            json!({"n": 2}),
        )
        .await?;

    let run1 = store
        .list_events(Some("run-1"), Some("transaction"), None, 100)
        .await?;
    assert_eq!(run1.len(), 1);
    assert_eq!(run1[0].entity_id, "10");

    let entity11 = store
        .list_events(None, Some("transaction"), Some("11"), 100)
        .await?;
    assert_eq!(entity11.len(), 1);
    assert_eq!(entity11[0].run_id.as_deref(), Some("run-2"));

    Ok(())
}

#[tokio::test]
async fn local_supported_mutations_emit_create_update_and_enqueue_actions() -> anyhow::Result<()> {
    let Some(store) = setup_store().await? else {
        return Ok(());
    };

    let created = store
        .upsert_entity_record(
            None,
            "local",
            "create",
            "categories",
            &json!({
                "id": 9001,
                "name": "Initial",
                "is_group": false,
                "archived": false,
                "updated_at": "2026-01-01T00:00:00Z"
            }),
        )
        .await?;
    assert!(created);

    let updated = store
        .upsert_entity_record(
            None,
            "local",
            "update",
            "categories",
            &json!({
                "id": 9001,
                "name": "Renamed",
                "is_group": false,
                "archived": false,
                "updated_at": "2026-01-01T00:01:00Z"
            }),
        )
        .await?;
    assert!(updated);

    store
        .upsert_category(
            Some("run-l"),
            "pull",
            "upsert",
            &category_json(2, "2026-01-01T00:00:00Z"),
        )
        .await?;
    store
        .upsert_transaction(
            Some("run-l"),
            "pull",
            "upsert",
            &transaction_json(42, 9001, "2026-01-01T00:10:00Z"),
        )
        .await?;
    store
        .apply_local_category_decision(
            None,
            &CategorizeDecision {
                transaction_id: 42,
                category_id: 2,
                reason: Some("model".to_string()),
            },
        )
        .await?;

    store
        .apply_local_category_delete(None, 9001, false, Some("cleanup"))
        .await?;

    let rows = sqlx::query("SELECT action FROM event_log WHERE source='local'")
        .fetch_all(store.pool())
        .await?;
    let mut actions = std::collections::BTreeSet::new();
    for row in rows {
        let action: String = row.try_get("action")?;
        actions.insert(action);
    }

    assert!(actions.contains("create"));
    assert!(actions.contains("update"));
    assert!(actions.contains("enqueue"));

    Ok(())
}

#[tokio::test]
async fn local_read_model_mutations_are_rejected() -> anyhow::Result<()> {
    let Some(store) = setup_store().await? else {
        return Ok(());
    };

    let asset_err = store
        .upsert_entity_record(
            None,
            "local",
            "create",
            "assets",
            &json!({
                "id": 1,
                "name": "Cash",
                "updated_at": "2026-03-05T00:00:00Z"
            }),
        )
        .await
        .expect_err("assets local create should be rejected");
    assert!(
        asset_err
            .to_string()
            .contains("local write operations are not supported")
    );

    let delete_err = store
        .delete_entity_record(None, "local", "categories", "9001")
        .await
        .expect_err("generic local delete should be rejected");
    assert!(
        delete_err
            .to_string()
            .contains("local delete operations are not supported")
    );

    Ok(())
}
