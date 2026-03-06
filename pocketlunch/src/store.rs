use anyhow::{Context, Result};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, PgPool, Postgres, QueryBuilder, Row};
use uuid::Uuid;

use crate::config::AppConfig;
use crate::models::{
    CategorizeDecision, CategoryDeletePayload, CategoryUpsertPayload, EventRow, OutboxChange,
    SyncStateRow, TransactionUpdatePayload,
};
use crate::outbox::{next_failure_status, OutboxStatus};
use crate::timeutil::now_rfc3339;

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
    default_lookback_seconds: i64,
}

impl Store {
    pub async fn new(config: &AppConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&config.database_url)
            .await
            .context("failed to connect to postgres")?;

        Ok(Self {
            pool,
            default_lookback_seconds: config.default_lookback_seconds,
        })
    }

    pub async fn bootstrap(&self) -> Result<()> {
        sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .context("failed to run migrations")?;

        let now = now_rfc3339()?;
        for entity in [
            "transactions",
            "categories",
            "assets",
            "me",
            "plaid_accounts",
            "manual_accounts",
            "tags",
            "recurring_items",
            "summary",
        ] {
            sqlx::query(
                r#"
                INSERT INTO sync_state (entity, last_remote_updated_at, lookback_seconds, last_success_run_id, updated_at)
                VALUES ($1, $2, $3, NULL, $4)
                ON CONFLICT(entity) DO NOTHING
                "#,
            )
            .bind(entity)
            .bind("1970-01-01T00:00:00Z")
            .bind(self.default_lookback_seconds)
            .bind(&now)
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn insert_or_get_outbox_change<'e, E>(
        executor: E,
        requested_change_id: &str,
        entity_type: &str,
        entity_id: &str,
        op: &str,
        payload_json: &str,
        idempotency_key: &str,
        now: &str,
    ) -> Result<(String, bool)>
    where
        E: sqlx::Executor<'e, Database = Postgres>,
    {
        let row = sqlx::query_as::<_, (String, bool)>(
            r#"
            WITH inserted AS (
                INSERT INTO outbox_changes (
                    change_id, entity_type, entity_id, op, payload_json, idempotency_key,
                    status, attempt_count, last_error, created_at, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, 'pending', 0, NULL, $7, $8)
                ON CONFLICT(idempotency_key) DO NOTHING
                RETURNING change_id
            )
            SELECT change_id, TRUE AS inserted FROM inserted
            UNION ALL
            SELECT change_id, FALSE AS inserted
            FROM outbox_changes
            WHERE idempotency_key = $6
              AND NOT EXISTS (SELECT 1 FROM inserted)
            LIMIT 1
            "#,
        )
        .bind(requested_change_id)
        .bind(entity_type)
        .bind(entity_id)
        .bind(op)
        .bind(payload_json)
        .bind(idempotency_key)
        .bind(now)
        .bind(now)
        .fetch_one(executor)
        .await?;

        Ok(row)
    }

    pub async fn begin_run(&self, direction: &str) -> Result<String> {
        let run_id = Uuid::new_v4().to_string();
        let now = now_rfc3339()?;

        sqlx::query(
            r#"
            INSERT INTO sync_runs (run_id, direction, started_at, finished_at, status, stats_json, error_json)
            VALUES ($1, $2, $3, NULL, 'running', '{}', NULL)
            "#,
        )
        .bind(&run_id)
        .bind(direction)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(run_id)
    }

    pub async fn finish_run(
        &self,
        run_id: &str,
        status: &str,
        stats: &Value,
        error: Option<&Value>,
    ) -> Result<()> {
        let now = now_rfc3339()?;
        let error_json = error.map(serde_json::to_string).transpose()?;

        sqlx::query(
            r#"
            UPDATE sync_runs
            SET finished_at = $2,
                status = $3,
                stats_json = $4,
                error_json = $5
            WHERE run_id = $1
            "#,
        )
        .bind(run_id)
        .bind(now)
        .bind(status)
        .bind(serde_json::to_string(stats)?)
        .bind(error_json)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_sync_state(&self, entity: &str) -> Result<SyncStateRow> {
        let row = sqlx::query_as::<_, SyncStateRow>(
            r#"
            SELECT entity, last_remote_updated_at, lookback_seconds, last_success_run_id, updated_at
            FROM sync_state
            WHERE entity = $1
            "#,
        )
        .bind(entity)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            return Ok(row);
        }

        let now = now_rfc3339()?;
        sqlx::query(
            r#"
            INSERT INTO sync_state (entity, last_remote_updated_at, lookback_seconds, last_success_run_id, updated_at)
            VALUES ($1, $2, $3, NULL, $4)
            "#,
        )
        .bind(entity)
        .bind("1970-01-01T00:00:00Z")
        .bind(self.default_lookback_seconds)
        .bind(now)
        .execute(&self.pool)
        .await?;

        let row = sqlx::query_as::<_, SyncStateRow>(
            r#"
            SELECT entity, last_remote_updated_at, lookback_seconds, last_success_run_id, updated_at
            FROM sync_state
            WHERE entity = $1
            "#,
        )
        .bind(entity)
        .fetch_one(&self.pool)
        .await?;

        Ok(row)
    }

    pub async fn update_sync_cursor(
        &self,
        entity: &str,
        last_remote_updated_at: &str,
        run_id: Option<&str>,
    ) -> Result<()> {
        let now = now_rfc3339()?;
        sqlx::query(
            r#"
            UPDATE sync_state
            SET last_remote_updated_at = $2,
                last_success_run_id = $3,
                updated_at = $4
            WHERE entity = $1
            "#,
        )
        .bind(entity)
        .bind(last_remote_updated_at)
        .bind(run_id)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn category_exists(&self, category_id: i64) -> Result<bool> {
        let count: (i64,) = sqlx::query_as("SELECT COUNT(1) FROM lm_categories WHERE id = $1")
            .bind(category_id)
            .fetch_one(&self.pool)
            .await?;
        Ok(count.0 > 0)
    }

    pub async fn get_transaction_remote_updated_at(
        &self,
        transaction_id: i64,
    ) -> Result<Option<String>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT remote_updated_at FROM lm_transactions WHERE id = $1")
                .bind(transaction_id)
                .fetch_optional(&self.pool)
                .await?;

        Ok(row.map(|v| v.0))
    }

    pub async fn get_category_remote_updated_at(&self, category_id: i64) -> Result<Option<String>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT remote_updated_at FROM lm_categories WHERE id = $1")
                .bind(category_id)
                .fetch_optional(&self.pool)
                .await?;

        Ok(row.map(|v| v.0))
    }

    pub async fn upsert_category(
        &self,
        run_id: Option<&str>,
        source: &str,
        action: &str,
        record: &Value,
    ) -> Result<bool> {
        let id = int_field(record, &["id"]).context("category missing id")?;
        let name = string_field(record, &["name"]).unwrap_or_else(|| "Unnamed".to_string());
        let is_group = bool_field(record, &["is_group", "group"]).unwrap_or(false) as i64;
        let group_id = int_field(record, &["group_id", "group_id_fk"]);
        let archived = bool_field(record, &["archived", "is_archived"]).unwrap_or(false) as i64;
        let remote_updated_at = remote_updated_at(record)?;
        let raw_json = serde_json::to_string(record)?;

        let existing: Option<(String, String)> =
            sqlx::query_as("SELECT remote_updated_at, raw_json FROM lm_categories WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;

        let mut before_json = None;
        let mut before_remote_updated_at = None;
        if let Some((existing_updated_at, existing_raw)) = existing {
            if existing_updated_at > remote_updated_at
                || (existing_updated_at == remote_updated_at && existing_raw == raw_json)
            {
                return Ok(false);
            }
            before_remote_updated_at = Some(existing_updated_at);
            before_json = Some(existing_raw);
        }

        let now = now_rfc3339()?;
        sqlx::query(
            r#"
            INSERT INTO lm_categories (id, name, is_group, group_id, archived, raw_json, remote_updated_at, local_updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                is_group = excluded.is_group,
                group_id = excluded.group_id,
                archived = excluded.archived,
                raw_json = excluded.raw_json,
                remote_updated_at = excluded.remote_updated_at,
                local_updated_at = excluded.local_updated_at
            "#,
        )
        .bind(id)
        .bind(&name)
        .bind(is_group)
        .bind(group_id)
        .bind(archived)
        .bind(&raw_json)
        .bind(&remote_updated_at)
        .bind(now)
        .execute(&self.pool)
        .await?;

        let normalized_action = normalize_upsert_action(source, action, before_json.is_some());
        self.append_event(
            run_id,
            source,
            normalized_action,
            "category",
            &id.to_string(),
            before_json.as_deref(),
            Some(&raw_json),
            json!({}),
        )
        .await?;

        if source == "local" && matches!(normalized_action, "create" | "update") {
            let payload = CategoryUpsertPayload {
                name: name.clone(),
                description: string_field(record, &["description"]),
                is_income: bool_field(record, &["is_income"]),
                exclude_from_budget: bool_field(record, &["exclude_from_budget"]),
                exclude_from_totals: bool_field(record, &["exclude_from_totals"]),
                is_group: Some(is_group != 0),
                group_id,
                archived: Some(archived != 0),
                base_remote_updated_at: if normalized_action == "update" {
                    before_remote_updated_at.clone()
                } else {
                    None
                },
            };
            let payload_json = serde_json::to_string(&payload)?;
            let now = now_rfc3339()?;
            let change_id = Uuid::new_v4().to_string();
            let idempotency_key = if normalized_action == "create" {
                format!("category:{id}:create:payload:{payload_json}")
            } else {
                let base = before_remote_updated_at.unwrap_or_else(|| remote_updated_at.clone());
                format!("category:{id}:update:base:{base}:payload:{payload_json}")
            };
            sqlx::query(
                r#"
                INSERT INTO outbox_changes (
                    change_id, entity_type, entity_id, op, payload_json, idempotency_key,
                    status, attempt_count, last_error, created_at, updated_at
                )
                VALUES ($1, 'category', $2, $3, $4, $5, 'pending', 0, NULL, $6, $7)
                ON CONFLICT(idempotency_key) DO NOTHING
                "#,
            )
            .bind(&change_id)
            .bind(id.to_string())
            .bind(normalized_action)
            .bind(payload_json)
            .bind(idempotency_key)
            .bind(&now)
            .bind(&now)
            .execute(&self.pool)
            .await?;
        }

        Ok(true)
    }

    pub async fn remap_category_id_references(
        &self,
        from_category_id: i64,
        to_category_id: i64,
    ) -> Result<()> {
        if from_category_id == to_category_id {
            return Ok(());
        }

        let now = now_rfc3339()?;
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r#"
            UPDATE lm_transactions
            SET category_id = $2,
                raw_json = jsonb_set(raw_json::jsonb, '{category_id}', to_jsonb($2::bigint), true)::text,
                local_updated_at = $3
            WHERE category_id = $1
            "#,
        )
        .bind(from_category_id)
        .bind(to_category_id)
        .bind(&now)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            UPDATE outbox_changes
            SET payload_json = jsonb_set(payload_json::jsonb, '{category_id}', to_jsonb($2::bigint), true)::text,
                updated_at = $3
            WHERE entity_type = 'transaction'
              AND op = 'update'
              AND status IN ('pending', 'failed', 'processing', 'failed_conflict')
              AND (payload_json::jsonb ->> 'category_id')::bigint = $1
            "#,
        )
        .bind(from_category_id)
        .bind(to_category_id)
        .bind(&now)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            UPDATE outbox_changes
            SET entity_id = $2,
                updated_at = $3
            WHERE entity_type = 'category'
              AND status IN ('pending', 'failed', 'processing', 'failed_conflict')
              AND entity_id = $1
            "#,
        )
        .bind(from_category_id.to_string())
        .bind(to_category_id.to_string())
        .bind(&now)
        .execute(&mut *tx)
        .await?;

        let target_exists: Option<(i64,)> =
            sqlx::query_as("SELECT 1 FROM lm_categories WHERE id = $1")
                .bind(to_category_id)
                .fetch_optional(&mut *tx)
                .await?;

        if target_exists.is_some() {
            sqlx::query("DELETE FROM lm_categories WHERE id = $1")
                .bind(from_category_id)
                .execute(&mut *tx)
                .await?;
        } else {
            sqlx::query("UPDATE lm_categories SET id = $2, local_updated_at = $3 WHERE id = $1")
                .bind(from_category_id)
                .bind(to_category_id)
                .bind(&now)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn upsert_asset(
        &self,
        run_id: Option<&str>,
        source: &str,
        action: &str,
        record: &Value,
    ) -> Result<bool> {
        let id = int_field(record, &["id"]).context("asset missing id")?;
        let name = string_field(record, &["name"]).unwrap_or_else(|| "Unnamed".to_string());
        let type_name = string_field(record, &["type_name", "type"]);
        let subtype_name = string_field(record, &["subtype_name", "subtype"]);
        let balance = string_field(record, &["balance", "display_balance"]);
        let currency = string_field(record, &["currency", "currency_code"]);
        let institution_name = string_field(record, &["institution_name", "institution"]);
        let remote_updated_at = remote_updated_at(record)?;
        let raw_json = serde_json::to_string(record)?;

        let existing: Option<(String, String)> =
            sqlx::query_as("SELECT remote_updated_at, raw_json FROM lm_assets WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;

        let mut before_json = None;
        if let Some((existing_updated_at, existing_raw)) = existing {
            if existing_updated_at > remote_updated_at
                || (existing_updated_at == remote_updated_at && existing_raw == raw_json)
            {
                return Ok(false);
            }
            before_json = Some(existing_raw);
        }

        let now = now_rfc3339()?;
        sqlx::query(
            r#"
            INSERT INTO lm_assets (
                id, name, type_name, subtype_name, balance, currency,
                institution_name, raw_json, remote_updated_at, local_updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                type_name = excluded.type_name,
                subtype_name = excluded.subtype_name,
                balance = excluded.balance,
                currency = excluded.currency,
                institution_name = excluded.institution_name,
                raw_json = excluded.raw_json,
                remote_updated_at = excluded.remote_updated_at,
                local_updated_at = excluded.local_updated_at
            "#,
        )
        .bind(id)
        .bind(name)
        .bind(type_name)
        .bind(subtype_name)
        .bind(balance)
        .bind(currency)
        .bind(institution_name)
        .bind(&raw_json)
        .bind(&remote_updated_at)
        .bind(now)
        .execute(&self.pool)
        .await?;

        let normalized_action = normalize_upsert_action(source, action, before_json.is_some());
        self.append_event(
            run_id,
            source,
            normalized_action,
            "asset",
            &id.to_string(),
            before_json.as_deref(),
            Some(&raw_json),
            json!({}),
        )
        .await?;

        Ok(true)
    }

    pub async fn upsert_me(
        &self,
        run_id: Option<&str>,
        source: &str,
        action: &str,
        record: &Value,
    ) -> Result<bool> {
        let id = int_field(record, &["id"]).context("me payload missing id")?;
        let account_id = int_field(record, &["account_id"]);
        let email = string_field(record, &["email"]);
        let name = string_field(record, &["name"]);
        let budget_name = string_field(record, &["budget_name"]);
        let primary_currency = string_field(record, &["primary_currency"]);
        let api_key_label = string_field(record, &["api_key_label"]);
        let remote_updated_at = remote_updated_at(record)?;
        let raw_json = serde_json::to_string(record)?;

        let existing: Option<(String, String)> =
            sqlx::query_as("SELECT remote_updated_at, raw_json FROM lm_me WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;

        let mut before_json = None;
        if let Some((existing_updated_at, existing_raw)) = existing {
            if existing_updated_at > remote_updated_at
                || (existing_updated_at == remote_updated_at && existing_raw == raw_json)
            {
                return Ok(false);
            }
            before_json = Some(existing_raw);
        }

        let now = now_rfc3339()?;
        sqlx::query(
            r#"
            INSERT INTO lm_me (
                id, account_id, email, name, budget_name, primary_currency, api_key_label,
                raw_json, remote_updated_at, local_updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT(id) DO UPDATE SET
                account_id = excluded.account_id,
                email = excluded.email,
                name = excluded.name,
                budget_name = excluded.budget_name,
                primary_currency = excluded.primary_currency,
                api_key_label = excluded.api_key_label,
                raw_json = excluded.raw_json,
                remote_updated_at = excluded.remote_updated_at,
                local_updated_at = excluded.local_updated_at
            "#,
        )
        .bind(id)
        .bind(account_id)
        .bind(email)
        .bind(name)
        .bind(budget_name)
        .bind(primary_currency)
        .bind(api_key_label)
        .bind(&raw_json)
        .bind(&remote_updated_at)
        .bind(now)
        .execute(&self.pool)
        .await?;

        let normalized_action = normalize_upsert_action(source, action, before_json.is_some());
        self.append_event(
            run_id,
            source,
            normalized_action,
            "me",
            &id.to_string(),
            before_json.as_deref(),
            Some(&raw_json),
            json!({}),
        )
        .await?;

        Ok(true)
    }

    pub async fn upsert_plaid_account(
        &self,
        run_id: Option<&str>,
        source: &str,
        action: &str,
        record: &Value,
    ) -> Result<bool> {
        let id = int_field(record, &["id"]).context("plaid account missing id")?;
        let name = string_field(record, &["name"]);
        let display_name = string_field(record, &["display_name"]);
        let institution_name = string_field(record, &["institution_name"]);
        let type_name = string_field(record, &["type"]);
        let subtype_name = string_field(record, &["subtype"]);
        let status = string_field(record, &["status"]);
        let balance = string_field(record, &["balance"]);
        let currency = string_field(record, &["currency"]);
        let remote_updated_at = remote_updated_at(record)?;
        let raw_json = serde_json::to_string(record)?;

        let existing: Option<(String, String)> = sqlx::query_as(
            "SELECT remote_updated_at, raw_json FROM lm_plaid_accounts WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        let mut before_json = None;
        if let Some((existing_updated_at, existing_raw)) = existing {
            if existing_updated_at > remote_updated_at
                || (existing_updated_at == remote_updated_at && existing_raw == raw_json)
            {
                return Ok(false);
            }
            before_json = Some(existing_raw);
        }

        let now = now_rfc3339()?;
        sqlx::query(
            r#"
            INSERT INTO lm_plaid_accounts (
                id, name, display_name, institution_name, type_name, subtype_name,
                status, balance, currency, raw_json, remote_updated_at, local_updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                display_name = excluded.display_name,
                institution_name = excluded.institution_name,
                type_name = excluded.type_name,
                subtype_name = excluded.subtype_name,
                status = excluded.status,
                balance = excluded.balance,
                currency = excluded.currency,
                raw_json = excluded.raw_json,
                remote_updated_at = excluded.remote_updated_at,
                local_updated_at = excluded.local_updated_at
            "#,
        )
        .bind(id)
        .bind(name)
        .bind(display_name)
        .bind(institution_name)
        .bind(type_name)
        .bind(subtype_name)
        .bind(status)
        .bind(balance)
        .bind(currency)
        .bind(&raw_json)
        .bind(&remote_updated_at)
        .bind(now)
        .execute(&self.pool)
        .await?;

        let normalized_action = normalize_upsert_action(source, action, before_json.is_some());
        self.append_event(
            run_id,
            source,
            normalized_action,
            "plaid_account",
            &id.to_string(),
            before_json.as_deref(),
            Some(&raw_json),
            json!({}),
        )
        .await?;

        Ok(true)
    }

    pub async fn upsert_manual_account(
        &self,
        run_id: Option<&str>,
        source: &str,
        action: &str,
        record: &Value,
    ) -> Result<bool> {
        let id = int_field(record, &["id"]).context("manual account missing id")?;
        let name = string_field(record, &["name"]);
        let type_name = string_field(record, &["type"]);
        let subtype_name = string_field(record, &["subtype"]);
        let balance = string_field(record, &["balance"]);
        let currency = string_field(record, &["currency"]);
        let remote_updated_at = remote_updated_at(record)?;
        let raw_json = serde_json::to_string(record)?;

        let existing: Option<(String, String)> = sqlx::query_as(
            "SELECT remote_updated_at, raw_json FROM lm_manual_accounts WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        let mut before_json = None;
        if let Some((existing_updated_at, existing_raw)) = existing {
            if existing_updated_at > remote_updated_at
                || (existing_updated_at == remote_updated_at && existing_raw == raw_json)
            {
                return Ok(false);
            }
            before_json = Some(existing_raw);
        }

        let now = now_rfc3339()?;
        sqlx::query(
            r#"
            INSERT INTO lm_manual_accounts (
                id, name, type_name, subtype_name, balance, currency,
                raw_json, remote_updated_at, local_updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                type_name = excluded.type_name,
                subtype_name = excluded.subtype_name,
                balance = excluded.balance,
                currency = excluded.currency,
                raw_json = excluded.raw_json,
                remote_updated_at = excluded.remote_updated_at,
                local_updated_at = excluded.local_updated_at
            "#,
        )
        .bind(id)
        .bind(name)
        .bind(type_name)
        .bind(subtype_name)
        .bind(balance)
        .bind(currency)
        .bind(&raw_json)
        .bind(&remote_updated_at)
        .bind(now)
        .execute(&self.pool)
        .await?;

        let normalized_action = normalize_upsert_action(source, action, before_json.is_some());
        self.append_event(
            run_id,
            source,
            normalized_action,
            "manual_account",
            &id.to_string(),
            before_json.as_deref(),
            Some(&raw_json),
            json!({}),
        )
        .await?;

        Ok(true)
    }

    pub async fn upsert_tag(
        &self,
        run_id: Option<&str>,
        source: &str,
        action: &str,
        record: &Value,
    ) -> Result<bool> {
        let id = int_field(record, &["id"]).context("tag missing id")?;
        let name = string_field(record, &["name"]);
        let description = string_field(record, &["description"]);
        let remote_updated_at = remote_updated_at(record)?;
        let raw_json = serde_json::to_string(record)?;

        let existing: Option<(String, String)> =
            sqlx::query_as("SELECT remote_updated_at, raw_json FROM lm_tags WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;

        let mut before_json = None;
        if let Some((existing_updated_at, existing_raw)) = existing {
            if existing_updated_at > remote_updated_at
                || (existing_updated_at == remote_updated_at && existing_raw == raw_json)
            {
                return Ok(false);
            }
            before_json = Some(existing_raw);
        }

        let now = now_rfc3339()?;
        sqlx::query(
            r#"
            INSERT INTO lm_tags (id, name, description, raw_json, remote_updated_at, local_updated_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                description = excluded.description,
                raw_json = excluded.raw_json,
                remote_updated_at = excluded.remote_updated_at,
                local_updated_at = excluded.local_updated_at
            "#,
        )
        .bind(id)
        .bind(name)
        .bind(description)
        .bind(&raw_json)
        .bind(&remote_updated_at)
        .bind(now)
        .execute(&self.pool)
        .await?;

        let normalized_action = normalize_upsert_action(source, action, before_json.is_some());
        self.append_event(
            run_id,
            source,
            normalized_action,
            "tag",
            &id.to_string(),
            before_json.as_deref(),
            Some(&raw_json),
            json!({}),
        )
        .await?;

        Ok(true)
    }

    pub async fn upsert_recurring_item(
        &self,
        run_id: Option<&str>,
        source: &str,
        action: &str,
        record: &Value,
    ) -> Result<bool> {
        let id = int_field(record, &["id"]).context("recurring item missing id")?;
        let payee = string_field(record, &["payee", "description"]);
        let amount = string_field(record, &["amount"]);
        let currency = string_field(record, &["currency"]);
        let cadence = string_field(record, &["cadence", "frequency"]);
        let category_id = int_field(record, &["category_id"]);
        let remote_updated_at = remote_updated_at(record)?;
        let raw_json = serde_json::to_string(record)?;

        let existing: Option<(String, String)> = sqlx::query_as(
            "SELECT remote_updated_at, raw_json FROM lm_recurring_items WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        let mut before_json = None;
        if let Some((existing_updated_at, existing_raw)) = existing {
            if existing_updated_at > remote_updated_at
                || (existing_updated_at == remote_updated_at && existing_raw == raw_json)
            {
                return Ok(false);
            }
            before_json = Some(existing_raw);
        }

        let now = now_rfc3339()?;
        sqlx::query(
            r#"
            INSERT INTO lm_recurring_items (
                id, payee, amount, currency, cadence, category_id,
                raw_json, remote_updated_at, local_updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT(id) DO UPDATE SET
                payee = excluded.payee,
                amount = excluded.amount,
                currency = excluded.currency,
                cadence = excluded.cadence,
                category_id = excluded.category_id,
                raw_json = excluded.raw_json,
                remote_updated_at = excluded.remote_updated_at,
                local_updated_at = excluded.local_updated_at
            "#,
        )
        .bind(id)
        .bind(payee)
        .bind(amount)
        .bind(currency)
        .bind(cadence)
        .bind(category_id)
        .bind(&raw_json)
        .bind(&remote_updated_at)
        .bind(now)
        .execute(&self.pool)
        .await?;

        let normalized_action = normalize_upsert_action(source, action, before_json.is_some());
        self.append_event(
            run_id,
            source,
            normalized_action,
            "recurring_item",
            &id.to_string(),
            before_json.as_deref(),
            Some(&raw_json),
            json!({}),
        )
        .await?;

        Ok(true)
    }

    pub async fn upsert_summary_period(
        &self,
        run_id: Option<&str>,
        source: &str,
        action: &str,
        start_date: &str,
        end_date: &str,
        summary: &Value,
    ) -> Result<bool> {
        let period_key = format!("{start_date}:{end_date}");
        let aligned = summary
            .get("aligned")
            .and_then(|v| v.as_bool())
            .unwrap_or(false) as i64;
        let remote_updated_at = now_rfc3339()?;
        let raw_json = serde_json::to_string(summary)?;

        let existing: Option<(String, String)> = sqlx::query_as(
            "SELECT remote_updated_at, raw_json FROM lm_summary_periods WHERE period_key = $1",
        )
        .bind(&period_key)
        .fetch_optional(&self.pool)
        .await?;

        let mut before_json = None;
        if let Some((existing_updated_at, existing_raw)) = existing {
            if existing_updated_at > remote_updated_at
                || (existing_updated_at == remote_updated_at && existing_raw == raw_json)
            {
                return Ok(false);
            }
            before_json = Some(existing_raw);
        }

        let now = now_rfc3339()?;
        sqlx::query(
            r#"
            INSERT INTO lm_summary_periods (
                period_key, start_date, end_date, aligned, raw_json, remote_updated_at, local_updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT(period_key) DO UPDATE SET
                aligned = excluded.aligned,
                raw_json = excluded.raw_json,
                remote_updated_at = excluded.remote_updated_at,
                local_updated_at = excluded.local_updated_at
            "#,
        )
        .bind(&period_key)
        .bind(start_date)
        .bind(end_date)
        .bind(aligned)
        .bind(&raw_json)
        .bind(&remote_updated_at)
        .bind(now)
        .execute(&self.pool)
        .await?;

        let normalized_action = normalize_upsert_action(source, action, before_json.is_some());
        self.append_event(
            run_id,
            source,
            normalized_action,
            "summary",
            &period_key,
            before_json.as_deref(),
            Some(&raw_json),
            json!({ "start_date": start_date, "end_date": end_date }),
        )
        .await?;

        Ok(true)
    }

    pub async fn upsert_transaction(
        &self,
        run_id: Option<&str>,
        source: &str,
        action: &str,
        record: &Value,
    ) -> Result<bool> {
        let id = int_field(record, &["id"]).context("transaction missing id")?;
        let date =
            string_field(record, &["date", "date_utc"]).unwrap_or_else(|| "1970-01-01".to_string());
        let amount = string_field(record, &["amount", "to_base", "num_amount"])
            .unwrap_or_else(|| "0".to_string());
        let currency = string_field(record, &["currency", "currency_code"])
            .unwrap_or_else(|| "USD".to_string());
        let payee = string_field(record, &["payee", "merchant"]);
        let notes = string_field(record, &["notes", "note"]);
        let category_id = int_field(record, &["category_id"]);
        let asset_id = int_field(record, &["asset_id", "plaid_account_id"]);
        let status = string_field(record, &["status"]);
        let is_pending = bool_field(record, &["is_pending", "pending"]).unwrap_or(false) as i64;
        let external_id = string_field(record, &["external_id"]);
        let remote_updated_at = remote_updated_at(record)?;
        let raw_json = serde_json::to_string(record)?;

        let existing: Option<(String, String)> =
            sqlx::query_as("SELECT remote_updated_at, raw_json FROM lm_transactions WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;

        let mut before_json = None;
        let mut previous_remote_updated_at = None;
        let mut previous_category_id = None;
        if let Some((existing_updated_at, existing_raw)) = existing {
            if existing_updated_at > remote_updated_at
                || (existing_updated_at == remote_updated_at && existing_raw == raw_json)
            {
                return Ok(false);
            }
            previous_remote_updated_at = Some(existing_updated_at);
            previous_category_id = Some(int_field(&json_from_raw(existing_raw.clone()), &["category_id"]));
            before_json = Some(existing_raw);
        }

        let now = now_rfc3339()?;
        sqlx::query(
            r#"
            INSERT INTO lm_transactions (
                id, date, amount, currency, payee, notes, category_id, asset_id,
                status, is_pending, external_id, raw_json, remote_updated_at, local_updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT(id) DO UPDATE SET
                date = excluded.date,
                amount = excluded.amount,
                currency = excluded.currency,
                payee = excluded.payee,
                notes = excluded.notes,
                category_id = excluded.category_id,
                asset_id = excluded.asset_id,
                status = excluded.status,
                is_pending = excluded.is_pending,
                external_id = excluded.external_id,
                raw_json = excluded.raw_json,
                remote_updated_at = excluded.remote_updated_at,
                local_updated_at = excluded.local_updated_at
            "#,
        )
        .bind(id)
        .bind(date)
        .bind(amount)
        .bind(currency)
        .bind(payee)
        .bind(notes)
        .bind(category_id)
        .bind(asset_id)
        .bind(status)
        .bind(is_pending)
        .bind(external_id)
        .bind(&raw_json)
        .bind(&remote_updated_at)
        .bind(now)
        .execute(&self.pool)
        .await?;

        let normalized_action = normalize_upsert_action(source, action, before_json.is_some());
        self.append_event(
            run_id,
            source,
            normalized_action,
            "transaction",
            &id.to_string(),
            before_json.as_deref(),
            Some(&raw_json),
            json!({}),
        )
        .await?;

        if source == "local" {
            if let (Some(previous_remote_updated_at), Some(next_category_id)) =
                (previous_remote_updated_at.as_deref(), category_id)
            {
                if previous_category_id.flatten() != Some(next_category_id) {
                    self.enqueue_transaction_category_change(
                        run_id,
                        id,
                        next_category_id,
                        previous_remote_updated_at,
                        before_json.as_deref(),
                        &raw_json,
                        None,
                    )
                    .await?;
                }
            }
        }

        Ok(true)
    }

    async fn enqueue_transaction_category_change(
        &self,
        run_id: Option<&str>,
        transaction_id: i64,
        category_id: i64,
        base_remote_updated_at: &str,
        before_raw: Option<&str>,
        after_raw: &str,
        reason: Option<&str>,
    ) -> Result<String> {
        let payload = TransactionUpdatePayload {
            category_id,
            base_remote_updated_at: Some(base_remote_updated_at.to_string()),
            reason: reason.map(str::to_string),
        };
        let payload_json = serde_json::to_string(&payload)?;
        let change_id = Uuid::new_v4().to_string();
        let idempotency_key = format!(
            "txn:{transaction_id}:cat:{category_id}:base:{base_remote_updated_at}"
        );
        let now = now_rfc3339()?;
        let (change_id, inserted) = Self::insert_or_get_outbox_change(
            &self.pool,
            &change_id,
            "transaction",
            &transaction_id.to_string(),
            "update",
            &payload_json,
            &idempotency_key,
            &now,
        )
        .await?;

        if inserted {
            self.append_event(
                run_id,
                "local",
                "enqueue",
                "transaction",
                &transaction_id.to_string(),
                before_raw,
                Some(after_raw),
                json!({ "category_id": category_id, "reason": reason }),
            )
            .await?;
        }

        Ok(change_id)
    }

    pub async fn apply_local_category_decision(
        &self,
        run_id: Option<&str>,
        decision: &CategorizeDecision,
    ) -> Result<String> {
        if !self.category_exists(decision.category_id).await? {
            anyhow::bail!("category {} does not exist locally", decision.category_id);
        }

        let mut tx = self.pool.begin().await?;

        let row =
            sqlx::query("SELECT raw_json, remote_updated_at FROM lm_transactions WHERE id = $1")
                .bind(decision.transaction_id)
                .fetch_optional(&mut *tx)
                .await?;

        let Some(row) = row else {
            anyhow::bail!("transaction {} not found locally", decision.transaction_id);
        };

        let before_raw: String = row.try_get("raw_json")?;
        let remote_updated_at: String = row.try_get("remote_updated_at")?;

        let mut after_value: Value =
            serde_json::from_str(&before_raw).unwrap_or_else(|_| json!({}));
        after_value["category_id"] = Value::from(decision.category_id);
        let after_raw = serde_json::to_string(&after_value)?;
        let now = now_rfc3339()?;

        sqlx::query(
            r#"
            UPDATE lm_transactions
            SET category_id = $2,
                raw_json = $3,
                local_updated_at = $4
            WHERE id = $1
            "#,
        )
        .bind(decision.transaction_id)
        .bind(decision.category_id)
        .bind(&after_raw)
        .bind(&now)
        .execute(&mut *tx)
        .await?;

        let payload = TransactionUpdatePayload {
            category_id: decision.category_id,
            base_remote_updated_at: Some(remote_updated_at.clone()),
            reason: decision.reason.clone(),
        };
        let payload_json = serde_json::to_string(&payload)?;
        let change_id = Uuid::new_v4().to_string();
        let idempotency_key = format!(
            "txn:{}:cat:{}:base:{}",
            decision.transaction_id, decision.category_id, remote_updated_at
        );
        let (change_id, inserted) = Self::insert_or_get_outbox_change(
            &mut *tx,
            &change_id,
            "transaction",
            &decision.transaction_id.to_string(),
            "update",
            &payload_json,
            &idempotency_key,
            &now,
        )
        .await?;

        if inserted {
            sqlx::query(
                r#"
                INSERT INTO event_log (
                    event_id, run_id, source, action, entity_type, entity_id,
                    before_json, after_json, meta_json, occurred_at
                )
                VALUES ($1, $2, 'local', 'enqueue', 'transaction', $3, $4, $5, $6, $7)
                "#,
            )
            .bind(Uuid::new_v4().to_string())
            .bind(run_id)
            .bind(decision.transaction_id.to_string())
            .bind(before_raw)
            .bind(after_raw)
            .bind(serde_json::to_string(
                &json!({ "category_id": decision.category_id, "reason": decision.reason }),
            )?)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        Ok(change_id)
    }

    pub async fn apply_local_category_delete(
        &self,
        run_id: Option<&str>,
        category_id: i64,
        force: bool,
        reason: Option<&str>,
    ) -> Result<String> {
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query(
            "SELECT raw_json, remote_updated_at, archived FROM lm_categories WHERE id = $1",
        )
        .bind(category_id)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            anyhow::bail!("category {} not found locally", category_id);
        };

        let before_raw: String = row.try_get("raw_json")?;
        let remote_updated_at: String = row.try_get("remote_updated_at")?;

        let mut after_value: Value =
            serde_json::from_str(&before_raw).unwrap_or_else(|_| json!({ "id": category_id }));
        after_value["archived"] = Value::from(true);
        let after_raw = serde_json::to_string(&after_value)?;
        let now = now_rfc3339()?;

        sqlx::query(
            r#"
            UPDATE lm_categories
            SET archived = 1,
                raw_json = $2,
                local_updated_at = $3
            WHERE id = $1
            "#,
        )
        .bind(category_id)
        .bind(&after_raw)
        .bind(&now)
        .execute(&mut *tx)
        .await?;

        let payload = CategoryDeletePayload {
            base_remote_updated_at: Some(remote_updated_at.clone()),
            force,
            reason: reason.map(str::to_string),
        };
        let payload_json = serde_json::to_string(&payload)?;
        let change_id = Uuid::new_v4().to_string();
        let idempotency_key =
            format!("category:{category_id}:delete:base:{remote_updated_at}:force:{force}");
        let (change_id, inserted) = Self::insert_or_get_outbox_change(
            &mut *tx,
            &change_id,
            "category",
            &category_id.to_string(),
            "delete",
            &payload_json,
            &idempotency_key,
            &now,
        )
        .await?;

        if inserted {
            sqlx::query(
                r#"
                INSERT INTO event_log (
                    event_id, run_id, source, action, entity_type, entity_id,
                    before_json, after_json, meta_json, occurred_at
                )
                VALUES ($1, $2, 'local', 'enqueue', 'category', $3, $4, $5, $6, $7)
                "#,
            )
            .bind(Uuid::new_v4().to_string())
            .bind(run_id)
            .bind(category_id.to_string())
            .bind(before_raw)
            .bind(after_raw)
            .bind(serde_json::to_string(
                &json!({ "force": force, "reason": reason }),
            )?)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        Ok(change_id)
    }

    pub async fn fetch_outbox_batch(&self, limit: i64) -> Result<Vec<OutboxChange>> {
        let rows = sqlx::query_as::<_, OutboxChange>(
            r#"
            SELECT
                change_id, entity_type, entity_id, op, payload_json, idempotency_key,
                status, attempt_count, last_error, created_at, updated_at
            FROM outbox_changes
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn list_outbox_changes(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<OutboxChange>, i64)> {
        let total_count: (i64,) = sqlx::query_as("SELECT COUNT(1) FROM outbox_changes")
            .fetch_one(&self.pool)
            .await?;

        let rows = sqlx::query_as::<_, OutboxChange>(
            r#"
            SELECT
                change_id, entity_type, entity_id, op, payload_json, idempotency_key,
                status, attempt_count, last_error, created_at, updated_at
            FROM outbox_changes
            ORDER BY created_at DESC, change_id DESC
            LIMIT $1
            OFFSET $2
            "#,
        )
        .bind(limit.clamp(1, 1000))
        .bind(offset.max(0))
        .fetch_all(&self.pool)
        .await?;

        Ok((rows, total_count.0))
    }

    pub async fn mark_outbox_processing(&self, change_id: &str) -> Result<()> {
        sqlx::query(
            "UPDATE outbox_changes SET status = 'processing', updated_at = $2 WHERE change_id = $1",
        )
        .bind(change_id)
        .bind(now_rfc3339()?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_outbox_applied(&self, change_id: &str) -> Result<()> {
        sqlx::query(
            "UPDATE outbox_changes SET status = 'applied', updated_at = $2 WHERE change_id = $1",
        )
        .bind(change_id)
        .bind(now_rfc3339()?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_outbox_conflict(&self, change_id: &str, reason: &str) -> Result<()> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT attempt_count FROM outbox_changes WHERE change_id = $1")
                .bind(change_id)
                .fetch_optional(&self.pool)
                .await?;

        let next_attempts = row.map(|v| v.0 + 1).unwrap_or(1);
        sqlx::query(
            r#"
            UPDATE outbox_changes
            SET status = 'failed_conflict',
                attempt_count = $2,
                last_error = $3,
                updated_at = $4
            WHERE change_id = $1
            "#,
        )
        .bind(change_id)
        .bind(next_attempts)
        .bind(reason)
        .bind(now_rfc3339()?)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn mark_outbox_failure(
        &self,
        change_id: &str,
        retryable: bool,
        max_attempts: i64,
        err_text: &str,
    ) -> Result<()> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT attempt_count FROM outbox_changes WHERE change_id = $1")
                .bind(change_id)
                .fetch_optional(&self.pool)
                .await?;

        let next_attempts = row.map(|v| v.0 + 1).unwrap_or(1);
        let next_status = match next_failure_status(next_attempts, retryable, max_attempts) {
            OutboxStatus::Failed => "failed",
            OutboxStatus::DeadLetter => "dead_letter",
        };

        sqlx::query(
            r#"
            UPDATE outbox_changes
            SET status = $2,
                attempt_count = $3,
                last_error = $4,
                updated_at = $5
            WHERE change_id = $1
            "#,
        )
        .bind(change_id)
        .bind(next_status)
        .bind(next_attempts)
        .bind(err_text)
        .bind(now_rfc3339()?)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn requeue_outbox_change(&self, change_id: &str) -> Result<()> {
        let row = sqlx::query(
            r#"
            SELECT entity_type, entity_id, op, status, attempt_count, last_error
            FROM outbox_changes
            WHERE change_id = $1
            "#,
        )
        .bind(change_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            anyhow::bail!("outbox change {} not found", change_id);
        };

        let entity_type: String = row.try_get("entity_type")?;
        let entity_id: String = row.try_get("entity_id")?;
        let op: String = row.try_get("op")?;
        let status_before: String = row.try_get("status")?;
        let attempt_count: i64 = row.try_get("attempt_count")?;
        let last_error: Option<String> = row.try_get("last_error")?;

        let before_json = serde_json::to_string(&json!({
            "entity_type": entity_type,
            "entity_id": entity_id,
            "op": op,
            "status": status_before,
            "attempt_count": attempt_count,
            "last_error": last_error,
        }))?;

        sqlx::query(
            r#"
            UPDATE outbox_changes
            SET status = 'pending',
                last_error = NULL,
                updated_at = $2
            WHERE change_id = $1
            "#,
        )
        .bind(change_id)
        .bind(now_rfc3339()?)
        .execute(&self.pool)
        .await?;

        let after_json = serde_json::to_string(&json!({
            "entity_type": entity_type,
            "entity_id": entity_id,
            "op": op,
            "status": "pending",
            "attempt_count": attempt_count,
            "last_error": null,
        }))?;

        self.append_event(
            None,
            "local",
            "enqueue",
            "outbox",
            change_id,
            Some(&before_json),
            Some(&after_json),
            json!({ "reason": "requeue" }),
        )
        .await?;

        Ok(())
    }

    pub async fn append_event(
        &self,
        run_id: Option<&str>,
        source: &str,
        action: &str,
        entity_type: &str,
        entity_id: &str,
        before_json: Option<&str>,
        after_json: Option<&str>,
        meta_json: Value,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO event_log (
                event_id, run_id, source, action, entity_type, entity_id,
                before_json, after_json, meta_json, occurred_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(Uuid::new_v4().to_string())
        .bind(run_id)
        .bind(source)
        .bind(action)
        .bind(entity_type)
        .bind(entity_id)
        .bind(before_json)
        .bind(after_json)
        .bind(serde_json::to_string(&meta_json)?)
        .bind(now_rfc3339()?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_events(
        &self,
        run_id: Option<&str>,
        entity_type: Option<&str>,
        entity_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<EventRow>> {
        let mut qb = QueryBuilder::<Postgres>::new(
            "SELECT event_id, run_id, source, action, entity_type, entity_id, before_json, after_json, meta_json, occurred_at FROM event_log WHERE 1 = 1",
        );

        if let Some(run_id) = run_id {
            qb.push(" AND run_id = ").push_bind(run_id);
        }
        if let Some(entity_type) = entity_type {
            qb.push(" AND entity_type = ").push_bind(entity_type);
        }
        if let Some(entity_id) = entity_id {
            qb.push(" AND entity_id = ").push_bind(entity_id);
        }

        qb.push(" ORDER BY occurred_at DESC LIMIT ")
            .push_bind(limit.max(1));
        let rows = qb
            .build_query_as::<EventRow>()
            .fetch_all(&self.pool)
            .await?;
        Ok(rows)
    }

    pub async fn list_entity_records(&self, entity: &str, limit: i64) -> Result<Vec<Value>> {
        let spec = entity_spec(entity).context("unsupported entity")?;
        let query = format!(
            "SELECT raw_json FROM {} ORDER BY local_updated_at DESC LIMIT $1",
            spec.table
        );
        let rows = sqlx::query_scalar::<_, String>(&query)
            .bind(limit.clamp(1, 1000))
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(json_from_raw).collect())
    }

    pub async fn list_transactions_filtered(
        &self,
        start_date: Option<&str>,
        end_date: Option<&str>,
        category_id: Option<i64>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<Value>> {
        let mut qb =
            QueryBuilder::<Postgres>::new("SELECT raw_json FROM lm_transactions WHERE 1 = 1");
        if let Some(start_date) = start_date {
            qb.push(" AND date >= ").push_bind(start_date);
        }
        if let Some(end_date) = end_date {
            qb.push(" AND date <= ").push_bind(end_date);
        }
        if let Some(category_id) = category_id {
            qb.push(" AND category_id = ").push_bind(category_id);
        }

        qb.push(" ORDER BY date DESC, id DESC LIMIT ")
            .push_bind(limit.clamp(1, 1000))
            .push(" OFFSET ")
            .push_bind(offset.max(0));

        let rows = qb
            .build_query_scalar::<String>()
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(json_from_raw).collect())
    }

    pub async fn get_entity_record(&self, entity: &str, entity_id: &str) -> Result<Option<Value>> {
        let spec = entity_spec(entity).context("unsupported entity")?;
        let query = format!(
            "SELECT raw_json FROM {} WHERE {} = $1",
            spec.table, spec.id_column
        );

        let row = match spec.id_kind {
            EntityIdKind::Int => {
                let parsed = entity_id.parse::<i64>().with_context(|| {
                    format!("invalid id for entity {}: {}", spec.api_name, entity_id)
                })?;
                sqlx::query_scalar::<_, String>(&query)
                    .bind(parsed)
                    .fetch_optional(&self.pool)
                    .await?
            }
            EntityIdKind::Text => {
                sqlx::query_scalar::<_, String>(&query)
                    .bind(entity_id)
                    .fetch_optional(&self.pool)
                    .await?
            }
        };

        Ok(row.map(json_from_raw))
    }

    pub async fn upsert_entity_record(
        &self,
        run_id: Option<&str>,
        source: &str,
        action: &str,
        entity: &str,
        record: &Value,
    ) -> Result<bool> {
        let spec = entity_spec(entity).context("unsupported entity")?;
        if source == "local" {
            ensure_local_upsert_supported(spec)?;
        }
        match spec.api_name {
            "transactions" => {
                self.upsert_transaction(run_id, source, action, record)
                    .await
            }
            "categories" => self.upsert_category(run_id, source, action, record).await,
            "assets" => self.upsert_asset(run_id, source, action, record).await,
            "me" => self.upsert_me(run_id, source, action, record).await,
            "plaid_accounts" => {
                self.upsert_plaid_account(run_id, source, action, record)
                    .await
            }
            "manual_accounts" => {
                self.upsert_manual_account(run_id, source, action, record)
                    .await
            }
            "tags" => self.upsert_tag(run_id, source, action, record).await,
            "recurring_items" => {
                self.upsert_recurring_item(run_id, source, action, record)
                    .await
            }
            "summary_periods" => {
                let (start_date, end_date) = summary_dates_from_record(record)?;
                self.upsert_summary_period(run_id, source, action, &start_date, &end_date, record)
                    .await
            }
            _ => anyhow::bail!("unsupported entity"),
        }
    }

    pub async fn delete_entity_record(
        &self,
        run_id: Option<&str>,
        source: &str,
        entity: &str,
        entity_id: &str,
    ) -> Result<bool> {
        let spec = entity_spec(entity).context("unsupported entity")?;
        if source == "local" {
            ensure_local_delete_supported(spec)?;
        }

        let select_sql = format!(
            "SELECT raw_json FROM {} WHERE {} = $1",
            spec.table, spec.id_column
        );
        let delete_sql = format!("DELETE FROM {} WHERE {} = $1", spec.table, spec.id_column);

        let before_raw = match spec.id_kind {
            EntityIdKind::Int => {
                let parsed = entity_id.parse::<i64>().with_context(|| {
                    format!("invalid id for entity {}: {}", spec.api_name, entity_id)
                })?;
                let before = sqlx::query_scalar::<_, String>(&select_sql)
                    .bind(parsed)
                    .fetch_optional(&self.pool)
                    .await?;
                if before.is_none() {
                    return Ok(false);
                }
                sqlx::query(&delete_sql)
                    .bind(parsed)
                    .execute(&self.pool)
                    .await?;
                before
            }
            EntityIdKind::Text => {
                let before = sqlx::query_scalar::<_, String>(&select_sql)
                    .bind(entity_id)
                    .fetch_optional(&self.pool)
                    .await?;
                if before.is_none() {
                    return Ok(false);
                }
                sqlx::query(&delete_sql)
                    .bind(entity_id)
                    .execute(&self.pool)
                    .await?;
                before
            }
        };

        self.append_event(
            run_id,
            source,
            "delete",
            spec.event_entity_type,
            entity_id,
            before_raw.as_deref(),
            None,
            json!({}),
        )
        .await?;

        Ok(true)
    }
}

#[derive(Copy, Clone)]
enum EntityIdKind {
    Int,
    Text,
}

#[derive(Copy, Clone)]
struct EntitySpec {
    api_name: &'static str,
    table: &'static str,
    id_column: &'static str,
    id_kind: EntityIdKind,
    event_entity_type: &'static str,
}

fn entity_spec(entity: &str) -> Option<EntitySpec> {
    let normalized = entity.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "transaction" | "transactions" => Some(EntitySpec {
            api_name: "transactions",
            table: "lm_transactions",
            id_column: "id",
            id_kind: EntityIdKind::Int,
            event_entity_type: "transaction",
        }),
        "category" | "categories" => Some(EntitySpec {
            api_name: "categories",
            table: "lm_categories",
            id_column: "id",
            id_kind: EntityIdKind::Int,
            event_entity_type: "category",
        }),
        "asset" | "assets" => Some(EntitySpec {
            api_name: "assets",
            table: "lm_assets",
            id_column: "id",
            id_kind: EntityIdKind::Int,
            event_entity_type: "asset",
        }),
        "me" => Some(EntitySpec {
            api_name: "me",
            table: "lm_me",
            id_column: "id",
            id_kind: EntityIdKind::Int,
            event_entity_type: "me",
        }),
        "plaid_account" | "plaid_accounts" => Some(EntitySpec {
            api_name: "plaid_accounts",
            table: "lm_plaid_accounts",
            id_column: "id",
            id_kind: EntityIdKind::Int,
            event_entity_type: "plaid_account",
        }),
        "manual_account" | "manual_accounts" => Some(EntitySpec {
            api_name: "manual_accounts",
            table: "lm_manual_accounts",
            id_column: "id",
            id_kind: EntityIdKind::Int,
            event_entity_type: "manual_account",
        }),
        "tag" | "tags" => Some(EntitySpec {
            api_name: "tags",
            table: "lm_tags",
            id_column: "id",
            id_kind: EntityIdKind::Int,
            event_entity_type: "tag",
        }),
        "recurring_item" | "recurring_items" => Some(EntitySpec {
            api_name: "recurring_items",
            table: "lm_recurring_items",
            id_column: "id",
            id_kind: EntityIdKind::Int,
            event_entity_type: "recurring_item",
        }),
        "summary" | "summary_period" | "summary_periods" => Some(EntitySpec {
            api_name: "summary_periods",
            table: "lm_summary_periods",
            id_column: "period_key",
            id_kind: EntityIdKind::Text,
            event_entity_type: "summary",
        }),
        _ => None,
    }
}

fn ensure_local_upsert_supported(spec: EntitySpec) -> Result<()> {
    if matches!(spec.api_name, "transactions" | "categories") {
        return Ok(());
    }

    anyhow::bail!(
        "local write operations are not supported for entity {}",
        spec.api_name
    );
}

fn ensure_local_delete_supported(spec: EntitySpec) -> Result<()> {
    anyhow::bail!(
        "local delete operations are not supported for entity {}; use a dedicated command path",
        spec.api_name
    );
}

fn json_from_raw(raw_json: String) -> Value {
    serde_json::from_str(&raw_json).unwrap_or_else(|_| Value::String(raw_json))
}

fn summary_dates_from_record(record: &Value) -> Result<(String, String)> {
    if let (Some(start_date), Some(end_date)) = (
        string_field(record, &["start_date"]),
        string_field(record, &["end_date"]),
    ) {
        return Ok((start_date, end_date));
    }

    if let Some(period_key) = string_field(record, &["period_key"]) {
        if let Some((start_date, end_date)) = period_key.split_once(':') {
            return Ok((start_date.to_string(), end_date.to_string()));
        }
    }

    anyhow::bail!("summary_periods upsert requires start_date and end_date");
}

fn normalize_upsert_action<'a>(source: &str, requested: &'a str, existed: bool) -> &'a str {
    match source {
        "local" => {
            if existed {
                "update"
            } else {
                "create"
            }
        }
        "pull" => {
            if existed {
                "update"
            } else {
                "insert"
            }
        }
        "push" => {
            if existed {
                "update"
            } else {
                "create"
            }
        }
        _ => requested,
    }
}

fn int_field(value: &Value, keys: &[&str]) -> Option<i64> {
    for key in keys {
        let field = value.get(*key)?;
        if let Some(v) = field.as_i64() {
            return Some(v);
        }
        if let Some(v) = field.as_u64() {
            return i64::try_from(v).ok();
        }
        if let Some(v) = field.as_str() {
            if let Ok(parsed) = v.parse::<i64>() {
                return Some(parsed);
            }
        }
    }
    None
}

fn string_field(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        let field = value.get(*key)?;
        if let Some(v) = field.as_str() {
            return Some(v.to_string());
        }
        if field.is_number() || field.is_boolean() {
            return Some(field.to_string());
        }
    }
    None
}

fn bool_field(value: &Value, keys: &[&str]) -> Option<bool> {
    for key in keys {
        let field = value.get(*key)?;
        if let Some(v) = field.as_bool() {
            return Some(v);
        }
        if let Some(v) = field.as_i64() {
            return Some(v != 0);
        }
        if let Some(v) = field.as_str() {
            match v {
                "true" | "1" => return Some(true),
                "false" | "0" => return Some(false),
                _ => {}
            }
        }
    }
    None
}

fn remote_updated_at(value: &Value) -> Result<String> {
    if let Some(v) = string_field(
        value,
        &[
            "updated_at",
            "updatedAt",
            "last_modified",
            "modified_at",
            "date_modified",
        ],
    ) {
        return Ok(v);
    }

    now_rfc3339()
}
