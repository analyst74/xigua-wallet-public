use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use time::Duration;
use time::OffsetDateTime;
use tokio::time::sleep;
use tracing::warn;

use crate::config::AppConfig;
use crate::lm_v2_client::{LmClientError, LmV2Client};
use crate::models::{
    CategoryDeletePayload, CategoryUpsertPayload, OutboxChange, TransactionUpdatePayload,
};
use crate::outbox::classify_retryable_http;
use crate::store::Store;
use crate::timeutil::{format_rfc3339, parse_rfc3339};

const MAX_RATE_LIMIT_RETRIES_PER_BATCH: usize = 3;

#[derive(Clone)]
pub struct SyncEngine {
    config: AppConfig,
    store: Store,
    client: LmV2Client,
}

#[derive(Clone)]
struct PreparedTransactionUpdate {
    change: OutboxChange,
    transaction_id: i64,
    payload: TransactionUpdatePayload,
}

impl SyncEngine {
    pub fn new(config: AppConfig, store: Store, client: LmV2Client) -> Self {
        Self {
            config,
            store,
            client,
        }
    }

    pub async fn sync_pull(&self) -> Result<String> {
        let run_id = self.store.begin_run("pull").await?;

        match self.pull_with_run(&run_id, true).await {
            Ok(stats) => {
                self.store
                    .finish_run(&run_id, "success", &stats, None)
                    .await
                    .context("failed to close pull run as success")?;
                Ok(run_id)
            }
            Err(err) => {
                let error_json = json!({ "error": format!("{err:#}") });
                self.store
                    .finish_run(&run_id, "failed", &json!({}), Some(&error_json))
                    .await
                    .context("failed to close pull run as failed")?;
                Err(err)
            }
        }
    }

    pub async fn sync_pull_non_transactions(&self) -> Result<String> {
        let run_id = self.store.begin_run("pull").await?;

        match self.pull_with_run(&run_id, false).await {
            Ok(stats) => {
                self.store
                    .finish_run(&run_id, "success", &stats, None)
                    .await
                    .context("failed to close pull run as success")?;
                Ok(run_id)
            }
            Err(err) => {
                let error_json = json!({ "error": format!("{err:#}") });
                self.store
                    .finish_run(&run_id, "failed", &json!({}), Some(&error_json))
                    .await
                    .context("failed to close pull run as failed")?;
                Err(err)
            }
        }
    }

    pub async fn sync_push(&self) -> Result<String> {
        let run_id = self.store.begin_run("push").await?;

        match self.push_with_run(&run_id).await {
            Ok(stats) => {
                let failed_count = stats
                    .get("failed")
                    .and_then(|v| v.as_i64())
                    .unwrap_or_default();
                let status = if failed_count > 0 {
                    "partial"
                } else {
                    "success"
                };
                self.store.finish_run(&run_id, status, &stats, None).await?;
                Ok(run_id)
            }
            Err(err) => {
                let error_json = json!({ "error": format!("{err:#}") });
                self.store
                    .finish_run(&run_id, "failed", &json!({}), Some(&error_json))
                    .await?;
                Err(err)
            }
        }
    }

    pub async fn sync_all(&self) -> Result<String> {
        let run_id = self.store.begin_run("all").await?;

        let result = async {
            let pull_stats = self.pull_with_run(&run_id, true).await?;
            let push_stats = self.push_with_run(&run_id).await?;
            Ok::<Value, anyhow::Error>(json!({
                "pull": pull_stats,
                "push": push_stats,
            }))
        }
        .await;

        match result {
            Ok(stats) => {
                let failed_count = stats
                    .get("push")
                    .and_then(|v| v.get("failed"))
                    .and_then(|v| v.as_i64())
                    .unwrap_or_default();
                let status = if failed_count > 0 {
                    "partial"
                } else {
                    "success"
                };
                self.store.finish_run(&run_id, status, &stats, None).await?;
                Ok(run_id)
            }
            Err(err) => {
                let error_json = json!({ "error": format!("{err:#}") });
                self.store
                    .finish_run(&run_id, "failed", &json!({}), Some(&error_json))
                    .await?;
                Err(err)
            }
        }
    }

    async fn pull_with_run(&self, run_id: &str, include_transactions: bool) -> Result<Value> {
        let me = self
            .client
            .get_me_optional()
            .await
            .context("failed listing me")?;
        let categories = self
            .client
            .list_categories()
            .await
            .context("failed listing categories")?;
        let assets = self
            .client
            .list_assets()
            .await
            .context("failed listing assets")?;
        let plaid_accounts = self
            .client
            .list_plaid_accounts()
            .await
            .context("failed listing plaid accounts")?;
        let manual_accounts = self
            .client
            .list_manual_accounts()
            .await
            .context("failed listing manual accounts")?;
        let tags = self
            .client
            .list_tags()
            .await
            .context("failed listing tags")?;
        let recurring_items = self
            .client
            .list_recurring_items()
            .await
            .context("failed listing recurring items")?;

        let (summary_start_date, summary_end_date) = summary_window_dates();
        let summary = self
            .client
            .get_summary_optional(&summary_start_date, &summary_end_date)
            .await
            .context("failed fetching summary")?;

        let mut me_upserted = 0;
        let mut categories_upserted = 0;
        let mut assets_upserted = 0;
        let mut plaid_accounts_upserted = 0;
        let mut manual_accounts_upserted = 0;
        let mut tags_upserted = 0;
        let mut recurring_items_upserted = 0;
        let mut summary_upserted = 0;
        let mut tx_upserted = 0;

        let mut max_me_updated = None;
        if let Some(me) = me.as_ref() {
            if self
                .store
                .upsert_me(Some(run_id), "pull", "upsert", me)
                .await?
            {
                me_upserted += 1;
            }
            max_me_updated = extract_updated_at(me);
        }

        let mut max_categories_updated = None;
        for record in &categories {
            if self
                .store
                .upsert_category(Some(run_id), "pull", "upsert", record)
                .await?
            {
                categories_upserted += 1;
            }
            max_categories_updated =
                max_timestamp(max_categories_updated, extract_updated_at(record));
        }

        let mut max_assets_updated = None;
        for record in &assets {
            if self
                .store
                .upsert_asset(Some(run_id), "pull", "upsert", record)
                .await?
            {
                assets_upserted += 1;
            }
            max_assets_updated = max_timestamp(max_assets_updated, extract_updated_at(record));
        }

        let mut max_plaid_accounts_updated = None;
        for record in &plaid_accounts {
            if self
                .store
                .upsert_plaid_account(Some(run_id), "pull", "upsert", record)
                .await?
            {
                plaid_accounts_upserted += 1;
            }
            max_plaid_accounts_updated =
                max_timestamp(max_plaid_accounts_updated, extract_updated_at(record));
        }

        let mut max_manual_accounts_updated = None;
        for record in &manual_accounts {
            if self
                .store
                .upsert_manual_account(Some(run_id), "pull", "upsert", record)
                .await?
            {
                manual_accounts_upserted += 1;
            }
            max_manual_accounts_updated =
                max_timestamp(max_manual_accounts_updated, extract_updated_at(record));
        }

        let mut max_tags_updated = None;
        for record in &tags {
            if self
                .store
                .upsert_tag(Some(run_id), "pull", "upsert", record)
                .await?
            {
                tags_upserted += 1;
            }
            max_tags_updated = max_timestamp(max_tags_updated, extract_updated_at(record));
        }

        let mut max_recurring_items_updated = None;
        for record in &recurring_items {
            if self
                .store
                .upsert_recurring_item(Some(run_id), "pull", "upsert", record)
                .await?
            {
                recurring_items_upserted += 1;
            }
            max_recurring_items_updated =
                max_timestamp(max_recurring_items_updated, extract_updated_at(record));
        }

        let mut max_summary_updated = None;
        if let Some(summary) = summary.as_ref() {
            if self
                .store
                .upsert_summary_period(
                    Some(run_id),
                    "pull",
                    "upsert",
                    &summary_start_date,
                    &summary_end_date,
                    summary,
                )
                .await?
            {
                summary_upserted += 1;
            }
            max_summary_updated = Some(format_rfc3339(OffsetDateTime::now_utc())?);
        }

        if let Some(ts) = max_me_updated {
            self.store
                .update_sync_cursor("me", &ts, Some(run_id))
                .await?;
        }
        if let Some(ts) = max_categories_updated {
            self.store
                .update_sync_cursor("categories", &ts, Some(run_id))
                .await?;
        }
        if let Some(ts) = max_assets_updated {
            self.store
                .update_sync_cursor("assets", &ts, Some(run_id))
                .await?;
        }
        if let Some(ts) = max_plaid_accounts_updated {
            self.store
                .update_sync_cursor("plaid_accounts", &ts, Some(run_id))
                .await?;
        }
        if let Some(ts) = max_manual_accounts_updated {
            self.store
                .update_sync_cursor("manual_accounts", &ts, Some(run_id))
                .await?;
        }
        if let Some(ts) = max_tags_updated {
            self.store
                .update_sync_cursor("tags", &ts, Some(run_id))
                .await?;
        }
        if let Some(ts) = max_recurring_items_updated {
            self.store
                .update_sync_cursor("recurring_items", &ts, Some(run_id))
                .await?;
        }
        if let Some(ts) = max_summary_updated {
            self.store
                .update_sync_cursor("summary", &ts, Some(run_id))
                .await?;
        }

        let updated_since = if include_transactions {
            let txn_state = self.store.get_sync_state("transactions").await?;
            let updated_since = compute_updated_since(
                &txn_state.last_remote_updated_at,
                txn_state.lookback_seconds,
            )?;

            let mut offset = 0;
            let mut max_transaction_updated = None;

            loop {
                let page = self
                    .client
                    .list_transactions(Some(&updated_since), offset, self.config.pull_page_size)
                    .await
                    .with_context(|| {
                        format!(
                            "failed listing transactions with updated_since={} offset={offset}",
                            updated_since
                        )
                    })?;

                if page.is_empty() {
                    break;
                }

                for record in &page {
                    if self
                        .store
                        .upsert_transaction(Some(run_id), "pull", "upsert", record)
                        .await?
                    {
                        tx_upserted += 1;
                    }
                    max_transaction_updated =
                        max_timestamp(max_transaction_updated, extract_updated_at(record));
                }

                let page_len = page.len() as i64;
                if page_len < self.config.pull_page_size {
                    break;
                }
                offset += page_len;
            }

            if let Some(ts) = max_transaction_updated {
                self.store
                    .update_sync_cursor("transactions", &ts, Some(run_id))
                    .await?;
            }

            Some(updated_since)
        } else {
            None
        };

        Ok(json!({
            "me_fetched": if me.is_some() { 1 } else { 0 },
            "me_upserted": me_upserted,
            "categories_fetched": categories.len(),
            "categories_upserted": categories_upserted,
            "assets_fetched": assets.len(),
            "assets_upserted": assets_upserted,
            "plaid_accounts_fetched": plaid_accounts.len(),
            "plaid_accounts_upserted": plaid_accounts_upserted,
            "manual_accounts_fetched": manual_accounts.len(),
            "manual_accounts_upserted": manual_accounts_upserted,
            "tags_fetched": tags.len(),
            "tags_upserted": tags_upserted,
            "recurring_items_fetched": recurring_items.len(),
            "recurring_items_upserted": recurring_items_upserted,
            "summary_fetched": if summary.is_some() { 1 } else { 0 },
            "summary_upserted": summary_upserted,
            "transactions_upserted": tx_upserted,
            "updated_since": updated_since,
            "fetch_modes": {
                "me": "full",
                "categories": "full",
                "assets": "full",
                "plaid_accounts": "full",
                "manual_accounts": "full",
                "tags": "full",
                "recurring_items": "full",
                "summary": "full",
                "transactions": if include_transactions { "incremental" } else { "full" },
            },
        }))
    }

    async fn push_with_run(&self, run_id: &str) -> Result<Value> {
        let mut processed = 0;
        let mut applied = 0;
        let mut failed = 0;
        let mut conflicts = 0;

        loop {
            let batch = self
                .store
                .fetch_outbox_batch(self.config.outbox_batch_size)
                .await?;

            if batch.is_empty() {
                break;
            }

            let mut pending_transaction_updates = Vec::new();
            for change in batch {
                processed += 1;
                self.store.mark_outbox_processing(&change.change_id).await?;

                match (change.entity_type.as_str(), change.op.as_str()) {
                    ("transaction", "update") => {
                        if let Some(update) = self
                            .prepare_transaction_update_for_batch(
                                run_id,
                                change,
                                &mut failed,
                                &mut conflicts,
                            )
                            .await?
                        {
                            pending_transaction_updates.push(update);
                        }
                    }
                    ("category", "create") => {
                        let local_category_id: i64 = match change.entity_id.parse() {
                            Ok(value) => value,
                            Err(err) => {
                                failed += 1;
                                let error_text = format!("invalid entity_id: {err}");
                                self.store
                                    .mark_outbox_failure(
                                        &change.change_id,
                                        false,
                                        self.config.max_push_attempts,
                                        &error_text,
                                    )
                                    .await?;
                                self.store
                                    .append_event(
                                        Some(run_id),
                                        "push",
                                        "fail",
                                        "category",
                                        &change.entity_id,
                                        None,
                                        None,
                                        json!({
                                            "change_id": change.change_id.clone(),
                                            "retryable": false,
                                            "error": error_text,
                                        }),
                                    )
                                    .await?;
                                continue;
                            }
                        };

                        let payload: CategoryUpsertPayload =
                            match serde_json::from_str(&change.payload_json) {
                                Ok(payload) => payload,
                                Err(err) => {
                                    failed += 1;
                                    let error_text = format!("invalid payload json: {err}");
                                    self.store
                                        .mark_outbox_failure(
                                            &change.change_id,
                                            false,
                                            self.config.max_push_attempts,
                                            &error_text,
                                        )
                                        .await?;
                                    self.store
                                        .append_event(
                                            Some(run_id),
                                            "push",
                                            "fail",
                                            "category",
                                            &local_category_id.to_string(),
                                            None,
                                            None,
                                            json!({
                                                "change_id": change.change_id.clone(),
                                                "retryable": false,
                                                "error": error_text,
                                            }),
                                        )
                                        .await?;
                                    continue;
                                }
                            };

                        let request_json = category_upsert_payload_to_request_json(&payload);
                        match self.client.create_category(&request_json).await {
                            Ok(created_category) => {
                                let remote_category_id =
                                    int_field(&created_category, "id").unwrap_or(local_category_id);

                                if remote_category_id != local_category_id {
                                    if let Err(err) = self
                                        .store
                                        .remap_category_id_references(
                                            local_category_id,
                                            remote_category_id,
                                        )
                                        .await
                                    {
                                        failed += 1;
                                        let error_text = format!(
                                            "failed remapping category id after create: {err:#}"
                                        );
                                        self.store
                                            .mark_outbox_failure(
                                                &change.change_id,
                                                true,
                                                self.config.max_push_attempts,
                                                &error_text,
                                            )
                                            .await?;
                                        self.store
                                            .append_event(
                                                Some(run_id),
                                                "push",
                                                "fail",
                                                "category",
                                                &local_category_id.to_string(),
                                                None,
                                                None,
                                                json!({
                                                    "change_id": change.change_id.clone(),
                                                    "remote_category_id": remote_category_id,
                                                    "retryable": true,
                                                    "error": error_text,
                                                }),
                                            )
                                            .await?;
                                        continue;
                                    }
                                }

                                if let Err(err) = self
                                    .store
                                    .upsert_category(
                                        Some(run_id),
                                        "push",
                                        "apply",
                                        &created_category,
                                    )
                                    .await
                                {
                                    failed += 1;
                                    let error_text =
                                        format!("failed to upsert pushed category: {err:#}");
                                    self.store
                                        .mark_outbox_failure(
                                            &change.change_id,
                                            true,
                                            self.config.max_push_attempts,
                                            &error_text,
                                        )
                                        .await?;
                                    self.store
                                        .append_event(
                                            Some(run_id),
                                            "push",
                                            "fail",
                                            "category",
                                            &remote_category_id.to_string(),
                                            None,
                                            None,
                                            json!({
                                                "change_id": change.change_id.clone(),
                                                "local_category_id": local_category_id,
                                                "retryable": true,
                                                "error": error_text,
                                            }),
                                        )
                                        .await?;
                                    continue;
                                }

                                self.store.mark_outbox_applied(&change.change_id).await?;
                                applied += 1;
                            }
                            Err(err) => {
                                failed += 1;
                                let retryable = classify_lm_error_retryable(&err);
                                let error_text = format!("{err}");
                                self.store
                                    .mark_outbox_failure(
                                        &change.change_id,
                                        retryable,
                                        self.config.max_push_attempts,
                                        &error_text,
                                    )
                                    .await?;
                                self.store
                                    .append_event(
                                        Some(run_id),
                                        "push",
                                        "fail",
                                        "category",
                                        &local_category_id.to_string(),
                                        None,
                                        None,
                                        json!({
                                            "change_id": change.change_id,
                                            "retryable": retryable,
                                            "error": error_text,
                                        }),
                                    )
                                    .await?;
                            }
                        }
                    }
                    ("category", "update") => {
                        let category_id: i64 = match change.entity_id.parse() {
                            Ok(value) => value,
                            Err(err) => {
                                failed += 1;
                                let error_text = format!("invalid entity_id: {err}");
                                self.store
                                    .mark_outbox_failure(
                                        &change.change_id,
                                        false,
                                        self.config.max_push_attempts,
                                        &error_text,
                                    )
                                    .await?;
                                self.store
                                    .append_event(
                                        Some(run_id),
                                        "push",
                                        "fail",
                                        "category",
                                        &change.entity_id,
                                        None,
                                        None,
                                        json!({
                                            "change_id": change.change_id.clone(),
                                            "retryable": false,
                                            "error": error_text,
                                        }),
                                    )
                                    .await?;
                                continue;
                            }
                        };

                        let payload: CategoryUpsertPayload =
                            match serde_json::from_str(&change.payload_json) {
                                Ok(payload) => payload,
                                Err(err) => {
                                    failed += 1;
                                    let error_text = format!("invalid payload json: {err}");
                                    self.store
                                        .mark_outbox_failure(
                                            &change.change_id,
                                            false,
                                            self.config.max_push_attempts,
                                            &error_text,
                                        )
                                        .await?;
                                    self.store
                                        .append_event(
                                            Some(run_id),
                                            "push",
                                            "fail",
                                            "category",
                                            &category_id.to_string(),
                                            None,
                                            None,
                                            json!({
                                                "change_id": change.change_id.clone(),
                                                "retryable": false,
                                                "error": error_text,
                                            }),
                                        )
                                        .await?;
                                    continue;
                                }
                            };

                        if let Some(base) = payload.base_remote_updated_at.as_ref() {
                            if let Some(current) = self
                                .store
                                .get_category_remote_updated_at(category_id)
                                .await?
                            {
                                if current > *base {
                                    conflicts += 1;
                                    failed += 1;
                                    let message = format!(
                                        "failed_conflict: remote version advanced from {base} to {current}"
                                    );
                                    self.store
                                        .mark_outbox_conflict(&change.change_id, &message)
                                        .await?;
                                    self.store
                                        .append_event(
                                            Some(run_id),
                                            "push",
                                            "conflict",
                                            "category",
                                            &category_id.to_string(),
                                            None,
                                            None,
                                            json!({
                                                "reason": "conflict",
                                                "change_id": change.change_id,
                                                "base_remote_updated_at": base,
                                                "current_remote_updated_at": current,
                                            }),
                                        )
                                        .await?;
                                    continue;
                                }
                            }
                        }

                        let request_json = category_upsert_payload_to_request_json(&payload);
                        match self
                            .client
                            .update_category(category_id, &request_json)
                            .await
                        {
                            Ok(updated_category) => {
                                if let Err(err) = self
                                    .store
                                    .upsert_category(
                                        Some(run_id),
                                        "push",
                                        "apply",
                                        &updated_category,
                                    )
                                    .await
                                {
                                    failed += 1;
                                    let error_text =
                                        format!("failed to upsert pushed category: {err:#}");
                                    self.store
                                        .mark_outbox_failure(
                                            &change.change_id,
                                            true,
                                            self.config.max_push_attempts,
                                            &error_text,
                                        )
                                        .await?;
                                    self.store
                                        .append_event(
                                            Some(run_id),
                                            "push",
                                            "fail",
                                            "category",
                                            &category_id.to_string(),
                                            None,
                                            None,
                                            json!({
                                                "change_id": change.change_id.clone(),
                                                "retryable": true,
                                                "error": error_text,
                                            }),
                                        )
                                        .await?;
                                    continue;
                                }

                                self.store.mark_outbox_applied(&change.change_id).await?;
                                applied += 1;
                            }
                            Err(err) => {
                                failed += 1;
                                let retryable = classify_lm_error_retryable(&err);
                                let error_text = format!("{err}");
                                self.store
                                    .mark_outbox_failure(
                                        &change.change_id,
                                        retryable,
                                        self.config.max_push_attempts,
                                        &error_text,
                                    )
                                    .await?;
                                self.store
                                    .append_event(
                                        Some(run_id),
                                        "push",
                                        "fail",
                                        "category",
                                        &category_id.to_string(),
                                        None,
                                        None,
                                        json!({
                                            "change_id": change.change_id,
                                            "retryable": retryable,
                                            "error": error_text,
                                        }),
                                    )
                                    .await?;
                            }
                        }
                    }
                    ("category", "delete") => {
                        let category_id: i64 = match change.entity_id.parse() {
                            Ok(value) => value,
                            Err(err) => {
                                failed += 1;
                                let error_text = format!("invalid entity_id: {err}");
                                self.store
                                    .mark_outbox_failure(
                                        &change.change_id,
                                        false,
                                        self.config.max_push_attempts,
                                        &error_text,
                                    )
                                    .await?;
                                self.store
                                    .append_event(
                                        Some(run_id),
                                        "push",
                                        "fail",
                                        "category",
                                        &change.entity_id,
                                        None,
                                        None,
                                        json!({
                                            "change_id": change.change_id.clone(),
                                            "retryable": false,
                                            "error": error_text,
                                        }),
                                    )
                                    .await?;
                                continue;
                            }
                        };

                        let payload: CategoryDeletePayload =
                            match serde_json::from_str(&change.payload_json) {
                                Ok(payload) => payload,
                                Err(err) => {
                                    failed += 1;
                                    let error_text = format!("invalid payload json: {err}");
                                    self.store
                                        .mark_outbox_failure(
                                            &change.change_id,
                                            false,
                                            self.config.max_push_attempts,
                                            &error_text,
                                        )
                                        .await?;
                                    self.store
                                        .append_event(
                                            Some(run_id),
                                            "push",
                                            "conflict",
                                            "category",
                                            &category_id.to_string(),
                                            None,
                                            None,
                                            json!({
                                                "change_id": change.change_id.clone(),
                                                "retryable": false,
                                                "error": error_text,
                                            }),
                                        )
                                        .await?;
                                    continue;
                                }
                            };

                        if let Some(base) = payload.base_remote_updated_at.as_ref() {
                            if let Some(current) = self
                                .store
                                .get_category_remote_updated_at(category_id)
                                .await?
                            {
                                if current > *base {
                                    conflicts += 1;
                                    failed += 1;
                                    let message = format!(
                                        "failed_conflict: remote version advanced from {base} to {current}"
                                    );
                                    self.store
                                        .mark_outbox_conflict(&change.change_id, &message)
                                        .await?;
                                    self.store
                                        .append_event(
                                            Some(run_id),
                                            "push",
                                            "fail",
                                            "category",
                                            &category_id.to_string(),
                                            None,
                                            None,
                                            json!({
                                                "reason": "conflict",
                                                "change_id": change.change_id,
                                                "base_remote_updated_at": base,
                                                "current_remote_updated_at": current,
                                            }),
                                        )
                                        .await?;
                                    continue;
                                }
                            }
                        }

                        match self
                            .client
                            .delete_category(category_id, payload.force)
                            .await
                        {
                            Ok(updated_category) => {
                                if let Some(category) = updated_category {
                                    if let Err(err) = self
                                        .store
                                        .upsert_category(Some(run_id), "push", "apply", &category)
                                        .await
                                    {
                                        failed += 1;
                                        let error_text =
                                            format!("failed to upsert pushed category: {err:#}");
                                        self.store
                                            .mark_outbox_failure(
                                                &change.change_id,
                                                true,
                                                self.config.max_push_attempts,
                                                &error_text,
                                            )
                                            .await?;
                                        self.store
                                            .append_event(
                                                Some(run_id),
                                                "push",
                                                "fail",
                                                "category",
                                                &category_id.to_string(),
                                                None,
                                                None,
                                                json!({
                                                    "change_id": change.change_id.clone(),
                                                    "force": payload.force,
                                                    "retryable": true,
                                                    "error": error_text,
                                                }),
                                            )
                                            .await?;
                                        continue;
                                    }
                                } else {
                                    self.store
                                        .append_event(
                                            Some(run_id),
                                            "push",
                                            "delete",
                                            "category",
                                            &category_id.to_string(),
                                            None,
                                            None,
                                            json!({
                                                "change_id": change.change_id,
                                                "force": payload.force,
                                            }),
                                        )
                                        .await?;
                                }

                                self.store.mark_outbox_applied(&change.change_id).await?;
                                applied += 1;
                            }
                            Err(err) => {
                                failed += 1;
                                let retryable = classify_lm_error_retryable(&err);
                                let error_text = format!("{err}");
                                self.store
                                    .mark_outbox_failure(
                                        &change.change_id,
                                        retryable,
                                        self.config.max_push_attempts,
                                        &error_text,
                                    )
                                    .await?;
                                self.store
                                    .append_event(
                                        Some(run_id),
                                        "push",
                                        "fail",
                                        "category",
                                        &category_id.to_string(),
                                        None,
                                        None,
                                        json!({
                                            "change_id": change.change_id,
                                            "force": payload.force,
                                            "retryable": retryable,
                                            "error": error_text,
                                        }),
                                    )
                                    .await?;
                            }
                        }
                    }
                    _ => {
                        failed += 1;
                        let error_text = "unsupported outbox row".to_string();
                        self.store
                            .mark_outbox_failure(
                                &change.change_id,
                                false,
                                self.config.max_push_attempts,
                                &error_text,
                            )
                            .await?;
                        self.store
                            .append_event(
                                Some(run_id),
                                "push",
                                "fail",
                                &change.entity_type,
                                &change.entity_id,
                                None,
                                None,
                                json!({
                                    "change_id": change.change_id,
                                    "op": change.op,
                                    "retryable": false,
                                    "error": error_text,
                                }),
                            )
                            .await?;
                    }
                }
            }

            self.apply_transaction_batch_updates(
                run_id,
                pending_transaction_updates,
                &mut applied,
                &mut failed,
                &mut conflicts,
            )
            .await?;

            if processed > 0 && processed % 500 == 0 {
                warn!(processed, "push run still processing outbox rows");
            }
        }

        Ok(json!({
            "processed": processed,
            "applied": applied,
            "failed": failed,
            "conflicts": conflicts,
        }))
    }

    async fn prepare_transaction_update_for_batch(
        &self,
        run_id: &str,
        change: OutboxChange,
        failed: &mut i64,
        conflicts: &mut i64,
    ) -> Result<Option<PreparedTransactionUpdate>> {
        let transaction_id: i64 = match change.entity_id.parse() {
            Ok(value) => value,
            Err(err) => {
                *failed += 1;
                let error_text = format!("invalid entity_id: {err}");
                self.store
                    .mark_outbox_failure(
                        &change.change_id,
                        false,
                        self.config.max_push_attempts,
                        &error_text,
                    )
                    .await?;
                self.store
                    .append_event(
                        Some(run_id),
                        "push",
                        "fail",
                        "transaction",
                        &change.entity_id,
                        None,
                        None,
                        json!({
                            "change_id": change.change_id,
                            "retryable": false,
                            "error": error_text,
                        }),
                    )
                    .await?;
                return Ok(None);
            }
        };

        let payload: TransactionUpdatePayload = match serde_json::from_str(&change.payload_json) {
            Ok(payload) => payload,
            Err(err) => {
                *failed += 1;
                let error_text = format!("invalid payload json: {err}");
                self.store
                    .mark_outbox_failure(
                        &change.change_id,
                        false,
                        self.config.max_push_attempts,
                        &error_text,
                    )
                    .await?;
                self.store
                    .append_event(
                        Some(run_id),
                        "push",
                        "fail",
                        "transaction",
                        &transaction_id.to_string(),
                        None,
                        None,
                        json!({
                            "change_id": change.change_id,
                            "retryable": false,
                            "error": error_text,
                        }),
                    )
                    .await?;
                return Ok(None);
            }
        };

        if let Some(base) = payload.base_remote_updated_at.as_ref() {
            if let Some(current) = self
                .store
                .get_transaction_remote_updated_at(transaction_id)
                .await?
            {
                if current > *base {
                    *conflicts += 1;
                    *failed += 1;
                    let message = format!(
                        "failed_conflict: remote version advanced from {base} to {current}"
                    );
                    self.store
                        .mark_outbox_conflict(&change.change_id, &message)
                        .await?;
                    self.store
                        .append_event(
                            Some(run_id),
                            "push",
                            "conflict",
                            "transaction",
                            &transaction_id.to_string(),
                            None,
                            None,
                            json!({
                                "reason": "conflict",
                                "change_id": change.change_id,
                                "base_remote_updated_at": base,
                                "current_remote_updated_at": current,
                            }),
                        )
                        .await?;
                    return Ok(None);
                }
            }
        }

        Ok(Some(PreparedTransactionUpdate {
            change,
            transaction_id,
            payload,
        }))
    }

    async fn apply_transaction_batch_updates(
        &self,
        run_id: &str,
        updates: Vec<PreparedTransactionUpdate>,
        applied: &mut i64,
        failed: &mut i64,
        _conflicts: &mut i64,
    ) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }

        let batch_size = usize::try_from(self.config.transaction_update_batch_size)
            .unwrap_or(1)
            .max(1);
        let mut queue = VecDeque::new();
        let mut remaining = updates;
        while !remaining.is_empty() {
            let tail = if remaining.len() > batch_size {
                remaining.split_off(batch_size)
            } else {
                Vec::new()
            };
            queue.push_back(remaining);
            remaining = tail;
        }

        while let Some(batch) = queue.pop_front() {
            self.apply_transaction_batch_chunk(run_id, batch, &mut queue, applied, failed)
                .await?;
        }

        Ok(())
    }

    async fn apply_transaction_batch_chunk(
        &self,
        run_id: &str,
        mut updates: Vec<PreparedTransactionUpdate>,
        queue: &mut VecDeque<Vec<PreparedTransactionUpdate>>,
        applied: &mut i64,
        failed: &mut i64,
    ) -> Result<()> {
        let mut rate_limit_retries = 0;

        loop {
            let update_tuples = updates
                .iter()
                .map(|update| (update.transaction_id, update.payload.category_id))
                .collect::<Vec<_>>();

            match self
                .client
                .update_transactions_categories_batch(&update_tuples)
                .await
            {
                Ok(updated_transactions) => {
                    let mut updated_by_id = HashMap::new();
                    for updated in updated_transactions {
                        if let Some(id) = int_field(&updated, "id") {
                            updated_by_id.insert(id, updated);
                        }
                    }

                    for update in updates {
                        let Some(updated_txn) = updated_by_id.get(&update.transaction_id) else {
                            *failed += 1;
                            let error_text = format!(
                                "batch update response missing transaction {}",
                                update.transaction_id
                            );
                            self.mark_transaction_update_failure(
                                run_id,
                                &update,
                                false,
                                &error_text,
                            )
                            .await?;
                            continue;
                        };

                        if let Err(err) = self
                            .store
                            .upsert_transaction(Some(run_id), "push", "apply", updated_txn)
                            .await
                        {
                            *failed += 1;
                            let error_text =
                                format!("failed to upsert pushed transaction: {err:#}");
                            self.mark_transaction_update_failure(
                                run_id,
                                &update,
                                true,
                                &error_text,
                            )
                            .await?;
                            continue;
                        }

                        self.store
                            .mark_outbox_applied(&update.change.change_id)
                            .await?;
                        *applied += 1;
                    }

                    return Ok(());
                }
                Err(err) if should_split_transaction_batch(&err, updates.len()) => {
                    let split_index = updates.len() / 2;
                    let right = updates.split_off(split_index);
                    queue.push_front(right);
                    queue.push_front(updates);
                    return Ok(());
                }
                Err(err)
                    if should_retry_rate_limited_batch(&err)
                        && rate_limit_retries < MAX_RATE_LIMIT_RETRIES_PER_BATCH =>
                {
                    rate_limit_retries += 1;
                    let retry_after_seconds = err.retry_after_seconds().unwrap_or(0);
                    warn!(
                        batch_size = updates.len(),
                        retry_after_seconds,
                        rate_limit_retries,
                        "rate limited while pushing transaction batch; waiting before retry"
                    );
                    sleep(std::time::Duration::from_secs(retry_after_seconds)).await;
                }
                Err(err) => {
                    let retryable = classify_lm_error_retryable(&err);
                    let error_text = format!("{err}");
                    for update in updates {
                        *failed += 1;
                        self.mark_transaction_update_failure(
                            run_id,
                            &update,
                            retryable,
                            &error_text,
                        )
                        .await?;
                    }
                    return Ok(());
                }
            }
        }
    }

    async fn mark_transaction_update_failure(
        &self,
        run_id: &str,
        update: &PreparedTransactionUpdate,
        retryable: bool,
        error_text: &str,
    ) -> Result<()> {
        self.store
            .mark_outbox_failure(
                &update.change.change_id,
                retryable,
                self.config.max_push_attempts,
                error_text,
            )
            .await?;
        self.store
            .append_event(
                Some(run_id),
                "push",
                "fail",
                "transaction",
                &update.transaction_id.to_string(),
                None,
                None,
                json!({
                    "change_id": update.change.change_id,
                    "retryable": retryable,
                    "error": error_text,
                }),
            )
            .await?;
        Ok(())
    }
}

fn classify_lm_error_retryable(err: &LmClientError) -> bool {
    classify_retryable_http(err.status_code())
}

fn should_split_transaction_batch(err: &LmClientError, batch_len: usize) -> bool {
    batch_len > 1 && matches!(err.status_code(), Some(400))
}

fn should_retry_rate_limited_batch(err: &LmClientError) -> bool {
    matches!(err.status_code(), Some(429)) && err.retry_after_seconds().is_some()
}

fn max_timestamp(current: Option<String>, incoming: Option<String>) -> Option<String> {
    match (current, incoming) {
        (None, None) => None,
        (Some(current), None) => Some(current),
        (None, Some(incoming)) => Some(incoming),
        (Some(current), Some(incoming)) => {
            if current >= incoming {
                Some(current)
            } else {
                Some(incoming)
            }
        }
    }
}

fn extract_updated_at(record: &Value) -> Option<String> {
    for key in [
        "updated_at",
        "updatedAt",
        "last_modified",
        "modified_at",
        "date_modified",
    ] {
        if let Some(Value::String(value)) = record.get(key) {
            return Some(value.clone());
        }
    }
    None
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

fn category_upsert_payload_to_request_json(payload: &CategoryUpsertPayload) -> Value {
    let mut map = serde_json::Map::new();
    map.insert("name".to_string(), Value::String(payload.name.clone()));
    if let Some(description) = payload.description.as_ref() {
        map.insert(
            "description".to_string(),
            Value::String(description.clone()),
        );
    }
    if let Some(is_income) = payload.is_income {
        map.insert("is_income".to_string(), Value::Bool(is_income));
    }
    if let Some(exclude_from_budget) = payload.exclude_from_budget {
        map.insert(
            "exclude_from_budget".to_string(),
            Value::Bool(exclude_from_budget),
        );
    }
    if let Some(exclude_from_totals) = payload.exclude_from_totals {
        map.insert(
            "exclude_from_totals".to_string(),
            Value::Bool(exclude_from_totals),
        );
    }
    if let Some(is_group) = payload.is_group {
        map.insert("is_group".to_string(), Value::Bool(is_group));
    }
    if let Some(group_id) = payload.group_id {
        map.insert("group_id".to_string(), Value::from(group_id));
    }
    Value::Object(map)
}

fn summary_window_dates() -> (String, String) {
    let date = OffsetDateTime::now_utc().date();
    let (year, month, day) = date.to_calendar_date();
    let month_num = month as u8;
    (
        format!("{year:04}-{month_num:02}-01"),
        format!("{year:04}-{month_num:02}-{day:02}"),
    )
}

pub fn compute_updated_since(cursor: &str, lookback_seconds: i64) -> Result<String> {
    let cursor_ts = parse_rfc3339(cursor).or_else(|_| parse_rfc3339("1970-01-01T00:00:00Z"))?;
    let looked_back = cursor_ts - Duration::seconds(lookback_seconds.max(0));
    format_rfc3339(looked_back)
}

#[cfg(test)]
mod tests {
    use super::{
        classify_lm_error_retryable, compute_updated_since, extract_updated_at, max_timestamp,
    };
    use crate::lm_v2_client::LmClientError;
    use serde_json::json;

    #[test]
    fn lookback_moves_cursor_backwards() {
        let since = compute_updated_since("2026-01-01T00:10:00Z", 300).unwrap();
        assert_eq!(since, "2026-01-01T00:05:00Z");
    }

    #[test]
    fn invalid_cursor_falls_back_to_epoch() {
        let since = compute_updated_since("invalid", 300).unwrap();
        assert_eq!(since, "1969-12-31T23:55:00Z");
    }

    #[test]
    fn negative_lookback_is_clamped_to_zero() {
        let since = compute_updated_since("2026-01-01T00:10:00Z", -10).unwrap();
        assert_eq!(since, "2026-01-01T00:10:00Z");
    }

    #[test]
    fn max_timestamp_prefers_larger_value() {
        assert_eq!(
            max_timestamp(
                Some("2026-01-01T00:10:00Z".to_string()),
                Some("2026-01-01T00:11:00Z".to_string())
            ),
            Some("2026-01-01T00:11:00Z".to_string())
        );
        assert_eq!(
            max_timestamp(Some("2026-01-01T00:10:00Z".to_string()), None),
            Some("2026-01-01T00:10:00Z".to_string())
        );
    }

    #[test]
    fn extract_updated_at_uses_supported_fields() {
        let record = json!({ "last_modified": "2026-02-01T00:00:00Z" });
        let ts = extract_updated_at(&record);
        assert_eq!(ts.as_deref(), Some("2026-02-01T00:00:00Z"));
    }

    #[test]
    fn classify_lm_error_retryable_matches_http_classification() {
        let retryable = LmClientError::Http {
            status: 500,
            body: "server".to_string(),
            retry_after_seconds: None,
        };
        let terminal = LmClientError::Http {
            status: 400,
            body: "bad".to_string(),
            retry_after_seconds: None,
        };
        assert!(classify_lm_error_retryable(&retryable));
        assert!(!classify_lm_error_retryable(&terminal));
    }
}
