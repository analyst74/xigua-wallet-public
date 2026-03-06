CREATE TABLE IF NOT EXISTS sync_state (
    entity TEXT PRIMARY KEY,
    last_remote_updated_at TEXT NOT NULL,
    lookback_seconds BIGINT NOT NULL DEFAULT 300,
    last_success_run_id TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_runs (
    run_id TEXT PRIMARY KEY,
    direction TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    stats_json TEXT NOT NULL,
    error_json TEXT
);

CREATE TABLE IF NOT EXISTS lm_transactions (
    id BIGINT PRIMARY KEY,
    date TEXT NOT NULL,
    amount TEXT NOT NULL,
    currency TEXT NOT NULL,
    payee TEXT,
    notes TEXT,
    category_id BIGINT,
    asset_id BIGINT,
    status TEXT,
    is_pending INTEGER,
    external_id TEXT,
    raw_json TEXT NOT NULL,
    remote_updated_at TEXT NOT NULL,
    local_updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lm_categories (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    is_group INTEGER NOT NULL,
    group_id BIGINT,
    archived INTEGER NOT NULL,
    raw_json TEXT NOT NULL,
    remote_updated_at TEXT NOT NULL,
    local_updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lm_assets (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    type_name TEXT,
    subtype_name TEXT,
    balance TEXT,
    currency TEXT,
    institution_name TEXT,
    raw_json TEXT NOT NULL,
    remote_updated_at TEXT NOT NULL,
    local_updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox_changes (
    change_id TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    op TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    attempt_count BIGINT NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_log (
    event_id TEXT PRIMARY KEY,
    run_id TEXT,
    source TEXT NOT NULL,
    action TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    before_json TEXT,
    after_json TEXT,
    meta_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_event_log_entity_time
    ON event_log(entity_type, entity_id, occurred_at);

CREATE INDEX IF NOT EXISTS idx_outbox_status_created
    ON outbox_changes(status, created_at);

CREATE INDEX IF NOT EXISTS idx_lm_transactions_remote_updated
    ON lm_transactions(remote_updated_at);

CREATE INDEX IF NOT EXISTS idx_sync_state_entity
    ON sync_state(entity);

CREATE TABLE IF NOT EXISTS lm_me (
    id BIGINT PRIMARY KEY,
    account_id BIGINT,
    email TEXT,
    name TEXT,
    budget_name TEXT,
    primary_currency TEXT,
    api_key_label TEXT,
    raw_json TEXT NOT NULL,
    remote_updated_at TEXT NOT NULL,
    local_updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lm_plaid_accounts (
    id BIGINT PRIMARY KEY,
    name TEXT,
    display_name TEXT,
    institution_name TEXT,
    type_name TEXT,
    subtype_name TEXT,
    status TEXT,
    balance TEXT,
    currency TEXT,
    raw_json TEXT NOT NULL,
    remote_updated_at TEXT NOT NULL,
    local_updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lm_manual_accounts (
    id BIGINT PRIMARY KEY,
    name TEXT,
    type_name TEXT,
    subtype_name TEXT,
    balance TEXT,
    currency TEXT,
    raw_json TEXT NOT NULL,
    remote_updated_at TEXT NOT NULL,
    local_updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lm_tags (
    id BIGINT PRIMARY KEY,
    name TEXT,
    description TEXT,
    raw_json TEXT NOT NULL,
    remote_updated_at TEXT NOT NULL,
    local_updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lm_recurring_items (
    id BIGINT PRIMARY KEY,
    payee TEXT,
    amount TEXT,
    currency TEXT,
    cadence TEXT,
    category_id BIGINT,
    raw_json TEXT NOT NULL,
    remote_updated_at TEXT NOT NULL,
    local_updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lm_summary_periods (
    period_key TEXT PRIMARY KEY,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    aligned INTEGER,
    raw_json TEXT NOT NULL,
    remote_updated_at TEXT NOT NULL,
    local_updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_lm_plaid_accounts_remote_updated
    ON lm_plaid_accounts(remote_updated_at);

CREATE INDEX IF NOT EXISTS idx_lm_manual_accounts_remote_updated
    ON lm_manual_accounts(remote_updated_at);

CREATE INDEX IF NOT EXISTS idx_lm_tags_remote_updated
    ON lm_tags(remote_updated_at);

CREATE INDEX IF NOT EXISTS idx_lm_recurring_items_remote_updated
    ON lm_recurring_items(remote_updated_at);

CREATE INDEX IF NOT EXISTS idx_lm_summary_periods_remote_updated
    ON lm_summary_periods(remote_updated_at);
