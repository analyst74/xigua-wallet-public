ALTER TABLE lm_plaid_accounts
    ADD COLUMN IF NOT EXISTS to_base TEXT,
    ADD COLUMN IF NOT EXISTS balance_last_update TEXT;

ALTER TABLE lm_manual_accounts
    ADD COLUMN IF NOT EXISTS to_base TEXT,
    ADD COLUMN IF NOT EXISTS balance_as_of TEXT,
    ADD COLUMN IF NOT EXISTS status TEXT,
    ADD COLUMN IF NOT EXISTS display_name TEXT,
    ADD COLUMN IF NOT EXISTS institution_name TEXT;

CREATE TABLE IF NOT EXISTS lm_account_balance_snapshots (
    snapshot_date DATE NOT NULL,
    account_source TEXT NOT NULL,
    account_key TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    run_id TEXT REFERENCES sync_runs(run_id),
    account_id BIGINT,
    name TEXT,
    display_name TEXT,
    institution_name TEXT,
    type_name TEXT,
    subtype_name TEXT,
    status TEXT,
    balance TEXT,
    currency TEXT,
    to_base TEXT,
    balance_as_of TEXT,
    primary_currency TEXT,
    networth_role TEXT NOT NULL,
    networth_amount TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    CONSTRAINT lm_account_balance_snapshots_to_base_required CHECK (
        networth_role NOT IN ('asset', 'liability')
        OR lower(COALESCE(status, '')) = 'closed'
        OR NULLIF(to_base, '') IS NOT NULL
    ),
    PRIMARY KEY (snapshot_date, account_source, account_key)
);

CREATE INDEX IF NOT EXISTS idx_lm_account_balance_snapshots_account
    ON lm_account_balance_snapshots(account_source, account_key, snapshot_date);

CREATE INDEX IF NOT EXISTS idx_lm_account_balance_snapshots_role_date
    ON lm_account_balance_snapshots(snapshot_date, networth_role);

CREATE OR REPLACE VIEW lm_net_worth_history AS
WITH snapshot_amounts AS (
    SELECT
        snapshot_date,
        account_source,
        account_key,
        networth_role,
        to_base::numeric AS base_amount,
        networth_amount::numeric AS networth_amount
    FROM lm_account_balance_snapshots
    WHERE lower(status) IS DISTINCT FROM 'closed'
      AND networth_role IN ('asset', 'liability')
)
SELECT
    snapshot_date,
    COUNT(*) FILTER (WHERE networth_role = 'asset') AS asset_account_count,
    COUNT(*) FILTER (WHERE networth_role = 'liability') AS liability_account_count,
    COALESCE(SUM(base_amount) FILTER (WHERE networth_role = 'asset'), 0) AS assets_total,
    COALESCE(SUM(base_amount) FILTER (WHERE networth_role = 'liability'), 0) AS liabilities_total,
    COALESCE(SUM(networth_amount), 0) AS net_worth
FROM snapshot_amounts
GROUP BY snapshot_date;

CREATE OR REPLACE VIEW lm_net_worth_snapshot_errors AS
SELECT *
FROM lm_account_balance_snapshots
WHERE lower(status) IS DISTINCT FROM 'closed'
  AND networth_role IN ('asset', 'liability')
  AND NULLIF(to_base, '') IS NULL;
