# PocketLunch Context For Codex Runner

This is the only workspace content mounted into `codex-runner`.

## Purpose
- Provide high-level Lunch Money concepts.
- Provide HTTP access instructions for the running `pocketlunch-service`.
- Avoid mounting repository source code into `codex-runner`.

## Lunch Money Concepts (Quick Reference)
- Categories classify transactions (e.g., groceries, travel, income).
- Transactions are synced from Lunch Money and can be locally recategorized.
- Manual and Plaid accounts are synced from Lunch Money with current balances.
- A pull records at most one account-balance snapshot per account per UTC day.
- Net-worth history is derived from account-balance snapshots, not transactions.
- Local changes are queued in an outbox, then pushed upstream.
- Audit events are append-only records for pull, local enqueue, and push/apply operations.

## PocketLunch Service Access
- Base URL inside Docker app network: `http://pocketlunch-service:8080`
- Env var in `codex-runner`: `POCKETLUNCH_SERVICE_URL`

Health check:
```bash
curl -sS "$POCKETLUNCH_SERVICE_URL/healthz"
```

Sync operations:
```bash
curl -sS -X POST "$POCKETLUNCH_SERVICE_URL/v1/sync/pull"
curl -sS -X POST "$POCKETLUNCH_SERVICE_URL/v1/sync/pull-non-transactions"
curl -sS -X POST "$POCKETLUNCH_SERVICE_URL/v1/sync/push"
curl -sS -X POST "$POCKETLUNCH_SERVICE_URL/v1/sync/all"
```

## Account-Balance Snapshots
- `sync pull` writes daily account-balance snapshots for Plaid and manual accounts.
- Snapshot grain is `(snapshot_date, account_source, account_key)`, where `snapshot_date` is UTC.
- Later pulls on the same UTC day update that account's daily snapshot with the latest observed balance.
- Active asset/liability accounts must include `to_base`; missing or invalid `to_base` is a sync error by design.
- Assets contribute `+to_base`; liabilities contribute `-to_base`, so negative liability balances contribute positively.
- `codex-runner` can only interact with this behavior through `pocketlunch-service` APIs such as `/v1/sync/pull`.
- `codex-runner` cannot connect to Postgres directly and cannot inspect snapshot tables on its own.
- Derived Postgres objects exist for host/operator inspection only: `lm_account_balance_snapshots`, `lm_net_worth_history`, and `lm_net_worth_snapshot_errors`.

Transaction recategorization (enqueue local change):
```bash
curl -sS -X PUT "$POCKETLUNCH_SERVICE_URL/v1/transactions/101" \
  -H 'content-type: application/json' \
  -d '{"category_id":2499018}'
```

Category delete (archive locally and enqueue remote delete):
```bash
curl -sS -X DELETE "$POCKETLUNCH_SERVICE_URL/v1/categories/2499018?force=false&reason=cleanup"
```

Outbox inspection:
```bash
curl -sS "$POCKETLUNCH_SERVICE_URL/v1/outbox?limit=50&offset=0"
```

Audit events:
```bash
curl -sS "$POCKETLUNCH_SERVICE_URL/v1/audit/events?entity=category&limit=50"
```

## Important Isolation Notes
- `codex-runner` is connected to `app_net` only.
- `postgres` is on `data_net` only.
- `codex-runner` has no direct network path to Postgres.
