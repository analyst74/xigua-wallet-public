# PocketLunch Context For Codex Runner

This is the only workspace content mounted into `codex-runner`.

## Purpose
- Provide high-level Lunch Money concepts.
- Provide HTTP access instructions for the running `pocketlunch-service`.
- Avoid mounting repository source code into `codex-runner`.

## Lunch Money Concepts (Quick Reference)
- Categories classify transactions (e.g., groceries, travel, income).
- Transactions are synced from Lunch Money and can be locally recategorized.
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
