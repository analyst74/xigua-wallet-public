# pocketlunch

A safe local LLM harness for reviewing and fixing Lunch Money transaction categories with full audit history.

## Commands

- `pocketlunch serve --host 0.0.0.0 --port 8080`
- `pocketlunch sync pull`
- `pocketlunch sync push`
- `pocketlunch sync all`
- `pocketlunch local categorize-apply --file <decisions.json>`
- `pocketlunch local category-delete --category-id <id> [--force] [--reason <text>]`
- `pocketlunch audit list --run-id <id> --entity transaction --entity-id 123`
- `pocketlunch outbox requeue --change-id <uuid>`

## Environment

- `LUNCH_MONEY_API_TOKEN` (required for `sync` commands)
- `LUNCH_MONEY_BASE_URL` (default: `https://api.lunchmoney.dev`)
- `DATABASE_URL` (required, e.g. `postgres://user:pass@localhost:5432/pocketlunch`)

## Notes

- Runs `sqlx` migrations on startup.
- Keeps append-only `event_log` for audit reconstruction.
- `pocketlunch-service` is now on-demand via HTTP API (no scheduled sync loop).

## Service API

Default base URL inside `app_net`: `http://pocketlunch-service:8080`

- `GET /healthz`
- `POST /v1/sync/pull`
- `POST /v1/sync/pull-non-transactions`
- `POST /v1/sync/push`
- `POST /v1/sync/all`
- `GET /v1/transactions?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&category_id=<id>&limit=<n>&offset=<n>`
- `POST /v1/transactions` and `PUT /v1/transactions/:id` auto-enqueue category changes when `category_id` changes
- `DELETE /v1/categories/:id?force=<bool>&reason=<text>` archives locally and enqueues remote category deletion
- `GET /v1/outbox?limit=<n>&offset=<n>`
- `POST /v1/outbox/requeue` with JSON body `{ "change_id": "..." }`
- `GET /v1/audit/events?entity=category&limit=50`

Example from `codex-runner`:

```bash
curl -sS http://pocketlunch-service:8080/healthz
curl -sS -X POST http://pocketlunch-service:8080/v1/sync/pull
curl -sS "http://pocketlunch-service:8080/v1/transactions?start_date=2026-03-01&end_date=2026-03-05&limit=100&offset=0"
curl -sS -X PUT http://pocketlunch-service:8080/v1/transactions/101 \
  -H 'content-type: application/json' \
  -d '{"category_id":22}'
curl -sS -X DELETE "http://pocketlunch-service:8080/v1/categories/123?reason=cleanup"
curl -sS "http://pocketlunch-service:8080/v1/outbox?limit=20&offset=0"
curl -sS -X POST http://pocketlunch-service:8080/v1/sync/push
curl -sS "http://pocketlunch-service:8080/v1/audit/events?entity=category&limit=10"
```

## Canonical Workflow

1. Read reference entities: `GET /v1/categories`, `GET /v1/assets`, and transactions with date filters.
2. Decide categories in your client or automation layer.
3. Apply local categorizations:
   - `POST /v1/transactions` and `PUT /v1/transactions/:id` auto-enqueue outbox changes when `category_id` changes
   - `DELETE /v1/categories/:id?force=<bool>&reason=<text>` enqueues category deletion
4. Inspect current outbox if needed: `GET /v1/outbox?limit=...&offset=...`
5. Push local outbox: `POST /v1/sync/push`
6. Verify via audit: `GET /v1/audit/events?...`

## Idempotency and Safe Retries

- Local category decisions are deduped by an outbox idempotency key (`txn + category + base remote timestamp`).
- `POST /v1/sync/push` is safe to retry; pending/failed rows are retried and applied rows are not reprocessed.
- `POST /v1/outbox/requeue` resets failed/conflict rows to pending and records an audit event.
- Audit log is append-only; inspect `source` + `action` to reconstruct mutation history.

## Stack

This stack includes an optional locked-down `codex-runner` container and strict network separation:
- `codex-runner` on `app_net` only
- `postgres` on `data_net` only (`internal: true`)
- `pocketlunch-service` on both networks
- only `./codex-context` is mounted into `codex-runner` (repo source is not mounted)
- non-root users, `read_only`, `cap_drop: [ALL]`, and `no-new-privileges`

Start:

```bash
./scripts/start.sh
```

Files:
- `docker-compose.yml`
- `.env.example` -> `.env`
- `Dockerfile.codex-runner`

Use Codex inside the isolated runner:

```bash
docker compose --env-file .env -f docker-compose.yml exec -it codex-runner codex --version
docker compose --env-file .env -f docker-compose.yml exec -it codex-runner sh
# then inside container:
cd /workspace/context
cat README.md
codex
```

If using ChatGPT login in `codex-runner`, run:

```bash
docker compose --env-file .env -f docker-compose.yml exec -it codex-runner codex login --device-auth
```

Manage:

```bash
docker compose --env-file .env -f docker-compose.yml logs -f
docker compose --env-file .env -f docker-compose.yml down
```

## Accessing Postgres

Postgres is not published to the host. Use `psql` inside the container:

```bash
docker compose --env-file .env -f docker-compose.yml exec postgres \
  psql -U "${POSTGRES_USER:-pocketlunch}" -d "${POSTGRES_DB:-pocketlunch}"
```

Defaults are `POSTGRES_USER=pocketlunch` and `POSTGRES_DB=pocketlunch`; the password comes from `.env` via `POSTGRES_PASSWORD`.

If you want host access from a local SQL client, publish `5432:5432` on the `postgres` service first.
