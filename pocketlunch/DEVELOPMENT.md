# pocketlunch Development Guide

This document covers the technical and operator-facing details for running and developing `pocketlunch`. For the end-user overview and category-fix workflow, see [README.md](README.md).

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

- `LUNCH_MONEY_API_TOKEN` is required for sync commands.
- `LUNCH_MONEY_BASE_URL` defaults to `https://api.lunchmoney.dev`.
- `DATABASE_URL` is required for direct CLI usage, for example `postgres://user:pass@localhost:5432/pocketlunch`.

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

## Idempotency And Safe Retries

- Local category decisions are deduped by an outbox idempotency key (`txn + category + base remote timestamp`).
- `POST /v1/sync/push` is safe to retry; pending and failed rows are retried and applied rows are not reprocessed.
- `POST /v1/outbox/requeue` resets failed or conflict rows to pending and records an audit event.
- The audit log is append-only; inspect `source` and `action` to reconstruct mutation history.

## Stack

This stack includes an optional locked-down `codex-runner` container and strict network separation:

- `codex-runner` on `app_net` only
- `postgres` on `data_net` only (`internal: true`)
- `pocketlunch-service` on both networks
- only `./codex-context` is mounted into `codex-runner` (repo source is not mounted)
- non-root users, `read_only`, `cap_drop: [ALL]`, and `no-new-privileges`

Files:

- `docker-compose.yml`
- `.env.example` -> `.env`
- `Dockerfile.codex-runner`

## Operator Setup

From the `pocketlunch/` directory:

```bash
cp .env.example .env
chmod 600 .env
```

Edit `.env` and set:

- `LUNCH_MONEY_API_TOKEN=...`
- `OPENAI_API_KEY=...` if you want Codex CLI to use API key auth

Notes:

- `LUNCH_MONEY_API_TOKEN` is only injected into `pocketlunch-service`.
- `codex-runner` does not get the Lunch Money token directly; it talks to `pocketlunch-service` over `POCKETLUNCH_SERVICE_URL`.
- If `OPENAI_API_KEY` is blank, you can log in later with `codex login --device-auth`.

## Starting And Managing The Stack

Start:

```bash
./scripts/start.sh
```

On first run, `start.sh` creates `.env` if missing, prompts for `LUNCH_MONEY_API_TOKEN`, and generates a strong `POSTGRES_PASSWORD` plus matching `POSTGRES_PASSWORD_URLENC`.

Check status:

```bash
docker compose --env-file .env -f docker-compose.yml ps
```

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

Optional health check from inside `codex-runner`:

```bash
docker compose --env-file .env -f docker-compose.yml exec -it codex-runner sh
curl -sS "$POCKETLUNCH_SERVICE_URL/healthz"
```

Manage:

```bash
docker compose --env-file .env -f docker-compose.yml logs -f
docker compose --env-file .env -f docker-compose.yml down
```

## Manual API Smoke Test

If you want to test the category-fix flow without an LLM first, run these inside `codex-runner`:

```bash
curl -sS -X POST "$POCKETLUNCH_SERVICE_URL/v1/sync/pull"
curl -sS "$POCKETLUNCH_SERVICE_URL/v1/categories"
curl -sS "$POCKETLUNCH_SERVICE_URL/v1/transactions?start_date=2026-03-01&end_date=2026-03-31&limit=100&offset=0"
curl -sS -X PUT "$POCKETLUNCH_SERVICE_URL/v1/transactions/101" \
  -H 'content-type: application/json' \
  -d '{"category_id":22}'
curl -sS "$POCKETLUNCH_SERVICE_URL/v1/outbox?limit=20&offset=0"
curl -sS -X POST "$POCKETLUNCH_SERVICE_URL/v1/sync/push"
curl -sS "$POCKETLUNCH_SERVICE_URL/v1/audit/events?entity=transaction&entity_id=101&limit=20"
```

## Accessing Postgres

Postgres is not published to the host. Use `psql` inside the container:

```bash
docker compose --env-file .env -f docker-compose.yml exec postgres \
  psql -U "${POSTGRES_USER:-pocketlunch}" -d "${POSTGRES_DB:-pocketlunch}"
```

Defaults are `POSTGRES_USER=pocketlunch` and `POSTGRES_DB=pocketlunch`; the password comes from `.env` via `POSTGRES_PASSWORD`.

If you want host access from a local SQL client, publish `5432:5432` on the `postgres` service first.
