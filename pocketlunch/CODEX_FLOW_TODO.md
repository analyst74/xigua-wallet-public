# Codex -> PocketLunch Remaining Work

This TODO tracks what is still needed to fully satisfy the desired flow:
1) fetch entities, 2) fetch transactions by date range, 3) category assignment by Codex, 4) local updates, 5) pull, 6) push, 7) optimized pull load, 8) auditable mutations, 9) clearly separated audit semantics.

## 1. Transaction Date/Range Query API
- [x] Add query filtering to `GET /v1/transactions`
  - `start_date` (inclusive, `YYYY-MM-DD`)
  - `end_date` (inclusive, `YYYY-MM-DD`)
  - `category_id` (optional)
  - `limit` + `offset`
- [x] Add store query method for date/range filters (index-aware).
- [x] Return deterministic ordering (`date DESC, id DESC`).
- [x] Add tests for:
  - single date filter
  - date range filter
  - invalid date validation
  - pagination behavior

## 2. Make Codex Category Changes Pushable by Default
- [x] Ensure Codex-driven category updates always enqueue outbox entries.
- [x] Decide and implement one canonical path:
  - Option A: Keep `POST /v1/local/categorize-apply` as required path and document strongly.
  - Option B: Make `PUT /v1/transactions/:id` smart-enqueue when `category_id` changes.
- [x] Add tests proving:
  - local change is written to DB
  - outbox row is created
  - `POST /v1/sync/push` applies change remotely
  - audit contains both local enqueue + push apply/fail

## 3. Pull Optimization (Reduce Lunch Money API Load)
- [x] Keep current transaction incremental pull (`updated_since` + lookback cursor).
- [x] Add metrics in `sync_runs.stats_json` for each entity fetch mode (`incremental` vs `full`).

## 4. Audit Taxonomy (Clear Source + Action Separation)
- [x] Expand `event_log.action` conventions to explicitly distinguish:
  - local: `create`, `update`, `delete`, `enqueue`
  - pull: `insert`, `update`
  - push: `create`, `update`, `delete`, `fail`, `conflict`
- [x] Keep `source` separation strict: `local`, `pull`, `push`.
- [x] Update all write paths to use normalized action names.
- [x] Add tests asserting expected `(source, action, entity_type)` tuples for each flow.

## 5. Ensure Every Mutating API Endpoint Is Audited
- [x] Audit-check every mutating route:
  - `POST /v1/:entity`
  - `PUT /v1/:entity/:id`
  - `DELETE /v1/:entity/:id`
  - `POST /v1/local/categorize-apply`
  - `POST /v1/local/category-delete`
  - `POST /v1/outbox/requeue`
  - `POST /v1/sync/push` side effects
- [x] Add regression test matrix to fail if any mutation path misses audit writes.

## 6. Docs + Operator Playbook
- [x] Update API docs with canonical Codex flow:
  1) read entities/transactions
  2) decide categories
  3) apply local categorizations
  4) push
  5) verify via audit
- [x] Add curl examples for date-ranged transaction fetch and category-apply workflow.
- [x] Document idempotency and safe retry behavior.

## Definition of Done
- [x] All above tests pass in `cargo test`.
- [ ] Live smoke checks in stack pass:
  - transaction date-range queries
  - local category change -> outbox enqueue
  - push applies change
  - audit shows clear source/action separation.
