# pocketlunch

Pocketlunch is a safe local workspace for letting an LLM review and fix your Lunch Money transaction categories, with a local copy of your data, controlled sync back to Lunch Money, and a full audit trail of every change.

`pocketlunch` syncs your Lunch Money data into a local store, lets Codex or another LLM stage category fixes locally, and only pushes changes back to Lunch Money when you decide to sync. Every staged and pushed change is recorded in an audit trail.

## Typical Use Case

You import or sync a batch of transactions and many of them are uncategorized or obviously wrong. `pocketlunch` gives an LLM a controlled place to read your local copy of that data, suggest better categories, and stage fixes. You can review the queued changes first, then push them back to Lunch Money only when the results look right.

## Category-Fix Workflow

Following instructions is for mac, it might work on linux, but definitely *NOT* work on Windows. Feel free to ask your favorite AI to translate it to your target environment.

1. Create a `.env` file from the template.
2. Add your `LUNCH_MONEY_API_TOKEN`.
3. Start the containers with `./scripts/start.sh`.
4. Open Codex inside `codex-runner`.
5. Ask it to pull fresh data, fetch categories, and stage category fixes locally.
6. Review the outbox and audit events.
7. Ask it to push the reviewed changes.

The safest pattern is a two-step flow:

- First prompt: pull data, analyze transactions, and stage fixes locally without pushing.
- Second prompt: inspect the staged outbox and push only if it matches what you reviewed.

## Step-By-Step

From the `pocketlunch/` directory:

1. Create your local config:

```bash
cp .env.example .env
chmod 600 .env
```

2. Edit `.env` and set your Lunch Money token:

```dotenv
LUNCH_MONEY_API_TOKEN=your_lunch_money_token_here
```

Optional:

- Set `OPENAI_API_KEY=...` if you want Codex CLI to use API key auth.
- If you leave `OPENAI_API_KEY` blank, you can log in later with `codex login --device-auth`.

3. Start the stack:

```bash
./scripts/start.sh
```

4. Open a shell inside the isolated runner:

```bash
docker compose --env-file .env -f docker-compose.yml exec -it codex-runner sh
```

5. Start Codex:

```bash
cd /workspace/context
codex
```

6. Paste the review-and-stage prompt below into Codex.

7. Review the results it reports:

- changed transaction ids
- queued outbox rows
- sample audit events

8. If the staged changes look right, paste the push prompt below.

9. Confirm the final audit summary after push.

## Example Prompts

Review and stage only:

```text
Use the pocketlunch service.

Task:
1. Pull the latest data.
2. Fetch categories.
3. Fetch transactions for the target date range.
4. Find transactions that are uncategorized or obviously miscategorized.
5. Reassign categories conservatively based on merchant/payee/description and existing category patterns.
6. Stage fixes locally by calling PUT /v1/transactions/:id with {"category_id": ...}.
7. Do not push yet.

Output:
- A short summary of what you changed and why.
- The transaction ids you changed.
- The outbox rows created.
- Audit events for sample changed transactions.
```

Push after review:

```text
Use the pocketlunch service.

Task:
1. Read the current outbox.
2. If there are only the staged category-fix changes we just reviewed, push them with POST /v1/sync/push.
3. Then fetch audit events for the affected transactions and summarize what was applied.
4. If any outbox item failed or conflicted, stop and report it instead of retrying blindly.
```

## Safety Model

- The Lunch Money API token is used by `pocketlunch-service`, not handed directly to the LLM client.
- The LLM works against the local service and local store.
- Category changes are staged before push.
- The outbox and audit log make every change inspectable.

## Getting Started

For HTTP API details, Docker topology, operator notes, and Postgres access, see [DEVELOPMENT.md](DEVELOPMENT.md).
