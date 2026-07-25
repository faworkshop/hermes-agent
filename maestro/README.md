# Maestro SDLC Automation

This directory contains the project-specific SDLC automation setup:

- `pipeline_orchestrator.py` — local end-to-end simulator (PM -> Dev -> Reviewer -> QA)
- `webhook_server.py` — FastAPI endpoint for Linear webhooks (`/linear-webhook`)
- `sdlc_roles.yaml` — role definitions (prompts, toolsets, circuit-breaker targets)
- `docker-compose.yml` — local **PostgreSQL** for the agent queue (locks, tasks, retries)
- `compose.postgres.env.example` — example **`FAW_DB_URL`** for that container
- `scripts/migrate_queue_pg.py` — copy queue tables from another Postgres (e.g. old `localhost:5432` → Docker `5433`)
- **`../docs/LINEAR_TICKET_WORKFLOWS.md`** — Linear states, agents, queue, and circuit breaker (canonical doc)

## Prerequisites

- Run from repository root: `hermes-agent/`
- Use project virtualenv:
  - `source venv/bin/activate`
- Configure Hermes runtime as usual:
  - `~/.hermes/config.yaml` (model/provider)
  - `~/.hermes/.env` (API keys)

Required env vars for Linear/GitHub flow:

- `LINEAR_API_KEY`
- `LINEAR_WEBHOOK_SECRET` (webhook server signature verification)
- `LINEAR_BOT_USER_ID` (recommended for PM auto-assignment behavior)
- `GITHUB_TOKEN` or `GH_TOKEN` (for GitHub tools)

Required for the webhook server (agent queue + locks):

- `FAW_DB_URL` — PostgreSQL connection URI (see **PostgreSQL (Docker)** below)

Optional queue/intake controls:

- `HERMES_MAX_ACTIVE_TICKETS` — max distinct tickets processed at once across Developer/Reviewer/QA (default: `1`). PM does not count toward this cap.
- `HERMES_MAX_ACTIVE_PM_TICKETS` — max PM triages running in parallel, isolated from the global cap so PM scanning is not starved by long-running dev/reviewer/qa work (default: `5`).
- `HERMES_WATCHDOG_POLL_SECONDS` — watchdog poll interval for stale-task recovery (default: `60`).
- `HERMES_WATCHDOG_STALE_SECONDS` — task is considered stale and recovered after this many seconds without a heartbeat (default: `900`, 15 min).
- `PM_SCANNER_ENABLED` — proactively enqueue Product Manager intake tickets (default: `true`)
- `PM_SCANNER_INTERVAL_SECONDS` — PM scanner interval (default: `900`, 15 minutes)
- `PM_SCANNER_LIMIT` — max Linear candidates fetched per PM scan (default: `50`)
- `PM_SCANNER_COOLDOWN_SECONDS` — per-ticket cooldown: PM skips re-firing on a ticket whose last PM run completed within this window. Prevents wasteful re-triage loops on tickets intentionally left in a no-progress state (e.g. blocked on upstream dependencies). Default: `7200` (2 hours).

## Unblock trigger

Independently of the PM scanner cadence, the webhook handler also schedules a fast PM re-triage when a ticket **transitions into Ready For Delivery / Approved For Delivery / Done**. It walks the just-landed ticket's outgoing `relations` for `type="blocks"` and enqueues a fresh PM task for each dependent still in a PM intake state. Each enqueue uses a distinct `dedup_key` so it does not collide with the regular scanner, and respects `PM_SCANNER_COOLDOWN_SECONDS` so it cannot double-fire with a recent PM run on the same dependent ticket.

This closes the gap where a dependent ticket would otherwise wait up to one scanner cycle (default 15 min) before being re-evaluated after its blocker lands. See `docs/LINEAR_TICKET_WORKFLOWS.md` for full details.

**Telegram gateway** (`/dashboard`, `/trigger`): set **`FAW_WEBHOOK_SERVER_URL`** to the maestro webhook base URL if it is not on the same host as the gateway (default `http://localhost:8000`).

## Local Pipeline Run

```bash
source venv/bin/activate
python maestro/pipeline_orchestrator.py
```

The orchestrator loads roles from `maestro/sdlc_roles.yaml`.

Optional override:

```bash
export SDLC_ROLES_PATH=/absolute/path/to/roles.yaml
python maestro/pipeline_orchestrator.py
```

## PostgreSQL (Docker)

The webhook queue uses PostgreSQL (`FAW_DB_URL`). Local dev container (port **5433** by default so it does not collide with other Postgres on 5432). To use another host port, set **`MAESTRO_PG_PORT`** before `docker compose up` (see `docker-compose.yml`).

```bash
docker compose -f maestro/docker-compose.yml up -d
```

Wait until healthy (`docker compose -f maestro/docker-compose.yml ps`), then set in `~/.hermes/.env` (or your shell):

```bash
export FAW_DB_URL='postgresql://hermes:hermes_dev_queue@127.0.0.1:5433/hermes_maestro_queue'
```

See `maestro/compose.postgres.env.example`. Tables are created on first webhook server startup.

### Migrate from another PostgreSQL (e.g. old `localhost:5432`)

If queue data already lives in PostgreSQL on **5432** (or any other URL) and you want to **replace** the data in the new queue database (e.g. Docker on **5433**), use **`maestro/scripts/migrate_queue_pg.py`** (PostgreSQL → PostgreSQL only; not for SQLite `agent_state.db`).

```bash
source venv/bin/activate
pip install 'psycopg2-binary>=2.9,<3'   # if not already installed via hermes-agent[web]

export HERMES_QUEUE_MIGRATE_SOURCE='postgresql://USER:PASS@127.0.0.1:5432/SOURCE_DB'
export FAW_DB_URL='postgresql://hermes:hermes_dev_queue@127.0.0.1:5433/hermes_maestro_queue'

python maestro/scripts/migrate_queue_pg.py --dry-run   # optional: only print row counts
python maestro/scripts/migrate_queue_pg.py
```

The script **truncates** `locks`, `retries`, and `agent_tasks` on the **target**, then copies all rows from the source and resets the `agent_tasks` id sequence. It refuses to run if source and target resolve to the same host/port/database.

You can pass URLs explicitly instead of env vars: `python maestro/scripts/migrate_queue_pg.py --source '...' --target '...'`. Override target only with `HERMES_QUEUE_MIGRATE_TARGET=...` if you do not want to touch `FAW_DB_URL`.

Stop / remove data volume when you want a clean DB:

```bash
docker compose -f maestro/docker-compose.yml down -v
```

## Webhook Server Run

Start server:

```bash
source venv/bin/activate
pip install 'hermes-agent[web]'   # FastAPI, uvicorn, psycopg2-binary (once per venv)
uvicorn maestro.webhook_server:app --host 0.0.0.0 --port 8000
```

Health check:

```bash
curl http://localhost:8000/health
```

Linear webhook endpoint:

- `POST /linear-webhook`

Queue/debug endpoints:

- `GET /agent-queue` — all active rows + recent terminal (default when unfiltered)
- `GET /agent-queue/by-ticket/FAW-49` — history rows for one ticket id
- `GET /agent-queue/db-snapshot` — `current_database`, row counts, `pg_is_in_recovery` (diagnose read-replica / empty DB)
- `GET /agent-queue?role=Developer&state=queued&limit=20` — filtered slice (`id` desc)
- `GET /agent-lock/<ticket_id>`
- `POST /trigger-agent` (manual Product Manager / Developer / Reviewer / QA trigger)

If using Cloudflare tunnel, point Linear webhook URL to:

- `https://<your-tunnel-host>/linear-webhook`

Webhook logs (auto-rotating):

- `maestro/logs/webhook_server.log`
- Rotation policy: 5 MB per file, keep 5 backups

## Notes

- Workflow semantics (state → agent, PM scanner, PostgreSQL queue): **`docs/LINEAR_TICKET_WORKFLOWS.md`** at repository root (path from here: `../docs/LINEAR_TICKET_WORKFLOWS.md`).
- Default Git integration branch for SDLC agents is **`develop`** (not `main`); see `maestro/sdlc_roles.yaml` `common_system` and role prompts.
- State-to-agent mapping is in `maestro/webhook_server.py` (`STATE_AGENT_MAPPING`).
- Webhook state matching is **case-insensitive** and ignores extra whitespace (Linear titles vary, e.g. `Ready for QA`).
- Role prompts/toolsets are in `maestro/sdlc_roles.yaml`.
- For Product Manager auto-assignment, set `LINEAR_BOT_USER_ID` to the bot user's Linear id (UUID).
- Product Manager can triage both AI-Ready and non-AI-Ready intake tickets, but only AI-Ready tickets may be moved to **In Progress** to start implementation.
- Linear **Triage** is loose raw intake. PM classifies screenshots, brief bug reports, enquiries, opinions, and suggestions, then consolidates engineering-worthy reports into backlog-ready tickets before Developer handoff.
- The PM scanner can proactively pick up intake tickets even if a webhook was missed or the ticket existed before the server started.
