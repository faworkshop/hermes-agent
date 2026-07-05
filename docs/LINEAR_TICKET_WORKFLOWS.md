# Linear tickets, workflows, and SDLC agents

This document explains how **Hermes `AIAgent` instances**, **Linear issue workflow states**, and the **maestro webhook / local orchestrator** fit together. It applies to the automation under `maestro/` (not the general-purpose Hermes CLI chat loop unless you enable the same toolsets and prompts).

For **running the webhook**, database setup, and env vars, see **`maestro/README.md`**.

## Big picture

1. **Linear** is the system of record for work: issues move through workflow states (Backlog → … → Ready For QA → **Ready For Delivery** → *human* **Approved For Delivery** → *human* **Done** / production). Automated agents stop after QA sets **Ready For Delivery**; humans own release sign-off and production **Done**.
2. **A FastAPI webhook server** (`maestro/webhook_server.py`) receives Linear **Issue** webhooks when an issue’s state changes. It also runs a Product Manager intake scanner so missed webhook events or already-existing intake tickets can still be triaged.
3. The server maps the **new state name** to a **role** (Product Manager, Developer, Reviewer, or QA) and queues an **`AIAgent`** run with role-specific **toolsets** and **system prompts**.
4. Each agent uses **Linear tools** (`tools/linear_tool.py`) plus GitHub / terminal / file tools as configured, then typically calls **`linear_update_status`** to move the ticket forward (or back to In Progress). That status change can fire the **next** webhook, which dispatches the **next** role—an event-driven pipeline.

Role text, shared rules, and per-role toolsets are defined in **`maestro/sdlc_roles.yaml`**. You can point the loaders at another file with the environment variable **`SDLC_ROLES_PATH`**.

## State → agent mapping

The webhook maps **Linear workflow state titles** to agents. Matching is **case-insensitive** and **whitespace-normalized** (so `Ready for QA` and `Ready For QA` both work).

| Linear state (examples) | Agent dispatched |
|-------------------------|------------------|
| Backlog, New, Todo, Unstarted, Triage | Product Manager |
| In Progress | Developer |
| In Review | Reviewer |
| Ready For QA | QA |

States **not** in this map are **ignored** (logged, no agent run) — including **Ready For Delivery**, **Approved For Delivery**, and **Done** (by design: QA moves to Ready For Delivery; humans move Approved For Delivery and Done after deploy). Your Linear team’s workflow **must** use state titles that match this map for automated steps (or you extend `_STATE_AGENT_MAPPING_RAW` in `webhook_server.py`).

**Special case — unblock trigger:** when a ticket transitions into **Ready For Delivery**, **Approved For Delivery**, or **Done** as a result of a real state change (`updatedFrom.stateId` present in the webhook payload), the server queries Linear for any tickets that list this one as a blocker (`issue.relations`, filtered to `type="blocks"`) and enqueues a fresh Product Manager run for each dependent ticket that is still in a PM intake state. This bypasses the regular PM scanner cadence so dependent tickets can be promoted to **In Progress** immediately when their blockers land, rather than waiting up to one scanner cycle. See **Unblock trigger** below.

## What each agent is for (high level)

Details and mandatory step order live in `sdlc_roles.yaml`. Summary:

- **Product Manager** — Linear + GitHub triage: roadmap context, backlog search, acceptance criteria, priority/labels, then move to **Todo** (human gate) or, only when the ticket is **AI-Ready**, **In Progress** (kicks off Developer). PM may triage non-AI-Ready tickets but must not start implementation for them. **Triage** is treated as loose raw intake (screenshots, brief bug reports, enquiries, opinions, suggestions); PM classifies and consolidates engineering-worthy reports into backlog-ready tickets before any dev handoff. Treats **Approved For Delivery** as **implementation done** (release-approved); **new triage is always allowed** and those tickets do not block routing other work.
- **Developer** — Linear + GitHub + terminal + file: branch from `develop`, implement, tests, draft PR, CI, then move to **In Review** when ready. **Frontend-scope tickets must reference a Stitch design export** under `design/ui/stitch` in the project repo (HTML / PNG / JPG / WebP / JSON). If the design file is missing for a FE-scope ticket, Developer must abort with `needs-human` + move to `Blocked` so the operator can produce the Stitch export before Developer picks it up again.
- **Reviewer** — Linear + GitHub: validate PR vs ticket, CI gates, review, approve/merge to `develop`, then **Ready For QA** or back to **In Progress** with feedback.
- **QA** — Linear + GitHub + terminal: verify merged work on `develop`, run checks (including browser/E2E as configured in YAML), post evidence to Linear, then **Ready For Delivery** on success or **In Progress** on failure. **Approved For Delivery** and **Done** are human-only (release approval and production deployment). QA must mirror the developer's design-file check: for FE-scope verification, the Stitch export under `design/ui/stitch` is the visual source of truth and the implementation is diffed against it. If the design file is missing at QA time, escalate back to the operator (label `qa-failed`, add `needs-human`, move ticket to **In Progress**) — the design-file gate is the developer's responsibility, but QA flags it if it slipped through.

Across roles, **`linear_update_status` is required to be the last tool call** when a step ends with a transition—so the webhook does not double-fire mid-run and costs stay predictable.

## Hermes integration: tools and configuration

### Linear tools (`tools/linear_tool.py`)

Registered on the **`linear`** toolset when `LINEAR_API_KEY` is set:

| Tool | Purpose |
|------|--------|
| `linear_read_ticket` | Full issue context; accepts **human id** (e.g. `FAW-26`) or **GraphQL issue UUID** |
| `linear_read_comments` | Comment thread |
| `linear_search_tickets` | Recent issues (title/id filter, optional state) |
| `linear_get_projects` | Project / roadmap snapshot |
| `linear_update_status` | Move issue to a state **by exact name** on the team workflow |
| `linear_update_priority` | Priority field |
| `linear_assign_user` | Assignee |
| `linear_add_label` | Label by id or **name** (resolved against team labels) |
| `linear_post_comment` | Activity / audit trail on the issue |

**IDs vs identifiers:** `linear_read_ticket` resolves `TEAM-NUMBER` to Linear’s internal issue `id`. Other mutations (`linear_update_status`, `linear_post_comment`, etc.) take `ticket_id` as supplied—**prefer the `id` returned from `linear_read_ticket`** when the automation only knows the human identifier (webhook payloads use **`identifier`**, e.g. `FAW-26`).

### Model and provider

The webhook server resolves the same **model and provider runtime** as the rest of Hermes via `load_config()` and `resolve_runtime_provider()` (see `maestro/pipeline_orchestrator.py`: `_hermes_model_and_runtime()`).

### Webhook security and Linear API

- **`LINEAR_WEBHOOK_SECRET`** or **`LINEAR_HMAC_SECRET`**: HMAC-SHA256 of the raw body, compared to the `linear-signature` header (`verify_linear_signature`). If unset, signatures are not enforced.
- **`LINEAR_API_KEY`**: used by the server for optional GraphQL helpers (labels/comments on lock conflict paths) and by agents via tools.
- **`LINEAR_BOT_USER_ID`**: present for future filtering; webhook logic does not blanket-ignore bot updates (state mapping drives dispatch, including forward transitions such as Developer → In Review).

## Product Manager intake scanner

Product Manager work is no longer only webhook-driven. The server also runs a configurable scanner that periodically searches recently updated Linear tickets for Product Manager intake states:

- Backlog
- New
- Todo
- Unstarted
- Triage

The scanner enqueues Product Manager tasks for both **AI-Ready** and non-**AI-Ready** tickets. Existing queue deduplication prevents repeated PM runs for the same ticket/state.

Default environment controls:

| Variable | Default | Purpose |
|----------|---------|---------|
| `PM_SCANNER_ENABLED` | `true` | Enable/disable proactive PM intake scanning |
| `PM_SCANNER_INTERVAL_SECONDS` | `900` | Delay between scans |
| `PM_SCANNER_LIMIT` | `50` | Max Linear candidates fetched per scan |
| `PM_SCANNER_COOLDOWN_SECONDS` | `7200` | Per-ticket cooldown: PM skips re-firing on a ticket whose last PM run completed within this window. Prevents wasteful re-triage loops on tickets intentionally parked in PM intake states (e.g. waiting on upstream dependencies, missing AI-Ready label). The unblock trigger also respects this cooldown so a regular scan and an unblock-trigger cannot both fire for the same dependent ticket inside the window. |

PM routing rules:

- Non-AI-Ready tickets may be clarified, labeled, prioritized, consolidated, and moved to **Todo**, but must not move to **In Progress**.
- AI-Ready tickets may move to **In Progress** only after PM has made them actionable enough for Developer.
- Raw **Triage** tickets should be classified first. If they need engineering work, PM posts a structured consolidation comment with problem statement, evidence/source, repro steps when applicable, expected vs actual behavior for bugs, proposed scope, acceptance criteria, duplicate/dependency check, labels, and priority.
- **Mandatory blocker re-verification.** When a ticket declares any blocker (via `inverseRelations` of type `blocks`, or a prior comment that names a blocker ticket), PM must call `linear_read_ticket(<blocker_id>)` for EACH declared blocker to verify its current state from the Linear API. The live read is the single source of truth — prior PM comments and prior scan outputs are stale-by-default and have caused incorrect "still blocked" holds in the past (the canonical example: a ticket held in Todo for nearly an hour because the regular PM scan trusted a 1-hour-old comment saying the blocker was "just promoted to In Progress", when in fact the blocker had landed at Ready For Delivery in the meantime). When a prior comment and a fresh `linear_read_ticket` disagree, the fresh read wins.

## Concurrency: ticket locks and active ticket cap

`maestro/webhook_server.py` uses **`ConcurrencyManager`** (`agent/concurrency.py`) backed by **PostgreSQL**. Connection string is **`FAW_DB_URL`** (required when the webhook server module loads). Tables (`locks`, `retries`, `agent_tasks`) and indexes are **created automatically** on first startup if they do not exist.

**Local development:** use **`docker compose -f maestro/docker-compose.yml up -d`** for a Postgres instance on host port **5433** by default (see **`maestro/README.md`** and **`maestro/compose.postgres.env.example`**). Override the published port with **`MAESTRO_PG_PORT`** if 5433 is taken.

**Migrating queue data** from another PostgreSQL (for example an older server on `localhost:5432`) into a new queue database: use **`maestro/scripts/migrate_queue_pg.py`** (see **`maestro/README.md`** — replaces target `locks` / `retries` / `agent_tasks` after truncate).

- **Forward transitions** (e.g. In Review → Ready For QA): if another role still holds the lock, the server can **`release_and_acquire`** so the finishing agent’s webhook races cleanly against lock release.
- **Backward transitions to In Progress** (rejections, QA failures): **lock check is skipped** so the Developer can be scheduled again without deadlock.
- **Stale locks** expire after a timeout (default 30 minutes) so a crashed run does not block forever.
- **Global active-ticket cap** defaults to **1** via `HERMES_MAX_ACTIVE_TICKETS=1`. Queue workers may accumulate many queued tasks, but only one distinct ticket should be actively locked/running at a time. This avoids codebase conflicts between concurrent tickets.
- **PM is isolated from the global cap** via `HERMES_MAX_ACTIVE_PM_TICKETS` (default `5`). PM triage is cheap (single-digit seconds of LLM work) and must not be starved by a long-running Developer/Reviewer/QA task that holds the single global slot. `_agent_worker` selects the right counter based on the role: PM uses the PM cap; Dev/Reviewer/QA use the global cap.

**Manual recovery:** `DELETE /agent-lock/{ticket_id}` clears a stuck lock; `GET /agent-lock/{ticket_id}` inspects it.

## Unblock trigger

When a ticket lands in **Ready For Delivery**, **Approved For Delivery**, or **Done** (real transitions only — the webhook payload must include `updatedFrom.stateId`), the webhook server does the following **asynchronously** so the standard webhook response stays fast:

1. Resolves the just-landed ticket's UUID from the payload.
2. Calls `issue(id).relations(first: 50)` on Linear and filters to `type="blocks"`. Each result is a ticket that this blocker is **blocking** (a dependent).
3. For each dependent ticket that is currently in a **PM intake state** (`Backlog`, `New`, `Todo`, `Unstarted`, `Triage`):
   - skips if it has the `needs-human` label
   - skips if `last_completed_task_for_role(dep_id, "Product Manager", max_age_seconds=PM_SCANNER_COOLDOWN_SECONDS)` returns a recent PM run (cooldown)
   - enqueues a fresh PM task via `ConcurrencyManager.enqueue_task` with `dedup_key = "{dep_id}:Product Manager:unblock-{blocker_id}-{epoch}"` so the unblock-triggered PM run is distinguishable from regular scanner runs and cannot dedup-collide with them
4. The PM prompt for an unblock-triggered run is a focused message: re-triage the dependent now that its blocker has shipped, with explicit reminders to re-verify blocker state live (per the **Mandatory blocker re-verification** rule above) and to NOT add `AI-Ready` themselves.

This closes the gap where a dependent ticket would otherwise wait up to one scanner cycle (default 15 min) before being re-evaluated after its blocker landed. Combined with the **Mandatory blocker re-verification** rule, the system has two layers of defense: (a) the unblock trigger fires immediately on blocker landing, and (b) when regular PM runs do happen, they trust live Linear state, not stale comments.

The trigger is implemented in `webhook_server.py` as `_find_dependent_tickets(blocker_uuid)` and `_trigger_unblock_pm(blocker_ticket_id, blocker_uuid)`. The helper is scheduled with `asyncio.create_task(...)` so the webhook response returns without waiting for the dependent lookup to complete.

## Retries and circuit breaker

For **Developer**, **Reviewer**, and **QA**, `run_agent_task` in `webhook_server.py` **checks** `ConcurrencyManager.get_retry_count(ticket_id)` and aborts if the count is **≥ 3**. The counter lives in the same PostgreSQL database as locks and tasks (`retries` table). Roles subject to this check are listed in **`circuit_breaker_agents`** in `sdlc_roles.yaml`.

`ConcurrencyManager` also implements **`increment_retry`** / **`reset_retries`**, but the webhook server **does not call them** today—so in a stock deployment the count stays at zero unless another process updates it. To enforce automatic backoff after repeated QA/review failures, wire **`increment_retry`** (for example on transitions back to **In Progress**) and **`reset_retries`** when a ticket advances past a milestone.

The **`pipeline_orchestrator`** demo script manipulates an in-memory retry counter only to illustrate the circuit breaker log path.

## Manual agent trigger (without moving Linear)

`POST /trigger-agent` accepts JSON:

- **`role`**: `Product Manager`, `Developer`, `Reviewer`, or `QA`.
- **`ticket_id`**: e.g. `FAW-26`.
- **`prompt`**: optional; default prompts the role for that ticket.

The same locking rules apply; conflict returns `{"status": "locked", ...}`.

## Local workflow simulation (no Linear webhooks)

**`maestro/pipeline_orchestrator.py`** can run the **same YAML-defined roles** in process, sequentially, for dry runs or debugging (`simulate_pipeline()` when run as `__main__`). It uses an in-memory lock dict rather than the webhook server’s **PostgreSQL** queue unless you wire them the same way.

This is useful to validate prompts and toolsets without configuring Linear webhooks.

## Operating the webhook server

Entry point: `uvicorn` on **`maestro.webhook_server:app`** (see **`maestro/README.md`** for the exact command).

- **`POST /linear-webhook`** — Linear Issue webhooks.
- **`GET /health`** — liveness.

**Dependencies:** install **`hermes-agent[web]`** (FastAPI, uvicorn, **psycopg2-binary**) in the virtualenv used to run the server.

**Environment:** the server calls `load_dotenv` on **`~/.hermes/.env`** for keys such as `GITHUB_TOKEN`, `LINEAR_API_KEY`, and **`FAW_DB_URL`** (see top of `webhook_server.py`).

## Customizing the pipeline

1. Edit **`maestro/sdlc_roles.yaml`**: `common_system`, each `pipeline[]` step (`agent_key`, `system`, `prompt`, `toolsets`, optional `model` / `max_iterations`).
2. Keep Linear **state names** in sync with both **`linear_update_status` calls** in prompts and **`_STATE_AGENT_MAPPING_RAW`** in `webhook_server.py`.
3. For a different repo or product, replace project-specific instructions in the QA/Developer sections (the current YAML references True-Review paths and Playwright/Firebase flows as an example deployment).

## Related material

- **Maestro operator guide**: `maestro/README.md` (Postgres Docker, `FAW_DB_URL`, queue endpoints, env-var controls including `PM_SCANNER_COOLDOWN_SECONDS` and `HERMES_MAX_ACTIVE_PM_TICKETS`).
- **Hermes tool registration**: `tools/linear_tool.py`, discovery via `model_tools.py`, toolset name **`linear`** in `toolsets.py`.
- **True-Review Cursor skills** for human-authored tickets: `maestro/true-review/.cursor/skills/` (`linear-ticket-writer`, `linear-ticket-classifier`, `linear-issue-to-pr`).
