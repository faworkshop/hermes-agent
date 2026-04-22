# Linear tickets, workflows, and SDLC agents

This document explains how **Hermes `AIAgent` instances**, **Linear issue workflow states**, and the **maestro webhook / local orchestrator** fit together. It applies to the automation under `maestro/` (not the general-purpose Hermes CLI chat loop unless you enable the same toolsets and prompts).

## Big picture

1. **Linear** is the system of record for work: issues move through workflow states (Backlog → … → Ready For QA → **Ready For Delivery** → *human* **Approved For Delivery** → *human* **Done** / production). Automated agents stop after QA sets **Ready For Delivery**; humans own release sign-off and production **Done**.
2. **A FastAPI webhook server** (`maestro/webhook_server.py`) receives Linear **Issue** webhooks when an issue’s state changes.
3. The server maps the **new state name** to a **role** (Product Manager, Developer, Reviewer, or QA) and starts an **`AIAgent`** run in a background task with role-specific **toolsets** and **system prompts**.
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

## What each agent is for (high level)

Details and mandatory step order live in `sdlc_roles.yaml`. Summary:

- **Product Manager** — Linear-only triage: roadmap context, backlog search, acceptance criteria, priority/labels, then move to **Todo** (human gate) or **In Progress** (kicks off Developer). Treats **Approved For Delivery** as **implementation done** (release-approved); **new triage is always allowed** and those tickets do not block routing other work.
- **Developer** — Linear + GitHub + terminal + file: branch from `develop`, implement, tests, draft PR, CI, then move to **In Review** when ready.
- **Reviewer** — Linear + GitHub: validate PR vs ticket, CI gates, review, approve/merge to `develop`, then **Ready For QA** or back to **In Progress** with feedback.
- **QA** — Linear + GitHub + terminal: verify merged work on `develop`, run checks (including browser/E2E as configured in YAML), post evidence to Linear, then **Ready For Delivery** on success or **In Progress** on failure. **Approved For Delivery** and **Done** are human-only (release approval and production deployment).

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

## Concurrency: ticket locks

`maestro/webhook_server.py` uses **`ConcurrencyManager`** (`agent/concurrency.py`) with a SQLite DB (default **`agent_state.db`** next to the process working directory) to record **one active “owner” role per ticket**.

- **Forward transitions** (e.g. In Review → Ready For QA): if another role still holds the lock, the server can **`release_and_acquire`** so the finishing agent’s webhook races cleanly against lock release.
- **Backward transitions to In Progress** (rejections, QA failures): **lock check is skipped** so the Developer can be scheduled again without deadlock.
- **Stale locks** expire after a timeout (default 30 minutes) so a crashed run does not block forever.

**Manual recovery:** `DELETE /agent-lock/{ticket_id}` clears a stuck lock; `GET /agent-lock/{ticket_id}` inspects it.

## Retries and circuit breaker

For **Developer**, **Reviewer**, and **QA**, `run_agent_task` in `webhook_server.py` **checks** `ConcurrencyManager.get_retry_count(ticket_id)` and aborts if the count is **≥ 3**. The counter lives in the same SQLite DB as locks (`retries` table). Roles subject to this check are listed in **`circuit_breaker_agents`** in `sdlc_roles.yaml`.

`ConcurrencyManager` also implements **`increment_retry`** / **`reset_retries`**, but the webhook server **does not call them** today—so in a stock deployment the count stays at zero unless another process updates it. To enforce automatic backoff after repeated QA/review failures, wire **`increment_retry`** (for example on transitions back to **In Progress**) and **`reset_retries`** when a ticket advances past a milestone.

The **`pipeline_orchestrator`** demo script manipulates an in-memory retry counter only to illustrate the circuit breaker log path.

## Manual agent trigger (without moving Linear)

`POST /trigger-agent` accepts JSON:

- **`role`**: `Developer`, `Reviewer`, or `QA` (not Product Manager in this endpoint).
- **`ticket_id`**: e.g. `FAW-26`.
- **`prompt`**: optional; default prompts the role for that ticket.

The same locking rules apply; conflict returns `{"status": "locked", ...}`.

## Local workflow simulation (no Linear webhooks)

**`maestro/pipeline_orchestrator.py`** can run the **same YAML-defined roles** in process, sequentially, for dry runs or debugging (`simulate_pipeline()` when run as `__main__`). It uses an in-memory lock dict rather than the webhook server’s SQLite path unless you wire them the same way.

This is useful to validate prompts and toolsets without configuring Linear webhooks.

## Operating the webhook server

Entry point: `uvicorn` on **`webhook_server:app`** (default `host=0.0.0.0`, `port=8000` in `__main__`).

- **`POST /linear-webhook`** — Linear Issue webhooks.
- **`GET /health`** — liveness.

Environment loading: the server calls `load_dotenv` on **`~/.hermes/.env`** for keys such as `GITHUB_TOKEN` (see top of `webhook_server.py`).

## Customizing the pipeline

1. Edit **`maestro/sdlc_roles.yaml`**: `common_system`, each `pipeline[]` step (`agent_key`, `system`, `prompt`, `toolsets`, optional `model` / `max_iterations`).
2. Keep Linear **state names** in sync with both **`linear_update_status` calls** in prompts and **`_STATE_AGENT_MAPPING_RAW`** in `webhook_server.py`.
3. For a different repo or product, replace project-specific instructions in the QA/Developer sections (the current YAML references True-Review paths and Playwright/Firebase flows as an example deployment).

## Related material

- **Hermes tool registration**: `tools/linear_tool.py`, discovery via `model_tools.py`, toolset name **`linear`** in `toolsets.py`.
- **True-Review Cursor skills** for human-authored tickets: `maestro/true-review/.cursor/skills/` (`linear-ticket-writer`, `linear-ticket-classifier`, `linear-issue-to-pr`).
