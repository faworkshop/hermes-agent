# Linear tickets, workflows, and SDLC agents

The canonical guide for this topic lives at the repository root:

**[`docs/LINEAR_TICKET_WORKFLOWS.md`](../../docs/LINEAR_TICKET_WORKFLOWS.md)**

That document covers Linear ↔ maestro mapping, **`ConcurrencyManager`** / **PostgreSQL** (`FAW_DB_URL`), the PM scanner (with cooldown), the per-role active-ticket caps, the **unblock trigger** for dependent tickets when a blocker lands, the mandatory blocker re-verification rule, the frontend design-file gate, circuit breaker behavior, and related configuration.

For **running the webhook**, database setup, and env-var controls (`PM_SCANNER_*`, `HERMES_MAX_ACTIVE_*`, `HERMES_WATCHDOG_*`), see **`maestro/README.md`**.
