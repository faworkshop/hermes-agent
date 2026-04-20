# Maestro SDLC Automation

This directory contains the project-specific SDLC automation setup:

- `pipeline_orchestrator.py` — local end-to-end simulator (PM -> Dev -> Reviewer -> QA)
- `webhook_server.py` — FastAPI endpoint for Linear webhooks (`/linear-webhook`)
- `sdlc_roles.yaml` — role definitions (prompts, toolsets, circuit-breaker targets)

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

## Webhook Server Run

Start server:

```bash
source venv/bin/activate
uvicorn maestro.webhook_server:app --host 0.0.0.0 --port 8000
```

Health check:

```bash
curl http://localhost:8000/health
```

Linear webhook endpoint:

- `POST /linear-webhook`

If using Cloudflare tunnel, point Linear webhook URL to:

- `https://<your-tunnel-host>/linear-webhook`

Webhook logs (auto-rotating):

- `maestro/logs/webhook_server.log`
- Rotation policy: 5 MB per file, keep 5 backups

## Notes

- Default Git integration branch for SDLC agents is **`develop`** (not `main`); see `maestro/sdlc_roles.yaml` `common_system` and role prompts.
- State-to-agent mapping is in `maestro/webhook_server.py` (`STATE_AGENT_MAPPING`).
- Webhook state matching is **case-insensitive** and ignores extra whitespace (Linear titles vary, e.g. `Ready for QA`).
- Role prompts/toolsets are in `maestro/sdlc_roles.yaml`.
- For Product Manager auto-assignment, set `LINEAR_BOT_USER_ID` to the bot user's Linear id (UUID).
