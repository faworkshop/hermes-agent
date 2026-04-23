import os
import hmac
import hashlib
import json
import logging
import asyncio
import requests
from pathlib import Path
from dotenv import load_dotenv

# Load GITHUB_TOKEN and other env vars from ~/.hermes/.env
load_dotenv(Path.home() / ".hermes" / ".env")

from logging.handlers import RotatingFileHandler
from typing import Dict, Any

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks

from agent.concurrency import ConcurrencyManager
from run_agent import AIAgent

try:
    from .pipeline_orchestrator import (
        _hermes_model_and_runtime,
        _normalize_minimax_runtime_if_no_anthropic_sdk,
        load_sdlc_config,
    )
except ImportError:
    from pipeline_orchestrator import (  # type: ignore
        _hermes_model_and_runtime,
        _normalize_minimax_runtime_if_no_anthropic_sdk,
        load_sdlc_config,
    )

logger = logging.getLogger("webhook_server")
logger.setLevel(logging.INFO)

_log_dir = Path(__file__).resolve().parent / "logs"
_log_dir.mkdir(parents=True, exist_ok=True)
WEBHOOK_LOG_PATH = _log_dir / "webhook_server.log"
_log_fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

if not logger.handlers:
    _stream = logging.StreamHandler()
    _stream.setFormatter(_log_fmt)
    logger.addHandler(_stream)
    _file = RotatingFileHandler(
        WEBHOOK_LOG_PATH,
        maxBytes=5_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    _file.setFormatter(_log_fmt)
    logger.addHandler(_file)

app = FastAPI(title="Hermes Webhook Server")
cm = ConcurrencyManager()

LINEAR_WEBHOOK_SECRET=os.getenv("LINEAR_WEBHOOK_SECRET") or os.getenv("LINEAR_HMAC_SECRET")
LINEAR_BOT_USER_ID = os.getenv("LINEAR_BOT_USER_ID")
LINEAR_API_KEY = os.getenv("LINEAR_API_KEY", "")
LINEAR_API_URL = "https://api.linear.app/graphql"
NEEDS_HUMAN_LABEL_ID = "331b7988-b4b2-4116-853a-ced489f5eb5f"


def _linear_gql(query: str, variables: Dict[str, Any] = None) -> Dict[str, Any]:
    """Execute a GraphQL mutation/query against the Linear API. Returns the data or {} on error."""
    if not LINEAR_API_KEY:
        return {}
    try:
        resp = requests.post(
            LINEAR_API_URL,
            headers={"Content-Type": "application/json", "Authorization": LINEAR_API_KEY},
            json={"query": query, "variables": variables or {}},
            timeout=10,
        )
        resp.raise_for_status()
        result = resp.json()
        if "errors" in result:
            logger.error("Linear API errors: %s", result["errors"])
            return {}
        return result.get("data", {})
    except Exception as e:
        logger.error("Linear API request failed: %s", e)
        return {}

# Linear state titles are user-defined; casing/spacing can differ from our canonical labels
# (e.g. "Ready for QA" vs "Ready For QA"). Lookups use a normalized key.
_STATE_AGENT_MAPPING_RAW: Dict[str, str] = {
    "Backlog": "Product Manager",
    "New": "Product Manager",
    "Todo": "Product Manager",
    "Unstarted": "Product Manager",
    "Triage": "Product Manager",
    "In Progress": "Developer",
    "In Review": "Reviewer",
    "Ready For QA": "QA",
}
# Intentionally unmapped (no agent webhook): Ready For Delivery, Approved For Delivery, Done —
# see sdlc_roles.yaml / common_system. QA ends automation at Ready For Delivery; humans advance
# Approved For Delivery and Done (production).


def _normalize_linear_state_name(name: str) -> str:
    """Collapse whitespace and compare case-insensitively (Linear UI casing varies)."""
    return " ".join((name or "").split()).casefold()


_STATE_AGENT_BY_NORMALIZED = {
    _normalize_linear_state_name(k): v for k, v in _STATE_AGENT_MAPPING_RAW.items()
}

# Public alias (canonical labels) for operators extending the map.
STATE_AGENT_MAPPING = dict(_STATE_AGENT_MAPPING_RAW)

ROLE_TOOLSETS = {
    "Product Manager": ["linear"],
    "Developer": ["linear", "github", "terminal", "file"],
    "Reviewer": ["linear", "github"],
    "QA": ["linear", "github", "terminal"],
}


def _issue_has_label(ticket_id: str, label_name: str) -> bool:
    """Return True when the Linear issue has the given label (case-insensitive)."""
    if not ticket_id or not label_name:
        return False
    query = """
    query IssueLabelsByIdentifier($id: String!) {
      issue(id: $id) {
        labels { nodes { name } }
      }
    }
    """
    data = _linear_gql(query, {"id": ticket_id})
    issue = (data or {}).get("issue") or {}
    labels = issue.get("labels", {}).get("nodes", []) or []
    want = label_name.strip().casefold()
    return any(((lbl.get("name") or "").strip().casefold() == want) for lbl in labels)

def _add_needs_human_label(ticket_id: str, blocked_role: str) -> None:
    """Add the needs-human label to a ticket. Idempotent — no error if already present."""
    # Fetch current labels and add needs-human if not already there
    query = """
    query IssueLabels($id: String!) {
      issue(id: $id) {
        labels { nodes { id } }
        team { labels { nodes { id name } } }
      }
    }
    """
    data = _linear_gql(query, {"id": ticket_id})
    if not data:
        return

    issue_labels = data.get("issue", {})
    current_ids = [l["id"] for l in issue_labels.get("labels", {}).get("nodes", [])]
    if NEEDS_HUMAN_LABEL_ID in current_ids:
        logger.info("Ticket %s already has needs-human label", ticket_id)
        return

    # Resolve label by name if needed (use hardcoded ID since we know it)
    team_labels = issue_labels.get("team", {}).get("labels", {}).get("nodes", [])
    label_id = NEEDS_HUMAN_LABEL_ID
    for lbl in team_labels:
        if lbl["name"].lower() == "needs-human":
            label_id = lbl["id"]
            break

    new_ids = current_ids + [label_id]
    mutation = """
    mutation IssueUpdate($id: String!, $input: IssueUpdateInput!) {
      issueUpdate(id: $id, input: $input) { success }
    }
    """
    result = _linear_gql(mutation, {"id": ticket_id, "input": {"labelIds": new_ids}})
    if result:
        logger.info("Added needs-human label to ticket %s", ticket_id)


def _post_locked_comment(ticket_id: str, blocked_role: str, attempted_state: str) -> None:
    """Post a comment explaining why the transition was blocked."""
    body = (
        f"⚠️ **Transition Blocked — Agent Conflict**\n\n"
        f"A `{attempted_state}` transition was attempted for this ticket, but a "
        f"`{blocked_role}` agent is currently in progress and holds the lock.\n\n"
        f"The development operation has been aborted and `needs-human` label applied. "
        f"Please resolve the conflict manually:\n\n"
        f"- If the in-progress agent is still working, wait for it to finish.\n"
        f"- If the agent is stuck, manually release the lock or move the ticket as appropriate."
    )
    mutation = """
    mutation CommentCreate($input: CommentCreateInput!) {
      commentCreate(input: $input) { success comment { id } }
    }
    """
    _linear_gql(mutation, {"input": {"issueId": ticket_id, "body": body}})
    logger.info("Posted lock-conflict comment on ticket %s", ticket_id)


def verify_linear_signature(body: bytes, signature: str) -> bool:
    if not LINEAR_WEBHOOK_SECRET:
        return True
    computed_signature = hmac.new(
        LINEAR_WEBHOOK_SECRET.encode(),
        body,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(computed_signature, signature)


async def run_agent_task(role: str, ticket_id: str, prompt: str, session_id: str = None):
    if role in ["Developer", "Reviewer", "QA"]:
        retries = cm.get_retry_count(ticket_id)
        if retries >= 3:
            logger.error(f"🚨 Circuit Breaker Tripped for {ticket_id}. Max retries reached.")
            return

    # Lock already acquired in the foreground before this background task was queued.
    # Skip the duplicate acquire here — just release it when done.
    # (We still record the role so release_lock knows who to release.)

    logger.info(f"Starting {role} Agent for {ticket_id}")

    # Load system message and toolsets from sdlc_roles.yaml
    cfg = load_sdlc_config()
    common = (cfg.get("common_system") or "").strip()
    role_step = next((s for s in cfg.get("pipeline", []) if s.get("agent_key") == role), None)
    role_system = ((role_step or {}).get("system") or "").strip() if role_step else ""
    system_message = f"{common}\n\n{role_system}".strip() if common else role_system

    # Fallback toolsets from hardcoded map if not in YAML
    if role_step and role_step.get("toolsets"):
        enabled_toolsets = role_step["toolsets"]
    else:
        enabled_toolsets = ROLE_TOOLSETS.get(role, ["linear"])

    try:
        model, rt = _hermes_model_and_runtime()
        rt = _normalize_minimax_runtime_if_no_anthropic_sdk(rt)
        agent = AIAgent(
            model=model,
            api_key=rt.get("api_key"),
            base_url=rt.get("base_url"),
            provider=rt.get("provider"),
            api_mode=rt.get("api_mode"),
            acp_command=rt.get("command"),
            acp_args=list(rt.get("args") or []),
            credential_pool=rt.get("credential_pool"),
            enabled_toolsets=enabled_toolsets,
            quiet_mode=False,
            session_id=session_id,
        )
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: agent.run_conversation(user_message=prompt, system_message=system_message),
        )
    except Exception as e:
        logger.error(f"Error running {role} Agent for {ticket_id}: {e}")
    finally:
        cm.release_lock(ticket_id, role)


def _log_ignored(reason: str, **ctx: Any) -> Dict[str, str]:
    logger.info("Linear webhook ignored: %s (%s)", reason, ctx)
    return {"status": "ignored", "reason": reason}


@app.post("/linear-webhook")
async def linear_webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.body()
    signature = request.headers.get("linear-signature")

    if signature and not verify_linear_signature(body, signature):
        logger.warning("Linear webhook rejected: invalid linear-signature")
        raise HTTPException(status_code=403, detail="Invalid signature")

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        logger.warning("Linear webhook rejected: body is not valid JSON")
        raise HTTPException(status_code=400, detail="Invalid JSON")

    action = payload.get("action")
    data = payload.get("data", {}) or {}
    type_ = payload.get("type")
    ticket_id = data.get("identifier") if isinstance(data, dict) else None
    state_name = (data.get("state") or {}).get("name") if isinstance(data.get("state"), dict) else None

    logger.info(
        "Linear webhook received: type=%r action=%r identifier=%r state=%r",
        type_,
        action,
        ticket_id,
        state_name,
    )

    if type_ != "Issue":
        return _log_ignored("Not an Issue event", type=type_)
    if not ticket_id:
        return _log_ignored("No ticket identifier")

    # When an agent calls linear_update_status to move a ticket back to "In Progress"
    # (e.g. Reviewer rejecting a PR, QA failing a test), the webhook fires with
    # actor=bot+action=update. We still dispatch the next agent — but skip the
    # lock-check for backward transitions since the prior agent has finished and
    # Developer must be allowed to pick up the ticket again.
    # NOTE: We no longer blanket-ignore bot actions — the state mapping determines
    # which agent should run, including forward transitions like Developer->"In Review".
    actor_id = payload.get("actor", {}).get("id")
    backward_transition_to_in_progress = (
        action == "update"
        and state_name
        and _normalize_linear_state_name(state_name) == "in progress"
    )

    new_state = state_name
    if not new_state:
        updated_from = payload.get("updatedFrom") or {}
        if isinstance(updated_from, dict) and "stateId" in updated_from:
            return _log_ignored(
                "State id changed but state name missing in payload",
                updated_from=updated_from,
            )
        return _log_ignored("No state on payload and no state change", action=action)

    state_key = _normalize_linear_state_name(new_state)
    role = _STATE_AGENT_BY_NORMALIZED.get(state_key)
    if not role:
        return _log_ignored(
            f"No agent mapped to state: {new_state}",
            state=new_state,
            normalized=state_key,
        )
    if role in {"Product Manager", "Developer", "Reviewer", "QA"} and not _issue_has_label(ticket_id, "AI-Ready"):
        return _log_ignored(
            f"{role} dispatch skipped: missing required 'AI-Ready' label",
            ticket=ticket_id,
            state=new_state,
            role=role,
        )

    # Forward transitions (new work) require the lock to prevent concurrent agents.
    # Backward transitions (to In Progress) skip the lock-check so Developer can pick up.
    # We track BOTH role AND session_id — the session_id is generated here in the
    # foreground (before background dispatch) so the lock is held by a known session.
    # Only the SAME session can re-acquire for the same role. Different sessions block.
    import uuid
    session_id = f"{ticket_id}_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    current_lock = cm.get_lock(ticket_id)
    if not backward_transition_to_in_progress:
        if current_lock and current_lock.get("role") != role:
            # Lock held by a DIFFERENT agent. For forward transitions (e.g. Reviewer->QA),
            # atomically release the old lock and acquire for the new agent — the prior
            # agent is finishing its run and the Linear webhook fires before it releases.
            # For true concurrent conflict (two different agents for same state), block.
            logger.info(
                "Agent %s for %s: releasing lock held by '%s' and acquiring for '%s' "
                "(forward transition from %s's state to %s's state).",
                role, ticket_id, current_lock.get("role"), role,
                current_lock.get("role"), role,
            )
            cm.release_and_acquire(ticket_id, current_lock.get("role"), role, session_id)
        elif current_lock and current_lock.get("role") == role:
            # Same role holds the lock — but check if it's the SAME session (same process
            # re-triggering its own action) vs a DIFFERENT session trying to dispatch.
            # Only allow re-dispatch if the session_id matches the lock holder.
            lock_session = current_lock.get("session_id")
            if session_id is not None and lock_session is not None and session_id == lock_session:
                # Same session re-triggering — allow (intended: agent calls update_status,
                # its own webhook fires before lock released, it re-acquires its own lock).
                logger.info(
                    "Agent %s for %s already holds the lock (session %s) — "
                    "same session re-trigger, skipping lock-check.",
                    role, ticket_id, session_id,
                )
            else:
                # Different session trying to dispatch while this role is already working —
                # block entirely. This prevents two Reviewer instances from running
                # concurrently when Developer double-fires the same state transition.
                logger.info(
                    "Agent %s for %s: CANNOT DISPATCH — role already held by session '%s', "
                    "incoming session '%s'. Blocking to prevent concurrent %s instances.",
                    role, ticket_id, lock_session, session_id, role,
                )
                return _log_ignored(
                    f"Role '{role}' already locked by session '{lock_session}' — "
                    f"incoming session '{session_id}' is blocked. "
                    f"Ticket must complete current review before re-dispatch.",
                    ticket=ticket_id, role=role, state=new_state,
                )
        else:
            # No lock exists — acquire it
            cm.acquire_lock(ticket_id, role, session_id)

    prompt = f"Ticket {ticket_id} has moved to '{new_state}'. Please perform your duties as {role}."
    logger.info("Linear webhook accepted: ticket=%s state=%r -> agent=%s", ticket_id, new_state, role)
    background_tasks.add_task(run_agent_task, role, ticket_id, prompt, session_id)
    return {"status": "accepted", "agent": role, "ticket": ticket_id}


@app.get("/health")
async def Health():
    return {"status": "ok"}


@app.post("/trigger-agent")
async def trigger_agent(
    request: Request,
    background_tasks: BackgroundTasks = None,
):
    """Manually trigger an SDLC agent without requiring a Linear status change.

    Body (JSON):
        role: Which agent to run — "Developer", "Reviewer", or "QA"
        ticket_id: Linear ticket identifier, e.g. "FAW-26"
        prompt: Optional custom prompt. If omitted, a default is constructed from
                the ticket_id and role.

    Returns:
        {"status": "accepted", "agent": role, "ticket": ticket_id}
        or {"status": "locked", "detail": "..."} if another agent holds the lock.
    """
    try:
        body = await request.body()
        payload = json.loads(body) if body else {}
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    role = (payload.get("role") or "").strip()
    ticket_id = (payload.get("ticket_id") or "").strip()
    prompt = payload.get("prompt")

    valid_roles = {"Developer", "Reviewer", "QA"}
    if role not in valid_roles:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid role '{role}'. Must be one of: {', '.join(sorted(valid_roles))}",
        )

    if not ticket_id:
        raise HTTPException(status_code=400, detail="ticket_id is required")

    # Same lock-acquire pattern as the Linear webhook
    if not cm.acquire_lock(ticket_id, role):
        return {
            "status": "locked",
            "detail": (
                f"Cannot start {role} agent for {ticket_id}: "
                f"ticket is currently locked by an in-progress agent. "
                f"Use DELETE /agent-lock/{ticket_id} to clear the lock first."
            ),
        }

    if not prompt or not prompt.strip():
        prompt = f"Ticket {ticket_id} — please perform your {role} duties."

    logger.info("Manual trigger: role=%r ticket=%r", role, ticket_id)

    # Background task so this endpoint returns immediately
    if background_tasks is None:
        raise HTTPException(status_code=500, detail="BackgroundTasks not available")
    background_tasks.add_task(run_agent_task, role, ticket_id, prompt)

    return {"status": "accepted", "agent": role, "ticket": ticket_id}


@app.delete("/agent-lock/{ticket_id}")
async def clear_agent_lock(ticket_id: str):
    """Force-clear the lock for a ticket. Use this when an agent crashed
    or is stuck and you need to manually restart the pipeline.

    Returns {"status": "cleared", "ticket": ticket_id} or
            {"status": "not_found"} if there was no lock.
    """
    cleared = cm.force_clear(ticket_id)
    if cleared:
        logger.info("Lock force-cleared for %s", ticket_id)
        return {"status": "cleared", "ticket": ticket_id, "role": cleared}
    return {"status": "not_found", "ticket": ticket_id}


@app.get("/agent-lock/{ticket_id}")
async def get_agent_lock(ticket_id: str):
    """Check whether a ticket is currently locked, and if so by which role."""
    lock = cm.get_lock(ticket_id)
    if lock:
        return {"status": "locked", "ticket": ticket_id, "role": lock.get("role"), "acquired_at": lock.get("acquired_at")}
    return {"status": "unlocked", "ticket": ticket_id}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
