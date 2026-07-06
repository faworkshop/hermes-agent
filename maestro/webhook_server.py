import os
import re
import hmac
import hashlib
import json
import logging
import asyncio
import time
import uuid
import requests
from pathlib import Path
from dotenv import load_dotenv

# Load GITHUB_TOKEN and other env vars from ~/.hermes/.env
load_dotenv(Path.home() / ".hermes" / ".env")

from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from typing import Dict, Any, Optional, Callable

from fastapi import FastAPI, Request, HTTPException, Query

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


def _log_queue_backend_at_startup() -> None:
    """Log that the agent queue uses PostgreSQL (``FAW_DB_URL``)."""
    logger.info(
        "Agent queue backend: PostgreSQL (FAW_DB_URL is set; credentials not logged)."
    )
    try:
        snap = cm.queue_debug_snapshot()
        logger.info(
            "Agent queue DB snapshot: database=%r agent_tasks_count=%s max_id=%s pg_is_in_recovery=%s",
            snap.get("current_database"),
            snap.get("agent_tasks_count"),
            snap.get("agent_tasks_max_id"),
            snap.get("pg_is_in_recovery"),
        )
    except Exception as exc:
        logger.warning("Agent queue DB snapshot failed: %s", exc, exc_info=True)


def _normalize_linear_ticket_id(raw: object) -> str | None:
    """Normalize Linear ``identifier`` (e.g. strip, unify unicode hyphens)."""
    if not isinstance(raw, str):
        return None
    s = raw.strip()
    if not s:
        return None
    for bad, good in (
        ("\u2011", "-"),  # NON-BREAKING HYPHEN
        ("\u2010", "-"),  # HYPHEN
        ("\u2212", "-"),  # MINUS SIGN
    ):
        s = s.replace(bad, good)
    s = " ".join(s.split())
    return s or None


def _pg_queue_error_note(exc: BaseException) -> str:
    """Extra context for psycopg2 errors in queue workers."""
    mod = getattr(type(exc), "__module__", "") or ""
    if "psycopg2" not in mod:
        return ""
    return f" pg_context type={type(exc).__name__} cwd={os.getcwd()!r}"


_worker_tasks: dict[str, asyncio.Task] = {}
_ci_poll_task: asyncio.Task | None = None
_pm_scanner_task: asyncio.Task | None = None
_watchdog_task: asyncio.Task | None = None
_worker_stop_event = asyncio.Event()
_STALE_RUNNING_SECONDS = float(os.getenv("HERMES_QUEUE_STALE_RUNNING_SECONDS", "1800"))
# Watch-dog: aggressively reaps stuck tasks based on heartbeat staleness.
# Runs independently of agent workers so a frozen worker still gets cleaned.
# Default 60s poll × 900s (15min) stale threshold = ~15min max zombie lifetime.
_WATCHDOG_POLL_SECONDS = float(os.getenv("HERMES_WATCHDOG_POLL_SECONDS", "60"))
_WATCHDOG_STALE_SECONDS = float(os.getenv("HERMES_WATCHDOG_STALE_SECONDS", "900"))
_MAX_ACTIVE_TICKETS = int(os.getenv("HERMES_MAX_ACTIVE_TICKETS", "1"))
# PM triage is cheap (12-90s) and must not be starved by dev/reviewer/qa runs that
# hold the single global slot. Product Manager runs against its own counter so
# scanning can proceed concurrently with active implementation/verification work.
_MAX_ACTIVE_PM_TICKETS = int(os.getenv("HERMES_MAX_ACTIVE_PM_TICKETS", "5"))
_PM_SCANNER_ENABLED = os.getenv("PM_SCANNER_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}
_PM_SCANNER_INTERVAL_SECONDS = float(os.getenv("PM_SCANNER_INTERVAL_SECONDS", "900"))
_PM_SCANNER_LIMIT = int(os.getenv("PM_SCANNER_LIMIT", "50"))
# Skip re-firing PM on a ticket whose most recent PM run completed within
# the last N seconds. Prevents wasteful re-triage loops on tickets where the
# PM has already triaged and intentionally left the ticket in a no-progress
# state (e.g. blocked on upstream dependencies). Override with env if needed.
_PM_SCANNER_COOLDOWN_SECONDS = float(os.getenv("PM_SCANNER_COOLDOWN_SECONDS", "7200"))

LINEAR_WEBHOOK_SECRET=os.getenv("LINEAR_WEBHOOK_SECRET") or os.getenv("LINEAR_HMAC_SECRET")
LINEAR_BOT_USER_ID = os.getenv("LINEAR_BOT_USER_ID")
LINEAR_API_KEY=os.getenv("LINEAR_API_KEY", "")
LINEAR_API_URL = "https://api.linear.app/graphql"
NEEDS_HUMAN_LABEL_ID = "331b7988-b4b2-4116-853a-ced489f5eb5f"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", os.getenv("GH_TOKEN", ""))
GITHUB_API_URL = "https://api.github.com"
CI_POLL_INTERVAL_SECONDS = float(os.getenv("CI_POLL_INTERVAL_SECONDS", "60"))
CI_POLL_TIMEOUT_SECONDS = float(os.getenv("CI_POLL_TIMEOUT_SECONDS", "7200"))

# Tickets currently waiting for CI to complete. Key = ticket_id.
# Value = {"pr_number": int, "repo": str, "owner": str, "added_at": float, "branch": str,
#          "in_progress_state_id": str}
_ci_polling_tickets: Dict[str, dict] = {}
_ci_poll_lock = asyncio.Lock()


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
    "CI in Progress": "CI-Poll",  # Server-side polling only; no agent dispatched
    "In Review": "Reviewer",
    "Ready For QA": "QA",
}
# Intentionally unmapped (no agent webhook): Blocked, Ready For Delivery, Approved For Delivery, Done —
# see sdlc_roles.yaml / common_system. Blocked is used when the queue marks a run failed; humans
# unblock. QA ends automation at Ready For Delivery; humans advance Approved For Delivery and Done.


def _normalize_linear_state_name(name: str) -> str:
    """Collapse whitespace and compare case-insensitively (Linear UI casing varies)."""
    return " ".join((name or "").split()).casefold()


# CI poller only touches GitHub / comments while the issue stays in this workflow state.
CI_POLL_ALLOWED_STATE_KEY = _normalize_linear_state_name("CI in Progress")


_STATE_AGENT_BY_NORMALIZED = {
    _normalize_linear_state_name(k): v for k, v in _STATE_AGENT_MAPPING_RAW.items()
}

# Public alias (canonical labels) for operators extending the map.
STATE_AGENT_MAPPING = dict(_STATE_AGENT_MAPPING_RAW)
_PM_INTAKE_STATES = {
    _normalize_linear_state_name(k)
    for k, v in _STATE_AGENT_MAPPING_RAW.items()
    if v == "Product Manager"
}


# ── Unblock trigger ─────────────────────────────────────────────────────
# When a ticket transitions to a state where its work is "landed"
# (Ready For Delivery / Approved For Delivery / Done), find any tickets
# that had this ticket as a blocker and re-enqueue PM for them so the
# dev agent queue doesn't idle waiting for the next scanner cycle.
_UNBLOCK_TRIGGER_STATES = {
    _normalize_linear_state_name("Ready For Delivery"),
    _normalize_linear_state_name("Approved For Delivery"),
    _normalize_linear_state_name("Done"),
}

# Hold strong references to background asyncio.Tasks so they are not
# garbage-collected before the event loop gets to schedule them.
# Without this, ``asyncio.create_task(_trigger_unblock_pm(...))`` from
# the sync webhook handler would log "scheduled" and then be dropped
# silently before the coroutine ever executes — this is a documented
# asyncio footgun. We add on create and remove on completion.
_background_tasks: set[asyncio.Task] = set()


def _schedule_background_task(coro) -> asyncio.Task:
    """Create an asyncio Task and retain a strong reference until it finishes.

    Use this for fire-and-forget background work scheduled from sync code
    paths (or async handlers that return immediately afterwards). Without
    holding the reference, the task can be garbage-collected before it
    runs, and the coroutine body never executes.
    """
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


def _find_dependent_tickets(blocker_id: str) -> list[dict[str, Any]]:
    """Return Linear issues that the given blocker is blocking.

    Walks the blocker's outgoing ``relations`` (this issue is the source of
    the relation) and filters to ``type="blocks"`` to find tickets that
    list this issue as a blocker. Returns a list of issue dicts with at
    least ``id``, ``identifier``, ``state``, ``labels``.

    Note: in Linear's data model, ``issue.relations`` lists relations where
    THIS issue is the source (i.e. tickets that this issue points to).
    So when PTD-38 has ``blocks → PTD-39``, that means PTD-38 blocks PTD-39,
    and PTD-39 is a dependent of PTD-38.
    """
    query = """
    query FindDependents($blockerId: String!) {
      issue(id: $blockerId) {
        relations(first: 50) {
          nodes {
            type
            relatedIssue {
              id
              identifier
              state { name }
              labels(first: 25) { nodes { name } }
            }
          }
        }
      }
    }
    """
    data = _linear_gql(query, {"blockerId": blocker_id})
    issue = (data or {}).get("issue") or {}
    rels = (issue.get("relations") or {}).get("nodes") or []
    dependents: list[dict[str, Any]] = []
    for rel in rels:
        if rel.get("type") != "blocks":
            continue
        dep = rel.get("relatedIssue") or {}
        if dep.get("identifier"):
            dependents.append(dep)
    return dependents


async def _trigger_unblock_pm(blocker_ticket_id: str, blocker_uuid: str) -> int:
    """Find tickets blocked by ``blocker_ticket_id`` and enqueue PM for each.

    Returns the count of PM tasks newly enqueued. Each enqueue uses a
    distinct dedup_key so it does not collide with the regular PM scanner
    and so a duplicate webhook does not create a duplicate PM run.
    """
    if not blocker_uuid:
        logger.warning(
            "Unblock trigger: no UUID for blocker=%s — skipping dependent lookup",
            blocker_ticket_id,
        )
        return 0
    try:
        dependents = _find_dependent_tickets(blocker_uuid)
    except Exception as exc:  # pragma: no cover — defensive
        logger.warning("Unblock trigger: dependent lookup failed: %s", exc)
        return 0
    if not dependents:
        logger.info(
            "Unblock trigger: no dependents found for blocker=%s", blocker_ticket_id
        )
        return 0

    queued = 0
    unblock_ts = int(time.time())
    for dep in dependents:
        dep_id = (dep.get("identifier") or "").strip()
        if not dep_id:
            continue
        # Only act on tickets still in PM intake states (Todo/Backlog/Triage/New).
        # Tickets already In Progress / In Review / Done are not blocked-on-us.
        dep_state = ((dep.get("state") or {}).get("name") or "").strip()
        if _normalize_linear_state_name(dep_state) not in _PM_INTAKE_STATES:
            logger.debug(
                "Unblock trigger: skipping %s (state=%s is not PM intake)",
                dep_id, dep_state,
            )
            continue
        # Skip if the dependent already has needs-human (operator escalation pending)
        labels = (dep.get("labels") or {}).get("nodes") or []
        if any(
            ((lbl.get("name") or "").strip().casefold() == "needs-human")
            for lbl in labels
        ):
            logger.debug(
                "Unblock trigger: skipping %s (has needs-human label)", dep_id
            )
            continue
        # Skip if a recent PM run already touched this ticket within the cooldown
        # window — avoids double-firing with the regular scanner cycle.
        last_done = cm.last_completed_task_for_role(
            dep_id, "Product Manager",
            max_age_seconds=_PM_SCANNER_COOLDOWN_SECONDS,
        )
        if last_done is not None:
            logger.debug(
                "Unblock trigger: skipping %s (PM cooldown active, last run task_id=%s)",
                dep_id, last_done["id"],
            )
            continue

        prompt = (
            f"Ticket {dep_id} just had its blocker {blocker_ticket_id} ship to "
            f"Ready For Delivery (or further). Re-triage {dep_id} NOW — if it has "
            f"AI-Ready and no remaining blockers, promote to In Progress so Developer "
            f"can pick it up without waiting for the next scanner cycle. "
            f"Do NOT add AI-Ready yourself; that remains human-only."
        )
        dedup_key = f"{dep_id}:Product Manager:unblock-{blocker_ticket_id}-{unblock_ts}"
        try:
            created, task_row = cm.enqueue_task(
                dep_id,
                "Product Manager",
                prompt,
                dedup_key=dedup_key,
                source_state=_normalize_linear_state_name(dep_state),
            )
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning("Unblock trigger: enqueue failed for %s: %s", dep_id, exc)
            continue
        if created:
            queued += 1
            logger.info(
                "Unblock trigger: queued PM for dependent=%s (blocker=%s) task_id=%s",
                dep_id, blocker_ticket_id, (task_row or {}).get("id"),
            )
    return queued


ROLE_TOOLSETS = {
    "Product Manager": ["linear"],
    "Developer": ["linear", "github", "terminal", "file"],
    "Reviewer": ["linear", "github"],
    "QA": ["linear", "github", "terminal"],
}


def _linear_issue_uuid_from_payload(data: dict) -> str | None:
    """Return Linear GraphQL issue UUID from webhook ``data`` (``id`` field), if present."""
    raw = (data.get("id") or "").strip() if isinstance(data, dict) else ""
    return raw or None


def _linear_issue_uuid_from_identifier(identifier: str) -> str | None:
    """Resolve issue UUID from human identifier (e.g. ``FAW-34``) via Linear API."""
    if not identifier or not LINEAR_API_KEY:
        return None
    m = re.match(r"^([A-Za-z0-9]+)-(\d+)$", identifier.strip())
    if not m:
        return None
    team_key, num_str = m.group(1), m.group(2)
    try:
        num = float(num_str)
    except ValueError:
        return None
    query = """
    query IssueByTeamNumber($teamKey: String!, $number: Float!) {
      issues(filter: { team: { key: { eq: $teamKey } }, number: { eq: $number } }, first: 1) {
        nodes { id }
      }
    }
    """
    gql_data = _linear_gql(query, {"teamKey": team_key, "number": num})
    nodes = (gql_data or {}).get("issues", {}).get("nodes") or []
    if not nodes:
        return None
    out = (nodes[0].get("id") or "").strip()
    return out or None


def _linear_issue_uuid_for_api(data: dict, identifier: str | None) -> str | None:
    """Issue UUID for GraphQL ``issue(id:)`` — never use human identifier as ``id``."""
    u = _linear_issue_uuid_from_payload(data)
    if u:
        return u
    if identifier:
        return _linear_issue_uuid_from_identifier(identifier)
    return None


def _issue_has_label(linear_issue_uuid: str, label_name: str) -> bool:
    """Return True when the Linear issue has the given label (case-insensitive).

    ``linear_issue_uuid`` must be the Linear issue **UUID** (webhook ``data.id``).
    GraphQL ``issue(id:)`` does not accept team identifiers like ``FAW-34``.
    """
    if not linear_issue_uuid or not label_name:
        return False
    query = """
    query IssueLabelsById($id: String!) {
      issue(id: $id) {
        labels { nodes { name } }
      }
    }
    """
    data = _linear_gql(query, {"id": linear_issue_uuid})
    issue = (data or {}).get("issue") or {}
    labels = issue.get("labels", {}).get("nodes", []) or []
    want = label_name.strip().casefold()
    return any(((lbl.get("name") or "").strip().casefold() == want) for lbl in labels)


def _find_pm_intake_candidates(limit: int = 50) -> list[dict[str, Any]]:
    """Return Linear issues in PM-owned intake states.

    PM is allowed to triage both AI-Ready and non-AI-Ready tickets. The agent
    rules prevent non-AI-Ready tickets from moving into implementation.
    This scanner intentionally over-fetches and filters states locally to avoid
    depending on Linear state-name filter edge cases.
    """
    if not LINEAR_API_KEY:
        return []
    query = """
    query PMIntakeCandidates($first: Int!) {
      issues(first: $first, orderBy: updatedAt) {
        nodes {
          id
          identifier
          state { name }
          labels { nodes { name } }
        }
      }
    }
    """
    data = _linear_gql(query, {"first": max(1, min(int(limit), 250))})
    nodes = (data or {}).get("issues", {}).get("nodes") or []
    candidates: list[dict[str, Any]] = []
    for issue in nodes:
        state_name = ((issue.get("state") or {}).get("name") or "").strip()
        if _normalize_linear_state_name(state_name) not in _PM_INTAKE_STATES:
            continue
        candidates.append(issue)
    return candidates

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


_MAX_TASK_LAST_ERROR_LEN = 4000


@dataclass(frozen=True)
class AgentTaskRunResult:
    """Outcome of ``run_agent_task`` for ``complete_task`` / requeue bookkeeping."""

    outcome: str  # success | failed | requeued_after_timeout | ci_redirect
    error: Optional[str] = None

    def terminal_error_for_task(self) -> Optional[str]:
        """Error string to persist on ``agent_tasks.last_error`` (failed runs only)."""
        if self.outcome == "success":
            return None
        if self.error:
            msg = self.error.strip()
            if len(msg) > _MAX_TASK_LAST_ERROR_LEN:
                return msg[:_MAX_TASK_LAST_ERROR_LEN] + "…(truncated)"
            return msg
        return "Agent run failed"


async def run_agent_task(
    role: str, ticket_id: str, prompt: str, session_id: str = None,
    hard_timeout_seconds: float = None,
    heartbeat_callback: Optional[Callable[[], None]] = None,
    heartbeat_interval_seconds: float = 60.0,
) -> AgentTaskRunResult:
    if role in ["Developer", "Reviewer", "QA"]:
        retries = cm.get_retry_count(ticket_id)
        if retries >= 3:
            logger.error(f"Circuit Breaker Tripped for {ticket_id}. Max retries reached.")
            return AgentTaskRunResult(
                "failed",
                f"Circuit breaker: ticket {ticket_id} has retry count >= 3 ({retries})",
            )

    logger.info(f"Starting {role} Agent for {ticket_id} (timeout=%ss)", hard_timeout_seconds)

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
        future = loop.run_in_executor(
            None,
            lambda: agent.run_conversation(user_message=prompt, system_message=system_message),
        )
        heartbeat_task: Optional[asyncio.Task] = None
        if hard_timeout_seconds is not None and hard_timeout_seconds > 0:
            if heartbeat_callback is not None and heartbeat_interval_seconds > 0:
                async def _heartbeat_loop():
                    while True:
                        try:
                            await asyncio.sleep(heartbeat_interval_seconds)
                            try:
                                heartbeat_callback()
                            except Exception as hb_exc:
                                logger.warning(
                                    "Heartbeat callback failed for %s/%s: %s",
                                    role, ticket_id, hb_exc,
                                )
                        except asyncio.CancelledError:
                            return
                        except Exception:
                            logger.exception("Unexpected error in heartbeat loop")
                heartbeat_task = asyncio.create_task(_heartbeat_loop())
            async def _wait_with_heartbeat():
                await asyncio.wait_for(future, timeout=hard_timeout_seconds)
            try:
                await _wait_with_heartbeat()
            except asyncio.TimeoutError:
                logger.warning(
                    "Hard timeout reached for %s Agent on %s (%.0fs). "
                    "Killing agent and requeueing task.",
                    role, ticket_id, hard_timeout_seconds,
                )
                future.cancel()
                try:
                    await future
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
                # Requeue the task so another worker can pick it up
                active_task = cm.get_task_by_ticket(ticket_id, role)
                if active_task and active_task.get("id"):
                    cm.requeue_task(
                        active_task["id"],
                        delay_seconds=30.0,
                        error=(
                            f"Hard timeout after {hard_timeout_seconds}s — "
                            "agent exceeded allowed runtime (task requeued)"
                        ),
                    )
                    # Caller must NOT call complete_task — row is already ``queued`` again.
                    return AgentTaskRunResult("requeued_after_timeout")
                logger.error(
                    "Hard timeout for %s/%s but no active task row to requeue",
                    role,
                    ticket_id,
                )
                return AgentTaskRunResult(
                    "failed",
                    f"Hard timeout after {hard_timeout_seconds}s (requeue skipped: no active task row)",
                )
        else:
            await future
    except asyncio.CancelledError:
        logger.warning("Agent task cancelled for %s/%s", role, ticket_id)
        raise
    except Exception as e:
        logger.error(f"Error running {role} Agent for {ticket_id}: {e}")
        return AgentTaskRunResult("failed", f"{type(e).__name__}: {e}")
    finally:
        if heartbeat_task is not None and not heartbeat_task.done():
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except (asyncio.CancelledError, Exception):
                pass
        cm.release_lock(ticket_id, role)
    return AgentTaskRunResult("success")


def _queue_roles() -> list[str]:
    cfg = load_sdlc_config()
    roles: list[str] = []
    for step in cfg.get("pipeline", []):
        role = (step.get("agent_key") or "").strip()
        if role and role not in roles:
            roles.append(role)
    return roles


async def _agent_worker(role: str) -> None:
    """Continuously process queued tasks for a single role."""
    logger.info("Starting queue worker for role=%s", role)
    while not _worker_stop_event.is_set():
        try:
            recovered = cm.recover_stale_running_tasks(
                role=role,
                stale_after_seconds=_STALE_RUNNING_SECONDS,
                requeue_delay_seconds=0.0,
            )
            if recovered:
                logger.warning(
                    "Recovered %d stale running task(s) for role=%s (missing lock).",
                    recovered,
                    role,
                )

            worker_session_id = f"{role}_{int(time.time())}_{uuid.uuid4().hex[:6]}"
            # PM has its own throttle — see _MAX_ACTIVE_PM_TICKETS comment above.
            effective_max_active = (
                _MAX_ACTIVE_PM_TICKETS if role == "Product Manager" else _MAX_ACTIVE_TICKETS
            )
            task = cm.claim_next_task(
                role,
                session_id=worker_session_id,
                max_active_tickets=effective_max_active,
                active_timeout_seconds=_STALE_RUNNING_SECONDS,
            )
            if not task:
                await asyncio.sleep(0.8)
                continue

            task_id = int(task["id"])
            ticket_id = str(task["ticket_id"])
            prompt = str(task["prompt"])
            session_id = str(task.get("session_id") or worker_session_id)

            if not cm.acquire_lock(ticket_id, role, session_id=session_id):
                cm.requeue_task(
                    task_id,
                    delay_seconds=20.0,
                    error=f"Ticket lock busy for role={role}, ticket={ticket_id}",
                )
                continue

            try:
                run_result = await run_agent_task(
                    role, ticket_id, prompt, session_id=session_id,
                    hard_timeout_seconds=_STALE_RUNNING_SECONDS,
                    heartbeat_callback=lambda: cm.touch_heartbeat(task_id),
                    heartbeat_interval_seconds=60.0,
                )
                if run_result.outcome == "requeued_after_timeout":
                    logger.info(
                        "Task %s (%s/%s) requeued after hard timeout — skipping complete_task",
                        task_id,
                        role,
                        ticket_id,
                    )
                elif run_result.outcome == "ci_redirect":
                    # CI gate intercepted the dispatch before the agent ran.
                    # Complete as success — the task correctly redirected to CI polling.
                    logger.info(
                        "Task %s (%s/%s) ci_redirect — completing task (CI polling now drives ticket)",
                        task_id,
                        role,
                        ticket_id,
                    )
                    cm.complete_task(task_id, success=True, error=None)
                elif run_result.outcome == "success":
                    cm.complete_task(task_id, success=True, error=None)
                else:
                    err = run_result.terminal_error_for_task() or "Agent run failed"
                    cm.complete_task(task_id, success=False, error=err)
                    try:
                        _notify_linear_agent_blocked_on_queue_failure(
                            ticket_id, role, err, task_id=task_id
                        )
                    except Exception as notify_exc:
                        logger.error(
                            "After queue failure, Linear Blocked notify failed: %s",
                            notify_exc,
                            exc_info=True,
                        )
            except Exception as exc:
                logger.error("Worker %s failed task %s: %s", role, task_id, exc, exc_info=True)
                msg = str(exc).strip()
                if len(msg) > _MAX_TASK_LAST_ERROR_LEN:
                    msg = msg[:_MAX_TASK_LAST_ERROR_LEN] + "…(truncated)"
                cm.complete_task(task_id, success=False, error=msg or type(exc).__name__)
                try:
                    _notify_linear_agent_blocked_on_queue_failure(
                        ticket_id, role, msg or type(exc).__name__, task_id=task_id
                    )
                except Exception as notify_exc:
                    logger.error(
                        "After queue failure, Linear Blocked notify failed: %s",
                        notify_exc,
                        exc_info=True,
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            note = _pg_queue_error_note(exc)
            logger.error(
                "Queue worker loop error for role=%s: %s.%s Backing off and continuing.",
                role,
                exc,
                note,
                exc_info=True,
            )
            await asyncio.sleep(1.5)
    logger.info("Stopped queue worker for role=%s", role)


async def _watchdog_worker() -> None:
    """Proactive zombie reaper.

    Runs independently of agent workers (which only call recover_stale when
    idle/idle-spinning). Polls every HERMES_WATCHDOG_POLL_SECONDS and reaps any
    'running' task whose heartbeat is stale by HERMES_WATCHDOG_STALE_SECONDS.

    Cuts zombie lifetime from "until the next worker idle-spin" (~hours if a
    worker is busy) to "watchdog poll × 1" (~60s + 900s = 15min worst case).

    Recovered tasks are returned to 'queued' (not 'failed') so the worker can
    retry — same behavior as agent_worker.recover_stale_running_tasks.
    """
    logger.info(
        "Starting watchdog worker (poll=%.0fs, stale=%.0fs)",
        _WATCHDOG_POLL_SECONDS, _WATCHDOG_STALE_SECONDS,
    )
    while not _worker_stop_event.is_set():
        try:
            # Run recovery for ALL roles in one shot (no role filter)
            recovered = cm.recover_stale_running_tasks(
                role=None,
                stale_after_seconds=_WATCHDOG_STALE_SECONDS,
                requeue_delay_seconds=2.0,  # small delay to avoid hot-loop on same task
            )
            if recovered:
                logger.warning(
                    "Watchdog recovered %d stale running task(s) (stale_after=%.0fs)",
                    recovered, _WATCHDOG_STALE_SECONDS,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Watchdog loop error: %s", exc, exc_info=True)

        try:
            await asyncio.wait_for(
                _worker_stop_event.wait(),
                timeout=_WATCHDOG_POLL_SECONDS,
            )
            # stop event set → exit loop
            break
        except asyncio.TimeoutError:
            # normal: poll again
            continue
    logger.info("Stopped watchdog worker")


def _github_headers() -> Dict[str, str]:
    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return headers


def _resolve_repo_owner_repo(pr_info: Dict[str, Any] = None, data: Dict[str, Any] = None) -> tuple:
    """Resolve (owner, repo) from PR data or GitHub API."""
    if pr_info:
        repo_url = pr_info.get("head", {}).get("repo", {}).get("full_name", "")
        if repo_url and "/" in repo_url:
            return tuple(repo_url.split("/", 1))

    # Fallback: query GitHub for the repo associated with the Linear team's GitHub label
    # or just use the env-known repo (faworkshop/ptdashboard)
    return (os.getenv("GITHUB_REPO_OWNER", "faworkshop"),
            os.getenv("GITHUB_REPO_NAME", "ptdashboard"))


def _find_pr_by_branch(branch: str) -> Dict[str, Any]:
    """Find an open PR whose head branch matches the given branch name.
    Returns {} if not found."""
    owner, repo = _resolve_repo_owner_repo()
    # Search all open PRs — the head branch matches ours
    data = _github_get(f"/repos/{owner}/{repo}/pulls?state=open&head={owner}:{branch}")
    if isinstance(data, list) and len(data) > 0:
        return data[0]
    return {}


def _find_branch_for_ticket(ticket_id: str) -> str:
    """Try to find the branch name for a ticket.

    Strategy:
    1. Search repo branches for one whose name contains the ticket ID
       (e.g. feature/FAW-34-final matches FAW-34).
    2. If no branch found, search open PR head branches for the ticket ID.
       The PR title/branch often contains the ticket ID even when the local
       branch has been rebased away from the default branch list.

    Returns the branch name or '' if not found.
    """
    import re
    owner, repo = _resolve_repo_owner_repo()

    # 1. Search branches
    data = _github_get(f"/repos/{owner}/{repo}/branches")
    if isinstance(data, list):
        escaped_id = re.escape(ticket_id)
        pattern = re.compile(rf"(?:^|[/])({escaped_id})(?:-|$)", re.IGNORECASE)
        for branch in data:
            name = branch.get("name", "")
            if pattern.search(name):
                return name

    # 2. Fallback: search open PR head branches for the ticket ID
    prs = _github_get(f"/repos/{owner}/{repo}/pulls?state=open")
    if isinstance(prs, list):
        for pr in prs:
            head_ref = pr.get("head", {}).get("ref", "")
            if ticket_id.lower() in head_ref.lower():
                return head_ref

    return ""


def _github_get(fpath: str) -> Dict[str, Any]:
    """GET against the GitHub API. Returns {} on error."""
    if not GITHUB_TOKEN:
        return {}
    try:
        resp = requests.get(
            GITHUB_API_URL + fpath,
            headers=_github_headers(),
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning("GitHub API GET %s failed: %s", fpath, e)
        return {}


def _github_post(fpath: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """POST against the GitHub API. Returns {} on error.

    Reserved for future use. CI-poll's DRAFT-PROMOTE GATE uses
    ``_promote_pr_to_ready`` directly (GraphQL mutation) instead, because
    the REST draft-toggle endpoint silently no-ops in the
    ready_for_review direction (verified Jul 6 2026).
    """
    if not GITHUB_TOKEN:
        return {}
    try:
        resp = requests.post(
            GITHUB_API_URL + fpath,
            headers=_github_headers(),
            json=payload or {},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning("GitHub API POST %s failed: %s", fpath, e)
        return {}


def _promote_pr_to_ready(owner: str, repo: str, pr_number: int) -> bool:
    """Auto-promote a draft PR to ready_for_review.

    Used by the CI-poll DRAFT-PROMOTE GATE (PTD-43/PTD-44 case, Jul 6 2026).
    Developer exits after pushing the commit and moving the ticket to
    'CI in Progress' — they never get the chance to call step 7a's
    ``ready_for_review=True`` because CI is still running when they exit.
    When CI passes, the CI-poll fires this helper so the PR leaves draft
    state in lockstep with the ticket leaving 'CI in Progress'.

    Implementation note: GitHub confirmed (community discussion 70061,
    Jul 6 2026) that the REST ``PATCH /pulls/{n} {"draft": false}``
    silently no-ops on some PRs (returns 200 but ``draft`` stays True).
    The reliable path is the GraphQL mutation
    ``markPullRequestReadyForReview(input: {pullRequestId: "PR_..."})``.
    ``gh pr ready`` uses this same mutation under the hood.

    Returns True on success, False on any error (caller leaves the ticket
    in 'CI in Progress' for the next Developer dispatch to retry).
    """
    if not GITHUB_TOKEN:
        logger.warning(
            "Cannot promote PR #%d — no GITHUB_TOKEN configured", pr_number,
        )
        return False
    # Step 1: resolve PR number → node ID via REST (GET /pulls/{n} returns
    # the node_id field as ``id``).
    pr_data = _github_get(f"/repos/{owner}/{repo}/pulls/{pr_number}")
    pr_node_id = pr_data.get("node_id") if pr_data else None
    if not pr_node_id:
        logger.warning(
            "Cannot resolve PR #%d node_id for %s/%s", pr_number, owner, repo,
        )
        return False
    # Step 2: GraphQL mutation markPullRequestReadyForReview.
    graphql_url = "https://api.github.com/graphql"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/vnd.github+json",
    }
    mutation = (
        'mutation MarkReady($id: ID!) { '
        'markPullRequestReadyForReview(input: {pullRequestId: $id}) { '
        'pullRequest { id isDraft } '
        '} }'
    )
    try:
        resp = requests.post(
            graphql_url,
            headers=headers,
            json={"query": mutation, "variables": {"id": pr_node_id}},
            timeout=15,
        )
        resp.raise_for_status()
        body = resp.json()
    except Exception as e:
        logger.warning(
            "Promote PR #%d: GraphQL markPullRequestReadyForReview failed: %s",
            pr_number, e,
        )
        return False
    # GraphQL errors come back as ``errors`` even with 200; treat as failure.
    if body.get("errors"):
        logger.warning(
            "Promote PR #%d: GraphQL errors: %s",
            pr_number, body["errors"],
        )
        return False
    pr = (body.get("data") or {}).get("markPullRequestReadyForReview") or {}
    inner = pr.get("pullRequest") or {}
    if inner.get("isDraft") is False:
        return True
    logger.warning(
        "Promote PR #%d: mutation succeeded but isDraft=%r",
        pr_number, inner.get("isDraft"),
    )
    return False


def _get_ci_status_for_pr(owner: str, repo: str, pr_number: int) -> Dict[str, Any]:
    """Poll GitHub for CI status on a PR.

    Returns a dict with three top-level CI-state keys so the poller can
    distinguish a still-running check from a real failure:

    * ``status``      - one of ``"passed"``, ``"failed"``, ``"pending"``,
                        ``"no_runs"`` (no check runs reported yet), or
                        ``"error"`` (could not read from GitHub).
    * ``all_passed``  - **back-compat** boolean: ``True`` iff every
                        completed check has a passing conclusion. Kept so
                        older callers that only know the two-state result
                        keep working; new callers should branch on
                        ``status`` directly. ``None`` when the CI status
                        is ``pending`` (semantic difference: the run is
                        still in-flight, NOT a failure).
    * ``runs``        - raw check-runs list from GitHub.
    * ``branch``      - head branch ref.

    The legacy two-state collapse (``False`` for both ``failed`` and
    ``pending``) was the root cause of spurious "CI failed" dispatches
    whenever the poll fired while a check was still in
    ``queued``/``in_progress`` status. The fetch
    ``c.get("conclusion")`` is ``None`` mid-run, which used to make
    ``all_passed`` ``False`` and trigger the developer-failure path.
    """
    data = _github_get(f"/repos/{owner}/{repo}/pulls/{pr_number}")
    if not data:
        return {
            "runs": [],
            "all_passed": None,
            "status": "error",
            "error": "no response",
        }

    head_sha = data.get("head", {}).get("sha", "")
    branch = data.get("head", {}).get("ref", "")
    # Capture the draft flag now while we have the PR payload, so the
    # CI-poll DRAFT-PROMOTE GATE can act on it without an extra round trip.
    is_draft = bool(data.get("draft", False))

    # Get check runs for the head SHA
    check_data = _github_get(f"/repos/{owner}/{repo}/commits/{head_sha}/check-runs")
    if not check_data:
        return {
            "runs": [],
            "all_passed": None,
            "status": "error",
            "error": "no check runs",
            "branch": branch,
            "is_draft": is_draft,
        }

    runs = check_data.get("check_runs", []) or []
    if not runs:
        return {
            "runs": [],
            "all_passed": None,
            "status": "no_runs",
            "branch": branch,
            "is_draft": is_draft,
        }

    # Tally per-run state. GitHub sets ``status`` to ``queued`` /
    # ``in_progress`` while the run is live (conclusion is ``None``);
    # and ``completed`` once the run has a real ``conclusion``
    # (``success``, ``failure``, ``cancelled``, ``timed_out``, ``skipped``,
    # ``neutral``, etc.). We must NOT treat a live run as a failure.
    pending = any(r.get("status") in ("queued", "in_progress", "waiting",
                                      "pending", "requested")
                  for r in runs)
    failed = any(
        r.get("status") == "completed"
        and r.get("conclusion") not in ("success", "skipped", "neutral")
        for r in runs
    )

    if pending and not failed:
        # Every still-running check is good news for now — keep polling,
        # but do NOT push a developer fix (root-cause was a premature
        # ``all_passed=False`` here).
        return {
            "runs": runs,
            "all_passed": None,
            "status": "pending",
            "branch": branch,
            "is_draft": is_draft,
        }
    if failed:
        failed_names = [
            r.get("name", "?") for r in runs
            if r.get("status") == "completed"
            and r.get("conclusion") not in ("success", "skipped", "neutral")
        ]
        return {
            "runs": runs,
            "all_passed": False,
            "status": "failed",
            "failed_names": failed_names,
            "branch": branch,
            "is_draft": is_draft,
        }

    # All runs completed with passing conclusions.
    return {
        "runs": runs,
        "all_passed": True,
        "status": "passed",
        "branch": branch,
        "is_draft": is_draft,
    }


async def _ci_poll_worker() -> None:
    """Background worker: every CI_POLL_INTERVAL_SECONDS, check each pending ticket's CI status.

    Runs GitHub checks and success/failure handling only while the Linear issue remains
    in ``CI in Progress``; otherwise the ticket is dropped from the poll set.
    When all checks pass, move the ticket to 'In Review' (so Reviewer dispatch fires).
    When CI fails, move the ticket back to 'In Progress' and enqueue Developer.
    When timeout is exceeded, remove the ticket from the polling set."""
    while not _worker_stop_event.is_set():
        try:
            await asyncio.sleep(CI_POLL_INTERVAL_SECONDS)

            async with _ci_poll_lock:
                if not _ci_polling_tickets:
                    continue

                expired = []
                for ticket_id, info in _ci_polling_tickets.items():
                    owner = info["owner"]
                    repo = info["repo"]
                    pr_number = info["pr_number"]
                    added_at = info["added_at"]

                    elapsed = time.time() - added_at
                    if elapsed > CI_POLL_TIMEOUT_SECONDS:
                        expired.append(ticket_id)
                        logger.warning(
                            "CI polling timed out for %s after %ds — removing from poll set",
                            ticket_id,
                            elapsed,
                        )
                        continue

                    linear_state = _get_issue_state_normalized_from_identifier(ticket_id)
                    if linear_state is None:
                        logger.warning(
                            "CI poll %s: could not read Linear state — skipping this cycle",
                            ticket_id,
                        )
                        continue
                    if linear_state != CI_POLL_ALLOWED_STATE_KEY:
                        logger.info(
                            "CI poll %s: not in 'CI in Progress' (state=%r) — stopping poll",
                            ticket_id,
                            linear_state,
                        )
                        expired.append(ticket_id)
                        continue

                    ci = _get_ci_status_for_pr(owner, repo, pr_number)
                    ci_status = ci.get("status")
                    is_draft = ci.get("is_draft")
                    logger.info(
                        "CI poll %s: status=%s all_passed=%s isDraft=%s runs=%d (elapsed=%.0fs)",
                        ticket_id,
                        ci_status,
                        ci.get("all_passed"),
                        is_draft,
                        len(ci.get("runs", [])),
                        elapsed,
                    )

                    if ci_status == "passed":
                        # DRAFT-PROMOTE GATE: Developer left the PR as draft while
                        # CI was running (per persona step 7a — "ready_for_review
                        # AFTER CI is SUCCESS"). But Developer exits before that
                        # final call lands, leaving the PR draft. Reviewer's
                        # step 0d then refuses to act on draft PRs. So when CI
                        # passes, CI-poll must auto-promote the PR out of draft
                        # before moving the ticket to In Review — otherwise the
                        # pipeline strands at "In Review" with a draft PR (the
                        # PTD-43/PTD-44 bug, Jul 6 2026).
                        promoted = True
                        if is_draft:
                            promoted = _promote_pr_to_ready(owner, repo, pr_number)
                            if not promoted:
                                logger.warning(
                                    "CI poll %s: CI passed but PR #%d is still draft and "
                                    "could not auto-promote — leaving ticket in 'CI in "
                                    "Progress' so the next Developer dispatch can re-attempt.",
                                    ticket_id, pr_number,
                                )
                                _post_linear_comment(
                                    ticket_id,
                                    f"⚠️ CI checks passed on branch `{ci.get('branch', 'unknown')}` "
                                    f"but PR #{pr_number} is still a draft and the auto-promote "
                                    f"to `ready_for_review=True` failed. Leaving ticket in "
                                    f"`CI in Progress`; the next Developer dispatch will retry "
                                    f"step 7a (per persona).",
                                )
                                continue  # leave in CI in Progress, do NOT move to In Review
                            logger.info(
                                "CI poll %s: auto-promoted draft PR #%d to ready_for_review",
                                ticket_id, pr_number,
                            )

                        # Move to 'In Review' (NOT 'In Progress'). 'In Review' is the
                        # state that triggers Reviewer dispatch via the
                        # ``_ROLE_FOR_STATE`` mapping (line ~181). Sending the ticket
                        # back to 'In Progress' here stranded it: the Reviewer would
                        # never be auto-dispatched and an operator had to nudge the
                        # ticket manually. See faworkshop CI-pass dispatch fix.
                        state_id = info.get("in_review_state_id") or info.get("in_progress_state_id")
                        target_state = "In Review" if info.get("in_review_state_id") else "In Progress"
                        if state_id:
                            _move_linear_ticket_state(ticket_id, state_id)
                        logger.info("CI PASSED for %s — moved to %s (auto-promoted draft=%s)",
                                    ticket_id, target_state, is_draft)
                        _post_linear_comment(
                            ticket_id,
                            f"✅ CI checks passed on branch `{ci.get('branch', 'unknown')}` — "
                            f"ticket moved to {target_state}."
                            + (" (PR auto-promoted from draft by CI-poll.)" if is_draft else ""),
                        )
                        expired.append(ticket_id)
                    elif ci_status == "failed":
                        # CI failed — move ticket back to In Progress and enqueue
                        # Developer for follow-up. Keep polling so we catch the
                        # re-run when Developer pushes a fix; don't expire yet.
                        failed_names = ci.get("failed_names") or [
                            r.get("name", "?") for r in ci.get("runs", [])
                            if r.get("status") == "completed"
                            and r.get("conclusion") not in ("success", "skipped", "neutral")
                        ]
                        logger.warning(
                            "CI FAILED for %s — moving back to In Progress, enqueuing Developer",
                            ticket_id,
                        )
                        state_id = info.get("in_progress_state_id")
                        if state_id:
                            _move_linear_ticket_state(ticket_id, state_id)
                        _post_linear_comment(
                            ticket_id,
                            f"❌ CI failed on branch `{ci.get('branch', 'unknown')}`: "
                            f"{', '.join(failed_names)}. "
                            f"Ticket moved to In Progress — Developer will claim and fix.",
                        )
                        # Always enqueue Developer for follow-up, even if the Linear
                        # move failed (ticket may already be in In Progress from a
                        # concurrent webhook or manual action).
                        dedup_key = f"{ticket_id}:Developer:in progress"
                        prompt = (
                            f"CI failed for Ticket {ticket_id} on branch `{ci.get('branch', 'unknown')}`: "
                            f"{', '.join(failed_names)}. "
                            f"Please fix the failing tests/checks and push a new commit."
                        )
                        created, _ = cm.enqueue_task(
                            ticket_id,
                            "Developer",
                            prompt,
                            dedup_key=dedup_key,
                            source_state="in progress",
                        )

                        if created:
                            logger.info(
                                "CI failure enqueued Developer for %s (new commit will trigger fresh CI)",
                                ticket_id,
                            )
                        else:
                            logger.info(
                                "CI failure %s: Developer already queued/running — will follow up on next cycle",
                                ticket_id,
                            )
                        # Do NOT expire — keep polling so we catch CI re-run after Developer pushes.
                        # The ticket stays in CI in Progress in Linear while Developer works.
                    elif ci_status == "pending":
                        # CI check is still running (``queued`` / ``in_progress``).
                        # Historically this was mis-classified as ``False`` (the
                        # legacy ``all_passed`` shape) and the developer was
                        # spuriously dispatched. Keep polling and wait.
                        logger.debug(
                            "CI poll %s: still pending — keeping poll, no action",
                            ticket_id,
                        )
                    else:
                        # ``no_runs`` / ``error`` — nothing to act on yet, just
                        # continue polling until GitHub reports the check.
                        logger.debug(
                            "CI poll %s: no conclusion yet (status=%s) — keeping poll",
                            ticket_id,
                            ci_status,
                        )
                for tid in expired:
                    _ci_polling_tickets.pop(tid, None)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            note = _pg_queue_error_note(exc)
            logger.error("CI poll worker loop error: %s%s", exc, note, exc_info=True)
            await asyncio.sleep(2.0)


async def _pm_intake_scanner_worker() -> None:
    """Periodically enqueue missed intake tickets for Product Manager."""
    logger.info(
        "Starting PM intake scanner enabled=%s interval=%ss limit=%s",
        _PM_SCANNER_ENABLED,
        _PM_SCANNER_INTERVAL_SECONDS,
        _PM_SCANNER_LIMIT,
    )
    while not _worker_stop_event.is_set():
        try:
            if not _PM_SCANNER_ENABLED:
                await asyncio.sleep(max(5.0, _PM_SCANNER_INTERVAL_SECONDS))
                continue

            candidates = _find_pm_intake_candidates(_PM_SCANNER_LIMIT)
            queued = 0
            for issue in candidates:
                ticket_id = (issue.get("identifier") or "").strip()
                state_name = ((issue.get("state") or {}).get("name") or "").strip()
                if not ticket_id or not state_name:
                    continue

                state_key = _normalize_linear_state_name(state_name)
                dedup_key = f"{ticket_id}:Product Manager:{state_key}"
                labels = (issue.get("labels") or {}).get("nodes") or []
                has_ai_ready = any(
                    ((lbl.get("name") or "").strip().casefold() == "ai-ready")
                    for lbl in labels
                )
                has_needs_human = any(
                    ((lbl.get("name") or "").strip().casefold() == "needs-human")
                    for lbl in labels
                )
                if has_needs_human:
                    logger.debug("PM scanner skipping %s — has needs-human label", ticket_id)
                    continue
                # Cooldown: avoid re-firing PM on a ticket whose last PM run
                # is still recent (e.g. blocked-on-deps tickets intentionally
                # left in Todo by an earlier PM triage). Without this guard,
                # every scanner cycle would re-enqueue the same ticket
                # indefinitely, wasting tokens and spamming the queue.
                last_done = cm.last_completed_task_for_role(
                    ticket_id, "Product Manager",
                    max_age_seconds=_PM_SCANNER_COOLDOWN_SECONDS,
                )
                if last_done is not None:
                    age = int(time.time() - float(last_done["finished_at"]))
                    logger.debug(
                        "PM scanner skipping %s — last PM run finished %ds ago "
                        "(task_id=%s, state=%s, err=%s). Cooldown=%ds.",
                        ticket_id, age, last_done["id"], last_done["state"],
                        (last_done.get("last_error") or "")[:80],
                        _PM_SCANNER_COOLDOWN_SECONDS,
                    )
                    continue
                prompt = (
                    f"Ticket {ticket_id} is currently in '{state_name}' "
                    f"and {'has' if has_ai_ready else 'does not have'} AI-Ready. "
                    "Please perform your duties as Product Manager. "
                    "Do not move non-AI-Ready tickets to In Progress."
                )
                created, task_row = cm.enqueue_task(
                    ticket_id,
                    "Product Manager",
                    prompt,
                    dedup_key=dedup_key,
                    source_state=state_key,
                )
                if created:
                    queued += 1
                    logger.info(
                        "PM scanner queued ticket=%s state=%r task_id=%s",
                        ticket_id,
                        state_name,
                        task_row.get("id"),
                    )

            if candidates:
                logger.info("PM scanner checked %d candidate(s), queued %d", len(candidates), queued)
            await asyncio.sleep(max(5.0, _PM_SCANNER_INTERVAL_SECONDS))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            note = _pg_queue_error_note(exc)
            logger.error("PM intake scanner loop error: %s%s", exc, note, exc_info=True)
            await asyncio.sleep(2.0)


def _linear_issue_graphql_id(ticket_ref: str) -> str | None:
    """Resolve ``FAW-123``-style identifiers to Linear issue UUID; pass UUIDs through unchanged."""
    s = (ticket_ref or "").strip()
    if not s:
        return None
    if re.match(r"^[A-Za-z0-9]+-\d+$", s):
        return _linear_issue_uuid_from_identifier(s) or None
    return s


def _get_team_workflow_state_id_by_normalized_name(
    issue_uuid: str, normalized_name: str
) -> str | None:
    """Return workflow state id on the issue's team whose name normalizes to ``normalized_name``."""
    query = """
    query IssueTeamStates($id: String!) {
      issue(id: $id) {
        team {
          states {
            nodes { id name }
          }
        }
      }
    }
    """
    data = _linear_gql(query, {"id": issue_uuid})
    nodes = (data.get("issue") or {}).get("team", {}).get("states", {}).get("nodes") or []
    for state in nodes:
        if _normalize_linear_state_name(state.get("name") or "") == normalized_name:
            sid = (state.get("id") or "").strip()
            return sid or None
    return None


def _notify_linear_agent_blocked_on_queue_failure(
    ticket_identifier: str,
    role: str,
    error_text: str,
    *,
    task_id: int | None = None,
) -> None:
    """On terminal queue failure: label ``needs-human``, move to **Blocked** when available, post comment.

    Skipped when ``MAESTRO_SKIP_LINEAR_FAILURE_NOTIFY`` is truthy or ``LINEAR_API_KEY`` is missing.
    """
    skip = os.getenv("MAESTRO_SKIP_LINEAR_FAILURE_NOTIFY", "").strip().lower()
    if skip in {"1", "true", "yes", "on"}:
        logger.info("Skipping Linear Blocked notify (MAESTRO_SKIP_LINEAR_FAILURE_NOTIFY set)")
        return
    if not LINEAR_API_KEY:
        logger.warning("Skipping Linear Blocked notify: no LINEAR_API_KEY")
        return

    issue_uuid = _linear_issue_graphql_id(ticket_identifier)
    if not issue_uuid:
        logger.warning("Skipping Linear Blocked notify: cannot resolve issue id for %r", ticket_identifier)
        return

    blocked_key = _normalize_linear_state_name("Blocked")
    current = _get_issue_state_normalized_from_identifier(ticket_identifier)

    err_trim = (error_text or "").strip()
    if len(err_trim) > 3500:
        err_trim = err_trim[:3500] + "\n…(truncated)"

    task_note = f" (queue task #{task_id})" if task_id is not None else ""
    blocked_sid: str | None = None
    if current != blocked_key:
        blocked_sid = _get_team_workflow_state_id_by_normalized_name(issue_uuid, blocked_key)
        if not blocked_sid:
            logger.warning(
                "No Linear workflow state named 'Blocked' for ticket %s — skipping state move",
                ticket_identifier,
            )

    try:
        moved = False
        if current != blocked_key and blocked_sid:
            moved = bool(_move_linear_ticket_state(issue_uuid, blocked_sid))

        if not _add_linear_label(issue_uuid, "needs-human"):
            logger.warning("needs-human label may not have applied for %s", ticket_identifier)

        if current == blocked_key:
            move_note = "Already in **Blocked**; no state change."
        elif moved:
            move_note = "Moved to **Blocked**."
        elif blocked_sid:
            move_note = (
                "Could not move to **Blocked** (Linear state update failed); "
                "left in current workflow state."
            )
        else:
            move_note = (
                "No **Blocked** state on this team workflow; left in current workflow state."
            )
        body = (
            f"## Queue run failed — needs human\n\n"
            f"**Agent:** `{role}`{task_note}\n\n"
            f"{move_note}\n\n"
            f"**Error:**\n```\n{err_trim}\n```\n\n"
            f"Labeled **needs-human** for triage."
        )
        _post_linear_comment(issue_uuid, body)
        logger.info(
            "Linear Blocked notify for %s role=%s moved=%s",
            ticket_identifier,
            role,
            moved,
        )
    except Exception as exc:
        logger.error(
            "Linear Blocked notify failed for %s: %s",
            ticket_identifier,
            exc,
            exc_info=True,
        )


def _post_linear_comment(ticket_id: str, body: str) -> None:
    """Post a comment to a Linear issue."""
    issue_id = _linear_issue_graphql_id(ticket_id) or ticket_id.strip()
    mutation = """
    mutation CommentCreate($input: CommentCreateInput!) {
      commentCreate(input: $input) { success comment { id } }
    }
    """
    _linear_gql(mutation, {"input": {"issueId": issue_id, "body": body}})


def _add_linear_label(ticket_id: str, label_name: str) -> bool:
    """Add a label to a Linear issue by name. Creates the label if it doesn't exist."""
    issue_graph_id = _linear_issue_graphql_id(ticket_id) or ticket_id.strip()
    # First try to find the label ID by name
    query = """
    query Organization($teamId: String!) {
      team(id: $teamId) {
        issueLabels(first: 100) {
          nodes { id name }
        }
      }
    }
    """
    issue_query = """
    query Issue($id: String!) {
      issue(id: $id) { team { id } }
    }
    """
    issue_result = _linear_gql(issue_query, {"id": issue_graph_id})
    if not issue_result:
        return False
    team_id = issue_result.get("issue", {}).get("team", {}).get("id")
    if not team_id:
        return False

    label_result = _linear_gql(query, {"teamId": team_id})
    if not label_result:
        return False

    label_nodes = label_result.get("team", {}).get("issueLabels", {}).get("nodes", [])
    label_id = None
    for label in label_nodes:
        if label["name"].lower() == label_name.lower():
            label_id = label["id"]
            break

    if not label_id:
        # Create the label
        create_mutation = """
        mutation LabelCreate($teamId: String!, $name: String!) {
          issueLabelCreate(input: {teamId: $teamId, name: $name}) {
            success issueLabel { id }
          }
        }
        """
        create_result = _linear_gql(create_mutation, {"teamId": team_id, "name": label_name})
        if not create_result:
            return False
        label_id = create_result.get("issueLabelCreate", {}).get("issueLabel", {}).get("id")
        if not label_id:
            return False

    # Add label to issue
    add_mutation = """
    mutation IssueAddLabel($id: String!, $labelId: String!) {
      issueAddLabel(id: $id, labelId: $labelId) { success }
    }
    """
    result = _linear_gql(add_mutation, {"id": issue_graph_id, "labelId": label_id})
    if result:
        logger.info("Added label '%s' to ticket %s", label_name, ticket_id)
    return bool(result)


def _move_linear_ticket_state(ticket_id: str, state_id: str) -> bool:
    """Move a Linear issue to a specific state by state ID."""
    issue_graph_id = _linear_issue_graphql_id(ticket_id) or ticket_id.strip()
    mutation = """
    mutation UpdateState($id: String!, $stateId: String!) {
      issueUpdate(id: $id, input: {stateId: $stateId}) { success }
    }
    """
    result = _linear_gql(mutation, {"id": issue_graph_id, "stateId": state_id})
    if result:
        logger.info("Moved ticket %s to state %s", ticket_id, state_id)
        return True
    else:
        logger.error("Failed to move ticket %s to state %s", ticket_id, state_id)
        return False


def _get_issue_state_normalized_from_identifier(identifier: str) -> str | None:
    """Current Linear workflow state name for a ticket identifier, normalized.

    Returns None if the issue UUID cannot be resolved or Linear returns no state.
    """
    issue_uuid = _linear_issue_uuid_from_identifier(identifier)
    if not issue_uuid:
        return None
    query = """
    query IssueState($id: String!) {
      issue(id: $id) {
        state { name }
      }
    }
    """
    data = _linear_gql(query, {"id": issue_uuid})
    name = ((data.get("issue") or {}).get("state") or {}).get("name")
    if not name:
        return None
    return _normalize_linear_state_name(str(name))


def _get_in_progress_state_id(ticket_id: str) -> str | None:
    """Get the Linear state ID for 'In Progress' for a ticket's team.
    Returns None if not found."""
    query = """
    query Issue($id: String!) {
      issue(id: $id) {
        team {
          states {
            nodes { id name }
          }
        }
      }
    }
    """
    data = _linear_gql(query, {"id": ticket_id})
    if not data:
        return None
    nodes = (data.get("issue") or {}).get("team", {}).get("states", {}).get("nodes") or []
    for state in nodes:
        if _normalize_linear_state_name(state.get("name") or "") == "in progress":
            return state["id"]
    return None


def _get_in_review_state_id(ticket_id: str) -> str | None:
    """Get the Linear state ID for 'In Review' for a ticket's team.
    Returns None if the team has no 'In Review' state (e.g. teams whose
    pipeline skips human review).

    Used by the CI-pass branch of ``_ci_poll_worker`` to move the ticket
    into the state that triggers Reviewer dispatch."""
    query = """
    query Issue($id: String!) {
      issue(id: $id) {
        team {
          states {
            nodes { id name }
          }
        }
      }
    }
    """
    data = _linear_gql(query, {"id": ticket_id})
    if not data:
        return None
    nodes = (data.get("issue") or {}).get("team", {}).get("states", {}).get("nodes") or []
    for state in nodes:
        if _normalize_linear_state_name(state.get("name") or "") == "in review":
            return state["id"]
    return None


def start_ci_poll(ticket_id: str, pr_number: int, owner: str, repo: str, branch: str,
                  in_progress_state_id: str = None,
                  in_review_state_id: str = None) -> None:
    """Register a ticket for CI polling. Idempotent — replaces existing entry.

    ``in_progress_state_id`` is the state used when CI FAILS (Developer re-engages).
    ``in_review_state_id`` is the state used when CI PASSES (Reviewer dispatch fires).
    Either may be None if the team lacks that state — fallbacks are applied at the
    move site so an old in-flight poll set can still drain safely."""
    if in_progress_state_id is None:
        in_progress_state_id = _get_in_progress_state_id(ticket_id)
    if in_review_state_id is None:
        in_review_state_id = _get_in_review_state_id(ticket_id)
    _ci_polling_tickets[ticket_id] = {
        "pr_number": pr_number,
        "owner": owner,
        "repo": repo,
        "branch": branch,
        "added_at": time.time(),
        "in_progress_state_id": in_progress_state_id,
        "in_review_state_id": in_review_state_id,
        "failure_comment_posted": False,
    }
    logger.info(
        "Started CI polling for %s (PR #%d, branch=%s, in_progress_state_id=%s, in_review_state_id=%s)",
        ticket_id, pr_number, branch, in_progress_state_id, in_review_state_id,
    )


def stop_ci_poll(ticket_id: str) -> None:
    _ci_polling_tickets.pop(ticket_id, None)


def _redirect_to_ci_in_progress(
    ticket_id: str, pr_number: int, owner: str, repo: str,
    branch: str, data: Dict[str, Any],
) -> None:
    """Move ticket to 'CI in Progress' and start server-side CI polling.

    Called when a Developer moves to 'In Review' before CI has passed.
    Instead of dispatching Reviewer, we redirect the ticket through
    CI polling so it auto-advances when checks eventually pass.
    """
    ci_in_progress_state_id = _get_ci_in_progress_state_id(ticket_id)
    if ci_in_progress_state_id is None:
        logger.warning(
            "Could not resolve 'CI in Progress' state id for %s — "
            "CI polling will still run but state transition may fail.",
            ticket_id,
        )
    else:
        _move_linear_ticket_state(ticket_id, ci_in_progress_state_id)

    # Register for server-side polling regardless of whether the state
    # transition succeeded — we want to poll and log even if Linear fails.
    start_ci_poll(
        ticket_id=ticket_id,
        pr_number=pr_number,
        owner=owner,
        repo=repo,
        branch=branch or "",
        in_progress_state_id=_get_in_progress_state_id(ticket_id),
        in_review_state_id=_get_in_review_state_id(ticket_id),
    )
    logger.info(
        "Redirected %s to CI polling (PR #%d, branch=%s) — "
        "ticket will auto-advance to 'In Review' when CI passes.",
        ticket_id, pr_number, branch,
    )


def _post_pr_draft_blocked_comment(ticket_id: str, pr_number: int) -> None:
    """Post a Linear comment explaining why Reviewer dispatch was blocked.

    Triggered when a Developer agent moves a ticket to 'In Review' but the
    associated GitHub PR is still a draft. The right fix is to mark the PR
    ready for review (or close the draft) — until then the ticket cannot
    advance to the Reviewer stage.
    """
    body = (
        f"⚠️ **Reviewer dispatch blocked** — "
        f"the ticket was moved to 'In Review' but the associated "
        f"[PR #{pr_number}]({_resolve_repo_owner_repo()[0]}/{_resolve_repo_owner_repo()[1]}/pull/{pr_number}) "
        f"is still a **Draft**.\n\n"
        f"\n\n"
        f"### What to do\n\n"
        f"- Review the diff and the CI status on the PR\n"
        f"- When the work is ready, mark the PR *Ready for review* "
        f"(`gh pr ready {pr_number}`) — the next event will re-dispatch Reviewer\n"
        f"- Or close the draft PR if the work isn't ready yet — and move this ticket back to 'In Progress'\n\n"
        f"### Why this happens\n\n"
        f"The pipeline gates Reviewer dispatch on `isDraft == false` so reviewers "
        f"don't get pinged on work-in-progress branches. A draft PR means the work "
        f"isn't yet ready for human/code review, so the ticket stays in its current state."
    )
    try:
        _post_linear_comment(ticket_id, body)
        logger.info(
            "Posted draft-blocked comment on %s (PR #%d)",
            ticket_id, pr_number,
        )
    except Exception as exc:
        logger.warning(
            "Failed to post draft-blocked comment on %s: %s",
            ticket_id, exc,
        )


def _get_ci_in_progress_state_id(ticket_id: str) -> str | None:
    """Resolve the Linear state id for 'CI in Progress'.

    Note: Linear's WorkflowStateFilter only supports a singular ``name``
    StringComparator — there is no plural ``names`` filter. We fetch all
    of the team's states and casefold-match locally, which also tolerates
    casing variants like 'CI in Progress' / 'CI In Progress'.
    """
    query = """
    query GetStates($id: String!) {
      issue(id: $id) {
        team {
          states {
            nodes { id name }
          }
        }
      }
    }
    """
    result = _linear_gql(query, {"id": ticket_id})
    if not result:
        return None
    nodes = (result.get("issue") or {}).get("team", {}).get("states", {}).get("nodes") or []
    for state in nodes:
        name = (state.get("name") or "").strip()
        if name.casefold() == "ci in progress":
            return state["id"]
    return None


@app.on_event("startup")
async def _startup_workers() -> None:
    global _ci_poll_task, _pm_scanner_task, _watchdog_task
    _log_queue_backend_at_startup()
    _worker_stop_event.clear()
    for role in _queue_roles():
        if role in _worker_tasks and not _worker_tasks[role].done():
            continue
        _worker_tasks[role] = asyncio.create_task(_agent_worker(role))
    if _ci_poll_task is None or _ci_poll_task.done():
        _ci_poll_task = asyncio.create_task(_ci_poll_worker())
    if _pm_scanner_task is None or _pm_scanner_task.done():
        _pm_scanner_task = asyncio.create_task(_pm_intake_scanner_worker())
    if _watchdog_task is None or _watchdog_task.done():
        _watchdog_task = asyncio.create_task(_watchdog_worker())


@app.on_event("shutdown")
async def _shutdown_workers() -> None:
    global _ci_poll_task, _pm_scanner_task, _watchdog_task
    _worker_stop_event.set()
    tasks = [t for t in _worker_tasks.values() if not t.done()]
    if _ci_poll_task and not _ci_poll_task.done():
        tasks.append(_ci_poll_task)
    if _pm_scanner_task and not _pm_scanner_task.done():
        tasks.append(_pm_scanner_task)
    if _watchdog_task and not _watchdog_task.done():
        tasks.append(_watchdog_task)
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    _worker_tasks.clear()
    _ci_poll_task = None
    _pm_scanner_task = None
    _watchdog_task = None


def _log_ignored(reason: str, **ctx: Any) -> Dict[str, str]:
    logger.info("Linear webhook ignored: %s (%s)", reason, ctx)
    return {"status": "ignored", "reason": reason}


@app.post("/linear-webhook")
async def linear_webhook(request: Request):
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
    ticket_id = _normalize_linear_ticket_id(data.get("identifier") if isinstance(data, dict) else None)
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

    # ── Unblock trigger: schedule dependent-ticket PM enqueue as a background
    # task so it runs regardless of the standard dispatch path's early-returns.
    # Fire only on real transitions (updatedFrom shows a previous state) and only
    # when entering a "landed" state. We schedule and don't await so the webhook
    # response stays fast.
    if state_key in _UNBLOCK_TRIGGER_STATES:
        updated_from = payload.get("updatedFrom") or {}
        if isinstance(updated_from, dict) and updated_from.get("stateId"):
            blocker_uuid = _linear_issue_uuid_for_api(data, ticket_id)
            if blocker_uuid:
                logger.info(
                    "Unblock trigger scheduled: blocker=%s landed in '%s'",
                    ticket_id, new_state,
                )
                # Use the helper instead of raw asyncio.create_task so the
                # task is held by a strong reference until completion —
                # otherwise the coroutine is silently GC'd before it runs.
                _schedule_background_task(
                    _trigger_unblock_pm(ticket_id, blocker_uuid)
                )
    role = _STATE_AGENT_BY_NORMALIZED.get(state_key)
    if not role:
        return _log_ignored(
            f"No agent mapped to state: {new_state}",
            state=new_state,
            normalized=state_key,
        )

    # ── CI-Poll: ticket moved to "CI in Progress" ───────────────────────────
    # Server-side polling. No agent dispatched. Look up the PR by branch name
    # and start the background CI polling loop.
    if role == "CI-Poll":
        # Extract branch/PR from Linear's pullRequest data if available
        pull_req = data.get("pullRequest") or {}
        branch = pull_req.get("headRefName", "")
        pr_number = pull_req.get("number", None)
        pr_info = {"head": {"ref": branch, "repo": pull_req.get("repository", {})}} if pull_req else {}

        if not branch:
            # Fallback: search GitHub for a PR whose head branch matches the ticket
            branch = _find_branch_for_ticket(ticket_id)
            if branch:
                pr_info = _find_pr_by_branch(branch)
                if pr_info:
                    pr_number = pr_info.get("number")

        if branch and pr_number:
            # Resolve repo owner/name from the PR data or repo label
            owner, repo = _resolve_repo_owner_repo(pr_info=pr_info if pr_info else None, data=data)
            in_progress_state_id = _get_in_progress_state_id(ticket_id)
            in_review_state_id = _get_in_review_state_id(ticket_id)
            start_ci_poll(ticket_id, pr_number, owner, repo, branch,
                          in_progress_state_id=in_progress_state_id,
                          in_review_state_id=in_review_state_id)
            return {
                "status": "ci_polling_started",
                "ticket": ticket_id,
                "pr": pr_number,
                "branch": branch,
            }
        else:
            logger.warning("CI-Poll: could not resolve branch/PR for %s", ticket_id)
            return _log_ignored("CI-Poll: no branch/PR found for ticket", ticket=ticket_id)

    linear_issue_uuid = _linear_issue_uuid_for_api(data, ticket_id)
    if role in {"Developer", "Reviewer", "QA"}:
        if not linear_issue_uuid:
            logger.warning(
                "Cannot resolve Linear issue UUID for identifier=%r — skipping AI-Ready check "
                "would be unsafe; dispatch blocked. Ensure webhook payload includes data.id.",
                ticket_id,
            )
            return _log_ignored(
                "Cannot resolve Linear issue id for label check",
                ticket=ticket_id,
                state=new_state,
                role=role,
            )
        if not _issue_has_label(linear_issue_uuid, "AI-Ready"):
            return _log_ignored(
                f"{role} dispatch skipped: missing required 'AI-Ready' label (or Linear API returned no labels)",
                ticket=ticket_id,
                linear_issue_id=linear_issue_uuid[:8] + "…",
                state=new_state,
                role=role,
            )

    # ── CI Gate: Reviewer must not enter 'In Review' unless CI has passed ───
    # If Developer moved to 'In Review' without waiting for CI, intercept here
    # and redirect to 'CI in Progress' so the server-side poller can drive the
    # ticket forward automatically when CI eventually passes.
    if role == "Reviewer" and new_state == "In Review":
        branch = _find_branch_for_ticket(ticket_id)
        pr_info = _find_pr_by_branch(branch) if branch else {}
        pr_number = pr_info.get("number") if pr_info else None
        owner, repo = _resolve_repo_owner_repo(pr_info=pr_info if pr_info else None, data=data)
        if pr_number:
            # Draft guard: if the PR is still a draft, the agent marked
            # Linear "In Review" prematurely. Don't dispatch a Reviewer;
            # leave the ticket alone and post a comment telling the human
            # (and the developer agent next time it dispatches) what to do.
            if pr_info.get("draft"):
                logger.warning(
                    "Reviewer dispatch for %s blocked: PR #%d is still a draft. "
                    "Mark it ready for review (or close the draft) before the "
                    "ticket can advance to 'In Review'.",
                    ticket_id, pr_number,
                )
                _post_pr_draft_blocked_comment(ticket_id, pr_number)
                return _log_ignored(
                    f"Reviewer dispatch skipped: PR #{pr_number} is still a draft (ticket stays in {new_state} until PR is marked ready for review)",
                    ticket=ticket_id,
                    state=new_state,
                    role=role,
                )
            ci = _get_ci_status_for_pr(owner, repo, pr_number)
            ci_status = ci.get("status")
            if ci_status != "passed":
                logger.warning(
                    "CI not passed for %s (PR #%d, status=%s) — redirecting to 'CI in "
                    "Progress' instead of dispatching Reviewer. CI state: %s",
                    ticket_id, pr_number, ci_status, ci,
                )
                _redirect_to_ci_in_progress(ticket_id, pr_number, owner, repo, branch, data)
                # The task was never dispatched to an agent — it was redirected to CI polling.
                # Complete it as success (it did its job: triggered CI monitoring).
                return {"status": "ci_redirect", "ticket": ticket_id, "pr": pr_number}
            else:
                logger.info(
                    "CI PASSED for %s (PR #%d) — proceeding with Reviewer dispatch.",
                    ticket_id, pr_number,
                )

    dedup_key = f"{ticket_id}:{role}:{state_key}"
    prompt = f"Ticket {ticket_id} has moved to '{new_state}'. Please perform your duties as {role}."
    created, task_row = cm.enqueue_task(
        ticket_id,
        role,
        prompt,
        dedup_key=dedup_key,
        source_state=state_key,
    )
    if not created:
        # Dedup blocked — but check if the existing task is a stale Developer run
        # (agent exited without updating Linear). Revive it so it can finish
        # properly instead of being permanently stuck.
        revived = cm.revive_stale_developer_task(ticket_id, dedup_key, stale_after_seconds=_STALE_RUNNING_SECONDS)
        if revived:
            logger.info(
                "Revived stale Developer task for %s (prior agent exited without updating Linear).",
                ticket_id,
            )
            return {
                "status": "revived",
                "agent": role,
                "ticket": ticket_id,
                "note": "stale Developer task revived",
            }
        return _log_ignored(
            "Duplicate task already queued/running",
            ticket=ticket_id,
            role=role,
            dedup_key=dedup_key,
            task_id=task_row.get("id") if task_row else None,
        )

    try:
        snap = cm.queue_debug_snapshot()
        n = int(snap.get("agent_tasks_count") or 0)
        if n == 0:
            logger.error(
                "Post-enqueue DB visibility FAILED: agent_tasks_count=0 after enqueue "
                "ticket=%s role=%s reported_task_id=%r — row did not persist for this FAW_DB_URL. "
                "Confirm primary DB (not a read-only URL), disk space, and redeploy with "
                "enqueue_task autocommit=False fix.",
                ticket_id,
                role,
                (task_row or {}).get("id"),
            )
    except Exception as exc:
        logger.warning("Post-enqueue snapshot skipped: %s", exc)

    logger.info(
        "Linear webhook queued: ticket=%s state=%r -> agent=%s task_id=%s",
        ticket_id,
        new_state,
        role,
        task_row.get("id"),
    )
    return {"status": "accepted", "agent": role, "ticket": ticket_id, "queued_task_id": task_row.get("id")}


@app.get("/health")
async def Health():
    return {"status": "ok"}


@app.get("/agent-queue")
async def get_agent_queue(
    role: str | None = Query(default=None),
    state: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    terminal_recent: int = Query(
        default=150,
        ge=1,
        le=500,
        description=(
            "When role and state are omitted: max recent done/failed/cancelled rows to append "
            "after all queued+running rows (active tasks always included regardless of id)."
        ),
    ),
):
    """List queue tasks for debugging.

    With **no** ``role`` / ``state`` filters: returns **all** queued and running rows, then
    up to ``terminal_recent`` terminal rows — so low-``id`` active tasks are never hidden
    behind a large history (unlike a plain ``ORDER BY id DESC LIMIT``).
    With filters: returns up to ``limit`` rows matching the filter (``id`` descending).
    """
    if role is None and state is None:
        tasks = cm.list_tasks_for_dashboard(terminal_recent=terminal_recent)
        eff_limit = None
        mode = "dashboard"
    else:
        tasks = cm.list_tasks(role=role, state=state, limit=limit)
        eff_limit = limit
        mode = "filtered"
    counts: dict[str, int] = {}
    for item in tasks:
        st = str(item.get("state") or "unknown")
        counts[st] = counts.get(st, 0) + 1
    return {
        "status": "ok",
        "filters": {
            "role": role,
            "state": state,
            "limit": eff_limit,
            "mode": mode,
            "terminal_recent": terminal_recent if mode == "dashboard" else None,
        },
        "count": len(tasks),
        "counts_in_result": counts,
        "tasks": tasks,
    }


@app.get("/agent-queue/stats")
async def get_agent_queue_stats(
    stale_after_seconds: int = Query(default=900, ge=30, le=86400),
):
    """Get aggregate queue stats and stale-running candidates."""
    stats = cm.get_queue_stats(stale_after_seconds=float(stale_after_seconds))
    return {"status": "ok", **stats}


@app.get("/agent-queue/locks")
async def get_agent_queue_locks():
    """Fetch all active locks from the ConcurrencyManager."""
    locks = cm.get_all_locks_with_age()
    return {
        "status": "ok",
        "count": len(locks),
        "locks": [
            {
                "ticket_id": tid,
                "assignee": assignee,
                "locked_at": lat,
                "age_seconds": age,
            }
            for tid, assignee, lat, age in locks
        ],
    }


@app.get("/agent-queue/db-snapshot")
async def get_agent_queue_db_snapshot():
    """DB identity + row counts (helps debug ``INSERT`` vs ``SELECT`` mismatches)."""
    return {"status": "ok", **cm.queue_debug_snapshot()}


@app.get("/agent-queue/by-ticket/{ticket_id}")
async def get_agent_queue_tasks_for_ticket(ticket_id: str, limit: int = Query(default=50, ge=1, le=200)):
    """All recent queue rows for a ticket identifier (e.g. ``FAW-49``), newest ``id`` first."""
    tid = _normalize_linear_ticket_id(ticket_id) or ticket_id.strip()
    tasks = cm.list_tasks_for_ticket_id(tid, limit=limit)
    return {
        "status": "ok",
        "ticket_id": tid,
        "count": len(tasks),
        "tasks": tasks,
    }


@app.get("/agent-queue/stuck-summary")
async def get_stuck_summary(
    window_hours: int = Query(default=48, ge=1, le=720),
    stale_after_seconds: int = Query(default=900, ge=30, le=86400),
):
    """Taxonomy of stuck/zombie agent tasks.

    Returns counts of failure modes:
      - currently_stuck: tasks currently 'running' with stale heartbeat (true zombies)
      - hard_timeouts: failed tasks with 'Hard timeout' error in window
      - api_hangs: failed tasks with heartbeat-stale error in window (silent API freeze)
      - bogus_short_passes: 'done' tasks with attempts>=2 AND dur<300s in window
      - now_running / now_queued: queue counts
      - by_role_48h: per-role done/failed counts for failure-rate denominator
    """
    now = time.time()
    cutoff_ts = now - window_hours * 3600
    stale_cutoff = now - stale_after_seconds

    import psycopg2
    import psycopg2.extras

    with cm._connect() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute(
            """
            SELECT id, role, ticket_id, attempts,
                   to_timestamp(started_at) AS started,
                   to_timestamp(last_heartbeat_at) AS last_hb,
                   EXTRACT(EPOCH FROM (now() - to_timestamp(COALESCE(last_heartbeat_at, started_at))))::int AS idle_s,
                   COALESCE(left(last_error, 100), '') AS err
            FROM agent_tasks
            WHERE state = 'running'
            ORDER BY started_at
            """
        )
        running_rows = [dict(r) for r in cur.fetchall()]

        cur.execute(
            """
            SELECT id, role, ticket_id, attempts,
                   EXTRACT(EPOCH FROM (now() - to_timestamp(COALESCE(started_at, EXTRACT(EPOCH FROM now())))))::int AS age_s,
                   COALESCE(left(last_error, 100), '') AS err
            FROM agent_tasks
            WHERE state = 'queued'
            ORDER BY id
            """
        )
        queued_rows = [dict(r) for r in cur.fetchall()]

        cur.execute(
            """
            SELECT id, role, ticket_id, attempts,
                   to_timestamp(started_at) AS started,
                   to_timestamp(last_heartbeat_at) AS last_hb,
                   EXTRACT(EPOCH FROM (now() - to_timestamp(COALESCE(last_heartbeat_at, started_at))))::int AS idle_s,
                   COALESCE(left(last_error, 100), '') AS err
            FROM agent_tasks
            WHERE state = 'running'
              AND (
                last_heartbeat_at IS NULL
                OR last_heartbeat_at <= %s
              )
            ORDER BY started_at
            """,
            (stale_cutoff,),
        )
        stuck_rows = [dict(r) for r in cur.fetchall()]

        cur.execute(
            """
            SELECT role, state, COUNT(*) AS n
            FROM agent_tasks
            WHERE started_at >= %s
              AND finished_at IS NOT NULL
            GROUP BY role, state
            ORDER BY role, state
            """,
            (cutoff_ts,),
        )
        per_role: dict[str, dict[str, int]] = {}
        for r in cur.fetchall():
            per_role.setdefault(str(r["role"]), {})[str(r["state"])] = int(r["n"])

        cur.execute(
            """
            SELECT state, COUNT(*) AS n,
                   COALESCE(ROUND(AVG(finished_at - started_at))::int, 0) AS avg_dur_s
            FROM agent_tasks
            WHERE started_at >= %s
              AND finished_at IS NOT NULL
            GROUP BY state
            """,
            (cutoff_ts,),
        )
        state_counts = {str(r["state"]): {"count": int(r["n"]), "avg_dur_s": int(r["avg_dur_s"])} for r in cur.fetchall()}

        cur.execute(
            """
            SELECT COUNT(*) AS n FROM agent_tasks
            WHERE started_at >= %s AND state = 'failed' AND last_error LIKE 'Hard timeout%%'
            """,
            (cutoff_ts,),
        )
        hard_timeouts = int(cur.fetchone()["n"])

        cur.execute(
            """
            SELECT COUNT(*) AS n FROM agent_tasks
            WHERE started_at >= %s AND state = 'failed'
              AND last_error LIKE '%%heartbeat stale%%' AND attempts = 1
            """,
            (cutoff_ts,),
        )
        api_hangs = int(cur.fetchone()["n"])

        cur.execute(
            """
            SELECT COUNT(*) AS n FROM agent_tasks
            WHERE started_at >= %s AND state = 'done'
              AND attempts >= 2 AND (finished_at - started_at) < 300
            """,
            (cutoff_ts,),
        )
        bogus_short_passes = int(cur.fetchone()["n"])

    return {
        "status": "ok",
        "window_hours": window_hours,
        "stale_after_seconds": stale_after_seconds,
        "now_running": len(running_rows),
        "now_queued": len(queued_rows),
        "currently_stuck": stuck_rows,
        "currently_stuck_count": len(stuck_rows),
        "by_state": state_counts,
        "by_role_48h": per_role,
        "failure_modes": {
            "hard_timeouts": hard_timeouts,
            "api_hangs": api_hangs,
            "bogus_short_passes": bogus_short_passes,
        },
        "watchdog": {
            "poll_seconds": _WATCHDOG_POLL_SECONDS,
            "stale_seconds": _WATCHDOG_STALE_SECONDS,
        },
    }


@app.get("/agent-queue/{task_id}")
async def get_agent_queue_task(task_id: int):
    """Fetch one queue task by id."""
    task = cm.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    return {"status": "ok", "task": task}


@app.post("/agent-queue/recover-stale")
async def recover_stale_queue_tasks(
    role: str | None = Query(default=None),
    stale_after_seconds: int = Query(default=900, ge=30, le=86400),
):
    """Manually requeue stale running tasks that no longer have a matching lock."""
    recovered = cm.recover_stale_running_tasks(
        role=role,
        stale_after_seconds=float(stale_after_seconds),
        requeue_delay_seconds=0.0,
    )
    logger.warning(
        "Manual stale queue recovery invoked: role=%s stale_after=%ss recovered=%d",
        role,
        stale_after_seconds,
        recovered,
    )
    return {
        "status": "ok",
        "role": role,
        "stale_after_seconds": stale_after_seconds,
        "recovered": recovered,
    }


@app.post("/ci-poll/restart")
async def restart_ci_poll(request: Request):
    """Re-add a ticket to CI polling. Useful after server restart.

    Body (JSON):
        ticket_id: Linear ticket identifier, e.g. "FAW-42"
        pr_number: Optional PR number. If not provided, searches GitHub.

    Returns:
        {"status": "ok", "ticket": ticket_id, "pr": pr_number}
    """
    try:
        body = await request.body()
        payload = json.loads(body) if body else {}
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    ticket_id = (payload.get("ticket_id") or "").strip()
    pr_number = payload.get("pr_number")

    if not ticket_id:
        raise HTTPException(status_code=400, detail="ticket_id is required")

    branch = _find_branch_for_ticket(ticket_id)
    if not branch:
        return {"status": "error", "detail": f"Could not find branch for {ticket_id}"}

    if not pr_number:
        pr_info = _find_pr_by_branch(branch)
        pr_number = pr_info.get("number") if pr_info else None

    if not pr_number:
        return {"status": "error", "detail": f"Could not find PR for branch {branch}"}

    owner, repo = _resolve_repo_owner_repo(pr_info={"number": pr_number}, data={})
    start_ci_poll(ticket_id, pr_number, owner, repo, branch)
    return {"status": "ok", "ticket": ticket_id, "pr": pr_number, "branch": branch}


@app.post("/trigger-agent")
async def trigger_agent(
    request: Request,
):
    """Manually trigger an SDLC agent without requiring a Linear status change.

    Body (JSON):
        role: Which agent to run — "Product Manager", "Developer", "Reviewer", or "QA"
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

    valid_roles = {"Product Manager", "Developer", "Reviewer", "QA"}
    if role not in valid_roles:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid role '{role}'. Must be one of: {', '.join(sorted(valid_roles))}",
        )

    if not ticket_id:
        raise HTTPException(status_code=400, detail="ticket_id is required")

    if not prompt or not prompt.strip():
        prompt = f"Ticket {ticket_id} — please perform your {role} duties."

    dedup_key = f"{ticket_id}:{role}:manual"
    created, task_row = cm.enqueue_task(
        ticket_id,
        role,
        prompt,
        dedup_key=dedup_key,
        source_state="manual",
    )
    if not created:
        return {
            "status": "duplicate",
            "detail": f"Task already queued/running for {role} on {ticket_id}",
            "queued_task_id": task_row.get("id") if task_row else None,
        }

    logger.info("Manual trigger queued: role=%r ticket=%r task_id=%s", role, ticket_id, task_row.get("id"))
    return {"status": "accepted", "agent": role, "ticket": ticket_id, "queued_task_id": task_row.get("id")}


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
