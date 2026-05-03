import os
import re
import hmac
import hashlib
import json
import logging
import asyncio
import sqlite3
import time
import uuid
import requests
from pathlib import Path
from dotenv import load_dotenv

# Load GITHUB_TOKEN and other env vars from ~/.hermes/.env
load_dotenv(Path.home() / ".hermes" / ".env")

from logging.handlers import RotatingFileHandler
from typing import Dict, Any

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
_REPO_ROOT = Path(__file__).resolve().parent.parent


def _resolve_webhook_agent_db_path() -> str:
    """Resolve agent queue SQLite path for this process.

    Relative ``HERMES_AGENT_STATE_DB`` values are anchored to the **repository root**
    (parent of ``maestro/``), not ``os.getcwd()``, so launchd/systemd/docker with a
    surprising cwd still opens the same file as local development.

    Ensures the parent directory exists so SQLite can create ``.db-wal`` / ``.db-shm``.
    """
    raw = (os.getenv("HERMES_AGENT_STATE_DB") or "").strip()
    if raw:
        p = Path(raw).expanduser()
        if not p.is_absolute():
            p = _REPO_ROOT / p
    else:
        p = _REPO_ROOT / "agent_state.db"
    p = p.resolve(strict=False)
    p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)


_AGENT_DB_PATH = _resolve_webhook_agent_db_path()
cm = ConcurrencyManager(db_path=_AGENT_DB_PATH)


def _resolved_agent_db_path() -> str:
    """Absolute path used for logs and SQLite probes (matches operator expectations)."""
    try:
        return str(Path(cm.db_path).expanduser().resolve())
    except Exception:
        return str(Path(cm.db_path).expanduser())


def _log_agent_state_db_at_startup() -> None:
    """Log queue DB path, directory permissions, and a trivial SQLite probe."""
    resolved = _resolved_agent_db_path()
    parent = Path(resolved).parent
    logger.info(
        "Agent queue DB: raw=%r resolved=%r env.HERMES_AGENT_STATE_DB=%r",
        cm.db_path,
        resolved,
        os.getenv("HERMES_AGENT_STATE_DB"),
    )
    logger.info(
        "Agent queue DB parent: %r is_dir=%s access[RWX]=%s/%s/%s",
        str(parent),
        parent.is_dir(),
        os.access(parent, os.R_OK),
        os.access(parent, os.W_OK),
        os.access(parent, os.X_OK),
    )
    try:
        with sqlite3.connect(resolved, timeout=5.0, isolation_level=None) as conn:
            conn.execute("SELECT 1").fetchone()
        logger.info("Agent queue DB probe: sqlite connect + SELECT 1 OK")
    except sqlite3.Error as exc:
        logger.error(
            "Agent queue DB probe FAILED (queue workers will fail until fixed): %s",
            exc,
            exc_info=True,
        )


def _sqlite_queue_error_note(exc: BaseException) -> str:
    """Extra context for sqlite3.OperationalError (e.g. unable to open database file)."""
    if not isinstance(exc, sqlite3.OperationalError):
        return ""
    try:
        resolved = _resolved_agent_db_path()
    except Exception:
        resolved = cm.db_path
    return (
        " SQLite_context"
        f" path={cm.db_path!r} resolved={resolved!r}"
        f" HERMES_AGENT_STATE_DB={os.getenv('HERMES_AGENT_STATE_DB')!r}"
        f" cwd={os.getcwd()!r}"
    )


_worker_tasks: dict[str, asyncio.Task] = {}
_ci_poll_task: asyncio.Task | None = None
_pm_scanner_task: asyncio.Task | None = None
_worker_stop_event = asyncio.Event()
_STALE_RUNNING_SECONDS = float(os.getenv("HERMES_QUEUE_STALE_RUNNING_SECONDS", "1200"))
_MAX_ACTIVE_TICKETS = int(os.getenv("HERMES_MAX_ACTIVE_TICKETS", "1"))
_PM_SCANNER_ENABLED = os.getenv("PM_SCANNER_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}
_PM_SCANNER_INTERVAL_SECONDS = float(os.getenv("PM_SCANNER_INTERVAL_SECONDS", "900"))
_PM_SCANNER_LIMIT = int(os.getenv("PM_SCANNER_LIMIT", "50"))

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
# Intentionally unmapped (no agent webhook): Ready For Delivery, Approved For Delivery, Done —
# see sdlc_roles.yaml / common_system. QA ends automation at Ready For Delivery; humans advance
# Approved For Delivery and Done (production).


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


async def run_agent_task(
    role: str, ticket_id: str, prompt: str, session_id: str = None,
    hard_timeout_seconds: float = None,
) -> bool:
    if role in ["Developer", "Reviewer", "QA"]:
        retries = cm.get_retry_count(ticket_id)
        if retries >= 3:
            logger.error(f"Circuit Breaker Tripped for {ticket_id}. Max retries reached.")
            return False

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
        if hard_timeout_seconds is not None and hard_timeout_seconds > 0:
            try:
                await asyncio.wait_for(future, timeout=hard_timeout_seconds)
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
                        error=f"Hard timeout after {hard_timeout_seconds}s — agent exceeded allowed runtime",
                    )
                return False
    except asyncio.CancelledError:
        logger.warning("Agent task cancelled for %s/%s", role, ticket_id)
        raise
    except Exception as e:
        logger.error(f"Error running {role} Agent for {ticket_id}: {e}")
        return False
    finally:
        cm.release_lock(ticket_id, role)
    return True


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
            task = cm.claim_next_task(
                role,
                session_id=worker_session_id,
                max_active_tickets=_MAX_ACTIVE_TICKETS,
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
                ok = await run_agent_task(
                    role, ticket_id, prompt, session_id=session_id,
                    hard_timeout_seconds=_STALE_RUNNING_SECONDS,
                )
                cm.complete_task(task_id, success=ok, error=None if ok else "Agent run failed")
            except Exception as exc:
                logger.error("Worker %s failed task %s: %s", role, task_id, exc, exc_info=True)
                cm.complete_task(task_id, success=False, error=str(exc))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            note = _sqlite_queue_error_note(exc)
            logger.error(
                "Queue worker loop error for role=%s: %s.%s Backing off and continuing.",
                role,
                exc,
                note,
                exc_info=True,
            )
            await asyncio.sleep(1.5)
    logger.info("Stopped queue worker for role=%s", role)


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
    # or just use the env-known repo (faworkshop/true-review)
    return (os.getenv("GITHUB_REPO_OWNER", "faworkshop"),
            os.getenv("GITHUB_REPO_NAME", "true-review"))


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


def _get_ci_status_for_pr(owner: str, repo: str, pr_number: int) -> Dict[str, Any]:
    """Poll GitHub for CI status on a PR. Returns dict with 'runs' and 'all_passed'."""
    data = _github_get(f"/repos/{owner}/{repo}/pulls/{pr_number}")
    if not data:
        return {"runs": [], "all_passed": False, "error": "no response"}

    head_sha = data.get("head", {}).get("sha", "")
    branch = data.get("head", {}).get("ref", "")

    # Get check runs for the head SHA
    check_data = _github_get(f"/repos/{owner}/{repo}/commits/{head_sha}/check-runs")
    if not check_data:
        return {"runs": [], "all_passed": False, "error": "no check runs", "branch": branch}

    runs = check_data.get("check_runs", []) or []
    conclusions = [r.get("conclusion") for r in runs]
    all_passed = (
        len(runs) > 0
        and all(c in ("success", "skipped", "neutral") for c in conclusions)
    )

    return {"runs": runs, "all_passed": all_passed, "branch": branch}


async def _ci_poll_worker() -> None:
    """Background worker: every CI_POLL_INTERVAL_SECONDS, check each pending ticket's CI status.

    Runs GitHub checks and success/failure handling only while the Linear issue remains
    in ``CI in Progress``; otherwise the ticket is dropped from the poll set.
    When all checks pass, move the ticket to 'In Progress'.
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
                    logger.info(
                        "CI poll %s: %d runs, all_passed=%s (elapsed=%.0fs)",
                        ticket_id,
                        len(ci.get("runs", [])),
                        ci.get("all_passed"),
                        elapsed,
                    )

                    if ci.get("all_passed"):
                        state_id = info.get("in_progress_state_id")
                        if state_id:
                            _move_linear_ticket_state(ticket_id, state_id)
                        logger.info("CI PASSED for %s — moved to In Progress", ticket_id)
                        _post_linear_comment(
                            ticket_id,
                            f"✅ CI checks passed on branch `{ci.get('branch', 'unknown')}` — "
                            f"ticket moved to In Progress.",
                        )
                        expired.append(ticket_id)
                    elif ci.get("all_passed") is False:
                        # CI failed — keep Developer on the ticket in In Progress.
                        # Do NOT add needs-human label — Developer can and should fix it.
                        # Keep polling: when Developer pushes a fix, CI will eventually pass.
                        # Re-dispatch Developer with CI failure details so they know what to fix.
                        failed_names = [
                            r["name"] for r in ci.get("runs", [])
                            if r.get("conclusion") == "failure"
                        ]
                        logger.warning(
                            "CI FAILED for %s — Developer stays on ticket, keeping poll active",
                            ticket_id,
                        )
                        # Only post the failure comment once — not every poll cycle.
                        if not info.get("failure_comment_posted"):
                            _post_linear_comment(
                                ticket_id,
                                f"❌ CI failed on branch `{ci.get('branch', 'unknown')}`: "
                                f"{', '.join(failed_names)}. "
                                f"Developer will fix and re-push.",
                            )
                            info["failure_comment_posted"] = True
                        # Re-dispatch Developer so they are immediately notified and can act.
                        # Dedup key matches the webhook-triggered Developer dispatch so a new
                        # task is only created if no active Developer task exists.
                        dev_dedup_key = f"{ticket_id}:Developer:in progress"
                        dev_prompt = (
                            f"CI failed on branch `{ci.get('branch', 'unknown')}`: "
                            f"{', '.join(failed_names)}. "
                            f"Please fix the failures and push a new commit to re-trigger CI."
                        )
                        created, _ = cm.enqueue_task(
                            ticket_id,
                            "Developer",
                            dev_prompt,
                            dedup_key=dev_dedup_key,
                            source_state="in progress",
                        )
                        if created:
                            logger.info(
                                "CI failure re-dispatched Developer for %s (task enqueued)",
                                ticket_id,
                            )
                        else:
                            logger.info(
                                "CI failure: active Developer task exists for %s — no new task enqueued",
                                ticket_id,
                            )
                        # Do NOT expired.append(ticket_id) — keep polling

                for tid in expired:
                    _ci_polling_tickets.pop(tid, None)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            note = _sqlite_queue_error_note(exc)
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
            note = _sqlite_queue_error_note(exc)
            logger.error("PM intake scanner loop error: %s%s", exc, note, exc_info=True)
            await asyncio.sleep(2.0)


def _post_linear_comment(ticket_id: str, body: str) -> None:
    """Post a comment to a Linear issue."""
    mutation = """
    mutation CommentCreate($input: CommentCreateInput!) {
      commentCreate(input: $input) { success comment { id } }
    }
    """
    _linear_gql(mutation, {"input": {"issueId": ticket_id, "body": body}})


def _add_linear_label(ticket_id: str, label_name: str) -> bool:
    """Add a label to a Linear issue by name. Creates the label if it doesn't exist."""
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
    issue_result = _linear_gql(issue_query, {"id": ticket_id})
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
    result = _linear_gql(add_mutation, {"id": ticket_id, "labelId": label_id})
    if result:
        logger.info("Added label '%s' to ticket %s", label_name, ticket_id)
    return bool(result)


def _move_linear_ticket_state(ticket_id: str, state_id: str) -> bool:
    """Move a Linear issue to a specific state by state ID."""
    mutation = """
    mutation UpdateState($id: String!, $stateId: String!) {
      issueUpdate(id: $id, input: {stateId: $stateId}) { success }
    }
    """
    result = _linear_gql(mutation, {"id": ticket_id, "stateId": state_id})
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


def start_ci_poll(ticket_id: str, pr_number: int, owner: str, repo: str, branch: str,
                  in_progress_state_id: str = None) -> None:
    """Register a ticket for CI polling. Idempotent — replaces existing entry."""
    if in_progress_state_id is None:
        in_progress_state_id = _get_in_progress_state_id(ticket_id)
    _ci_polling_tickets[ticket_id] = {
        "pr_number": pr_number,
        "owner": owner,
        "repo": repo,
        "branch": branch,
        "added_at": time.time(),
        "in_progress_state_id": in_progress_state_id,
        "failure_comment_posted": False,
    }
    logger.info("Started CI polling for %s (PR #%d, branch=%s, in_progress_state_id=%s)",
                ticket_id, pr_number, branch, in_progress_state_id)


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
    )
    logger.info(
        "Redirected %s to CI polling (PR #%d, branch=%s) — "
        "ticket will auto-advance to 'In Review' when CI passes.",
        ticket_id, pr_number, branch,
    )


def _get_ci_in_progress_state_id(ticket_id: str) -> str | None:
    """Resolve the Linear state id for 'CI in Progress'."""
    query = """
    query GetStates($id: String!) {
      issue(id: $id) {
        team {
          states(filter: {names: ["CI in Progress", "CI in progress"]}) {
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
    global _ci_poll_task, _pm_scanner_task
    _log_agent_state_db_at_startup()
    _worker_stop_event.clear()
    for role in _queue_roles():
        if role in _worker_tasks and not _worker_tasks[role].done():
            continue
        _worker_tasks[role] = asyncio.create_task(_agent_worker(role))
    if _ci_poll_task is None or _ci_poll_task.done():
        _ci_poll_task = asyncio.create_task(_ci_poll_worker())
    if _pm_scanner_task is None or _pm_scanner_task.done():
        _pm_scanner_task = asyncio.create_task(_pm_intake_scanner_worker())


@app.on_event("shutdown")
async def _shutdown_workers() -> None:
    global _ci_poll_task, _pm_scanner_task
    _worker_stop_event.set()
    tasks = [t for t in _worker_tasks.values() if not t.done()]
    if _ci_poll_task and not _ci_poll_task.done():
        tasks.append(_ci_poll_task)
    if _pm_scanner_task and not _pm_scanner_task.done():
        tasks.append(_pm_scanner_task)
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    _worker_tasks.clear()
    _ci_poll_task = None
    _pm_scanner_task = None


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
            start_ci_poll(ticket_id, pr_number, owner, repo, branch,
                          in_progress_state_id=in_progress_state_id)
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
            ci = _get_ci_status_for_pr(owner, repo, pr_number)
            if not ci.get("all_passed"):
                logger.warning(
                    "CI not passed for %s (PR #%d) — redirecting to 'CI in Progress' "
                    "instead of dispatching Reviewer. CI state: %s",
                    ticket_id, pr_number, ci,
                )
                _redirect_to_ci_in_progress(ticket_id, pr_number, owner, repo, branch, data)
                return {
                    "status": "ci_redirect",
                    "ticket": ticket_id,
                    "pr": pr_number,
                    "reason": "CI not passed — intercepted before Reviewer dispatch",
                }
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
):
    """List queued/running/completed tasks for queue debugging."""
    tasks = cm.list_tasks(role=role, state=state, limit=limit)
    counts: dict[str, int] = {}
    for item in tasks:
        st = str(item.get("state") or "unknown")
        counts[st] = counts.get(st, 0) + 1
    return {
        "status": "ok",
        "filters": {"role": role, "state": state, "limit": limit},
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
