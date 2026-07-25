#!/usr/bin/env python3
"""
Local Pipeline Orchestrator for Multi-Agent SDLC.

Simulates the event-driven Linear -> GitHub workflow locally without webhooks.
Runs the Product Manager, Developer, Reviewer, and QA agents sequentially.

Role definitions (prompts, toolsets, shared rules): ``maestro/sdlc_roles.yaml``.
Override path with env ``SDLC_ROLES_PATH``.
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from run_agent import AIAgent

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Mock Database for locks and retry counters
DB = {
    "locks": {},  # ticket_id -> assignee
    "retries": {},  # ticket_id -> count
}

_logged_minimax_openai_fallback = False


def _normalize_minimax_runtime_if_no_anthropic_sdk(rt: Dict[str, Any]) -> Dict[str, Any]:
    """If Anthropic SDK is missing, fall back MiniMax to OpenAI-compatible /v1."""
    global _logged_minimax_openai_fallback
    out = dict(rt)
    prov = (out.get("provider") or "").strip().lower()
    if prov not in ("minimax", "minimax-cn") or out.get("api_mode") != "anthropic_messages":
        return out
    try:
        import anthropic  # noqa: F401
    except ImportError:
        pass
    else:
        return out

    base = (out.get("base_url") or "").strip().rstrip("/")
    if base.endswith("/anthropic"):
        out["base_url"] = base[: -len("/anthropic")] + "/v1"
    elif prov == "minimax-cn":
        out["base_url"] = "https://api.minimaxi.com/v1"
    else:
        out["base_url"] = "https://api.minimax.io/v1"
    out["api_mode"] = "chat_completions"
    if not _logged_minimax_openai_fallback:
        logger.warning(
            "MiniMax targets the Anthropic-compatible API but the 'anthropic' package "
            "is not installed; using OpenAI-compatible %s (chat_completions). "
            "To use the default MiniMax transport instead, run: pip install 'anthropic>=0.39.0'",
            out["base_url"],
        )
        _logged_minimax_openai_fallback = True
    return out


def _hermes_model_and_runtime() -> Tuple[str, Dict[str, Any]]:
    """Resolve model + provider runtime from ~/.hermes/config.yaml."""
    from hermes_cli.config import load_config
    from hermes_cli.runtime_provider import resolve_runtime_provider

    cfg = load_config()
    block = cfg.get("model") or {}
    if isinstance(block, str):
        model = block.strip()
    else:
        model = (block.get("default") or block.get("model") or "").strip()

    rt = resolve_runtime_provider()
    provider = (rt.get("provider") or "").strip().lower()
    if not model and provider in ("minimax", "minimax-cn"):
        model = "MiniMax-M2.7"
    if not model:
        raise RuntimeError(
            "Set model.default in ~/.hermes/config.yaml (e.g. MiniMax-M2.7 for MiniMax), "
            "same as for `hermes chat`."
        )
    return model, rt


def _render_template(text: str, ticket_id: str) -> str:
    return (text or "").replace("{{ticket_id}}", ticket_id)


def _default_sdlc_config() -> Dict[str, Any]:
    """Fallback if roles yaml is missing or invalid."""
    return {
        "common_system": (
            "You are part of an automated SDLC pipeline. Read tickets and comments first; "
            "post Linear summaries; escalate to humans when blocked."
        ),
        "circuit_breaker_agents": ["Developer", "Reviewer", "QA"],
        "pipeline": [
            {
                "agent_key": "Product Manager",
                "toolsets": ["linear"],
                "system": "You are the Product Manager agent. Triage tickets and set priority.",
                "prompt": "A new ticket {{ticket_id}} has been created.",
            },
            {
                "agent_key": "Developer",
                "toolsets": ["linear", "github", "terminal", "file"],
                "system": "You are the Developer agent. Implement the ticket and open a PR.",
                "prompt": "Ticket {{ticket_id}} is 'In Progress'.",
            },
            {
                "agent_key": "Reviewer",
                "toolsets": ["linear", "github"],
                "system": "You are the Reviewer agent. Review the PR and update Linear.",
                "prompt": "Ticket {{ticket_id}} is 'In Review'.",
            },
            {
                "agent_key": "QA",
                "toolsets": ["linear", "github", "terminal"],
                "system": "You are the QA agent. Run tests, merge if appropriate, update Linear.",
                "prompt": "Ticket {{ticket_id}} is 'Ready For QA'.",
            },
        ],
    }


def load_sdlc_config() -> Dict[str, Any]:
    """Load ``maestro/sdlc_roles.yaml`` (or ``SDLC_ROLES_PATH``)."""
    override = (os.environ.get("SDLC_ROLES_PATH") or "").strip()
    if override:
        path = Path(override).expanduser()
    else:
        path = Path(__file__).resolve().parent / "sdlc_roles.yaml"
    if not path.is_file():
        logger.warning("SDLC roles file not found at %s — using built-in defaults", path)
        return _default_sdlc_config()
    try:
        with open(path, encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    except Exception as exc:
        logger.error("Failed to load SDLC roles from %s: %s — using defaults", path, exc)
        return _default_sdlc_config()
    pipeline = raw.get("pipeline")
    if not isinstance(pipeline, list) or not pipeline:
        logger.error("SDLC roles file %s has no pipeline list — using defaults", path)
        return _default_sdlc_config()
    for i, step in enumerate(pipeline):
        if not isinstance(step, dict) or not step.get("agent_key"):
            logger.error("Invalid pipeline step %s in %s — using defaults", i, path)
            return _default_sdlc_config()
    raw["_loaded_from"] = str(path)
    return raw


def check_lock(ticket_id: str, agent_name: str) -> bool:
    """Simulate assignee-based locking."""
    current = DB["locks"].get(ticket_id)
    if current and current != agent_name:
        logger.warning(f"Ticket {ticket_id} is locked by {current}. {agent_name} cannot proceed.")
        return False

    DB["locks"][ticket_id] = agent_name
    logger.info(f"Lock acquired on {ticket_id} by {agent_name}.")
    return True


def release_lock(ticket_id: str, agent_name: str):
    """Release the lock."""
    if DB["locks"].get(ticket_id) == agent_name:
        del DB["locks"][ticket_id]
        logger.info(f"Lock released on {ticket_id} by {agent_name}.")


def run_agent(
    role: str,
    ticket_id: str,
    prompt: str,
    enabled_toolsets: List[str],
    *,
    system_message: str,
    circuit_breaker_agents: Optional[List[str]] = None,
    model_override: Optional[str] = None,
    max_iterations: Optional[int] = None,
):
    """Run a specific AIAgent profile."""

    cb_agents = list(circuit_breaker_agents or ["Developer", "Reviewer", "QA"])
    if role in cb_agents:
        retries = DB["retries"].get(ticket_id, 0)
        if retries >= 3:
            logger.error(
                f"🚨 Circuit Breaker Tripped! Max retries (3) reached for {ticket_id}. Halting automation."
            )
            return False

    if not check_lock(ticket_id, role):
        return False

    logger.info(f"\n{'='*50}\nStarting {role} Agent for {ticket_id}\n{'='*50}")

    model, rt = _hermes_model_and_runtime()
    if model_override:
        model = model_override.strip()
    rt = _normalize_minimax_runtime_if_no_anthropic_sdk(rt)
    agent_kw: Dict[str, Any] = dict(
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
    )
    if max_iterations is not None:
        agent_kw["max_iterations"] = int(max_iterations)
    agent = AIAgent(**agent_kw)

    try:
        response = agent.run_conversation(
            user_message=prompt,
            system_message=system_message,
        )
        logger.info(f"{role} Agent finished. Final response:\n{response}")
    except Exception as e:
        logger.error(f"{role} Agent encountered an error: {e}")
    finally:
        release_lock(ticket_id, role)

    return True


def simulate_pipeline():
    """Simulate the full lifecycle of a dummy ticket."""
    ticket_id = "ENG-42"
    cfg = load_sdlc_config()
    common = (cfg.get("common_system") or "").strip()
    cb = cfg.get("circuit_breaker_agents")
    if not isinstance(cb, list) or not cb:
        cb = ["Developer", "Reviewer", "QA"]
    cb = [str(x).strip() for x in cb if str(x).strip()]

    for step in cfg["pipeline"]:
        agent_key = str(step["agent_key"]).strip()
        toolsets = step.get("toolsets") or []
        if not isinstance(toolsets, list):
            toolsets = list(toolsets)
        role_system = (step.get("system") or "").strip()
        system_message = f"{common}\n\n{role_system}".strip() if common else role_system
        prompt = _render_template(str(step.get("prompt") or ""), ticket_id)
        model_override = step.get("model")
        if isinstance(model_override, str):
            model_override = model_override.strip() or None
        else:
            model_override = None
        max_it = step.get("max_iterations")
        max_iterations = int(max_it) if max_it is not None else None
        run_agent(
            agent_key,
            ticket_id,
            prompt,
            toolsets,
            system_message=system_message,
            circuit_breaker_agents=cb,
            model_override=model_override,
            max_iterations=max_iterations,
        )

    logger.info("\n--- Simulating a Circuit Breaker (Retry Loop) ---")
    DB["retries"][ticket_id] = 3
    dev_step = next((s for s in cfg["pipeline"] if s.get("agent_key") == "Developer"), None)
    if dev_step:
        role_system = (dev_step.get("system") or "").strip()
        system_message = f"{common}\n\n{role_system}".strip() if common else role_system
    else:
        system_message = common or "You are the Developer agent."
    run_agent(
        "Developer",
        ticket_id,
        f"Ticket {ticket_id} bounced back from QA.",
        ["linear", "github"],
        system_message=system_message,
        circuit_breaker_agents=cb,
    )


if __name__ == "__main__":
    logger.info("Initializing multi-agent CI/CD pipeline simulation...")
    simulate_pipeline()
    logger.info("Simulation complete.")
