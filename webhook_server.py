import os
import hmac
import hashlib
import json
import logging
import asyncio
from typing import Dict, Any, Optional

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from pydantic import BaseModel

from agent.concurrency import ConcurrencyManager
from run_agent import AIAgent
from pipeline_orchestrator import _hermes_model_and_runtime, _normalize_minimax_runtime_if_no_anthropic_sdk

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("webhook_server")

app = FastAPI(title="Hermes Webhook Server")
cm = ConcurrencyManager()

# Configuration
LINEAR_WEBHOOK_SECRET = os.getenv("LINEAR_WEBHOOK_SECRET")
LINEAR_BOT_USER_ID = os.getenv("LINEAR_BOT_USER_ID")

# Role mapping based on Linear states
STATE_AGENT_MAPPING = {
    "Backlog": "Product Manager",
    "New": "Product Manager",
    "In Progress": "Developer",
    "In Review": "Reviewer",
    "Ready For QA": "QA"
}

# Toolset mapping for each role
ROLE_TOOLSETS = {
    "Product Manager": ["linear"],
    "Developer": ["linear", "github", "terminal", "file"],
    "Reviewer": ["linear", "github"],
    "QA": ["linear", "github", "terminal"]
}

def verify_linear_signature(body: bytes, signature: str) -> bool:
    if not LINEAR_WEBHOOK_SECRET:
        return True # Skip verification if secret not set
    
    computed_signature = hmac.new(
        LINEAR_WEBHOOK_SECRET.encode(),
        body,
        hashlib.sha256
    ).hexdigest()
    
    return hmac.compare_digest(computed_signature, signature)

async def run_agent_task(role: str, ticket_id: str, prompt: str):
    """Asynchronous background task to run the agent."""
    
    # 1. Check Circuit Breaker
    if role in ["Developer", "Reviewer", "QA"]:
        # We only count backward transitions as retries in a real scenario,
        # but for this simple version, we'll check the count.
        # In a more advanced version, we'd detect if this state was reached before.
        retries = cm.get_retry_count(ticket_id)
        if retries >= 3:
            logger.error(f"🚨 Circuit Breaker Tripped for {ticket_id}. Max retries reached.")
            # TODO: Post comment to Linear about halting automation
            return

    # 2. Acquire Lock
    if not cm.acquire_lock(ticket_id, role):
        return

    logger.info(f"Starting {role} Agent for {ticket_id}")
    
    system_message = f"""You are the {role} Agent in our SDLC pipeline.
You must always:
1. Read the ticket details and historical comments first to gather context.
2. Execute your specific workflow duties.
3. If you are the Developer Agent and have tried to fix feedback twice and failed, SURRENDER and tag a human.
4. When finished, update the ticket status if applicable.
5. Post a summary comment on the Linear ticket detailing your actions, findings, and blockers.
"""

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
            enabled_toolsets=ROLE_TOOLSETS.get(role, ["linear"]),
            quiet_mode=False,
        )

        # Run in a separate thread/process if it's CPU intensive, 
        # but AIAgent is mostly I/O bound on API calls.
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None, 
            lambda: agent.run_conversation(user_message=prompt, system_message=system_message)
        )
        
    except Exception as e:
        logger.error(f"Error running {role} Agent for {ticket_id}: {e}")
    finally:
        cm.release_lock(ticket_id, role)

@app.post("/linear-webhook")
async def linear_webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.body()
    signature = request.headers.get("linear-signature")
    
    if signature and not verify_linear_signature(body, signature):
        raise HTTPException(status_code=403, detail="Invalid signature")
    
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    action = payload.get("action")
    data = payload.get("data", {})
    type_ = payload.get("type")

    if type_ != "Issue":
        return {"status": "ignored", "reason": "Not an Issue event"}

    ticket_id = data.get("identifier")
    if not ticket_id:
        return {"status": "ignored", "reason": "No ticket identifier"}

    # Ignore actions by the bot itself, unless it's a state change
    actor_id = payload.get("actor", {}).get("id")
    if actor_id == LINEAR_BOT_USER_ID and action != "update":
        return {"status": "ignored", "reason": "Action performed by bot"}

    # Detect state change
    new_state = data.get("state", {}).get("name")
    if not new_state:
        # Check if state changed in an update
        updated_from = payload.get("updatedFrom", {})
        if "stateId" in updated_from:
             # State actually changed, but we need to fetch the new state name 
             # if it's not in 'data'. Usually Linear provides it.
             pass
        else:
            return {"status": "ignored", "reason": "No state change detected"}

    role = STATE_AGENT_MAPPING.get(new_state)
    if not role:
        return {"status": "ignored", "reason": f"No agent mapped to state: {new_state}"}

    # Prepare prompt based on role and state
    prompt = f"Ticket {ticket_id} has moved to '{new_state}'. Please perform your duties as {role}."
    
    background_tasks.add_task(run_agent_task, role, ticket_id, prompt)
    
    return {"status": "accepted", "agent": role, "ticket": ticket_id}

@app.get("/health")
async def health():
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
