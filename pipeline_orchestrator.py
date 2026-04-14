#!/usr/bin/env python3
"""
Local Pipeline Orchestrator for Multi-Agent SDLC

Simulates the event-driven Linear -> GitHub workflow locally without needing webhooks.
Runs the Product Manager, Developer, Reviewer, and QA agents sequentially.
"""

import json
import logging
from typing import List

from run_agent import AIAgent

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Mock Database for locks and retry counters
DB = {
    "locks": {},  # ticket_id -> assignee
    "retries": {} # ticket_id -> count
}

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

def run_agent(role: str, ticket_id: str, prompt: str, enabled_toolsets: List[str]):
    """Run a specific AIAgent profile."""
    
    if role in ["Developer", "Reviewer", "QA"]:
        # Circuit breaker logic
        retries = DB["retries"].get(ticket_id, 0)
        if retries >= 3:
            logger.error(f"🚨 Circuit Breaker Tripped! Max retries (3) reached for {ticket_id}. Halting automation.")
            return False

    if not check_lock(ticket_id, role):
        return False

    logger.info(f"\n{'='*50}\nStarting {role} Agent for {ticket_id}\n{'='*50}")
    
    # We define the system message to enforce the architectural rules
    system_message = f"""You are the {role} Agent in our SDLC pipeline.
You must always:
1. Read the ticket details and historical comments first to gather context.
2. Execute your specific workflow duties.
3. If you are the Developer Agent and have tried to fix feedback twice and failed, SURRENDER and tag a human.
4. When finished, update the ticket status if applicable.
5. Post a summary comment on the Linear ticket detailing your actions, findings, and blockers.
"""

    agent = AIAgent(
        model="anthropic/claude-3-5-haiku-20241022", # fast model for simulation
        enabled_toolsets=enabled_toolsets,
        quiet_mode=False
    )

    try:
        response = agent.run_conversation(
            user_message=prompt,
            system_message=system_message
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
    
    # 1. Product Manager Agent (Triage)
    # Trigger: Ticket created
    pm_success = run_agent(
        role="Product Manager",
        ticket_id=ticket_id,
        prompt=f"A new ticket {ticket_id} has been created. Please read it, assign a priority, move it to 'To-do' or 'In Progress', and post a summary.",
        enabled_toolsets=["linear"]
    )
    
    # 2. Developer Agent
    # Trigger: Ticket moved to "In Progress"
    dev_success = run_agent(
        role="Developer",
        ticket_id=ticket_id,
        prompt=f"Ticket {ticket_id} is 'In Progress'. Please read it, create a branch, write the code, verify the Docker image, open a PR, link it to Linear, move the ticket to 'In Review', and post a summary.",
        enabled_toolsets=["linear", "github", "terminal", "file"]
    )
    
    # 3. Reviewer Agent
    # Trigger: Ticket moved to "In Review"
    review_success = run_agent(
        role="Reviewer",
        ticket_id=ticket_id,
        prompt=f"Ticket {ticket_id} is 'In Review'. Please read the PR diff, review the code, approve the PR, move the ticket to 'Ready For QA', and post a summary.",
        enabled_toolsets=["linear", "github"]
    )
    
    # 4. QA Agent
    # Trigger: Ticket moved to "Ready For QA"
    qa_success = run_agent(
        role="QA",
        ticket_id=ticket_id,
        prompt=f"Ticket {ticket_id} is 'Ready For QA'. Please move the ticket to 'QA Testing', run tests, merge the PR, move the ticket to 'Done', and post a summary.",
        enabled_toolsets=["linear", "github", "terminal"]
    )

    # 5. Simulate a loop rejection (Circuit Breaker Test)
    logger.info("\n--- Simulating a Circuit Breaker (Retry Loop) ---")
    DB["retries"][ticket_id] = 3
    run_agent(
        role="Developer",
        ticket_id=ticket_id,
        prompt=f"Ticket {ticket_id} bounced back from QA.",
        enabled_toolsets=["linear", "github"]
    )

if __name__ == "__main__":
    logger.info("Initializing multi-agent CI/CD pipeline simulation...")
    simulate_pipeline()
    logger.info("Simulation complete.")