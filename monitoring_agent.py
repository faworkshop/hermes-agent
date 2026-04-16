import os
import logging
import json
import time
from typing import List, Dict, Any

from run_agent import AIAgent
from tools.linear_tool import linear_read_ticket, linear_post_comment

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("monitoring_agent")

def check_monitoring_requirements() -> bool:
    return bool(os.getenv("LINEAR_API_KEY")) # And Grafana MCP should be active

def triage_production_issues():
    """
    Agent that polls Grafana for active alerts and triages them into Linear tickets.
    """
    if not check_monitoring_requirements():
        logger.error("Missing requirements for monitoring agent.")
        return

    # Instantiate the Product Manager Agent for triaging
    agent = AIAgent(
        model=os.getenv("HERMES_MODEL", "gpt-4o"),
        api_key=os.getenv("OPENAI_API_KEY") or os.getenv("ANTHROPIC_API_KEY"),
        enabled_toolsets=["linear", "grafana"], # Assuming 'grafana' toolset is registered via MCP
        quiet_mode=False,
    )

    prompt = """
    Please check for any active high-severity alert groups in Grafana.
    For each critical alert:
    1. Analyze the metrics/logs to understand the root cause.
    2. Check if a Linear ticket already exists for this issue (search by title/summary).
    3. If no ticket exists, create a new Linear ticket with 'Critical' priority, 
       detailed description of the alert, and the root cause analysis.
    4. If a ticket exists, post a comment with updated information from the live metrics.
    """
    
    system_message = """You are the Monitoring & Triage Agent. 
    You watch production systems via Grafana and ensure every critical issue is tracked in Linear.
    Be precise and avoid creating duplicate tickets.
    """

    try:
        logger.info("Starting monitoring triage run...")
        response = agent.run_conversation(user_message=prompt, system_message=system_message)
        logger.info(f"Triage run complete. Summary:\n{response}")
    except Exception as e:
        logger.error(f"Error during monitoring triage: {e}")

if __name__ == "__main__":
    # In a real scenario, this would run on a cron or loop
    triage_production_issues()
