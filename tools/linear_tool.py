import json
import logging
import os
import requests
from typing import Dict, Any, Optional, List

from tools.registry import registry

logger = logging.getLogger(__name__)

LINEAR_URL = "https://api.linear.app/graphql"

def check_linear_requirements() -> bool:
    """Check if Linear integration is configured."""
    return bool(os.getenv("LINEAR_API_KEY"))

def _execute_linear_query(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Helper to execute a GraphQL query against the Linear API."""
    api_key = os.getenv("LINEAR_API_KEY")
    headers = {
        "Content-Type": "application/json",
        "Authorization": api_key
    }
    payload = {"query": query, "variables": variables or {}}
    
    try:
        response = requests.post(LINEAR_URL, headers=headers, json=payload)
        response.raise_for_status()
        result = response.json()
        if "errors" in result:
            logger.error(f"Linear API Errors: {result['errors']}")
            return {"success": False, "error": result["errors"][0].get("message")}
        return {"success": True, "data": result.get("data")}
    except Exception as e:
        logger.error(f"Linear API Request Failed: {e}")
        return {"success": False, "error": str(e)}

# -----------------------------------------------------------------------------
# Tool Handlers
# -----------------------------------------------------------------------------

def linear_read_ticket(ticket_id: str, task_id: str = None) -> str:
    """Read details of a Linear ticket."""
    query = """
    query Issue($id: String!) {
      issue(id: $id) {
        id
        identifier
        title
        description
        priority
        state {
          id
          name
        }
        assignee {
          id
          name
        }
        labels {
          nodes {
            id
            name
          }
        }
      }
    }
    """
    result = _execute_linear_query(query, {"id": ticket_id})
    return json.dumps(result)

def linear_read_comments(ticket_id: str, task_id: str = None) -> str:
    """Read comments from a Linear ticket."""
    query = """
    query IssueComments($id: String!) {
      issue(id: $id) {
        comments {
          nodes {
            id
            body
            user {
              name
            }
            createdAt
          }
        }
      }
    }
    """
    result = _execute_linear_query(query, {"id": ticket_id})
    if result["success"]:
        comments = result["data"]["issue"]["comments"]["nodes"]
        return json.dumps({"success": True, "data": comments})
    return json.dumps(result)

def linear_update_status(ticket_id: str, status: str, task_id: str = None) -> str:
    """Update the status of a Linear ticket by mapping name to stateId."""
    # 1. Fetch the issue to find its team and available states
    issue_query = """
    query IssueTeam($id: String!) {
      issue(id: $id) {
        team {
          states {
            nodes {
              id
              name
            }
          }
        }
      }
    }
    """
    issue_result = _execute_linear_query(issue_query, {"id": ticket_id})
    if not issue_result["success"]:
        return json.dumps(issue_result)
    
    states = issue_result["data"]["issue"]["team"]["states"]["nodes"]
    state_id = next((s["id"] for s in states if s["name"].lower() == status.lower()), None)
    
    if not state_id:
        return json.dumps({"success": False, "error": f"State '{status}' not found in team workflow."})

    # 2. Update the issue
    mutation = """
    mutation IssueUpdate($id: String!, $input: IssueUpdateInput!) {
      issueUpdate(id: $id, input: $input) {
        success
      }
    }
    """
    variables = {"id": ticket_id, "input": {"stateId": state_id}}
    result = _execute_linear_query(mutation, variables)
    return json.dumps(result)

def linear_update_priority(ticket_id: str, priority: int, task_id: str = None) -> str:
    """Update the priority of a Linear ticket."""
    mutation = """
    mutation IssueUpdate($id: String!, $input: IssueUpdateInput!) {
      issueUpdate(id: $id, input: $input) {
        success
      }
    }
    """
    variables = {"id": ticket_id, "input": {"priority": priority}}
    result = _execute_linear_query(mutation, variables)
    return json.dumps(result)

def linear_assign_user(ticket_id: str, user_id: str, task_id: str = None) -> str:
    """Assign a Linear ticket to a user."""
    mutation = """
    mutation IssueUpdate($id: String!, $input: IssueUpdateInput!) {
      issueUpdate(id: $id, input: $input) {
        success
      }
    }
    """
    variables = {"id": ticket_id, "input": {"assigneeId": user_id}}
    result = _execute_linear_query(mutation, variables)
    return json.dumps(result)

def linear_add_label(ticket_id: str, label_id: str, task_id: str = None) -> str:
    """Add a label to a Linear ticket. Supports label name or ID."""
    # First, get current labels to avoid overwriting or to find ID by name
    query = """
    query IssueLabels($id: String!) {
      issue(id: $id) {
        labels { nodes { id } }
        team { labels { nodes { id name } } }
      }
    }
    """
    info = _execute_linear_query(query, {"id": ticket_id})
    if not info["success"]: return json.dumps(info)
    
    current_ids = [l["id"] for l in info["data"]["issue"]["labels"]["nodes"]]
    team_labels = info["data"]["issue"]["team"]["labels"]["nodes"]
    
    # Resolve label_id if it's a name
    target_id = label_id
    for l in team_labels:
        if l["name"].lower() == label_id.lower() or l["id"] == label_id:
            target_id = l["id"]
            break
            
    if target_id not in current_ids:
        current_ids.append(target_id)
        
    mutation = """
    mutation IssueUpdate($id: String!, $input: IssueUpdateInput!) {
      issueUpdate(id: $id, input: $input) { success }
    }
    """
    return json.dumps(_execute_linear_query(mutation, {"id": ticket_id, "input": {"labelIds": current_ids}}))

def linear_post_comment(ticket_id: str, body: str, task_id: str = None) -> str:
    """Post a comment to a Linear ticket."""
    mutation = """
    mutation CommentCreate($input: CommentCreateInput!) {
      commentCreate(input: $input) {
        success
        comment { id }
      }
    }
    """
    variables = {"input": {"issueId": ticket_id, "body": body}}
    result = _execute_linear_query(mutation, variables)
    return json.dumps(result)

# -----------------------------------------------------------------------------
# Tool Registrations
# -----------------------------------------------------------------------------

registry.register(
    name="linear_read_ticket",
    toolset="linear",
    schema={
        "name": "linear_read_ticket",
        "description": "Read details of a Linear ticket. Use this to understand the task.",
        "parameters": {
            "type": "object",
            "properties": {
                "ticket_id": {
                    "type": "string",
                    "description": "The Linear ticket ID (e.g., 'ENG-123')."
                }
            },
            "required": ["ticket_id"]
        }
    },
    handler=lambda args, **kw: linear_read_ticket(args.get("ticket_id", ""), kw.get("task_id")),
    check_fn=check_linear_requirements,
    requires_env=["LINEAR_API_KEY"],
)

registry.register(
    name="linear_read_comments",
    toolset="linear",
    schema={
        "name": "linear_read_comments",
        "description": "Read comments on a Linear ticket to understand historical context and previous agent actions.",
        "parameters": {
            "type": "object",
            "properties": {
                "ticket_id": {
                    "type": "string",
                    "description": "The Linear ticket ID (e.g., 'ENG-123')."
                }
            },
            "required": ["ticket_id"]
        }
    },
    handler=lambda args, **kw: linear_read_comments(args.get("ticket_id", ""), kw.get("task_id")),
    check_fn=check_linear_requirements,
    requires_env=["LINEAR_API_KEY"],
)

registry.register(
    name="linear_update_status",
    toolset="linear",
    schema={
        "name": "linear_update_status",
        "description": "Transition the status of a Linear ticket.",
        "parameters": {
            "type": "object",
            "properties": {
                "ticket_id": {
                    "type": "string",
                    "description": "The Linear ticket ID."
                },
                "status": {
                    "type": "string",
                    "description": "The new status (e.g., 'To-do', 'In Progress', 'In Review', 'Ready For QA', 'QA Testing', 'Ready For Delivery', 'Done')."
                }
            },
            "required": ["ticket_id", "status"]
        }
    },
    handler=lambda args, **kw: linear_update_status(args.get("ticket_id", ""), args.get("status", ""), kw.get("task_id")),
    check_fn=check_linear_requirements,
    requires_env=["LINEAR_API_KEY"],
)

registry.register(
    name="linear_update_priority",
    toolset="linear",
    schema={
        "name": "linear_update_priority",
        "description": "Set the priority of a Linear ticket.",
        "parameters": {
            "type": "object",
            "properties": {
                "ticket_id": {
                    "type": "string",
                    "description": "The Linear ticket ID."
                },
                "priority": {
                    "type": "integer",
                    "description": "Priority level: 0 (No priority), 1 (Urgent), 2 (High), 3 (Medium), 4 (Low)."
                }
            },
            "required": ["ticket_id", "priority"]
        }
    },
    handler=lambda args, **kw: linear_update_priority(args.get("ticket_id", ""), args.get("priority", 0), kw.get("task_id")),
    check_fn=check_linear_requirements,
    requires_env=["LINEAR_API_KEY"],
)

registry.register(
    name="linear_assign_user",
    toolset="linear",
    schema={
        "name": "linear_assign_user",
        "description": "Assign a Linear ticket to a specific user or bot account. Use this to claim a ticket for processing.",
        "parameters": {
            "type": "object",
            "properties": {
                "ticket_id": {
                    "type": "string",
                    "description": "The Linear ticket ID."
                },
                "user_id": {
                    "type": "string",
                    "description": "The ID of the user to assign."
                }
            },
            "required": ["ticket_id", "user_id"]
        }
    },
    handler=lambda args, **kw: linear_assign_user(args.get("ticket_id", ""), args.get("user_id", ""), kw.get("task_id")),
    check_fn=check_linear_requirements,
    requires_env=["LINEAR_API_KEY"],
)

registry.register(
    name="linear_add_label",
    toolset="linear",
    schema={
        "name": "linear_add_label",
        "description": "Add a label to a Linear ticket. Used for tagging states like 'bot-processing'.",
        "parameters": {
            "type": "object",
            "properties": {
                "ticket_id": {
                    "type": "string",
                    "description": "The Linear ticket ID."
                },
                "label_id": {
                    "type": "string",
                    "description": "The ID or name of the label."
                }
            },
            "required": ["ticket_id", "label_id"]
        }
    },
    handler=lambda args, **kw: linear_add_label(args.get("ticket_id", ""), args.get("label_id", ""), kw.get("task_id")),
    check_fn=check_linear_requirements,
    requires_env=["LINEAR_API_KEY"],
)

registry.register(
    name="linear_post_comment",
    toolset="linear",
    schema={
        "name": "linear_post_comment",
        "description": "Post a summary comment to a Linear ticket detailing actions, findings, and blockers.",
        "parameters": {
            "type": "object",
            "properties": {
                "ticket_id": {
                    "type": "string",
                    "description": "The Linear ticket ID."
                },
                "body": {
                    "type": "string",
                    "description": "The comment text."
                }
            },
            "required": ["ticket_id", "body"]
        }
    },
    handler=lambda args, **kw: linear_post_comment(args.get("ticket_id", ""), args.get("body", ""), kw.get("task_id")),
    check_fn=check_linear_requirements,
    requires_env=["LINEAR_API_KEY"],
)
