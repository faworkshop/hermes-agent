import json
import logging
import os
from typing import Dict, Any

from tools.registry import registry

logger = logging.getLogger(__name__)

def check_linear_requirements() -> bool:
    """Check if Linear integration is configured."""
    return bool(os.getenv("LINEAR_API_KEY"))

# -----------------------------------------------------------------------------
# Tool Handlers
# -----------------------------------------------------------------------------

def linear_read_ticket(ticket_id: str, task_id: str = None) -> str:
    """Read details of a Linear ticket."""
    if not check_linear_requirements():
        return json.dumps({"success": False, "error": "LINEAR_API_KEY not set"})
    
    # TODO: Implement actual Linear API call
    return json.dumps({
        "success": True,
        "data": {
            "id": ticket_id,
            "title": "Example Ticket",
            "description": "This is a placeholder for the actual Linear ticket.",
            "state": "In Progress",
            "priority": 1
        }
    })

def linear_read_comments(ticket_id: str, task_id: str = None) -> str:
    """Read comments from a Linear ticket."""
    if not check_linear_requirements():
        return json.dumps({"success": False, "error": "LINEAR_API_KEY not set"})
    
    # TODO: Implement actual Linear API call
    return json.dumps({
        "success": True,
        "data": [
            {"body": "This ticket was created automatically.", "user": "System"}
        ]
    })

def linear_update_status(ticket_id: str, status: str, task_id: str = None) -> str:
    """Update the status of a Linear ticket."""
    if not check_linear_requirements():
        return json.dumps({"success": False, "error": "LINEAR_API_KEY not set"})
    
    # TODO: Implement actual Linear API call
    return json.dumps({"success": True, "message": f"Ticket {ticket_id} moved to {status}."})

def linear_update_priority(ticket_id: str, priority: int, task_id: str = None) -> str:
    """Update the priority of a Linear ticket."""
    if not check_linear_requirements():
        return json.dumps({"success": False, "error": "LINEAR_API_KEY not set"})
    
    # TODO: Implement actual Linear API call
    return json.dumps({"success": True, "message": f"Ticket {ticket_id} priority set to {priority}."})

def linear_assign_user(ticket_id: str, user_id: str, task_id: str = None) -> str:
    """Assign a Linear ticket to a user."""
    if not check_linear_requirements():
        return json.dumps({"success": False, "error": "LINEAR_API_KEY not set"})
    
    # TODO: Implement actual Linear API call
    return json.dumps({"success": True, "message": f"Ticket {ticket_id} assigned to {user_id}."})

def linear_add_label(ticket_id: str, label_id: str, task_id: str = None) -> str:
    """Add a label to a Linear ticket."""
    if not check_linear_requirements():
        return json.dumps({"success": False, "error": "LINEAR_API_KEY not set"})
    
    # TODO: Implement actual Linear API call
    return json.dumps({"success": True, "message": f"Added label {label_id} to ticket {ticket_id}."})

def linear_post_comment(ticket_id: str, body: str, task_id: str = None) -> str:
    """Post a comment to a Linear ticket."""
    if not check_linear_requirements():
        return json.dumps({"success": False, "error": "LINEAR_API_KEY not set"})
    
    # TODO: Implement actual Linear API call
    return json.dumps({"success": True, "message": f"Comment posted on ticket {ticket_id}."})

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