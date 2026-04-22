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
    """Read details of a Linear ticket.

    Accepts either a Linear GlobalID (e.g., '969b83a8-0682-42f9-955e-68202c3b6c03')
    or a human-readable identifier (e.g., 'FAW-26').

    When given a human-readable identifier, resolves it via the team key and issue number:
    - Extracts the team key (e.g., 'FAW') and issue number (e.g., '26') from 'FAW-26'
    - Queries teams to find the team's GlobalID
    - Filters issues by team.id + number
    """
    # Normalize: if ticket_id looks like a GlobalID (contains dashes, 36+ chars), use it directly
    if isinstance(ticket_id, str) and len(ticket_id) >= 32 and "-" in ticket_id:
        issue_id = ticket_id
    elif isinstance(ticket_id, str) and "-" in ticket_id:
        # Human-readable identifier like 'FAW-26' — resolve to GlobalID via team + number
        parts = ticket_id.split("-", 1)
        team_key = parts[0]
        issue_number = parts[1] if len(parts) > 1 else None
        if issue_number:
            issue_id = _resolve_issue_id_by_team_and_number(team_key, issue_number)
            if not issue_id:
                return json.dumps({"success": False, "error": f"Could not resolve {ticket_id} to a Linear issue ID"})
        else:
            return json.dumps({"success": False, "error": f"Invalid ticket ID format: {ticket_id}"})
    else:
        return json.dumps({"success": False, "error": f"Invalid ticket ID format: {ticket_id}"})

    # NOTE: pullRequest field does NOT exist on Issue type in Linear's API.
    # PR info must be obtained from: (a) attachments (URLs contain PR number), or
    # (b) github_get_pr directly using PR number from attachment URL.
    # NOTE: project.slug does not exist — use project { id name } only.
    # NOTE: attachments.contentType causes 400 — use { id title url } only.
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
        team {
          id
          name
          key
        }
        project {
          id
          name
        }
        labels {
          nodes {
            id
            name
          }
        }
        attachments {
          nodes {
            id
            title
            url
          }
        }
        comments {
          nodes {
            id
            body
            user { name }
            createdAt
          }
        }
      }
    }
    """
    result = _execute_linear_query(query, {"id": issue_id})
    return json.dumps(result)


def _resolve_issue_id_by_team_and_number(team_key: str, issue_number: str) -> str:
    """Resolve a Linear issue GlobalID from team key + issue number (e.g., 'FAW' + '26' -> GlobalID).

    Uses linear_search_tickets (which fetches recent issues) and filters client-side by identifier.
    Falls back to fetching all issues from a team and filtering.
    """
    import re
    target_identifier = f"{team_key}-{issue_number}"

    # Try linear_search_tickets approach (fetches recent issues with identifier field)
    search_query = """
    query Issues($first: Int!) {
      issues(first: $first) {
        nodes { id identifier number }
      }
    }
    """
    result = _execute_linear_query(search_query, {"first": 100})
    if result.get("success"):
        issues = result.get("data", {}).get("issues", {}).get("nodes", [])
        for issue in issues:
            if issue.get("identifier") == target_identifier:
                return issue.get("id")

    # Fallback: get all teams, find the team, then fetch issues by team.id
    teams_query = """
    query Teams($first: Int!) {
      teams(first: $first) {
        nodes { id key }
      }
    }
    """
    teams_result = _execute_linear_query(teams_query, {"first": 20})
    if teams_result.get("success"):
        teams = teams_result.get("data", {}).get("teams", {}).get("nodes", [])
        team_id = next((t.get("id") for t in teams if t.get("key") == team_key), None)
        if team_id:
            # Fetch a small batch of issues from this team (updated recently)
            team_issues_query = """
            query TeamIssues($first: Int!, $filter: IssueFilter!) {
              issues(first: $first, filter: $filter) {
                nodes { id identifier number }
              }
            }
            """
            team_result = _execute_linear_query(
                team_issues_query,
                {"first": 50, "filter": {"team": {"id": {"in": [team_id]}}}}
            )
            if team_result.get("success"):
                team_issues = team_result.get("data", {}).get("issues", {}).get("nodes", [])
                for issue in team_issues:
                    if issue.get("identifier") == target_identifier:
                        return issue.get("id")

    return None

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

def linear_search_tickets(query_string: str = "", state_name: str = "", limit: int = 10, task_id: str = None) -> str:
    """Search for tickets in Linear to understand dependencies and current work."""
    # Linear's issueSearch or simple issues query with filters
    # For simplicity, we'll query issues with an optional state filter
    query = """
    query Issues($first: Int!) {
      issues(first: $first, orderBy: updatedAt) {
        nodes {
          identifier
          title
          priority
          state { name }
          project { name }
        }
      }
    }
    """
    # Note: A real implementation would use advanced filters based on `query_string` and `state_name`
    # This provides a basic overview of recent/active issues
    result = _execute_linear_query(query, {"first": limit})
    if result.get("success"):
        issues = result["data"]["issues"]["nodes"]
        if state_name:
            issues = [i for i in issues if i.get("state", {}).get("name", "").lower() == state_name.lower()]
        if query_string:
            issues = [i for i in issues if query_string.lower() in i.get("title", "").lower() or query_string.lower() in i.get("identifier", "").lower()]
        return json.dumps({"success": True, "data": issues})
    return json.dumps(result)

def linear_get_projects(limit: int = 5, task_id: str = None) -> str:
    """Get active projects and roadmap context."""
    query = """
    query Projects($first: Int!) {
      projects(first: $first, filter: { state: { in: ["started", "planned", "backlog"] } }) {
        nodes {
          name
          description
          state
          progress
          targetDate
        }
      }
    }
    """
    result = _execute_linear_query(query, {"first": limit})
    return json.dumps(result)

# -----------------------------------------------------------------------------
# Tool Registrations
# -----------------------------------------------------------------------------

registry.register(
    name="linear_search_tickets",
    toolset="linear",
    schema={
        "name": "linear_search_tickets",
        "description": "Search for other tickets to understand dependencies, backlog, or in-progress work.",
        "parameters": {
            "type": "object",
            "properties": {
                "query_string": {"type": "string", "description": "Optional keyword to search in title or ID."},
                "state_name": {"type": "string", "description": "Optional state to filter by (e.g., 'Backlog', 'In Progress')."},
                "limit": {"type": "integer", "description": "Max number of tickets to return (default 10)."}
            }
        }
    },
    handler=lambda args, **kw: linear_search_tickets(args.get("query_string", ""), args.get("state_name", ""), args.get("limit", 10), kw.get("task_id")),
    check_fn=check_linear_requirements,
    requires_env=["LINEAR_API_KEY"],
)

registry.register(
    name="linear_get_projects",
    toolset="linear",
    schema={
        "name": "linear_get_projects",
        "description": "Get active projects and their progress to understand the roadmap and strategic priorities.",
        "parameters": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Max number of projects to return (default 5)."}
            }
        }
    },
    handler=lambda args, **kw: linear_get_projects(args.get("limit", 5), kw.get("task_id")),
    check_fn=check_linear_requirements,
    requires_env=["LINEAR_API_KEY"],
)

registry.register(
    name="linear_read_ticket",
    toolset="linear",
    schema={
        "name": "linear_read_ticket",
        "description": "Read full details of a Linear ticket including team/project context, pullRequest with base/head branches, attachments (PR URLs), and recent comments. Use this as the first step to gather all context about a ticket.",
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
                    "description": (
                        "The new Linear workflow state name for this issue's team (must exist on the team). "
                        "Pipeline examples: 'Todo', 'In Progress', 'In Review', 'Ready For QA', 'Ready For Delivery'. "
                        "Human-only in this SDLC — agents must not set: 'Approved For Delivery', 'Done' (production)."
                    )
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
        "description": "Post a comment to a Linear ticket. Always include a clear summary of what you did, what you found, and what the next steps are. Use this to keep a record of agent actions.",
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

