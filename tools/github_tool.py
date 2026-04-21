import json
import logging
import os
import requests
import base64
from typing import Dict, Any, Optional, List

from tools.registry import registry

logger = logging.getLogger(__name__)

GITHUB_API_BASE = "https://api.github.com"

def check_github_requirements() -> bool:
    """Check if GitHub integration is configured."""
    return bool(os.getenv("GITHUB_TOKEN"))

def _execute_github_request(method: str, path: str, data: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Helper to execute a GitHub REST API request."""
    token = os.getenv("GITHUB_TOKEN")
    default_headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
        "Content-Type": "application/json"
    }
    if headers:
        default_headers.update(headers)
        
    url = f"{GITHUB_API_BASE}/{path.lstrip('/')}"
    
    try:
        response = requests.request(method, url, headers=default_headers, json=data)
        response.raise_for_status()
        
        # Handle special response formats (like diff)
        if "application/vnd.github.v3.diff" in default_headers.get("Accept", ""):
            return {"success": True, "data": response.text}
            
        return {"success": True, "data": response.json() if response.text else {}}
    except Exception as e:
        logger.error(f"GitHub API Request Failed ({method} {path}): {e}")
        return {"success": False, "error": str(e)}

# -----------------------------------------------------------------------------
# Tool Handlers
# -----------------------------------------------------------------------------

def github_create_branch(repo: str, branch_name: str, base_branch: str = "main", task_id: str = None) -> str:
    """Create a new branch in a GitHub repository."""
    # 1. Get the SHA of the base branch
    ref_res = _execute_github_request("GET", f"repos/{repo}/git/ref/heads/{base_branch}")
    if not ref_res["success"]:
        return json.dumps(ref_res)
    
    sha = ref_res["data"]["object"]["sha"]
    
    # 2. Create the new reference
    payload = {
        "ref": f"refs/heads/{branch_name}",
        "sha": sha
    }
    result = _execute_github_request("POST", f"repos/{repo}/git/refs", data=payload)
    return json.dumps(result)

def github_open_pr(repo: str, title: str, head: str, base: str = "main", body: str = "", task_id: str = None) -> str:
    """Open a pull request on GitHub."""
    payload = {
        "title": title,
        "head": head,
        "base": base,
        "body": body
    }
    result = _execute_github_request("POST", f"repos/{repo}/pulls", data=payload)
    return json.dumps(result)

def github_read_diff(repo: str, pr_number: int, task_id: str = None) -> str:
    """Read the diff of a pull request."""
    headers = {"Accept": "application/vnd.github.v3.diff"}
    result = _execute_github_request("GET", f"repos/{repo}/pulls/{pr_number}", headers=headers)
    return json.dumps(result)

def github_resolve_conflict(repo: str, pr_number: int, file_path: str, resolution: str, task_id: str = None) -> str:
    """
    Resolve a merge conflict. In this simplified version, we just update the file on the PR branch.
    Requires fetching the PR info first to find the branch name.
    """
    # 1. Get PR info
    pr_res = _execute_github_request("GET", f"repos/{repo}/pulls/{pr_number}")
    if not pr_res["success"]: return json.dumps(pr_res)
    
    branch = pr_res["data"]["head"]["ref"]
    
    # 2. Get file SHA on that branch
    file_res = _execute_github_request("GET", f"repos/{repo}/contents/{file_path}?ref={branch}")
    sha = file_res["data"]["sha"] if file_res["success"] else None
    
    # 3. Update the file
    payload = {
        "message": f"Resolve conflicts in {file_path}",
        "content": base64.b64encode(resolution.encode()).decode(),
        "branch": branch
    }
    if sha: payload["sha"] = sha
    
    result = _execute_github_request("PUT", f"repos/{repo}/contents/{file_path}", data=payload)
    return json.dumps(result)

def github_assign_pr(repo: str, pr_number: int, assignee: str, task_id: str = None) -> str:
    """Assign a PR to a user."""
    payload = {"assignees": [assignee]}
    result = _execute_github_request("POST", f"repos/{repo}/issues/{pr_number}/assignees", data=payload)
    return json.dumps(result)

def github_post_review_comment(repo: str, pr_number: int, body: str, commit_id: str = None, path: str = None, line: int = None, task_id: str = None) -> str:
    """Post a review comment on a PR."""
    if commit_id and path and line:
        # Inline comment
        payload = {
            "body": body,
            "commit_id": commit_id,
            "path": path,
            "line": line
        }
        result = _execute_github_request("POST", f"repos/{repo}/pulls/{pr_number}/comments", data=payload)
    else:
        # General PR review/comment
        payload = {
            "event": "COMMENT",
            "body": body
        }
        result = _execute_github_request("POST", f"repos/{repo}/pulls/{pr_number}/reviews", data=payload)

    return json.dumps(result)


def github_approve_pr(repo: str, pr_number: int, body: str = "", task_id: str = None) -> str:
    """Approve a Pull Request."""
    payload = {
        "event": "APPROVE",
        "body": body or "Code review approved. Ready for QA."
    }
    result = _execute_github_request("POST", f"repos/{repo}/pulls/{pr_number}/reviews", data=payload)
    return json.dumps(result)


def github_merge_pr(repo: str, pr_number: int, task_id: str = None) -> str:
    """Merge a Pull Request into its base branch."""
    payload = {"merge_method": "merge"}
    result = _execute_github_request("PUT", f"repos/{repo}/pulls/{pr_number}/merge", data=payload)
    return json.dumps(result)


def github_update_pr(repo: str, pr_number: int, ready_for_review: bool = True, task_id: str = None) -> str:
    """Update a Pull Request — commonly used to convert a draft PR to ready for review.

    Set ready_for_review=True to mark the PR as OPEN (ready for review).
    Set ready_for_review=False to convert back to draft.
    """
    payload = {"draft": not ready_for_review}
    result = _execute_github_request("PATCH", f"repos/{repo}/pulls/{pr_number}", data=payload)
    return json.dumps(result)

def github_get_pr(repo: str, pr_number: int, task_id: str = None) -> str:
    """Get full details of a Pull Request including head/base branch info.

    Normalizes GitHub API fields to common names:
    - baseRefName: the base branch name (from base.ref)
    - headRefName: the head branch name (from head.ref)
    - isDraft: whether the PR is a draft
    """
    result = _execute_github_request("GET", f"repos/{repo}/pulls/{pr_number}")
    if isinstance(result, dict) and "success" not in result:
        # Normalize field names for easier access
        if "base" in result and isinstance(result["base"], dict):
            result["baseRefName"] = result["base"].get("ref")
            result["baseSha"] = result["base"].get("sha")
        if "head" in result and isinstance(result["head"], dict):
            result["headRefName"] = result["head"].get("ref")
            result["headSha"] = result["head"].get("sha")
        if "draft" in result:
            result["isDraft"] = result["draft"]
    return json.dumps(result)

def github_get_pr_checks(repo: str, pr_number: int, task_id: str = None) -> str:
    """Get the status of all CI checks (GitHub Actions, status checks) for a PR.

    This function first fetches the PR to get the head commit SHA, then retrieves
    the check runs for that commit.

    Returns a dict with a 'check_runs' list. Each check run has 'name', 'status',
    'conclusion', and 'html_url'.
    Status values: 'queued', 'in_progress', 'completed'.
    Conclusion values: 'success', 'failure', 'cancelled', 'action_required', 'timed_out',
                      'neutral', 'skipped', 'stale', or None (if status is not completed).
    """
    # First get the PR to find the head commit SHA
    pr_data = _execute_github_request("GET", f"repos/{repo}/pulls/{pr_number}")
    if isinstance(pr_data, dict) and "head" in pr_data and pr_data["head"] and "sha" in pr_data["head"]:
        head_sha = pr_data["head"]["sha"]
    elif isinstance(pr_data, dict) and "head_sha" in pr_data:
        head_sha = pr_data["head_sha"]
    else:
        return json.dumps({"error": "Could not determine PR head commit SHA", "pr_data": pr_data})

    # Get check runs for the head commit
    result = _execute_github_request("GET", f"repos/{repo}/commits/{head_sha}/check-runs")
    return json.dumps(result)

def github_add_label(repo: str, issue_number: int, labels: List[str], task_id: str = None) -> str:
    """Add labels to a PR or Issue. Used by Reviewer to trigger QA."""
    payload = {"labels": labels}
    result = _execute_github_request("POST", f"repos/{repo}/issues/{issue_number}/labels", data=payload)
    return json.dumps(result)

# -----------------------------------------------------------------------------
# Tool Registrations
# -----------------------------------------------------------------------------

registry.register(
    name="github_create_branch",
    toolset="github",
    schema={
        "name": "github_create_branch",
        "description": "Create a new branch in a repository.",
        "parameters": {
            "type": "object",
            "properties": {
                "repo": {"type": "string", "description": "Repository in format owner/repo."},
                "branch_name": {"type": "string", "description": "Name of the new branch."},
                "base_branch": {"type": "string", "description": "Branch to branch off from. Default is 'main'."}
            },
            "required": ["repo", "branch_name"]
        }
    },
    handler=lambda args, **kw: github_create_branch(args.get("repo", ""), args.get("branch_name", ""), args.get("base_branch", "main"), kw.get("task_id")),
    check_fn=check_github_requirements,
    requires_env=["GITHUB_TOKEN"],
)

registry.register(
    name="github_open_pr",
    toolset="github",
    schema={
        "name": "github_open_pr",
        "description": "Open a Pull Request.",
        "parameters": {
            "type": "object",
            "properties": {
                "repo": {"type": "string", "description": "Repository in format owner/repo."},
                "title": {"type": "string", "description": "Title of the PR."},
                "head": {"type": "string", "description": "The name of the branch where your changes are implemented."},
                "base": {"type": "string", "description": "The name of the branch you want the changes pulled into. Default is 'main'."},
                "body": {"type": "string", "description": "The contents of the pull request."}
            },
            "required": ["repo", "title", "head"]
        }
    },
    handler=lambda args, **kw: github_open_pr(args.get("repo", ""), args.get("title", ""), args.get("head", ""), args.get("base", "main"), args.get("body", ""), kw.get("task_id")),
    check_fn=check_github_requirements,
    requires_env=["GITHUB_TOKEN"],
)

registry.register(
    name="github_read_diff",
    toolset="github",
    schema={
        "name": "github_read_diff",
        "description": "Read the diff of a PR to analyze changes.",
        "parameters": {
            "type": "object",
            "properties": {
                "repo": {"type": "string", "description": "Repository in format owner/repo."},
                "pr_number": {"type": "integer", "description": "The PR number."}
            },
            "required": ["repo", "pr_number"]
        }
    },
    handler=lambda args, **kw: github_read_diff(args.get("repo", ""), args.get("pr_number", 0), kw.get("task_id")),
    check_fn=check_github_requirements,
    requires_env=["GITHUB_TOKEN"],
)

registry.register(
    name="github_resolve_conflict",
    toolset="github",
    schema={
        "name": "github_resolve_conflict",
        "description": "Resolve a merge conflict in a specific file of a PR.",
        "parameters": {
            "type": "object",
            "properties": {
                "repo": {"type": "string", "description": "Repository in format owner/repo."},
                "pr_number": {"type": "integer", "description": "The PR number."},
                "file_path": {"type": "string", "description": "Path to the conflicted file."},
                "resolution": {"type": "string", "description": "The resolved file content."}
            },
            "required": ["repo", "pr_number", "file_path", "resolution"]
        }
    },
    handler=lambda args, **kw: github_resolve_conflict(args.get("repo", ""), args.get("pr_number", 0), args.get("file_path", ""), args.get("resolution", ""), kw.get("task_id")),
    check_fn=check_github_requirements,
    requires_env=["GITHUB_TOKEN"],
)

registry.register(
    name="github_assign_pr",
    toolset="github",
    schema={
        "name": "github_assign_pr",
        "description": "Assign a Pull Request to a user.",
        "parameters": {
            "type": "object",
            "properties": {
                "repo": {"type": "string", "description": "Repository in format owner/repo."},
                "pr_number": {"type": "integer", "description": "The PR number."},
                "assignee": {"type": "string", "description": "GitHub username to assign."}
            },
            "required": ["repo", "pr_number", "assignee"]
        }
    },
    handler=lambda args, **kw: github_assign_pr(args.get("repo", ""), args.get("pr_number", 0), args.get("assignee", ""), kw.get("task_id")),
    check_fn=check_github_requirements,
    requires_env=["GITHUB_TOKEN"],
)

registry.register(
    name="github_post_review_comment",
    toolset="github",
    schema={
        "name": "github_post_review_comment",
        "description": "Post a review comment on a PR. Can be an overarching review or an inline comment.",
        "parameters": {
            "type": "object",
            "properties": {
                "repo": {"type": "string", "description": "Repository in format owner/repo."},
                "pr_number": {"type": "integer", "description": "The PR number."},
                "body": {"type": "string", "description": "The text of the review comment."},
                "commit_id": {"type": "string", "description": "Required for inline comment: The SHA of the commit needing a comment."},
                "path": {"type": "string", "description": "Required for inline comment: The relative path to the file that necessitates a comment."},
                "line": {"type": "integer", "description": "Required for inline comment: The line of the blob in the pull request diff that the comment applies to."}
            },
            "required": ["repo", "pr_number", "body"]
        }
    },
    handler=lambda args, **kw: github_post_review_comment(args.get("repo", ""), args.get("pr_number", 0), args.get("body", ""), args.get("commit_id"), args.get("path"), args.get("line"), kw.get("task_id")),
    check_fn=check_github_requirements,
    requires_env=["GITHUB_TOKEN"],
)

registry.register(
    name="github_approve_pr",
    toolset="github",
    schema={
        "name": "github_approve_pr",
        "description": "Approve a Pull Request. Required before the QA agent can merge.",
        "parameters": {
            "type": "object",
            "properties": {
                "repo": {"type": "string", "description": "Repository in format owner/repo."},
                "pr_number": {"type": "integer", "description": "The PR number to approve."},
                "body": {"type": "string", "description": "Optional review body message."}
            },
            "required": ["repo", "pr_number"]
        }
    },
    handler=lambda args, **kw: github_approve_pr(args.get("repo", ""), args.get("pr_number", 0), args.get("body", ""), kw.get("task_id")),
    check_fn=check_github_requirements,
    requires_env=["GITHUB_TOKEN"],
)

registry.register(
    name="github_merge_pr",
    toolset="github",
    schema={
        "name": "github_merge_pr",
        "description": "Merge a Pull Request into its base branch. Only call this after QA has verified the PR.",
        "parameters": {
            "type": "object",
            "properties": {
                "repo": {"type": "string", "description": "Repository in format owner/repo."},
                "pr_number": {"type": "integer", "description": "The PR number to merge."}
            },
            "required": ["repo", "pr_number"]
        }
    },
    handler=lambda args, **kw: github_merge_pr(args.get("repo", ""), args.get("pr_number", 0), kw.get("task_id")),
    check_fn=check_github_requirements,
    requires_env=["GITHUB_TOKEN"],
)

registry.register(
    name="github_get_pr",
    toolset="github",
    schema={
        "name": "github_get_pr",
        "description": "Get full details of a Pull Request including base/head branch, mergeability, and state.",
        "parameters": {
            "type": "object",
            "properties": {
                "repo": {"type": "string", "description": "Repository in format owner/repo."},
                "pr_number": {"type": "integer", "description": "The PR number."}
            },
            "required": ["repo", "pr_number"]
        }
    },
    handler=lambda args, **kw: github_get_pr(args.get("repo", ""), args.get("pr_number", 0), kw.get("task_id")),
    check_fn=check_github_requirements,
    requires_env=["GITHUB_TOKEN"],
)

registry.register(
    name="github_update_pr",
    toolset="github",
    schema={
        "name": "github_update_pr",
        "description": "Update a Pull Request. Most commonly used to convert a draft PR to OPEN (ready for review) by setting ready_for_review=True. Can also convert an open PR back to draft by setting ready_for_review=False.",
        "parameters": {
            "type": "object",
            "properties": {
                "repo": {"type": "string", "description": "Repository in format owner/repo."},
                "pr_number": {"type": "integer", "description": "The PR number to update."},
                "ready_for_review": {"type": "boolean", "description": "Set True to convert draft PR to open (ready for review). Set False to convert back to draft. Default is True."}
            },
            "required": ["repo", "pr_number"]
        }
    },
    handler=lambda args, **kw: github_update_pr(args.get("repo", ""), args.get("pr_number", 0), args.get("ready_for_review", True), kw.get("task_id")),
    check_fn=check_github_requirements,
    requires_env=["GITHUB_TOKEN"],
)

registry.register(
    name="github_get_pr_checks",
    toolset="github",
    schema={
        "name": "github_get_pr_checks",
        "description": "Get the status of all CI checks (GitHub Actions, status checks) for a Pull Request. Returns a list of checks with their name, status (queued/in_progress/completed), and conclusion (success/failure/cancelled/timed_out/etc.). This is the mandatory gate — Reviewer MUST verify that 'test-frontend' and 'test-backend' checks have passed before doing code review.",
        "parameters": {
            "type": "object",
            "properties": {
                "repo": {"type": "string", "description": "Repository in format owner/repo."},
                "pr_number": {"type": "integer", "description": "The PR number."}
            },
            "required": ["repo", "pr_number"]
        }
    },
    handler=lambda args, **kw: github_get_pr_checks(args.get("repo", ""), args.get("pr_number", 0), kw.get("task_id")),
    check_fn=check_github_requirements,
    requires_env=["GITHUB_TOKEN"],
)

registry.register(
    name="github_add_label",
    toolset="github",
    schema={
        "name": "github_add_label",
        "description": "Add labels to a Pull Request or Issue. Use 'ready-for-qa' to trigger the QA agent.",
        "parameters": {
            "type": "object",
            "properties": {
                "repo": {"type": "string", "description": "Repository in format owner/repo."},
                "issue_number": {"type": "integer", "description": "The PR or Issue number."},
                "labels": {"type": "array", "items": {"type": "string"}, "description": "List of labels to add."}
            },
            "required": ["repo", "issue_number", "labels"]
        }
    },
    handler=lambda args, **kw: github_add_label(args.get("repo", ""), args.get("issue_number", 0), args.get("labels", []), kw.get("task_id")),
    check_fn=check_github_requirements,
    requires_env=["GITHUB_TOKEN"],
)
