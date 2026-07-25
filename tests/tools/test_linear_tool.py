"""Tests for tools/linear_tool.py — focused on label name → UUID resolution.

Regression suite for PTD-169 / PTD-168: ``linear_add_label`` previously
silently forwarded an unresolvable name to Linear, which rejected the
mutation and made the QA agent escalate a perfectly good ticket to
FAIL_ENVIRONMENT. The tool now returns ``resolved: false`` on the failure
path so QAs can detect it and treat the bookkeeping label as decorative.
"""

import json
import sys
from pathlib import Path
from unittest.mock import patch as mock_patch

# Make sure the in-repo 'tools' package is importable when pytest is run
# from the project root without an editable install.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import tools.linear_tool as linear_tool  # noqa: E402


# ---- shared fixtures --------------------------------------------------------

TEAM_LABELS = [
    {"id": "label-uuid-frontend", "name": "qa-frontend-only"},
    {"id": "label-uuid-backend", "name": "qa-backend-only"},
    {"id": "label-uuid-fullstack", "name": "qa-fullstack"},
    {"id": "label-uuid-needs-human", "name": "needs-human"},
]


def _issue_labels_response(current_label_ids):
    """Build the team/issue GraphQL response dispatched by linear_add_label."""
    return {
        "success": True,
        "data": {
            "issue": {
                "labels": {"nodes": [{"id": i} for i in current_label_ids]},
                "team": {"labels": {"nodes": TEAM_LABELS}},
            }
        },
    }


def _mutate_response(success: bool = True):
    return {
        "success": success,
        "data": {"issueUpdate": {"success": success}},
    }


# ---- the actual tests -------------------------------------------------------


class TestLinearAddLabelResolution:
    """Name → UUID resolution and the unresolved-label failure path."""

    def test_name_resolves_to_uuid_and_applies(self):
        """A name that exists on the team resolves to its UUID and is applied."""
        captured_mutation = {}

        def fake_query(query, variables=None):
            if "IssueLabels" in query:
                return _issue_labels_response(current_label_ids=[])
            if "IssueUpdate" in query:
                captured_mutation["variables"] = variables
                return _mutate_response(success=True)
            raise AssertionError(f"unexpected query: {query}")

        with mock_patch.object(linear_tool, "_execute_linear_query", side_effect=fake_query):
            result = linear_tool.linear_add_label("PTD-169", "qa-frontend-only")

        payload = json.loads(result)
        assert payload["success"] is True
        assert payload["resolved"] is True
        assert payload["applied_label_id"] == "label-uuid-frontend"
        # Mutation sent the resolved UUID, NOT the raw name string.
        sent_ids = captured_mutation["variables"]["input"]["labelIds"]
        assert "label-uuid-frontend" in sent_ids
        assert "qa-frontend-only" not in sent_ids

    def test_uuid_passes_through_unchanged(self):
        """A valid UUID that matches a team label is accepted as-is."""
        captured_mutation = {}

        def fake_query(query, variables=None):
            if "IssueLabels" in query:
                return _issue_labels_response(current_label_ids=[])
            if "IssueUpdate" in query:
                captured_mutation["variables"] = variables
                return _mutate_response(success=True)
            raise AssertionError(f"unexpected query: {query}")

        with mock_patch.object(linear_tool, "_execute_linear_query", side_effect=fake_query):
            result = linear_tool.linear_add_label("PTD-169", "label-uuid-backend")

        payload = json.loads(result)
        assert payload["success"] is True
        assert payload["resolved"] is True
        assert "label-uuid-backend" in captured_mutation["variables"]["input"]["labelIds"]

    def test_name_is_case_insensitive(self):
        """Name resolution is case-insensitive — QA prompts may type 'QA-FullStack'."""
        captured_mutation = {}

        def fake_query(query, variables=None):
            if "IssueLabels" in query:
                return _issue_labels_response(current_label_ids=[])
            if "IssueUpdate" in query:
                captured_mutation["variables"] = variables
                return _mutate_response(success=True)
            raise AssertionError(f"unexpected query: {query}")

        with mock_patch.object(linear_tool, "_execute_linear_query", side_effect=fake_query):
            result = linear_tool.linear_add_label(
                "PTD-169", "QA-FullStack"
            )

        payload = json.loads(result)
        assert payload["resolved"] is True
        assert payload["applied_label_id"] == "label-uuid-fullstack"

    def test_unknown_name_returns_resolved_false_no_mutation(self):
        """PTD-168 regression: unknown name short-circuits with resolved=False
        and does NOT call issueUpdate. The QA agent's bookkeeping label
        failure path must see this clearly."""
        mutation_called = {"count": 0}

        def fake_query(query, variables=None):
            if "IssueLabels" in query:
                return _issue_labels_response(current_label_ids=[])
            if "IssueUpdate" in query:
                mutation_called["count"] += 1
                return _mutate_response(success=True)
            raise AssertionError(f"unexpected query: {query}")

        with mock_patch.object(linear_tool, "_execute_linear_query", side_effect=fake_query):
            result = linear_tool.linear_add_label("PTD-169", "qa-this-label-does-not-exist")

        payload = json.loads(result)
        assert payload["success"] is False
        assert payload["resolved"] is False
        assert payload["missing_label"] == "qa-this-label-does-not-exist"
        assert payload["reason"] == "label_not_found_on_team"
        assert payload["team_label_count"] == len(TEAM_LABELS)
        # Crucially: no GraphQL mutation was issued.
        assert mutation_called["count"] == 0

    def test_unknown_uuid_also_returns_resolved_false(self):
        """A raw UUID that doesn't match any team label also returns resolved=False."""
        orphan_uuid = "00000000-0000-0000-0000-000000000000"
        mutation_called = {"count": 0}

        def fake_query(query, variables=None):
            if "IssueLabels" in query:
                return _issue_labels_response(current_label_ids=[])
            if "IssueUpdate" in query:
                mutation_called["count"] += 1
                return _mutate_response(success=True)
            raise AssertionError(f"unexpected query: {query}")

        with mock_patch.object(linear_tool, "_execute_linear_query", side_effect=fake_query):
            result = linear_tool.linear_add_label("PTD-169", orphan_uuid)

        payload = json.loads(result)
        assert payload["success"] is False
        assert payload["resolved"] is False
        assert payload["missing_label"] == orphan_uuid
        assert mutation_called["count"] == 0

    def test_idempotent_re_add_does_not_double_post(self):
        """Re-adding a label that's already on the ticket still returns success
        and the mutation is sent with the same labelIds list (Linear treats
        this as a no-op rather than rejecting)."""
        captured_mutation = {}

        def fake_query(query, variables=None):
            if "IssueLabels" in query:
                # Label is already on the ticket.
                return _issue_labels_response(
                    current_label_ids=["label-uuid-frontend"]
                )
            if "IssueUpdate" in query:
                captured_mutation["variables"] = variables
                return _mutate_response(success=True)
            raise AssertionError(f"unexpected query: {query}")

        with mock_patch.object(linear_tool, "_execute_linear_query", side_effect=fake_query):
            result = linear_tool.linear_add_label("PTD-169", "qa-frontend-only")

        payload = json.loads(result)
        assert payload["success"] is True
        assert payload["resolved"] is True
        # Linear still receives the labelIds list — the label is referenced
        # once (idempotent dedup).
        sent_ids = captured_mutation["variables"]["input"]["labelIds"]
        assert sent_ids.count("label-uuid-frontend") == 1

    def test_initial_query_failure_surfaces_resolved_false(self):
        """If the IssueLabels lookup itself fails (network, auth), the
        failure also carries resolved=False so callers don't think the
        label was applied."""
        def fake_query(query, variables=None):
            return {"success": False, "error": "401 unauthorized"}

        with mock_patch.object(linear_tool, "_execute_linear_query", side_effect=fake_query):
            result = linear_tool.linear_add_label("PTD-169", "qa-frontend-only")

        payload = json.loads(result)
        assert payload["success"] is False
        assert payload["resolved"] is False
        assert payload["missing_label"] == "qa-frontend-only"
        assert "401" in payload.get("error", "")
