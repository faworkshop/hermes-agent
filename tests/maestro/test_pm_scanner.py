"""Unit tests for the PM intake scanner candidate filter in maestro/webhook_server.py.

The scanner returns only tickets in PM-intake states (Backlog, New, Todo,
Unstarted, Triage) that ALSO have the ``AI-Ready`` label. Non-AI-Ready
tickets are out of scope — the human adds AI-Ready when they want PM to
act.

Run with the FAW Workshop venv:
    ./venv/bin/python -m pytest tests/maestro/test_pm_scanner.py -v
"""
import os
import sys


# Allow importing the webhook_server module directly without spinning up
# the full FastAPI app (which needs FAW_DB_URL and other infra at import time).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


def _make_issue(identifier, state_name, label_names):
    """Build a minimal issue node shaped like the Linear API response."""
    return {
        "id": f"uuid-{identifier}",
        "identifier": identifier,
        "state": {"name": state_name},
        "labels": {"nodes": [{"name": n} for n in label_names]},
    }


def _patched_find(monkeypatch, issues):
    """Import the production function with ``_linear_gql`` stubbed to return
    a fixed list of issues. Returns ``(find, ws)`` so the test can assert.
    """
    from maestro import webhook_server as ws
    # Force token presence so the function doesn't bail on the first guard.
    monkeypatch.setattr(ws, "LINEAR_API_KEY", "lin_api_key_for_test")
    monkeypatch.setattr(
        ws, "_linear_gql", lambda query, variables=None: {"issues": {"nodes": issues}}
    )
    return ws._find_pm_intake_candidates, ws


class TestFindPmIntakeCandidates:
    def test_includes_ai_ready_ticket_in_todo(self, monkeypatch):
        find, _ = _patched_find(
            monkeypatch,
            [_make_issue("PTD-100", "Todo", ["AI-Ready", "Feature"])],
        )
        candidates = find(50)
        assert len(candidates) == 1
        assert candidates[0]["identifier"] == "PTD-100"

    def test_excludes_non_ai_ready_ticket(self, monkeypatch):
        """The change under test: PM scanner ignores non-AI-Ready tickets."""
        find, _ = _patched_find(
            monkeypatch,
            [_make_issue("PTD-101", "Todo", ["Feature"])],
        )
        assert find(50) == []

    def test_excludes_ticket_in_non_intake_state_even_with_ai_ready(self, monkeypatch):
        """State filter is unchanged — non-intake states still excluded."""
        find, _ = _patched_find(
            monkeypatch,
            [_make_issue("PTD-102", "In Progress", ["AI-Ready"])],
        )
        assert find(50) == []

    def test_mixed_candidates_returns_only_ai_ready(self, monkeypatch):
        find, _ = _patched_find(
            monkeypatch,
            [
                _make_issue("PTD-110", "Todo", ["AI-Ready", "Feature"]),  # included
                _make_issue("PTD-111", "Todo", ["Feature"]),  # excluded: no AI-Ready
                _make_issue("PTD-112", "Triage", ["AI-Ready", "Bug"]),  # included
                _make_issue("PTD-113", "Triage", []),  # excluded: no AI-Ready
                _make_issue("PTD-114", "In Progress", ["AI-Ready"]),  # excluded: wrong state
                _make_issue("PTD-115", "Backlog", ["AI-Ready"]),  # included
            ],
        )
        result = find(50)
        identifiers = sorted(c["identifier"] for c in result)
        assert identifiers == ["PTD-110", "PTD-112", "PTD-115"]

    def test_ai_ready_label_case_insensitive(self, monkeypatch):
        """Label match is case-folded — 'ai-ready' and 'AI-Ready' both pass."""
        find, _ = _patched_find(
            monkeypatch,
            [_make_issue("PTD-120", "Todo", ["ai-ready"])],
        )
        assert len(find(50)) == 1

    def test_tickets_with_needs_human_still_returned(self, monkeypatch):
        """The candidate fetcher does not filter needs-human; the worker loop
        does. The fetcher's job is state + AI-Ready only."""
        find, _ = _patched_find(
            monkeypatch,
            [_make_issue("PTD-130", "Todo", ["AI-Ready", "needs-human"])],
        )
        assert len(find(50)) == 1

    def test_no_linear_api_key_returns_empty(self, monkeypatch):
        from maestro import webhook_server as ws
        monkeypatch.setattr(ws, "LINEAR_API_KEY", "")
        assert ws._find_pm_intake_candidates(50) == []

    def test_empty_response_returns_empty(self, monkeypatch):
        find, _ = _patched_find(monkeypatch, [])
        assert find(50) == []
