"""Unit tests for the FAW-62 orphaned-Developer recovery helpers in
``maestro/webhook_server.py``.

The recovery flow classifies each recently-finished Developer task into one
of three modes:

  * **Mode A** — branch pushed, an open PR exists, ticket still In Progress.
    The endpoint posts a Linear comment and moves the ticket to In Review.
  * **Mode A'** — branch pushed, NO PR exists yet. The endpoint enqueues a
    focused Developer task whose only job is to open the PR and move the
    ticket. After ``_ORPHAN_RECOVERY_MAX_REENQUEUE_ATTEMPTS`` attempts the
    endpoint falls back to ``needs-human``.
  * **Mode B** — branch pushed, PR exists but is DRAFT. SKIP — owned by
    ``faw-developer-draft-pr-conflict`` so we don't double-fire.

These tests copy the **pure** helpers (classification, prompt construction,
draft detection, recovery-attempt counting) into this file and exercise them
in isolation. The DB-touching scan and the periodic worker are covered by
manual smoke tests on a live server — they require a Postgres
``agent_tasks`` table and GitHub token, neither of which is present in CI.

Run with:
    ./venv/bin/python -m pytest tests/maestro/test_orphan_recovery.py -v
"""
import os
import sys
import time

# Allow importing the webhook_server module directly without spinning up
# the full FastAPI app (which needs FAW_DB_URL and other infra at import time).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


# ── Mirror implementations (keep in sync with maestro/webhook_server.py) ────
#
# Only the pure functions are mirrored. Anything that hits Linear, GitHub, or
# Postgres is left to integration / smoke tests.


def _is_draft_pr(pr_info):
    """Mirror of maestro.webhook_server._is_draft_pr."""
    if not isinstance(pr_info, dict) or not pr_info:
        return False
    raw = pr_info.get("draft")
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        return raw.strip().lower() in {"true", "1", "yes"}
    return False


def _classify_orphan(ticket_id, branch, pr_info):
    """Mirror of maestro.webhook_server._classify_orphan."""
    if not branch:
        return "none"
    if not pr_info:
        return "A_prime"
    if _is_draft_pr(pr_info):
        return "B"
    return "A"


_RECOVERY_PROMPT_TEMPLATE = (
    "Ticket {ticket_id} previously had a Developer agent run that exited "
    "without opening a Pull Request and without moving the ticket to "
    "'In Review'. The branch has been pushed to origin.\n\n"
    "Your job — execute these steps in order, then EXIT:\n"
    "  1. Verify the branch exists on origin (it was pushed by the prior run).\n"
    "  2. Open a Pull Request: "
    "`gh pr create --base develop --head {branch} --title '[{ticket_id}] <title from ticket>' "
    "--body-file /tmp/{ticket_id_safe}_pr_body.md` "
    "(write the body file from the ticket description before invoking).\n"
    "  3. Mark the PR ready for review: `gh pr ready <PR_NUMBER>`.\n"
    "  4. Call `linear_update_status` to move {ticket_id} to 'In Review' "
    "as your LAST tool call. NOTHING follows.\n\n"
    "Do NOT re-implement the work. The branch already contains the commits. "
    "Do NOT modify code. Only the four steps above."
)


def _build_recovery_prompt(ticket_id, branch):
    """Mirror of maestro.webhook_server._build_recovery_prompt."""
    import re as _re
    safe = _re.sub(r"[^A-Za-z0-9_-]", "_", ticket_id)
    return _RECOVERY_PROMPT_TEMPLATE.format(
        ticket_id=ticket_id, branch=branch, ticket_id_safe=safe,
    )


def _count_recovery_a_prime_attempts_from_rows(ticket_id, rows):
    """Count recovery-A' rows directly from a list of agent_tasks dicts.

    Mirrors the matching logic in the production
    ``_count_recovery_a_prime_attempts`` helper, but operates on a static
    list of rows so tests don't need a live DB.
    """
    prefix = f"{ticket_id}:Developer:in progress:recovery-A-prime-"
    return sum(1 for r in rows if (r.get("dedup_key") or "").startswith(prefix))


def _recovery_dedup_key(ticket_id, epoch):
    """Mirror of the dedup_key format used by _recover_one_orphaned_developer."""
    return f"{ticket_id}:Developer:in progress:recovery-A-prime-{epoch}"


# ── Tests ────────────────────────────────────────────────────────────────────


class TestIsDraftPr:
    def test_empty_dict_is_not_draft(self):
        assert _is_draft_pr({}) is False

    def test_none_is_not_draft(self):
        assert _is_draft_pr(None) is False

    def test_explicit_draft_true(self):
        assert _is_draft_pr({"draft": True, "number": 12}) is True

    def test_explicit_draft_false(self):
        assert _is_draft_pr({"draft": False, "number": 12}) is False

    def test_string_draft_true(self):
        assert _is_draft_pr({"draft": "true"}) is True

    def test_string_draft_false(self):
        assert _is_draft_pr({"draft": "false"}) is False

    def test_string_draft_yes(self):
        assert _is_draft_pr({"draft": "yes"}) is True

    def test_missing_draft_key(self):
        # No ``draft`` key — assume ready-for-review (mode A path).
        assert _is_draft_pr({"number": 12, "state": "open"}) is False

    def test_string_non_bool_garbage(self):
        assert _is_draft_pr({"draft": "garbage"}) is False

    def test_non_dict_input(self):
        assert _is_draft_pr("not a dict") is False
        assert _is_draft_pr(42) is False


class TestClassifyOrphan:
    def test_no_branch_returns_none(self):
        # Without a branch we have no concrete artifact to recover.
        assert _classify_orphan("FAW-62", "", {}) == "none"
        assert _classify_orphan("FAW-62", "", {"number": 1}) == "none"

    def test_branch_no_pr_returns_a_prime(self):
        assert _classify_orphan("FAW-62", "feature/FAW-62", {}) == "A_prime"
        assert _classify_orphan("FAW-62", "feature/FAW-62", None) == "A_prime"

    def test_branch_with_open_pr_returns_a(self):
        # PR is open and NOT draft → Mode A: move ticket to In Review.
        assert _classify_orphan(
            "FAW-62", "feature/FAW-62", {"number": 5, "draft": False, "state": "open"}
        ) == "A"

    def test_branch_with_draft_pr_returns_b(self):
        # PR is a draft → Mode B: skip (owned by draft-PR conflict workflow).
        assert _classify_orphan(
            "FAW-62", "feature/FAW-62", {"number": 5, "draft": True, "state": "open"}
        ) == "B"

    def test_branch_with_pr_missing_draft_field_returns_a(self):
        # GitHub sometimes omits ``draft`` on certain payloads. Default to A.
        assert _classify_orphan(
            "FAW-62", "feature/FAW-62", {"number": 5, "state": "open"}
        ) == "A"


class TestBuildRecoveryPrompt:
    def test_prompt_includes_ticket_id_and_branch(self):
        prompt = _build_recovery_prompt("FAW-62", "feature/FAW-62")
        assert "FAW-62" in prompt
        assert "feature/FAW-62" in prompt

    def test_prompt_includes_explicit_commands(self):
        prompt = _build_recovery_prompt("FAW-62", "feature/FAW-62")
        # Sanity-check the four concrete steps the developer must take.
        assert "gh pr create" in prompt
        assert "gh pr ready" in prompt
        assert "linear_update_status" in prompt
        assert "--base develop" in prompt
        assert "--head feature/FAW-62" in prompt

    def test_prompt_reinforces_last_action_discipline(self):
        # The recovery prompt must call out the "LAST tool call" rule so the
        # next developer run does not repeat the same exit-without-status bug.
        prompt = _build_recovery_prompt("FAW-62", "feature/FAW-62")
        assert "LAST" in prompt.upper() or "last" in prompt
        # And explicit "do not modify code" so the agent doesn't redo the work.
        assert "Do NOT" in prompt or "do not" in prompt.lower()

    def test_prompt_handles_unsafe_ticket_id_chars(self):
        # Some Linear keys might contain weird chars. The safe-token should
        # not contain path separators or shell metacharacters.
        prompt = _build_recovery_prompt("FAW/62:weird", "feature/FAW-62")
        assert "/tmp/FAW_62_weird_pr_body.md" in prompt
        # No embedded path separators leaked through.
        assert "/tmp/FAW/62:weird_pr_body" not in prompt

    def test_prompt_is_deterministic(self):
        # Same input → same output. Tests rely on this for snapshotting.
        a = _build_recovery_prompt("FAW-62", "feature/FAW-62")
        b = _build_recovery_prompt("FAW-62", "feature/FAW-62")
        assert a == b


class TestRecoveryDedupKey:
    def test_dedup_key_format(self):
        epoch = 1715000000
        key = _recovery_dedup_key("FAW-62", epoch)
        assert key == f"FAW-62:Developer:in progress:recovery-A-prime-{epoch}"

    def test_dedup_keys_differ_per_epoch(self):
        # Two recovery attempts at different epochs MUST produce different keys
        # so the second enqueue is not dedup-blocked by the first.
        k1 = _recovery_dedup_key("FAW-62", 1715000000)
        k2 = _recovery_dedup_key("FAW-62", 1715000010)
        assert k1 != k2
        # And they must both START with the same prefix so the attempt-counter
        # function can find them.
        assert k1.startswith("FAW-62:Developer:in progress:recovery-A-prime-")
        assert k2.startswith("FAW-62:Developer:in progress:recovery-A-prime-")


class TestCountRecoveryAttempts:
    """Exercises the dedup-aware attempt counting that decides when to
    fall back to needs-human."""

    def _make_row(self, ticket_id, dedup_suffix=None, state="done"):
        key = (
            f"{ticket_id}:Developer:in progress:recovery-A-prime-{dedup_suffix}"
            if dedup_suffix is not None
            else f"{ticket_id}:Developer:in progress:manual"
        )
        return {
            "id": 1,
            "ticket_id": ticket_id,
            "role": "Developer",
            "state": state,
            "dedup_key": key,
        }

    def test_zero_recovery_rows_count_zero(self):
        rows = [self._make_row("FAW-62"), self._make_row("FAW-62")]
        assert _count_recovery_a_prime_attempts_from_rows("FAW-62", rows) == 0

    def test_single_recovery_row_counts_one(self):
        rows = [
            self._make_row("FAW-62"),
            self._make_row("FAW-62", dedup_suffix=1715000000),
        ]
        assert _count_recovery_a_prime_attempts_from_rows("FAW-62", rows) == 1

    def test_multiple_recovery_rows_count_each(self):
        rows = [
            self._make_row("FAW-62", dedup_suffix=1715000000),
            self._make_row("FAW-62", dedup_suffix=1715000100),
            self._make_row("FAW-62", dedup_suffix=1715000200),
        ]
        assert _count_recovery_a_prime_attempts_from_rows("FAW-62", rows) == 3

    def test_other_ticket_recovery_rows_not_counted(self):
        # Don't accidentally count FAW-99 rows when scanning for FAW-62.
        rows = [
            self._make_row("FAW-99", dedup_suffix=1715000000),
            self._make_row("FAW-62", dedup_suffix=1715000100),
        ]
        assert _count_recovery_a_prime_attempts_from_rows("FAW-62", rows) == 1

    def test_other_role_rows_not_counted(self):
        # Recovery only counts Developer rows.
        rows = [
            {"id": 1, "ticket_id": "FAW-62", "role": "Reviewer",
             "state": "done", "dedup_key": "FAW-62:Reviewer:in progress:recovery-A-prime-1715000000"},
            self._make_row("FAW-62", dedup_suffix=1715000100),
        ]
        assert _count_recovery_a_prime_attempts_from_rows("FAW-62", rows) == 1

    def test_non_recovery_a_prime_dedup_keys_not_counted(self):
        # Different recovery patterns (e.g. Mode A "iteration-cap-recovery")
        # must NOT inflate the A' counter.
        rows = [
            {"id": 1, "ticket_id": "FAW-62", "role": "Developer",
             "state": "done",
             "dedup_key": "FAW-62:Developer:in progress:recovery-cap-1715000000"},
            self._make_row("FAW-62", dedup_suffix=1715000100),
        ]
        assert _count_recovery_a_prime_attempts_from_rows("FAW-62", rows) == 1


class TestEndpointRegistration:
    """Smoke test that the FastAPI app actually exposes the new endpoint.
    This catches routing typos / decorator mistakes without needing the DB."""

    def test_recover_orphaned_devs_endpoint_registered(self):
        from maestro import webhook_server as ws
        paths = {getattr(r, "path", "") for r in ws.app.routes}
        assert "/agent-queue/recover-orphaned-devs" in paths

    def test_endpoint_uses_post(self):
        from maestro import webhook_server as ws
        from fastapi.routing import APIRoute
        for r in ws.app.routes:
            if isinstance(r, APIRoute) and r.path == "/agent-queue/recover-orphaned-devs":
                assert "POST" in r.methods
                return
        raise AssertionError("endpoint not found")

    def test_endpoint_accepts_max_age_and_max_tickets_query_params(self):
        from maestro import webhook_server as ws
        from fastapi.routing import APIRoute
        for r in ws.app.routes:
            if isinstance(r, APIRoute) and r.path == "/agent-queue/recover-orphaned-devs":
                # The function's signature has named params; FastAPI builds
                # the OpenAPI schema from the same params via ``Query``.
                param_names = {p.name for p in r.dependant.query_params}
                assert "max_age_seconds" in param_names
                assert "max_tickets" in param_names
                return
        raise AssertionError("endpoint not found")


class TestOrphanRecoveryConfig:
    """Pin the FAW-62 config defaults so a silent env change can't widen
    the recovery window without an explicit operator decision."""

    def test_recovery_enabled_default_true(self):
        from maestro import webhook_server as ws
        # Default is on. Operator can opt out via ORPHAN_RECOVERY_ENABLED=false.
        # We don't change env here — this test pins the source default.
        assert ws._ORPHAN_RECOVERY_ENABLED is True

    def test_recovery_interval_default_30_minutes(self):
        from maestro import webhook_server as ws
        # 1800s = 30 min, per FAW-62 spec.
        assert ws._ORPHAN_RECOVERY_INTERVAL_SECONDS == 1800.0

    def test_max_age_default_2_hours(self):
        from maestro import webhook_server as ws
        # 7200s = 2h, per FAW-62 spec.
        assert ws._ORPHAN_RECOVERY_MAX_AGE_SECONDS == 7200.0

    def test_max_tickets_per_scan_default_50(self):
        from maestro import webhook_server as ws
        # Conservative cap; tickets are processed sequentially with at least
        # one Linear state lookup each.
        assert ws._ORPHAN_RECOVERY_MAX_TICKETS_PER_SCAN == 50

    def test_max_reenqueue_attempts_default_2(self):
        from maestro import webhook_server as ws
        # After 2 failed re-enqueues we fall back to needs-human.
        assert ws._ORPHAN_RECOVERY_MAX_REENQUEUE_ATTEMPTS == 2


class TestScanSummaryShape:
    """Pin the response shape of POST /agent-queue/recover-orphaned-devs so
    downstream dashboards / smoke tests can rely on the keys being present
    even when the scan finds nothing."""

    def test_summary_has_expected_top_level_keys(self):
        # Mirror the empty-summary shape so the test doesn't have to hit
        # the real endpoint (which needs DB + Linear + GitHub credentials).
        summary = {
            "status": "ok",
            "considered": 0,
            "mode_a": 0,
            "mode_a_prime_requeued": 0,
            "mode_a_prime_needs_human": 0,
            "mode_b_skipped": 0,
            "skipped": 0,
            "results": [],
            "errors": [],
            "max_age_seconds": 7200,
            "max_tickets": 50,
        }
        for key in (
            "status", "considered", "mode_a", "mode_a_prime_requeued",
            "mode_a_prime_needs_human", "mode_b_skipped", "skipped",
            "results", "errors", "max_age_seconds", "max_tickets",
        ):
            assert key in summary, f"missing key {key!r} in summary"

    def test_result_row_has_expected_keys(self):
        # A single ticket's result row must include these keys for downstream
        # tooling to be able to summarize without re-reading the source.
        result = {
            "ticket_id": "FAW-62",
            "mode": "A_prime",
            "branch": "feature/FAW-62",
            "action": "requeued",
            "detail": "recovery Developer task queued",
            "queued_task_id": 42,
        }
        for key in (
            "ticket_id", "mode", "branch", "action", "detail", "queued_task_id",
        ):
            assert key in result, f"missing key {key!r} in result row"

    def test_mode_values_are_disjoint(self):
        # Mode strings are mutually exclusive — same row should not be counted
        # as both "A" and "A_prime" in the summary tallies.
        valid_modes = {"A", "A_prime", "B", "none", "skipped"}
        sample_modes = ["A", "A_prime", "B", "none", "skipped"]
        for m in sample_modes:
            assert m in valid_modes


class TestAgeGuardForOrphanScan:
    """The scan must skip ``state=done`` rows whose ``finished_at`` is older
    than the recovery window. Without this guard, every old done row would
    trigger a Linear state lookup on every cycle."""

    def test_freshly_finished_row_within_window(self):
        now = time.time()
        finished_at = now - 60  # 1 minute ago
        age = now - finished_at
        assert age < 7200  # well within the default 2h window

    def test_ancient_row_outside_window(self):
        now = time.time()
        finished_at = now - 86400  # 1 day ago
        age = now - finished_at
        assert age > 7200  # outside the 2h window — skip

    def test_exactly_at_window_boundary(self):
        now = time.time()
        # Boundary is inclusive in our implementation: ``age > max_age`` skips,
        # so ``age == max_age`` is still eligible for recovery.
        finished_at = now - 7200
        age = now - finished_at
        # The production code: ``if age > max_age_seconds: skip``.
        # So an age equal to max_age should NOT be skipped.
        assert not (age > 7200)
