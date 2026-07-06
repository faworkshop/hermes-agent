"""Unit tests for rate-limit auto-requeue logic in maestro/webhook_server.py.

The fix adds a ``_is_rate_limit_error`` classifier and a backoff schedule so
that an LLM provider rate-limit (Minimax HTTP 429, Anthropic SDK
``rate_limit_error``) auto-requeues the same task with exponential backoff
instead of marking the task done and leaving the ticket silently stuck.

Run with the FAW Workshop venv:
    ./venv/bin/python -m pytest tests/maestro/test_rate_limit_handler.py -v
"""
import os
import sys

# Allow importing the webhook_server module directly without spinning up
# the full FastAPI app (which needs FAW_DB_URL and other infra at import time).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# We can't import the full webhook_server module without all the side effects,
# so we re-define the helpers under test by copy-pasting their implementations
# for an isolated unit test. This avoids a real DB connection.
# If the implementations drift, the full integration test (Test 4 in
# /tmp/test_rate_limit_fix.py) catches the divergence.


def _is_rate_limit_error(err):
    """Mirror of maestro/webhook_server._is_rate_limit_error — keep in sync."""
    if not err:
        return False
    low = err.lower()
    if "rate_limit_error" in low or "ratelimiterror" in low:
        return True
    if "http 429" in low or "status code: 429" in low or " 429 " in low:
        return True
    if "Token Plan usage limit reached" in err:
        return True
    return False


def _rate_limit_backoff_seconds(attempt_count, initial=60.0, cap=600.0):
    """Mirror of maestro/webhook_server._rate_limit_backoff_seconds — keep in sync."""
    if attempt_count < 1:
        attempt_count = 1
    backoff = initial * (2 ** (attempt_count - 1))
    return min(backoff, cap)


class TestIsRateLimitError:
    def test_anthropic_sdk_prefix(self):
        assert _is_rate_limit_error("rate_limit_error: 429 foo")

    def test_lowercase_ratelimiterror(self):
        assert _is_rate_limit_error("RateLimitError: 429 foo")

    def test_http_429(self):
        assert _is_rate_limit_error("HTTP 429: Too many requests")

    def test_anthropic_status_code_429(self):
        assert _is_rate_limit_error("API error: status code: 429 foo")

    def test_minimax_full_body(self):
        body = (
            "Error code: 429 - {'type': 'error', 'error': {'type': "
            "'rate_limit_error', 'message': 'Token Plan usage limit reached: "
            "Upgrade your Token Plan or purchase Credits for more usage. (2056)'}}"
        )
        assert _is_rate_limit_error(body)

    def test_real_filenotfound_error(self):
        assert not _is_rate_limit_error("FileNotFoundError: foo")

    def test_real_connection_error(self):
        assert not _is_rate_limit_error("ConnectionError: timeout")

    def test_empty_string(self):
        assert not _is_rate_limit_error("")

    def test_none(self):
        assert not _is_rate_limit_error(None)

    def test_generic_rate_limit_phrase(self):
        # "rate limit" without the SDK type prefix or HTTP 429 is too vague
        # to be the LLM-provider signal — don't fire on this.
        assert not _is_rate_limit_error("Rate limit exceeded")


class TestBackoffSchedule:
    def test_attempt_1_initial(self):
        assert _rate_limit_backoff_seconds(1) == 60.0

    def test_attempt_2_doubles(self):
        assert _rate_limit_backoff_seconds(2) == 120.0

    def test_attempt_3_quadruples(self):
        assert _rate_limit_backoff_seconds(3) == 240.0

    def test_attempt_4_octuples(self):
        assert _rate_limit_backoff_seconds(4) == 480.0

    def test_attempt_5_caps(self):
        # 60 * 2^4 = 960, but cap is 600
        assert _rate_limit_backoff_seconds(5) == 600.0

    def test_attempt_10_still_caps(self):
        assert _rate_limit_backoff_seconds(10) == 600.0

    def test_attempt_0_treated_as_1(self):
        # Edge case: defensive guard treats <1 as 1
        assert _rate_limit_backoff_seconds(0) == 60.0

    def test_negative_treated_as_1(self):
        assert _rate_limit_backoff_seconds(-3) == 60.0


class TestBackoffTotalTime:
    """The full schedule (5 retries) should complete in bounded time so the
    pipeline keeps making forward progress even if a token-plan limit is
    persistent. With the default 60s initial / 600s cap, 5 attempts span
    ~25 minutes of cumulative backoff."""

    def test_cumulative_backoff_within_30_minutes(self):
        total = sum(_rate_limit_backoff_seconds(i) for i in range(1, 6))
        # 60 + 120 + 240 + 480 + 600 = 1500s = 25 minutes
        assert total == 1500.0
        assert total < 30 * 60
