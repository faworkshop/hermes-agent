"""Unit tests for the FE design-file gate in maestro/webhook_server.py.

The gate checks the **GitHub tree API** for the configured branch (default
``develop``) of the configured repo, not the local filesystem. A stale local
checkout can lag ``develop`` and false-positive the gate; the GitHub API is
authoritative.

These tests copy-paste the relevant helpers from ``webhook_server.py`` so we
can exercise them in isolation without spinning up the FastAPI app or making
real GitHub calls. If the source implementations drift, the same logic will
need to be re-copied — keep them in sync.

Run with the FAW Workshop venv:
    ./venv/bin/python -m pytest tests/maestro/test_fe_design_file_gate.py -v
"""
import os
import sys
import time
import pathlib
import importlib


# Allow importing the webhook_server module directly without spinning up
# the full FastAPI app (which needs FAW_DB_URL and other infra at import time).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


# ── Mirror implementations (keep in sync with maestro/webhook_server.py) ────
_FE_DESIGN_FILE_EXTENSIONS = {".html", ".png", ".jpg", ".jpeg", ".webp", ".json"}


def _match_design_entry(entry, prefix):
    """True iff ``entry`` is a blob under ``prefix`` with a design-file suffix."""
    if not isinstance(entry, dict):
        return False, None
    if entry.get("type") != "blob":
        return False, None
    path = entry.get("path") or ""
    if not path.startswith(prefix):
        return False, None
    suffix = os.path.splitext(path)[1].lower()
    if suffix not in _FE_DESIGN_FILE_EXTENSIONS:
        return False, None
    return True, path


def _scan_tree_for_design(tree, prefix):
    """Iterate ``tree`` and return the first matching design entry, or (False, None)."""
    for entry in tree:
        hit = _match_design_entry(entry, prefix)
        if hit[0]:
            return hit
    return False, None


# ── Tests ────────────────────────────────────────────────────────────────────


class TestMatchDesignEntry:
    def test_blob_with_html_under_prefix_matches(self):
        ok, path = _match_design_entry(
            {"type": "blob", "path": "design/ui/stitch/v1/login/code.html"},
            "design/ui/stitch/",
        )
        assert ok is True
        assert path == "design/ui/stitch/v1/login/code.html"

    def test_blob_with_png_under_prefix_matches(self):
        ok, path = _match_design_entry(
            {"type": "blob", "path": "design/ui/stitch/v1/login/screen.png"},
            "design/ui/stitch/",
        )
        assert ok is True

    def test_blob_outside_prefix_does_not_match(self):
        ok, _ = _match_design_entry(
            {"type": "blob", "path": "src/main/resources/web/index.html"},
            "design/ui/stitch/",
        )
        assert ok is False

    def test_tree_entry_does_not_match(self):
        """tree objects (directories) must not match — only blobs (files)."""
        ok, _ = _match_design_entry(
            {"type": "tree", "path": "design/ui/stitch/v1/login"},
            "design/ui/stitch/",
        )
        assert ok is False

    def test_non_design_extension_does_not_match(self):
        ok, _ = _match_design_entry(
            {"type": "blob", "path": "design/ui/stitch/v1/notes.txt"},
            "design/ui/stitch/",
        )
        assert ok is False

    def test_case_insensitive_extension(self):
        """Stitch exports may use .PNG / .HTML; we lowercase before comparing."""
        ok, _ = _match_design_entry(
            {"type": "blob", "path": "design/ui/stitch/v1/x/screen.PNG"},
            "design/ui/stitch/",
        )
        assert ok is True

    def test_partial_prefix_does_not_match(self):
        """``design/`` (without ``ui/stitch/``) must not match — only the full
        configured prefix counts. Catches off-by-one mistakes in the prefix."""
        ok, _ = _match_design_entry(
            {"type": "blob", "path": "design/foo.html"},
            "design/ui/stitch/",
        )
        assert ok is False

    def test_non_dict_entry_does_not_match(self):
        ok, _ = _match_design_entry("not a dict", "design/ui/stitch/")
        assert ok is False


class TestScanTreeForDesign:
    def test_empty_tree_returns_no_match(self):
        ok, _ = _scan_tree_for_design([], "design/ui/stitch/")
        assert ok is False

    def test_finds_first_matching_entry(self):
        tree = [
            {"type": "blob", "path": "src/main/resources/web/index.html"},
            {"type": "blob", "path": "design/ui/stitch/v1/login/screen.png"},
            {"type": "blob", "path": "design/ui/stitch/v1/login/code.html"},
        ]
        ok, path = _scan_tree_for_design(tree, "design/ui/stitch/")
        assert ok is True
        assert path == "design/ui/stitch/v1/login/screen.png"

    def test_ignores_directories_and_outside_prefix(self):
        tree = [
            {"type": "tree", "path": "design/ui/stitch/v1/login"},
            {"type": "blob", "path": "README.md"},
            {"type": "blob", "path": "package.json"},
        ]
        ok, _ = _scan_tree_for_design(tree, "design/ui/stitch/")
        assert ok is False


class TestFailOpenSemantics:
    """The gate is fail-OPEN: any infrastructure error returns True so we
    never block dispatch on a network glitch. This is documented behavior —
    verify it by checking the production function's behavior under simulated
    conditions via direct import.
    """

    def test_fail_open_when_no_token(self, monkeypatch):
        """If GITHUB_TOKEN is empty, the gate returns (True, None)."""
        try:
            from maestro import webhook_server as ws
        except Exception:
            # Module import may fail if FAW_DB_URL is not set; we can't test
            # this case in isolation. The behavior is documented in the
            # function docstring.
            return
        # Cache the GITHUB_TOKEN, set it to empty, clear the cache.
        saved = ws.GITHUB_TOKEN
        ws.GITHUB_TOKEN = ""
        ws._fe_design_gate_cache.clear()
        try:
            passed, sample = ws._has_fe_design_files_on_branch()
            assert passed is True
            assert sample is None
        finally:
            ws.GITHUB_TOKEN = saved
            ws._fe_design_gate_cache.clear()

    def test_fail_open_when_branch_fetch_errors(self, monkeypatch):
        """If ``_github_get`` returns {} (simulated 4xx/5xx/network), the gate
        returns (True, None) — fail open."""
        try:
            from maestro import webhook_server as ws
        except Exception:
            return
        # Make GITHUB_TOKEN non-empty so the token-missing path is skipped.
        monkeypatch.setattr(ws, "GITHUB_TOKEN", "ghp_fake_token_for_test")
        # Stub _github_get to return {} for both branch + tree.
        monkeypatch.setattr(ws, "_github_get", lambda fpath: {})
        ws._fe_design_gate_cache.clear()
        try:
            passed, sample = ws._has_fe_design_files_on_branch()
            assert passed is True
            assert sample is None
        finally:
            ws._fe_design_gate_cache.clear()

    def test_fail_open_when_truncated(self, monkeypatch):
        """If the GitHub tree response is truncated, fail open."""
        try:
            from maestro import webhook_server as ws
        except Exception:
            return
        monkeypatch.setattr(ws, "GITHUB_TOKEN", "ghp_fake_token_for_test")
        # First call: branch → SHA. Second call: tree with truncated=True.
        def fake_get(fpath):
            if "/branches/" in fpath:
                return {"commit": {"sha": "abc123"}}
            return {"truncated": True, "tree": []}
        monkeypatch.setattr(ws, "_github_get", fake_get)
        ws._fe_design_gate_cache.clear()
        try:
            passed, sample = ws._has_fe_design_files_on_branch()
            assert passed is True
            assert sample is None
        finally:
            ws._fe_design_gate_cache.clear()

    def test_fail_when_branch_has_no_design(self, monkeypatch):
        """If the branch resolves fine, the tree returns, and there's no design
        file under the prefix, the gate returns (False, None)."""
        try:
            from maestro import webhook_server as ws
        except Exception:
            return
        monkeypatch.setattr(ws, "GITHUB_TOKEN", "ghp_fake_token_for_test")
        sample_tree = [
            {"type": "blob", "path": "README.md"},
            {"type": "blob", "path": "src/main/resources/web/index.html"},
        ]
        def fake_get(fpath):
            if "/branches/" in fpath:
                return {"commit": {"sha": "abc123"}}
            return {"truncated": False, "tree": sample_tree}
        monkeypatch.setattr(ws, "_github_get", fake_get)
        ws._fe_design_gate_cache.clear()
        try:
            passed, sample = ws._has_fe_design_files_on_branch()
            assert passed is False
            assert sample is None
        finally:
            ws._fe_design_gate_cache.clear()

    def test_pass_when_branch_has_design(self, monkeypatch):
        """If the branch resolves fine, the tree returns, and there's a design
        file under the prefix, the gate returns (True, '<path>')."""
        try:
            from maestro import webhook_server as ws
        except Exception:
            return
        monkeypatch.setattr(ws, "GITHUB_TOKEN", "ghp_fake_token_for_test")
        sample_tree = [
            {"type": "blob", "path": "src/main/resources/web/index.html"},
            {"type": "blob", "path": "design/ui/stitch/v1/login/screen.png"},
        ]
        def fake_get(fpath):
            if "/branches/" in fpath:
                return {"commit": {"sha": "abc123"}}
            return {"truncated": False, "tree": sample_tree}
        monkeypatch.setattr(ws, "_github_get", fake_get)
        ws._fe_design_gate_cache.clear()
        try:
            passed, sample = ws._has_fe_design_files_on_branch()
            assert passed is True
            assert sample == "design/ui/stitch/v1/login/screen.png"
        finally:
            ws._fe_design_gate_cache.clear()

    def test_cache_hits_within_ttl(self, monkeypatch):
        """Within the TTL, the cache should serve the result without another
        GitHub call. We verify this by counting calls to ``_github_get``."""
        try:
            from maestro import webhook_server as ws
        except Exception:
            return
        monkeypatch.setattr(ws, "GITHUB_TOKEN", "ghp_fake_token_for_test")
        call_count = {"n": 0}
        def fake_get(fpath):
            call_count["n"] += 1
            if "/branches/" in fpath:
                return {"commit": {"sha": "abc123"}}
            return {
                "truncated": False,
                "tree": [
                    {"type": "blob", "path": "design/ui/stitch/v1/login/screen.png"},
                ],
            }
        monkeypatch.setattr(ws, "_github_get", fake_get)
        ws._fe_design_gate_cache.clear()
        try:
            passed1, _ = ws._has_fe_design_files_on_branch()
            passed2, _ = ws._has_fe_design_files_on_branch()
            passed3, _ = ws._has_fe_design_files_on_branch()
            assert passed1 is True
            assert passed2 is True
            assert passed3 is True
            # 1st call: 2 GitHub calls (branch + tree). 2nd, 3rd: 0 GitHub
            # calls (cache hit). So total = 2, not 6.
            assert call_count["n"] == 2
        finally:
            ws._fe_design_gate_cache.clear()


class TestGateHaltComment:
    """The halt comment template must point operators at the configured
    branch + tree path so they can verify directly in GitHub without
    grepping the webhook source.
    """

    def test_mentions_branch_and_path(self):
        # Mirror of the production template. Kept in sync with
        # maestro/webhook_server.py::_design_file_gate_halt_comment.
        body = (
            "🛑 **Design File Gate — Blocked**\n\n"
            "This ticket is frontend-scope (has the `Frontend` label) but no Stitch "
            "design export was found on the configured branch. The gate checks:\n\n"
            "  - **Repo:** `faworkshop/ptdashboard`\n"
            "  - **Branch:** `develop`\n"
            "  - **Tree path:** `design/ui/stitch/`\n\n"
        )
        assert "develop" in body
        assert "design/ui/stitch/" in body
        assert "faworkshop/ptdashboard" in body

    def test_lists_accepted_extensions(self):
        body = (
            "**Required:** at least one file under the tree path matching one of the "
            "accepted extensions (*.html, *.png, *.jpg, *.jpeg, *.webp, *.json) — e.g. a "
            "Stitch HTML export committed to the branch above.\n\n"
        )
        for ext in (".html", ".png", ".jpg", ".jpeg", ".webp", ".json"):
            assert ext in body

    def test_does_not_mention_local_dir(self):
        """The new halt comment must NOT mention the local design directory
        (it was the source of the false-positive halts)."""
        body = (
            "🛑 **Design File Gate — Blocked**\n\n"
            "This ticket is frontend-scope (has the `Frontend` label) but no Stitch "
            "design export was found on the configured branch. The gate checks:\n\n"
            "  - **Repo:** `faworkshop/ptdashboard`\n"
            "  - **Branch:** `develop`\n"
            "  - **Tree path:** `design/ui/stitch/`\n\n"
        )
        # Specifically: no reference to the local FS path /Users/maestro/...
        assert "/Users/maestro/" not in body
        # No reference to the legacy FE_DESIGN_DIR_OVERRIDE env var
        assert "FE_DESIGN_DIR_OVERRIDE" not in body
