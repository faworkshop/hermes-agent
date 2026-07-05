"""PostgreSQL-backed agent queue, locks, and retry counters for the FAW
Workshop SDLC pipeline.

The webhook server (``maestro/webhook_server.py``) is the sole owner of the
database connection in production. Required env var (loaded from
``~/.hermes/.env`` by the webhook server before this module is imported)::

    FAW_DB_URL = postgresql://hermes:hermes_dev_queue@127.0.0.1:5433/hermes_maestro_queue

Schema is created automatically on first connect — see ``_init_schema``.
``webhook_server.py`` passes ``FAW_DB_URL`` to every worker via the process
environment, so we read it once at import time.

Historically this class was SQLite-backed (file-based ``agent_state.db``).
The webhook server, migration scripts (``maestro/scripts/migrate_queue_pg.py``),
and Docker setup (``maestro/docker-compose.yml``) all target PostgreSQL. SQLite
support was removed (Jul 5 2026) because two engines in parallel produced
inconsistent state — webhook server wrote to one DB, operator queries to the
other, and dedup/cooldown logic silently disagreed.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator, Optional

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


def _parse_pg_dsn(raw: str) -> dict[str, Any]:
    """Parse a libpq-style space-separated DSN into a ``connect()`` kwargs dict.

    Supports either ``postgresql://user:pass@host:port/db`` (URI) or
    ``host=... port=... dbname=... user=... password=...`` (keyword) form.
    Adds ``connect_timeout`` so a dead PG doesn't hang the worker.
    """
    raw = (raw or "").strip()
    if not raw:
        raise RuntimeError(
            "FAW_DB_URL is not set. The maestro webhook queue requires PostgreSQL — "
            "set FAW_DB_URL in ~/.hermes/.env or the process env, e.g. "
            "FAW_DB_URL='postgresql://hermes:hermes_dev_queue@127.0.0.1:5433/hermes_maestro_queue'"
        )
    if raw.startswith("postgresql://") or raw.startswith("postgres://"):
        kw = psycopg2.extensions.parse_dsn(raw)
    else:
        kw = {}
        for part in raw.split():
            if "=" not in part:
                continue
            k, v = part.split("=", 1)
            kw[k] = v
    if "port" in kw:
        try:
            kw["port"] = int(kw["port"])
        except (TypeError, ValueError):
            pass
    kw["connect_timeout"] = 15
    return kw


class ConcurrencyManager:
    """PostgreSQL-backed queue + lock manager.

    All public methods accept the same arguments as the prior SQLite
    implementation. Returns are plain ``dict`` (column-name keyed) for rows
    so callers can use ``row["id"]`` etc. without caring about the cursor
    factory. Internal ``_connect()`` returns a stock psycopg2 connection so
    callers can override ``cursor_factory`` if they need ``RealDictCursor``
    for ad-hoc queries.
    """

    # Pool of long-lived connections. Each thread that needs DB access checks
    # one out, uses it briefly, returns it. psycopg2 connections are not
    # thread-safe so we keep one per thread rather than sharing.
    _local = threading.local()

    def __init__(self, _db_path: str = None) -> None:
        # The ``db_path`` argument is preserved for backwards compatibility with
        # call sites that pass ``agent_state.db`` (the historical SQLite file).
        # We deliberately ignore it — PG is the only supported backend.
        if _db_path:
            logger.info(
                "ConcurrencyManager: ignoring legacy SQLite db_path=%r; using PostgreSQL via FAW_DB_URL",
                _db_path,
            )
        self._pg_kw = _parse_pg_dsn(os.environ.get("FAW_DB_URL", ""))
        self._init_schema()

    # ------------------------------------------------------------------ schema

    def _init_schema(self) -> None:
        """Create locks / retries / agent_tasks tables and indexes if missing.

        Idempotent. ``last_heartbeat_at`` was added when heartbeat-based zombie
        detection was introduced — declared inline on fresh installs; an
        ``ALTER TABLE`` migration brings older installs up to date.
        """
        ddl_statements = [
            """
            CREATE TABLE IF NOT EXISTS locks (
                ticket_id TEXT PRIMARY KEY,
                assignee TEXT NOT NULL,
                locked_at DOUBLE PRECISION NOT NULL,
                session_id TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS retries (
                ticket_id TEXT PRIMARY KEY,
                count INTEGER NOT NULL DEFAULT 0
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS agent_tasks (
                id SERIAL PRIMARY KEY,
                ticket_id TEXT NOT NULL,
                role TEXT NOT NULL,
                prompt TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'queued',
                dedup_key TEXT NOT NULL,
                source_state TEXT,
                session_id TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_run_at DOUBLE PRECISION NOT NULL,
                last_error TEXT,
                created_at DOUBLE PRECISION NOT NULL,
                updated_at DOUBLE PRECISION NOT NULL,
                started_at DOUBLE PRECISION,
                finished_at DOUBLE PRECISION,
                last_heartbeat_at DOUBLE PRECISION
            )
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_tasks_active_dedup
            ON agent_tasks (dedup_key)
            WHERE state IN ('queued', 'running')
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_agent_tasks_role_ready
            ON agent_tasks (role, state, next_run_at, created_at)
            """,
        ]
        with self._connect() as conn:
            with conn.cursor() as cur:
                for ddl in ddl_statements:
                    cur.execute(ddl)
                # Backfill last_heartbeat_at on installs created before that column shipped.
                cur.execute(
                    """
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'agent_tasks'
                      AND column_name = 'last_heartbeat_at'
                    """
                )
                if cur.fetchone() is None:
                    cur.execute("ALTER TABLE agent_tasks ADD COLUMN last_heartbeat_at DOUBLE PRECISION")
            conn.commit()

    # ----------------------------------------------------------------- connect

    def _connect(self):
        """Return a short-lived psycopg2 connection bound to the calling thread.

        Returned as a raw connection (not a context manager) so callers can
        keep it open across multiple statements — the SQLite implementation
        used ``with self._connect() as conn:`` and the live webhook server
        depends on that idiom at line 2136.
        """
        conn = getattr(self._local, "conn", None)
        if conn is None or conn.closed:
            conn = psycopg2.connect(**self._pg_kw)
            conn.autocommit = False
            self._local.conn = conn
        return conn

    def close(self) -> None:
        """Close the per-thread connection (mainly for tests)."""
        conn = getattr(self._local, "conn", None)
        if conn is not None and not conn.closed:
            conn.close()
        self._local.conn = None

    # --------------------------------------------------------------- locks

    def acquire_lock(
        self,
        ticket_id: str,
        agent_name: str,
        session_id: Optional[str] = None,
        timeout: float = 1800.0,
    ) -> bool:
        """Acquire an exclusive lock for ``ticket_id`` on behalf of ``agent_name``.

        Stale locks (older than ``timeout`` seconds) are auto-expired so a
        crashed worker cannot wedge the pipeline forever. Same-role
        re-acquires pass through only when the new ``session_id`` matches the
        lock's session — otherwise we treat it as a concurrent same-role
        dispatch and reject.
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT assignee, locked_at, session_id FROM locks WHERE ticket_id = %s",
                    (ticket_id,),
                )
                row = cur.fetchone()
                if row:
                    current_assignee, locked_at, lock_session = row[0], float(row[1]), row[2]
                    age = time.time() - locked_at
                    if age > timeout:
                        logger.info(
                            "Ticket %s lock by %s is stale (%.0fs old, max %.0fs). Expiring and re-acquiring.",
                            ticket_id, current_assignee, age, timeout,
                        )
                        cur.execute("DELETE FROM locks WHERE ticket_id = %s", (ticket_id,))
                    elif current_assignee == agent_name:
                        if (
                            session_id is not None
                            and lock_session is not None
                            and session_id != lock_session
                        ):
                            logger.warning(
                                "Ticket %s already locked by %s (session %s); incoming session %s is DIFFERENT — blocking.",
                                ticket_id, current_assignee, lock_session, session_id,
                            )
                            conn.commit()
                            return False
                        conn.commit()
                        return True
                    else:
                        logger.warning(
                            "Ticket %s is already locked by %s (%.0fs old)",
                            ticket_id, current_assignee, age,
                        )
                        conn.commit()
                        return False
                cur.execute(
                    """
                    INSERT INTO locks (ticket_id, assignee, locked_at, session_id)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (ticket_id, agent_name, time.time(), session_id),
                )
            conn.commit()
        return True

    def release_lock(self, ticket_id: str, agent_name: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM locks WHERE ticket_id = %s AND assignee = %s",
                    (ticket_id, agent_name),
                )
            conn.commit()

    def release_and_acquire(
        self,
        ticket_id: str,
        current_agent: str,
        next_agent: str,
        next_session_id: Optional[str] = None,
    ) -> bool:
        """Atomically hand off the lock from ``current_agent`` to ``next_agent``."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM locks WHERE ticket_id = %s AND assignee = %s",
                    (ticket_id, current_agent),
                )
                cur.execute(
                    """
                    INSERT INTO locks (ticket_id, assignee, locked_at, session_id)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (ticket_id, next_agent, time.time(), next_session_id),
                )
            conn.commit()
        return True

    def force_clear(self, ticket_id: str) -> Optional[str]:
        """Force-clear any lock on a ticket. Returns the cleared role, or None."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT assignee FROM locks WHERE ticket_id = %s", (ticket_id,))
                row = cur.fetchone()
                if not row:
                    return None
                assignee = row[0]
                cur.execute("DELETE FROM locks WHERE ticket_id = %s", (ticket_id,))
            conn.commit()
        return assignee

    def get_lock(self, ticket_id: str) -> Optional[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT assignee, locked_at, session_id FROM locks WHERE ticket_id = %s",
                    (ticket_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return {"role": row[0], "acquired_at": float(row[1]), "session_id": row[2]}

    def get_all_locks(self) -> list[tuple[str, str, float]]:
        now = time.time()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT ticket_id, assignee, locked_at FROM locks ORDER BY locked_at")
                rows = cur.fetchall()
        return [(tid, assignee, float(lat)) for tid, assignee, lat in rows]

    def get_all_locks_with_age(self) -> list[tuple[str, str, float, float]]:
        """``(ticket_id, assignee, locked_at, age_seconds)``."""
        now = time.time()
        out: list[tuple[str, str, float, float]] = []
        for tid, assignee, lat in self.get_all_locks():
            out.append((tid, assignee, lat, now - float(lat)))
        return out

    # ------------------------------------------------------------- retries

    def get_retry_count(self, ticket_id: str) -> int:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count FROM retries WHERE ticket_id = %s", (ticket_id,))
                row = cur.fetchone()
                return int(row[0]) if row else 0

    def increment_retry(self, ticket_id: str) -> int:
        """Increment the retry counter, returning the new value. UPSERT semantics."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO retries (ticket_id, count) VALUES (%s, 1)
                    ON CONFLICT (ticket_id) DO UPDATE SET count = retries.count + 1
                    RETURNING count
                    """,
                    (ticket_id,),
                )
                new_count = int(cur.fetchone()[0])
            conn.commit()
        return new_count

    def reset_retries(self, ticket_id: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM retries WHERE ticket_id = %s", (ticket_id,))
            conn.commit()

    # ------------------------------------------------------------- tasks CRUD

    def enqueue_task(
        self,
        ticket_id: str,
        role: str,
        prompt: str,
        *,
        dedup_key: str,
        source_state: Optional[str] = None,
    ) -> tuple[bool, dict[str, Any]]:
        """Queue a role task if no active duplicate exists.

        Active = state is ``queued`` or ``running``. Terminal states
        (``done``, ``failed``, ``cancelled``) do NOT block — a fresh enqueue
        is allowed for the same dedup_key, which is how repeat dispatches
        after a crash are reconciled.
        """
        now = time.time()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id FROM agent_tasks
                    WHERE dedup_key = %s AND state IN ('queued', 'running')
                    ORDER BY id DESC LIMIT 1
                    """,
                    (dedup_key,),
                )
                row = cur.fetchone()
                if row:
                    existing_id = row[0]
                    cur.execute(
                        _TASK_SELECT + " WHERE id = %s",
                        (existing_id,),
                    )
                    existing = cur.fetchone()
                    conn.commit()
                    return False, _row_to_dict(existing) if existing else {}

                cur.execute(
                    """
                    INSERT INTO agent_tasks (
                        ticket_id, role, prompt, state, dedup_key, source_state,
                        next_run_at, created_at, updated_at
                    ) VALUES (%s, %s, %s, 'queued', %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (ticket_id, role, prompt, dedup_key, source_state, now, now, now),
                )
                new_id = cur.fetchone()[0]
                cur.execute(
                    _TASK_SELECT + " WHERE id = %s",
                    (new_id,),
                )
                new_row = cur.fetchone()
            conn.commit()
        return True, _row_to_dict(new_row)

    def claim_next_task(
        self,
        role: str,
        *,
        session_id: str,
        max_active_tickets: int = 0,
        active_timeout_seconds: float = 1800.0,
    ) -> Optional[dict[str, Any]]:
        """Atomically claim the oldest queued task for ``role``.

        Active-cap check is role-scoped — PM holding a slot does NOT block
        Developer/Reviewer/QA claim attempts (and vice versa). This avoids
        the starvation observed when orphan PM tasks exhausted the global
        cap for everyone.
        """
        now = time.time()
        cutoff = now - max(1.0, float(active_timeout_seconds))
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                if max_active_tickets > 0:
                    cur.execute(
                        """
                        SELECT COUNT(DISTINCT ticket_id) AS n
                        FROM agent_tasks
                        WHERE role = %s
                          AND state = 'running'
                          AND started_at IS NOT NULL
                          AND started_at > %s
                        """,
                        (role, cutoff),
                    )
                    active_n = int((cur.fetchone() or [0])[0])
                    if active_n >= max_active_tickets:
                        conn.commit()
                        return None

                cur.execute(
                    """
                    SELECT id FROM agent_tasks
                    WHERE role = %s AND state = 'queued' AND next_run_at <= %s
                    ORDER BY created_at ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    """,
                    (role, now),
                )
                row = cur.fetchone()
                if not row:
                    conn.commit()
                    return None
                task_id = int(row[0])

                cur.execute(
                    """
                    UPDATE agent_tasks
                    SET state = 'running',
                        session_id = %s,
                        attempts = attempts + 1,
                        started_at = %s,
                        updated_at = %s,
                        last_heartbeat_at = %s
                    WHERE id = %s AND state = 'queued'
                    """,
                    (session_id, now, now, now, task_id),
                )
                if cur.rowcount != 1:
                    conn.rollback()
                    return None

                cur.execute(
                    _TASK_SELECT + " WHERE id = %s",
                    (task_id,),
                )
                claimed = cur.fetchone()
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        return _row_to_dict(claimed) if claimed else None

    def complete_task(
        self,
        task_id: int,
        *,
        success: bool,
        error: Optional[str] = None,
    ) -> None:
        now = time.time()
        state = "done" if success else "failed"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE agent_tasks
                    SET state = %s,
                        finished_at = %s,
                        updated_at = %s,
                        last_error = %s
                    WHERE id = %s
                    """,
                    (state, now, now, error, int(task_id)),
                )
            conn.commit()

    def requeue_task(
        self,
        task_id: int,
        *,
        delay_seconds: float,
        error: Optional[str] = None,
    ) -> None:
        """Return a task to the queue with backoff. ``started_at`` is reset so
        zombie-detection doesn't immediately reap the requeue."""
        now = time.time()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE agent_tasks
                    SET state = 'queued',
                        session_id = NULL,
                        started_at = NULL,
                        next_run_at = %s,
                        updated_at = %s,
                        last_error = %s
                    WHERE id = %s
                    """,
                    (now + max(0.0, float(delay_seconds)), now, error, int(task_id)),
                )
            conn.commit()

    def touch_heartbeat(self, task_id: int) -> bool:
        """Update ``last_heartbeat_at`` on an active task. Returns False if the
        task no longer exists or is terminal — caller treats that as a clean
        stop signal so the heartbeat loop can exit quietly."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE agent_tasks
                    SET last_heartbeat_at = %s
                    WHERE id = %s AND state IN ('queued', 'running')
                    """,
                    (time.time(), int(task_id)),
                )
                ok = cur.rowcount == 1
            conn.commit()
        return ok

    def last_completed_task_for_role(
        self,
        ticket_id: str,
        role: str,
        *,
        max_age_seconds: float,
    ) -> Optional[dict[str, Any]]:
        """Return the most recent terminal task for ``(ticket_id, role)`` whose
        ``finished_at`` is within the last ``max_age_seconds``.

        Used by PM-intake and the unblock-trigger to enforce a cooldown —
        without this, the scanner would re-fire the same ticket every cycle.
        """
        cutoff = time.time() - max(0.0, float(max_age_seconds))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, ticket_id, role, state, finished_at, last_error
                    FROM agent_tasks
                    WHERE ticket_id = %s
                      AND role = %s
                      AND state IN ('done', 'failed', 'cancelled')
                      AND finished_at IS NOT NULL
                      AND finished_at >= %s
                    ORDER BY finished_at DESC, id DESC
                    LIMIT 1
                    """,
                    (str(ticket_id), str(role), cutoff),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return {
                    "id": int(row[0]),
                    "ticket_id": row[1],
                    "role": row[2],
                    "state": row[3],
                    "finished_at": float(row[4]) if row[4] is not None else None,
                    "last_error": row[5],
                }

    # ----------------------------------------------------------- task queries

    def get_task(self, task_id: int) -> Optional[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    _TASK_SELECT + " WHERE id = %s",
                    (int(task_id),),
                )
                row = cur.fetchone()
                return _row_to_dict(row) if row else None

    def get_task_by_ticket(self, ticket_id: str, role: str) -> Optional[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    _TASK_SELECT
                    + " WHERE ticket_id = %s AND role = %s AND state IN ('queued', 'running')"
                      " ORDER BY created_at DESC LIMIT 1",
                    (str(ticket_id), str(role)),
                )
                row = cur.fetchone()
                return _row_to_dict(row) if row else None

    def list_tasks(
        self,
        *,
        role: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        where_parts: list[str] = []
        params: list[Any] = []
        if role:
            where_parts.append("role = %s")
            params.append(role)
        if state:
            where_parts.append("state = %s")
            params.append(state)
        where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
        params.append(limit)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"{_TASK_SELECT} {where_sql} ORDER BY id DESC LIMIT %s",
                    tuple(params),
                )
                rows = cur.fetchall()
                return [_row_to_dict(r) for r in rows]

    def list_tasks_for_ticket_id(self, ticket_id: str, *, limit: int = 50) -> list[dict[str, Any]]:
        """All recent queue rows for a ticket, newest ``id`` first."""
        limit = max(1, min(int(limit), 200))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    _TASK_SELECT + " WHERE ticket_id = %s ORDER BY id DESC LIMIT %s",
                    (str(ticket_id), limit),
                )
                rows = cur.fetchall()
                return [_row_to_dict(r) for r in rows]

    def list_tasks_for_dashboard(
        self,
        *,
        terminal_recent: int = 150,
    ) -> list[dict[str, Any]]:
        """All queued+running rows, then the most recent ``terminal_recent``
        terminal rows. Low-id active tasks are never hidden behind a large
        history (unlike a plain ``ORDER BY id DESC LIMIT``)."""
        terminal_recent = max(1, min(int(terminal_recent), 500))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"{_TASK_SELECT} WHERE state IN ('queued', 'running') ORDER BY id ASC"
                )
                active = [_row_to_dict(r) for r in cur.fetchall()]
                cur.execute(
                    f"{_TASK_SELECT} WHERE state IN ('done', 'failed', 'cancelled')"
                    " ORDER BY id DESC LIMIT %s",
                    (terminal_recent,),
                )
                terminal = [_row_to_dict(r) for r in cur.fetchall()]
        return active + terminal

    # ----------------------------------------------------- watchdog helpers

    def recover_stale_running_tasks(
        self,
        *,
        role: Optional[str] = None,
        stale_after_seconds: float = 900.0,
        requeue_delay_seconds: float = 0.0,
    ) -> int:
        """Requeue ``running`` tasks that look dead.

        A task is stale-recoverable when **any** of:

          - ``last_heartbeat_at`` is older than ``stale_after_seconds``
            (heartbeat-based detection — primary signal; catches agents whose
            subprocess died without releasing the lock)
          - ``last_heartbeat_at`` is NULL and ``started_at`` is older than
            ``stale_after_seconds`` (pre-heartbeat tasks or fresh restarts
            where the heartbeat thread never ran)
          - no matching lock exists for the same ticket+role (the original
            lock-missing check — kept as a backstop)

        The heartbeat check is preferred because ``started_at`` alone is
        ambiguous: a task that died 30s after claim has ``started_at`` = now-30s,
        which is "fresh" by the lock-only rule, but ``last_heartbeat_at``
        hasn't ticked, so it's clearly dead.
        """
        now = time.time()
        cutoff = now - max(1.0, float(stale_after_seconds))
        requeue_at = now + max(0.0, float(requeue_delay_seconds))
        where = (
            "t.state = 'running' "
            "AND ("
            "  (t.last_heartbeat_at IS NOT NULL AND t.last_heartbeat_at <= %s) "
            "  OR (t.last_heartbeat_at IS NULL AND t.started_at IS NOT NULL AND t.started_at <= %s) "
            ")"
        )
        params: list[Any] = [cutoff, cutoff]
        if role:
            where += " AND t.role = %s"
            params.append(role)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT t.id
                    FROM agent_tasks t
                    LEFT JOIN locks l
                      ON l.ticket_id = t.ticket_id
                     AND l.assignee = t.role
                    WHERE {where}
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
                if not rows:
                    conn.commit()
                    return 0
                recovered = 0
                for row in rows:
                    task_id = int(row[0])
                    cur.execute(
                        """
                        UPDATE agent_tasks
                        SET state = 'queued',
                            session_id = NULL,
                            started_at = NULL,
                            next_run_at = %s,
                            updated_at = %s,
                            last_error = COALESCE(last_error, '') ||
                                          CASE WHEN last_error IS NULL OR last_error = '' THEN '' ELSE '; ' END ||
                                          'Auto-recovered stale running task (heartbeat stale or lock missing)'
                        WHERE id = %s AND state = 'running'
                        """,
                        (requeue_at, now, task_id),
                    )
                    if cur.rowcount == 1:
                        recovered += 1
            conn.commit()
        return recovered

    def revive_stale_developer_task(
        self,
        ticket_id: str,
        dedup_key: str,
        stale_after_seconds: float = 900.0,
    ) -> bool:
        """Revive a stale Developer task so it can be re-claimed.

        When a Developer agent exits without updating Linear (e.g. burned
        iterations waiting for CI), a second Linear webhook fires for the
        same state. The dedup index blocks a new task, but the existing
        task is stale (no lock held). This method resets that stale task
        so the worker can pick it up again.
        """
        cutoff = time.time() - max(1.0, float(stale_after_seconds))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT t.id
                    FROM agent_tasks t
                    LEFT JOIN locks l
                      ON l.ticket_id = t.ticket_id
                     AND l.assignee = t.role
                    WHERE t.dedup_key = %s
                      AND t.role = 'Developer'
                      AND t.state = 'running'
                      AND t.started_at IS NOT NULL
                      AND t.started_at <= %s
                      AND l.ticket_id IS NULL
                    ORDER BY t.id DESC
                    LIMIT 1
                    """,
                    (dedup_key, cutoff),
                )
                row = cur.fetchone()
                if not row:
                    conn.commit()
                    return False
                task_id = int(row[0])
                cur.execute(
                    """
                    UPDATE agent_tasks
                    SET state = 'queued',
                        session_id = NULL,
                        started_at = NULL,
                        next_run_at = %s,
                        updated_at = %s,
                        last_error = COALESCE(last_error, '') ||
                                      CASE WHEN last_error IS NULL OR last_error = '' THEN '' ELSE '; ' END ||
                                      'Revived: prior Developer agent exited without updating Linear (stale task)'
                    WHERE id = %s AND state = 'running'
                    """,
                    (time.time(), time.time(), task_id),
                )
                ok = cur.rowcount == 1
            conn.commit()
        return ok

    def get_queue_stats(
        self,
        *,
        stale_after_seconds: float = 900.0,
    ) -> dict[str, Any]:
        """Aggregate counts by state + stale-running candidate counts by role.

        "Stale" matches the ``recover_stale_running_tasks`` rule: heartbeat
        older than the cutoff, OR no heartbeat with ``started_at`` older than
        the cutoff. ``started_at`` alone is too permissive — see the
        ``recover_stale_running_tasks`` docstring for why.
        """
        cutoff = time.time() - max(1.0, float(stale_after_seconds))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT state, COUNT(*) AS n FROM agent_tasks GROUP BY state")
                rows = cur.fetchall()
                counts_by_state: dict[str, int] = {str(r[0]): int(r[1]) for r in rows}
                total = sum(counts_by_state.values())

                cur.execute(
                    """
                    SELECT COUNT(*) AS n
                    FROM agent_tasks t
                    WHERE t.state = 'running'
                      AND (
                        (t.last_heartbeat_at IS NOT NULL AND t.last_heartbeat_at <= %s)
                        OR (t.last_heartbeat_at IS NULL AND t.started_at IS NOT NULL AND t.started_at <= %s)
                      )
                    """,
                    (cutoff, cutoff),
                )
                stale_candidates = int((cur.fetchone() or [0])[0])

                cur.execute(
                    """
                    SELECT t.role, COUNT(*) AS n
                    FROM agent_tasks t
                    WHERE t.state = 'running'
                      AND (
                        (t.last_heartbeat_at IS NOT NULL AND t.last_heartbeat_at <= %s)
                        OR (t.last_heartbeat_at IS NULL AND t.started_at IS NOT NULL AND t.started_at <= %s)
                      )
                    GROUP BY t.role
                    ORDER BY n DESC
                    """,
                    (cutoff, cutoff),
                )
                stale_by_role = {str(r[0]): int(r[1]) for r in cur.fetchall()}
        return {
            "total": total,
            "counts_by_state": counts_by_state,
            "stale_after_seconds": int(stale_after_seconds),
            "stale_running_candidates": stale_candidates,
            "stale_running_by_role": stale_by_role,
        }

    # ------------------------------------------------------------ introspection

    def queue_debug_snapshot(self) -> dict[str, Any]:
        """DB identity + row counts. The webhook server uses this to confirm
        INSERTs are landing in the same DB it reads from (catches read-only
        URLs, accidentally pointing at a test DB, etc.)."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database()")
                db_name = cur.fetchone()[0]
                cur.execute("SELECT pg_is_in_recovery()")
                in_recovery = bool(cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM agent_tasks")
                agent_tasks_count = int(cur.fetchone()[0])
                cur.execute("SELECT COALESCE(MAX(id), 0) FROM agent_tasks")
                max_id = int(cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM locks")
                locks_count = int(cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM retries")
                retries_count = int(cur.fetchone()[0])
        return {
            "current_database": db_name,
            "pg_is_in_recovery": in_recovery,
            "agent_tasks_count": agent_tasks_count,
            "max_id": max_id,
            "locks_count": locks_count,
            "retries_count": retries_count,
        }


# ---------------------------------------------------------------- shared SQL


_TASK_SELECT = """
SELECT id, ticket_id, role, prompt, state, dedup_key, source_state, session_id,
       attempts, next_run_at, last_error,
       created_at, updated_at, started_at, finished_at, last_heartbeat_at
FROM agent_tasks
"""


def _row_to_dict(row) -> dict[str, Any]:
    """Convert a positional ``agent_tasks`` row tuple into a column-keyed dict.

    Column order MUST match ``_TASK_SELECT`` exactly. ``prompt`` is included
    because ``claim_next_task`` consumers (the worker loop in
    ``maestro/webhook_server.py``) read ``task["prompt"]`` to build the agent
    conversation's first user message.
    """
    if row is None:
        return {}
    return {
        "id": int(row[0]),
        "ticket_id": row[1],
        "role": row[2],
        "prompt": row[3],
        "state": row[4],
        "dedup_key": row[5],
        "source_state": row[6],
        "session_id": row[7],
        "attempts": int(row[8]),
        "next_run_at": float(row[9]) if row[9] is not None else None,
        "last_error": row[10],
        "created_at": float(row[11]) if row[11] is not None else None,
        "updated_at": float(row[12]) if row[12] is not None else None,
        "started_at": float(row[13]) if row[13] is not None else None,
        "finished_at": float(row[14]) if row[14] is not None else None,
        "last_heartbeat_at": float(row[15]) if row[15] is not None else None,
    }