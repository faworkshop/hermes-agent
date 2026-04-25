import sqlite3
import time
import logging
from pathlib import Path
from typing import Optional, Any

logger = logging.getLogger(__name__)

class ConcurrencyManager:
    def __init__(self, db_path: str = "agent_state.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS locks (
                    ticket_id TEXT PRIMARY KEY,
                    assignee TEXT,
                    locked_at REAL,
                    session_id TEXT
                )
            """)
            # Backward-compatible migration for pre-session_id databases.
            cur = conn.execute("PRAGMA table_info(locks)")
            cols = {row[1] for row in cur.fetchall()}
            if "session_id" not in cols:
                conn.execute("ALTER TABLE locks ADD COLUMN session_id TEXT")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS retries (
                    ticket_id TEXT PRIMARY KEY,
                    count INTEGER DEFAULT 0
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS agent_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticket_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    prompt TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'queued',
                    dedup_key TEXT NOT NULL,
                    source_state TEXT,
                    session_id TEXT,
                    attempts INTEGER DEFAULT 0,
                    next_run_at REAL NOT NULL,
                    last_error TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    started_at REAL,
                    finished_at REAL
                )
            """)
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_tasks_active_dedup
                ON agent_tasks(dedup_key)
                WHERE state IN ('queued', 'running')
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_agent_tasks_role_ready
                ON agent_tasks(role, state, next_run_at, created_at)
            """)
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=5.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def acquire_lock(self, ticket_id: str, agent_name: str, session_id: str = None, timeout: float = 1800.0) -> bool:
        """Try to acquire a lock for a ticket. Stale locks (older than `timeout` seconds) are auto-expired."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT assignee, locked_at, session_id FROM locks WHERE ticket_id = ?", (ticket_id,))
            row = cursor.fetchone()

            if row:
                current_assignee, locked_at = row[0], row[1]
                age = time.time() - locked_at
                if age > timeout:
                    # Stale lock — expire it and proceed to acquire
                    logger.info(
                        f"Ticket {ticket_id} lock by {current_assignee} is stale "
                        f"({age:.0f}s old, max {timeout}s). Expiring and re-acquiring."
                    )
                    conn.execute("DELETE FROM locks WHERE ticket_id = ?", (ticket_id,))
                    conn.commit()
                else:
                    if current_assignee == agent_name:
                        # Same agent re-acquiring — MUST match session_id or it's a stale
                        # dispatch from a prior run that was already finished.
                        # session_id is generated fresh per dispatch (timestamp+uuid), so a
                        # new session_id means a NEW webhook firing, not the same run.
                        lock_session = row[2]  # session_id is column 3 (index 2)
                        if session_id is not None and lock_session is not None and session_id != lock_session:
                            logger.warning(
                                f"Ticket {ticket_id} is already locked by {current_assignee} "
                                f"with session {lock_session}, incoming session {session_id} "
                                f"is DIFFERENT — blocking concurrent same-role dispatch."
                            )
                            return False
                        return True
                    logger.warning(f"Ticket {ticket_id} is already locked by {current_assignee} ({age:.0f}s old)")
                    return False

            cursor.execute(
                "INSERT INTO locks (ticket_id, assignee, locked_at, session_id) VALUES (?, ?, ?, ?)",
                (ticket_id, agent_name, time.time(), session_id)
            )
            conn.commit()
            return True

    def release_lock(self, ticket_id: str, agent_name: str):
        """Release a lock if held by the agent."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM locks WHERE ticket_id = ? AND assignee = ?",
                (ticket_id, agent_name)
            )
            conn.commit()

    def release_and_acquire(self, ticket_id: str, current_agent: str, next_agent: str, next_session_id: str = None) -> bool:
        """Atomically release current_agent's lock and acquire a lock for next_agent."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Release current holder
            cursor.execute(
                "DELETE FROM locks WHERE ticket_id = ? AND assignee = ?",
                (ticket_id, current_agent)
            )
            # Try to acquire for next agent
            cursor.execute(
                "INSERT INTO locks (ticket_id, assignee, locked_at, session_id) VALUES (?, ?, ?, ?)",
                (ticket_id, next_agent, time.time(), next_session_id)
            )
            conn.commit()
            return True

    def force_clear(self, ticket_id: str) -> Optional[str]:
        """Force-clear any lock on a ticket regardless of who holds it.
        Returns the role that was cleared, or None if there was no lock.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT assignee FROM locks WHERE ticket_id = ?", (ticket_id,))
            row = cursor.fetchone()
            if row:
                assignee = row[0]
                conn.execute("DELETE FROM locks WHERE ticket_id = ?", (ticket_id,))
                conn.commit()
                return assignee
            return None

    def get_lock(self, ticket_id: str) -> Optional[dict]:
        """Return lock info for a ticket, or None if unlocked."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT assignee, locked_at, session_id FROM locks WHERE ticket_id = ?", (ticket_id,))
            row = cursor.fetchone()
            if row:
                return {"role": row[0], "acquired_at": row[1], "session_id": row[2]}
            return None

    def get_retry_count(self, ticket_id: str) -> int:
        """Get the current retry count for a ticket."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT count FROM retries WHERE ticket_id = ?", (ticket_id,))
            row = cursor.fetchone()
            return row[0] if row else 0

    def increment_retry(self, ticket_id: str) -> int:
        """Increment and return the retry count for a ticket."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT count FROM retries WHERE ticket_id = ?", (ticket_id,))
            row = cursor.fetchone()
            
            if row:
                new_count = row[0] + 1
                cursor.execute("UPDATE retries SET count = ? WHERE ticket_id = ?", (new_count, ticket_id))
            else:
                new_count = 1
                cursor.execute("INSERT INTO retries (ticket_id, count) VALUES (?, ?)", (ticket_id, new_count))
            
            conn.commit()
            return new_count

    def reset_retries(self, ticket_id: str):
        """Reset retry count for a ticket (e.g. when moved forward)."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM retries WHERE ticket_id = ?", (ticket_id,))
            conn.commit()

    def get_all_locks(self) -> list[tuple[str, str, float]]:
        """Return all active locks as (ticket_id, assignee, locked_at) tuples."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ticket_id, assignee, locked_at FROM locks")
            return cursor.fetchall()

    def get_all_locks_with_age(self) -> list[tuple[str, str, float, float]]:
        """Return all active locks as (ticket_id, assignee, locked_at, age_seconds) tuples."""
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ticket_id, assignee, locked_at FROM locks")
            return [(tid, assignee, lat, now - lat) for tid, assignee, lat in cursor.fetchall()]

    def enqueue_task(
        self,
        ticket_id: str,
        role: str,
        prompt: str,
        *,
        dedup_key: str,
        source_state: Optional[str] = None,
    ) -> tuple[bool, dict[str, Any]]:
        """Queue a role task if no active duplicate exists."""
        now = time.time()
        with self._connect() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO agent_tasks (
                        ticket_id, role, prompt, state, dedup_key, source_state,
                        next_run_at, created_at, updated_at
                    )
                    VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, ?)
                    """,
                    (ticket_id, role, prompt, dedup_key, source_state, now, now, now),
                )
                row = conn.execute(
                    "SELECT * FROM agent_tasks WHERE id = last_insert_rowid()",
                ).fetchone()
                return True, dict(row) if row else {}
            except sqlite3.IntegrityError:
                row = conn.execute(
                    """
                    SELECT * FROM agent_tasks
                    WHERE dedup_key = ? AND state IN ('queued', 'running')
                    ORDER BY id DESC LIMIT 1
                    """,
                    (dedup_key,),
                ).fetchone()
                return False, dict(row) if row else {}

    def claim_next_task(self, role: str, *, session_id: str) -> Optional[dict[str, Any]]:
        """Atomically claim the next queued task for a role."""
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT id
                FROM agent_tasks
                WHERE role = ? AND state = 'queued' AND next_run_at <= ?
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (role, now),
            ).fetchone()
            if not row:
                conn.execute("ROLLBACK")
                return None

            task_id = int(row["id"])
            updated = conn.execute(
                """
                UPDATE agent_tasks
                SET state = 'running',
                    session_id = ?,
                    attempts = attempts + 1,
                    started_at = ?,
                    updated_at = ?
                WHERE id = ? AND state = 'queued'
                """,
                (session_id, now, now, task_id),
            )
            if updated.rowcount != 1:
                conn.execute("ROLLBACK")
                return None
            conn.execute("COMMIT")
            claimed = conn.execute("SELECT * FROM agent_tasks WHERE id = ?", (task_id,)).fetchone()
            return dict(claimed) if claimed else None

    def complete_task(self, task_id: int, *, success: bool, error: Optional[str] = None) -> None:
        """Mark a running task complete."""
        now = time.time()
        state = "done" if success else "failed"
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE agent_tasks
                SET state = ?, finished_at = ?, updated_at = ?, last_error = ?
                WHERE id = ?
                """,
                (state, now, now, error, task_id),
            )

    def requeue_task(self, task_id: int, *, delay_seconds: float, error: Optional[str] = None) -> None:
        """Return a claimed task to queue with backoff."""
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE agent_tasks
                SET state = 'queued',
                    session_id = NULL,
                    next_run_at = ?,
                    updated_at = ?,
                    last_error = ?
                WHERE id = ?
                """,
                (now + max(0.0, delay_seconds), now, error, task_id),
            )

    def list_tasks(
        self,
        *,
        role: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List queue tasks with optional filters."""
        limit = max(1, min(int(limit), 500))
        where_parts: list[str] = []
        params: list[Any] = []
        if role:
            where_parts.append("role = ?")
            params.append(role)
        if state:
            where_parts.append("state = ?")
            params.append(state)
        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

        query = f"""
            SELECT
                id, ticket_id, role, state, dedup_key, source_state, session_id,
                attempts, next_run_at, last_error,
                created_at, updated_at, started_at, finished_at
            FROM agent_tasks
            {where_sql}
            ORDER BY id DESC
            LIMIT ?
        """
        params.append(limit)

        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
            return [dict(r) for r in rows]

    def get_task(self, task_id: int) -> Optional[dict[str, Any]]:
        """Get one queue task by id."""
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    id, ticket_id, role, prompt, state, dedup_key, source_state,
                    session_id, attempts, next_run_at, last_error,
                    created_at, updated_at, started_at, finished_at
                FROM agent_tasks
                WHERE id = ?
                """,
                (int(task_id),),
            ).fetchone()
            return dict(row) if row else None
