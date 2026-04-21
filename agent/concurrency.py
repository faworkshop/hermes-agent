import sqlite3
import time
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

class ConcurrencyManager:
    def __init__(self, db_path: str = "agent_state.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS locks (
                    ticket_id TEXT PRIMARY KEY,
                    assignee TEXT,
                    locked_at REAL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS retries (
                    ticket_id TEXT PRIMARY KEY,
                    count INTEGER DEFAULT 0
                )
            """)
            conn.commit()

    def acquire_lock(self, ticket_id: str, agent_name: str, timeout: float = 1800.0) -> bool:
        """Try to acquire a lock for a ticket. Stale locks (older than `timeout` seconds) are auto-expired."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT assignee, locked_at FROM locks WHERE ticket_id = ?", (ticket_id,))
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
                        return True
                    logger.warning(f"Ticket {ticket_id} is already locked by {current_assignee} ({age:.0f}s old)")
                    return False

            cursor.execute(
                "INSERT INTO locks (ticket_id, assignee, locked_at) VALUES (?, ?, ?)",
                (ticket_id, agent_name, time.time())
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
            cursor.execute("SELECT assignee, locked_at FROM locks WHERE ticket_id = ?", (ticket_id,))
            row = cursor.fetchone()
            if row:
                return {"role": row[0], "acquired_at": row[1]}
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
