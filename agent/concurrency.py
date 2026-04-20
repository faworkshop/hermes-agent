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
                    locked_at REAL,
                    pending_assignee TEXT,
                    pending_at REAL
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
        """Try to acquire a lock for a ticket.
        
        Allows immediate acquisition if:
        - No lock exists (fresh ticket)
        - Lock is stale (> timeout seconds)
        - Current assignee is the same agent (re-entrancy)
        - A pending_handoff exists and the requesting agent IS the pending assignee
          (previous agent started a state transition to this agent's role)
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT assignee, locked_at, pending_assignee, pending_at FROM locks WHERE ticket_id = ?",
                (ticket_id,)
            )
            row = cursor.fetchone()

            if row:
                current_assignee, locked_at, pending_assignee, pending_at = row[0], row[1], row[2], row[3]
                age = time.time() - locked_at

                # Expire stale locks
                if age > timeout:
                    logger.info(
                        f"Ticket {ticket_id} lock by {current_assignee} is stale "
                        f"({age:.0f}s old, max {timeout}s). Expiring and re-acquiring."
                    )
                    conn.execute("DELETE FROM locks WHERE ticket_id = ?", (ticket_id,))
                    conn.commit()
                    cursor.execute(
                        "INSERT INTO locks (ticket_id, assignee, locked_at) VALUES (?, ?, ?)",
                        (ticket_id, agent_name, time.time())
                    )
                    conn.commit()
                    return True

                # Same agent re-entrancy — always allowed
                if current_assignee == agent_name:
                    return True

                # Pending handoff — previous agent transitioned this ticket to us.
                # Allow acquisition while the previous agent finishes.
                if pending_assignee == agent_name and pending_at and (time.time() - pending_at) < 60:
                    logger.info(
                        f"Ticket {ticket_id}: pending handoff from {current_assignee} to {agent_name} "
                        f"(initiated {time.time() - pending_at:.0f}s ago). Allowing."
                    )
                    # Upgrade: update assignee to us, clear pending
                    conn.execute(
                        "UPDATE locks SET assignee=?, pending_assignee=NULL, pending_at=NULL, locked_at=? "
                        "WHERE ticket_id=?",
                        (agent_name, time.time(), ticket_id)
                    )
                    conn.commit()
                    return True

                logger.warning(
                    f"Ticket {ticket_id} is already locked by {current_assignee} ({age:.0f}s old), "
                    f"pending={pending_assignee}"
                )
                return False

            # No lock — fresh acquire
            cursor.execute(
                "INSERT INTO locks (ticket_id, assignee, locked_at) VALUES (?, ?, ?)",
                (ticket_id, agent_name, time.time())
            )
            conn.commit()
            return True

    def release_lock(self, ticket_id: str, agent_name: str):
        """Release a lock if held by the agent. Clears any pending handoff too."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM locks WHERE ticket_id = ? AND assignee = ?",
                (ticket_id, agent_name)
            )
            conn.commit()

    def propose_handoff(self, ticket_id: str, current_assignee: str, next_assignee: str) -> bool:
        """Record a pending handoff from current_assignee to next_assignee.
        
        Called by an agent BEFORE it updates Linear's state to the next role's state.
        This allows the next agent to acquire the lock immediately without waiting
        for the current agent to finish.
        
        Returns True if handoff was recorded, False if lock not held by current_assignee.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT assignee FROM locks WHERE ticket_id = ? AND assignee = ?",
                (ticket_id, current_assignee)
            )
            if not cursor.fetchone():
                return False
            conn.execute(
                "UPDATE locks SET pending_assignee=?, pending_at=? WHERE ticket_id=?",
                (next_assignee, time.time(), ticket_id)
            )
            conn.commit()
            return True

    def clear_handoff(self, ticket_id: str, agent_name: str):
        """Clear any pending handoff. Call if the state transition was aborted."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE locks SET pending_assignee=NULL, pending_at=NULL "
                "WHERE ticket_id=? AND assignee=?",
                (ticket_id, agent_name)
            )
            conn.commit()

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
