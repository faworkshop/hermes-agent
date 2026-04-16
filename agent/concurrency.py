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

    def acquire_lock(self, ticket_id: str, agent_name: str) -> bool:
        """Try to acquire a lock for a ticket."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT assignee FROM locks WHERE ticket_id = ?", (ticket_id,))
            row = cursor.fetchone()
            
            if row:
                current_assignee = row[0]
                if current_assignee == agent_name:
                    return True
                logger.warning(f"Ticket {ticket_id} is already locked by {current_assignee}")
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
