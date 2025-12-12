"""SQLite database manager for user statistics."""

import asyncio
import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class StatsDatabase:
    """Manages SQLite database for user statistics tracking.

    Handles all data persistence including:
    - User activity tracking (messages, PMs, emotes)
    - Channel population snapshots
    - Media change logs
    - Kudos system (++ and phrase-based)
    - Username aliases
    """

    def __init__(self, db_path: str | Path, logger: logging.Logger):
        """Initialize database manager.

        Args:
            db_path: Path to SQLite database file
            logger: Logger instance
        """
        self.db_path = Path(db_path)
        self.logger = logger

    async def initialize(self) -> None:
        """Initialize database and create tables.

        Creates parent directories if they don't exist and sets up all required tables.
        Safe to call multiple times - uses CREATE TABLE IF NOT EXISTS.

        Raises:
            OSError: If directory creation fails
            sqlite3.Error: If database initialization fails
        """
        try:
            # Ensure parent directory exists
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"Database directory ensured: {self.db_path.parent}")
        except OSError as e:
            self.logger.error(f"Failed to create database directory: {e}")
            raise

        try:
            # Run in executor to avoid blocking
            await asyncio.get_event_loop().run_in_executor(
                None, self._create_tables
            )

            self.logger.info(f"Database initialized at {self.db_path}")
        except sqlite3.Error as e:
            self.logger.error(f"Failed to initialize database: {e}")
            raise

    def _create_tables(self) -> None:
        """Create all required tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Users table - track all seen usernames
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            )
        """)

        # User aliases - configurable username mappings
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_aliases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                alias TEXT NOT NULL,
                UNIQUE(username, alias)
            )
        """)

        # Message counts - public messages by user per channel
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS message_counts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                channel TEXT NOT NULL,
                domain TEXT NOT NULL,
                message_count INTEGER DEFAULT 0,
                UNIQUE(username, channel, domain)
            )
        """)

        # PM counts - private messages from each user
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pm_counts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                pm_count INTEGER DEFAULT 0,
                UNIQUE(username)
            )
        """)

        # Channel population snapshots - every 5 minutes
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS population_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                domain TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                connected_count INTEGER NOT NULL,
                chat_count INTEGER NOT NULL
            )
        """)

        # Media changes log
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS media_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                domain TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                media_title TEXT NOT NULL,
                media_type TEXT,
                media_id TEXT
            )
        """)

        # User activity time tracking
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                channel TEXT NOT NULL,
                domain TEXT NOT NULL,
                total_time_seconds INTEGER DEFAULT 0,
                not_afk_time_seconds INTEGER DEFAULT 0,
                UNIQUE(username, channel, domain)
            )
        """)

        # Emote usage tracking
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS emote_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                channel TEXT NOT NULL,
                domain TEXT NOT NULL,
                emote TEXT NOT NULL,
                usage_count INTEGER DEFAULT 0,
                UNIQUE(username, channel, domain, emote)
            )
        """)

        # Kudos system - ++ based
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS kudos_plusplus (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                channel TEXT NOT NULL,
                domain TEXT NOT NULL,
                kudos_count INTEGER DEFAULT 0,
                UNIQUE(username, channel, domain)
            )
        """)

        # Kudos system - phrase based
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS kudos_phrases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                channel TEXT NOT NULL,
                domain TEXT NOT NULL,
                phrase TEXT NOT NULL,
                kudos_count INTEGER DEFAULT 0,
                UNIQUE(username, channel, domain, phrase)
            )
        """)

        # Kudos trigger phrases configuration
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS kudos_trigger_phrases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phrase TEXT UNIQUE NOT NULL
            )
        """)

        # Population water marks - high and low marks for user counts
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS population_watermarks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                domain TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                total_users INTEGER NOT NULL,
                chat_users INTEGER NOT NULL,
                is_high_mark INTEGER NOT NULL,
                UNIQUE(channel, domain, timestamp, is_high_mark)
            )
        """)

        # Movie voting - track votes for media titles
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movie_votes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                domain TEXT NOT NULL,
                media_title TEXT NOT NULL,
                media_type TEXT,
                media_id TEXT,
                username TEXT NOT NULL,
                vote INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                UNIQUE(channel, domain, media_title, username)
            )
        """)

        # Create indices for common queries
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_message_counts_lookup ON message_counts(username, channel, domain)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_population_timestamp ON population_snapshots(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_media_changes_channel ON media_changes(channel, domain, timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_activity_lookup ON user_activity(username, channel, domain)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_emote_usage_lookup ON emote_usage(username, channel, domain, emote)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_watermarks_lookup ON population_watermarks(channel, domain, timestamp DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_movie_votes_lookup ON movie_votes(channel, domain, media_title)")

        conn.commit()

        # Log table counts for verification
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = cursor.fetchall()
        self.logger.debug(f"Database contains {len(tables)} tables: {[t[0] for t in tables]}")

        conn.close()

    async def track_user(self, username: str) -> None:
        """Track a seen username (insert or update last_seen)."""
        now = datetime.now(UTC).isoformat()

        def _track():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO users (username, first_seen_at, last_seen_at)
                VALUES (?, ?, ?)
                ON CONFLICT(username) DO UPDATE SET last_seen_at = ?
            """, (username, now, now, now))
            conn.commit()
            conn.close()

        await asyncio.get_event_loop().run_in_executor(None, _track)

    async def increment_message_count(self, username: str, channel: str, domain: str) -> None:
        """Increment public message count for user in channel."""
        def _increment():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO message_counts (username, channel, domain, message_count)
                VALUES (?, ?, ?, 1)
                ON CONFLICT(username, channel, domain) DO UPDATE SET message_count = message_count + 1
            """, (username, channel, domain))
            conn.commit()
            conn.close()

        await asyncio.get_event_loop().run_in_executor(None, _increment)

    async def increment_pm_count(self, username: str) -> None:
        """Increment PM count for user."""
        def _increment():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO pm_counts (username, pm_count)
                VALUES (?, 1)
                ON CONFLICT(username) DO UPDATE SET pm_count = pm_count + 1
            """, (username,))
            conn.commit()
            conn.close()

        await asyncio.get_event_loop().run_in_executor(None, _increment)

    async def save_population_snapshot(
        self, channel: str, domain: str, connected_count: int, chat_count: int
    ) -> None:
        """Save channel population snapshot and update water marks."""
        def _save():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            timestamp = datetime.now(UTC).isoformat()

            # Save snapshot
            cursor.execute("""
                INSERT INTO population_snapshots (channel, domain, timestamp, connected_count, chat_count)
                VALUES (?, ?, ?, ?, ?)
            """, (channel, domain, timestamp, connected_count, chat_count))

            # Check for high water mark (last 24 hours)
            cursor.execute("""
                SELECT MAX(connected_count) as max_count FROM population_snapshots
                WHERE channel = ? AND domain = ?
                AND datetime(timestamp) >= datetime('now', '-1 day')
            """, (channel, domain))
            result = cursor.fetchone()
            max_count = result[0] if result else 0

            if connected_count >= max_count:
                # New high water mark
                cursor.execute("""
                    INSERT OR REPLACE INTO population_watermarks
                    (channel, domain, timestamp, total_users, chat_users, is_high_mark)
                    VALUES (?, ?, ?, ?, ?, 1)
                """, (channel, domain, timestamp, connected_count, chat_count))

            # Check for low water mark (last 24 hours)
            cursor.execute("""
                SELECT MIN(connected_count) as min_count FROM population_snapshots
                WHERE channel = ? AND domain = ?
                AND datetime(timestamp) >= datetime('now', '-1 day')
            """, (channel, domain))
            result = cursor.fetchone()
            min_count = result[0] if result else float('inf')

            if connected_count <= min_count:
                # New low water mark
                cursor.execute("""
                    INSERT OR REPLACE INTO population_watermarks
                    (channel, domain, timestamp, total_users, chat_users, is_high_mark)
                    VALUES (?, ?, ?, ?, ?, 0)
                """, (channel, domain, timestamp, connected_count, chat_count))

            conn.commit()
            conn.close()

        await asyncio.get_event_loop().run_in_executor(None, _save)

    async def log_media_change(
        self, channel: str, domain: str, title: str, media_type: str = "", media_id: str = ""
    ) -> None:
        """Log media title change."""
        now = datetime.now(UTC).isoformat()

        def _log():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO media_changes (channel, domain, timestamp, media_title, media_type, media_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (channel, domain, now, title, media_type, media_id))
            conn.commit()
            conn.close()

        await asyncio.get_event_loop().run_in_executor(None, _log)

    async def update_user_activity(
        self, username: str, channel: str, domain: str, total_seconds: int, not_afk_seconds: int
    ) -> None:
        """Update user activity time."""
        def _update():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO user_activity (username, channel, domain, total_time_seconds, not_afk_time_seconds)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(username, channel, domain) DO UPDATE SET
                    total_time_seconds = total_time_seconds + ?,
                    not_afk_time_seconds = not_afk_time_seconds + ?
            """, (username, channel, domain, total_seconds, not_afk_seconds, total_seconds, not_afk_seconds))
            conn.commit()
            conn.close()

        await asyncio.get_event_loop().run_in_executor(None, _update)

    async def increment_emote_usage(
        self, username: str, channel: str, domain: str, emote: str
    ) -> None:
        """Increment emote usage count."""
        def _increment():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO emote_usage (username, channel, domain, emote, usage_count)
                VALUES (?, ?, ?, ?, 1)
                ON CONFLICT(username, channel, domain, emote) DO UPDATE SET usage_count = usage_count + 1
            """, (username, channel, domain, emote))
            conn.commit()
            conn.close()

        await asyncio.get_event_loop().run_in_executor(None, _increment)

    async def increment_kudos_plusplus(self, username: str, channel: str, domain: str) -> None:
        """Increment ++ kudos for user."""
        def _increment():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO kudos_plusplus (username, channel, domain, kudos_count)
                VALUES (?, ?, ?, 1)
                ON CONFLICT(username, channel, domain) DO UPDATE SET kudos_count = kudos_count + 1
            """, (username, channel, domain))
            conn.commit()
            conn.close()

        await asyncio.get_event_loop().run_in_executor(None, _increment)

    async def increment_kudos_phrase(
        self, username: str, channel: str, domain: str, phrase: str
    ) -> None:
        """Increment phrase-based kudos for user."""
        def _increment():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO kudos_phrases (username, channel, domain, phrase, kudos_count)
                VALUES (?, ?, ?, ?, 1)
                ON CONFLICT(username, channel, domain, phrase) DO UPDATE SET kudos_count = kudos_count + 1
            """, (username, channel, domain, phrase))
            conn.commit()
            conn.close()

        await asyncio.get_event_loop().run_in_executor(None, _increment)

    async def get_user_aliases(self, username: str) -> list[str]:
        """Get all aliases for a username."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT alias FROM user_aliases WHERE username = ?", (username,))
            aliases = [row[0] for row in cursor.fetchall()]
            conn.close()
            return aliases

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def add_user_alias(self, username: str, alias: str) -> None:
        """Add an alias for a username."""
        def _add():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO user_aliases (username, alias) VALUES (?, ?)
                """, (username, alias))
                conn.commit()
            except sqlite3.IntegrityError:
                pass  # Alias already exists
            finally:
                conn.close()

        await asyncio.get_event_loop().run_in_executor(None, _add)

    async def get_trigger_phrases(self) -> list[str]:
        """Get all kudos trigger phrases."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT phrase FROM kudos_trigger_phrases")
            phrases = [row[0] for row in cursor.fetchall()]
            conn.close()
            return phrases

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def add_trigger_phrase(self, phrase: str) -> None:
        """Add a kudos trigger phrase."""
        def _add():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            try:
                cursor.execute("INSERT INTO kudos_trigger_phrases (phrase) VALUES (?)", (phrase,))
                conn.commit()
            except sqlite3.IntegrityError:
                pass  # Phrase already exists
            finally:
                conn.close()

        await asyncio.get_event_loop().run_in_executor(None, _add)

    async def resolve_username(self, name: str) -> str:
        """Resolve alias to canonical username, or return name if not an alias."""
        def _resolve():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT username FROM user_aliases WHERE alias = ?", (name.lower(),))
            row = cursor.fetchone()
            conn.close()
            return row[0] if row else name

        return await asyncio.get_event_loop().run_in_executor(None, _resolve)

    # ===== Query methods for metrics and NATS endpoints =====

    async def get_total_users(self) -> int:
        """Get total number of tracked users."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users")
            count = cursor.fetchone()[0]
            conn.close()
            return count

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def get_total_messages(self) -> int:
        """Get total message count across all channels."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT SUM(message_count) FROM message_counts")
            result = cursor.fetchone()[0]
            conn.close()
            return result or 0

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def get_total_pms(self) -> int:
        """Get total PM count."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT SUM(pm_count) FROM pm_counts")
            result = cursor.fetchone()[0]
            conn.close()
            return result or 0

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def get_total_kudos_plusplus(self) -> int:
        """Get total ++ kudos count."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT SUM(kudos_count) FROM kudos_plusplus")
            result = cursor.fetchone()[0]
            conn.close()
            return result or 0

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def get_total_emote_usage(self) -> int:
        """Get total emote usage count."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT SUM(usage_count) FROM emote_usage")
            result = cursor.fetchone()[0]
            conn.close()
            return result or 0

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def get_total_media_changes(self) -> int:
        """Get total media changes logged."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM media_changes")
            count = cursor.fetchone()[0]
            conn.close()
            return count

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def get_user_message_count(self, username: str, channel: str, domain: str) -> int:
        """Get message count for a user in a channel."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT message_count FROM message_counts
                WHERE username = ? AND channel = ? AND domain = ?
            """, (username, channel, domain))
            row = cursor.fetchone()
            conn.close()
            return row[0] if row else 0

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def get_user_all_message_counts(self, username: str, domain: str) -> list[dict[str, Any]]:
        """Get all message counts for a user across channels."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT channel, message_count as count FROM message_counts
                WHERE username = ? AND domain = ?
                ORDER BY message_count DESC
            """, (username, domain))
            rows = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return rows

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def get_user_pm_count(self, username: str) -> int:
        """Get PM count for a user."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT pm_count FROM pm_counts WHERE username = ?", (username,))
            row = cursor.fetchone()
            conn.close()
            return row[0] if row else 0

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def get_user_activity_stats(self, username: str, channel: str, domain: str) -> dict[str, int] | None:
        """Get activity statistics for a user in a channel."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT total_time_seconds, not_afk_time_seconds FROM user_activity
                WHERE username = ? AND channel = ? AND domain = ?
            """, (username, channel, domain))
            row = cursor.fetchone()
            conn.close()
            return dict(row) if row else None

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def get_user_all_activity(self, username: str, domain: str) -> list[dict[str, Any]]:
        """Get all activity for a user across channels."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT channel, total_time_seconds, not_afk_time_seconds FROM user_activity
                WHERE username = ? AND domain = ?
            """, (username, domain))
            rows = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return rows

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def get_user_kudos_plusplus(self, username: str, domain: str) -> int:
        """Get ++ kudos count for a user."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT SUM(kudos_count) FROM kudos_plusplus
                WHERE username = ? AND domain = ?
            """, (username, domain))
            result = cursor.fetchone()[0]
            conn.close()
            return result or 0

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def get_user_kudos_phrases(self, username: str, domain: str) -> list[dict[str, Any]]:
        """Get phrase kudos for a user."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT phrase, SUM(kudos_count) as count FROM kudos_phrases
                WHERE username = ? AND domain = ?
                GROUP BY phrase
                ORDER BY count DESC
            """, (username, domain))
            rows = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return rows

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def get_user_emote_usage(self, username: str, domain: str) -> list[dict[str, Any]]:
        """Get emote usage for a user."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT emote, SUM(usage_count) as count FROM emote_usage
                WHERE username = ? AND domain = ?
                GROUP BY emote
                ORDER BY count DESC
            """, (username, domain))
            rows = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return rows

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def get_top_message_senders(self, channel: str, domain: str, limit: int = 10) -> list[dict[str, Any]]:
        """Get top message senders in a channel."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT username, message_count as count FROM message_counts
                WHERE channel = ? AND domain = ?
                ORDER BY message_count DESC
                LIMIT ?
            """, (channel, domain, limit))
            rows = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return rows

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def get_recent_population_snapshots(self, channel: str, domain: str, hours: int = 24) -> list[dict[str, Any]]:
        """Get recent population snapshots."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT timestamp, connected_count, chat_count FROM population_snapshots
                WHERE channel = ? AND domain = ?
                AND datetime(timestamp) >= datetime('now', '-' || ? || ' hours')
                ORDER BY timestamp DESC
            """, (channel, domain, hours))
            rows = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return rows

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def get_recent_media_changes(self, channel: str, domain: str, limit: int = 20) -> list[dict[str, Any]]:
        """Get recent media changes."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT timestamp, media_title, media_type, media_id FROM media_changes
                WHERE channel = ? AND domain = ?
                ORDER BY timestamp DESC
                LIMIT ?
            """, (channel, domain, limit))
            rows = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return rows

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def get_global_message_leaderboard(self, domain: str, limit: int = 20) -> list[dict[str, Any]]:
        """Get global message leaderboard across all channels."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT username, SUM(message_count) as count FROM message_counts
                WHERE domain = ?
                GROUP BY username
                ORDER BY count DESC
                LIMIT ?
            """, (domain, limit))
            rows = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return rows

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def get_global_kudos_leaderboard(self, domain: str, limit: int = 20) -> list[dict[str, Any]]:
        """Get global kudos leaderboard."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT username, SUM(kudos_count) as count FROM kudos_plusplus
                WHERE domain = ?
                GROUP BY username
                ORDER BY count DESC
                LIMIT ?
            """, (domain, limit))
            rows = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return rows

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def get_top_emotes(self, domain: str, limit: int = 20) -> list[dict[str, Any]]:
        """Get most used emotes."""
        def _get():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT emote, SUM(usage_count) as count FROM emote_usage
                WHERE domain = ?
                GROUP BY emote
                ORDER BY count DESC
                LIMIT ?
            """, (domain, limit))
            rows = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return rows

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def get_water_marks(self, channel: str, domain: str, days: int = None) -> dict[str, Any]:
        """Get high and low water marks for user population.

        Args:
            channel: Channel name
            domain: Domain name
            days: Number of days to look back (None for all time)

        Returns:
            Dict with 'high' and 'low' water mark data
        """
        def _get():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            date_filter = ""
            params = [channel, domain]
            if days is not None:
                date_filter = "AND datetime(timestamp) >= datetime('now', '-' || ? || ' days')"
                params.append(days)

            # Get high water mark
            cursor.execute(f"""
                SELECT timestamp, total_users, chat_users FROM population_watermarks
                WHERE channel = ? AND domain = ? AND is_high_mark = 1
                {date_filter}
                ORDER BY total_users DESC, timestamp DESC
                LIMIT 1
            """, params)
            high_mark = cursor.fetchone()

            # Get low water mark
            cursor.execute(f"""
                SELECT timestamp, total_users, chat_users FROM population_watermarks
                WHERE channel = ? AND domain = ? AND is_high_mark = 0
                {date_filter}
                ORDER BY total_users ASC, timestamp DESC
                LIMIT 1
            """, params)
            low_mark = cursor.fetchone()

            conn.close()

            return {
                'high': dict(high_mark) if high_mark else None,
                'low': dict(low_mark) if low_mark else None
            }

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def record_movie_vote(self, channel: str, domain: str, media_title: str,
                                media_type: str, media_id: str, username: str, vote: int) -> None:
        """Record a movie vote (1 for upvote, -1 for downvote)."""
        def _record():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            timestamp = datetime.now(UTC).isoformat()

            cursor.execute("""
                INSERT OR REPLACE INTO movie_votes
                (channel, domain, media_title, media_type, media_id, username, vote, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (channel, domain, media_title, media_type, media_id, username, vote, timestamp))

            conn.commit()
            conn.close()

        await asyncio.get_event_loop().run_in_executor(None, _record)

    async def get_movie_votes(self, channel: str, domain: str, media_title: str = None) -> dict[str, Any]:
        """Get movie voting statistics.

        Args:
            channel: Channel name
            domain: Domain name
            media_title: Specific movie title (None for all movies)

        Returns:
            Dict with vote statistics or list of movies
        """
        def _get():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            if media_title:
                # Get votes for specific movie
                cursor.execute("""
                    SELECT
                        media_title,
                        media_type,
                        media_id,
                        SUM(CASE WHEN vote > 0 THEN 1 ELSE 0 END) as upvotes,
                        SUM(CASE WHEN vote < 0 THEN 1 ELSE 0 END) as downvotes,
                        COUNT(*) as total_votes,
                        SUM(vote) as score
                    FROM movie_votes
                    WHERE channel = ? AND domain = ? AND media_title = ?
                    GROUP BY media_title, media_type, media_id
                """, (channel, domain, media_title))
                result = cursor.fetchone()
                conn.close()
                return dict(result) if result else None
            else:
                # Get all movies with votes
                cursor.execute("""
                    SELECT
                        media_title,
                        media_type,
                        media_id,
                        SUM(CASE WHEN vote > 0 THEN 1 ELSE 0 END) as upvotes,
                        SUM(CASE WHEN vote < 0 THEN 1 ELSE 0 END) as downvotes,
                        COUNT(*) as total_votes,
                        SUM(vote) as score,
                        MAX(timestamp) as last_vote
                    FROM movie_votes
                    WHERE channel = ? AND domain = ?
                    GROUP BY media_title, media_type, media_id
                    ORDER BY score DESC
                """, (channel, domain))
                rows = [dict(row) for row in cursor.fetchall()]
                conn.close()
                return rows

        return await asyncio.get_event_loop().run_in_executor(None, _get)

    async def get_time_series_messages(self, channel: str, domain: str,
                                       start_time: str = None, end_time: str = None) -> list[dict[str, Any]]:
        """Get message counts over time for charting.

        Returns hourly aggregated message counts.
        """
        def _get():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # For now, use population snapshots as a proxy for activity
            # In the future, could track messages by timestamp
            query = """
                SELECT
                    strftime('%Y-%m-%d %H:00:00', timestamp) as hour,
                    AVG(chat_count) as avg_active_users
                FROM population_snapshots
                WHERE channel = ? AND domain = ?
            """
            params = [channel, domain]

            if start_time:
                query += " AND datetime(timestamp) >= datetime(?)"
                params.append(start_time)
            if end_time:
                query += " AND datetime(timestamp) <= datetime(?)"
                params.append(end_time)

            query += " GROUP BY hour ORDER BY hour"

            cursor.execute(query, params)
            rows = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return rows

        return await asyncio.get_event_loop().run_in_executor(None, _get)
