"""User activity tracker for monitoring join/leave times and AFK status."""

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass
class UserSession:
    """Track a user's session in a channel."""
    username: str
    join_time: datetime
    last_activity: datetime
    is_afk: bool = False
    afk_start_time: datetime | None = None
    total_afk_seconds: int = 0


class ActivityTracker:
    """Tracks user presence and activity in channels.
    
    Monitors user join/leave events and AFK status to calculate:
    - Total time in channel
    - Active (not-AFK) time in channel
    
    Uses CyTube's setAFK events for accurate AFK tracking.
    """

    def __init__(self, logger: logging.Logger):
        """Initialize activity tracker.
        
        Args:
            logger: Logger instance
        """
        self.logger = logger

        # Track sessions: {(domain, channel, username): UserSession}
        self._sessions: dict[tuple[str, str, str], UserSession] = {}

        self._running = False

    def get_active_sessions(self) -> dict[tuple, list[str]]:
        """Get currently active sessions grouped by (domain, channel).
        
        Returns:
            Dict mapping (domain, channel) -> list of usernames
        """
        result: dict[tuple, list[str]] = {}

        for (domain, channel, username), session in self._sessions.items():
            key = (domain, channel)
            if key not in result:
                result[key] = []
            result[key].append(username)

        return result

    def get_active_session_count(self) -> int:
        """Get total number of active sessions."""
        return len(self._sessions)

    async def start(self) -> None:
        """Start the activity tracker."""
        if self._running:
            return

        self._running = True
        self.logger.info("Activity tracker started")

    async def stop(self) -> None:
        """Stop the activity tracker."""
        self._running = False
        self.logger.info("Activity tracker stopped")

    def user_joined(self, domain: str, channel: str, username: str) -> None:
        """Record user joining channel."""
        key = (domain, channel, username)
        now = datetime.now(UTC)

        self._sessions[key] = UserSession(
            username=username,
            join_time=now,
            last_activity=now,
            is_afk=False
        )

    def user_left(self, domain: str, channel: str, username: str) -> tuple[int, int] | None:
        """Record user leaving channel.
        
        Returns:
            Tuple of (total_seconds, not_afk_seconds) or None if user not tracked
        """
        key = (domain, channel, username)
        session = self._sessions.pop(key, None)

        if not session:
            return None

        now = datetime.now(UTC)
        total_seconds = int((now - session.join_time).total_seconds())

        # If user is currently AFK, add the current AFK period to total
        if session.is_afk and session.afk_start_time:
            afk_duration = int((now - session.afk_start_time).total_seconds())
            session.total_afk_seconds += afk_duration

        # Calculate not-AFK time
        not_afk_seconds = total_seconds - session.total_afk_seconds

        return (total_seconds, not_afk_seconds)

    def user_activity(self, domain: str, channel: str, username: str) -> None:
        """Record user activity (message, etc.)."""
        key = (domain, channel, username)
        session = self._sessions.get(key)

        if not session:
            # User not tracked, start tracking
            self.user_joined(domain, channel, username)
            return

        now = datetime.now(UTC)
        session.last_activity = now

    def set_afk_status(self, domain: str, channel: str, username: str, is_afk: bool) -> None:
        """Set user's AFK status based on CyTube setAFK event.
        
        Args:
            domain: Channel domain
            channel: Channel name
            username: Username
            is_afk: True if user is now AFK, False if returning from AFK
        """
        key = (domain, channel, username)
        session = self._sessions.get(key)

        if not session:
            # User not tracked, start tracking
            self.user_joined(domain, channel, username)
            session = self._sessions.get(key)
            if not session:
                return

        now = datetime.now(UTC)

        if is_afk and not session.is_afk:
            # User just went AFK
            session.is_afk = True
            session.afk_start_time = now
            self.logger.debug(f"User {username} went AFK in {channel}")

        elif not is_afk and session.is_afk:
            # User returned from AFK
            if session.afk_start_time:
                afk_duration = int((now - session.afk_start_time).total_seconds())
                session.total_afk_seconds += afk_duration
                self.logger.debug(f"User {username} returned from AFK in {channel} (was AFK for {afk_duration}s)")

            session.is_afk = False
            session.afk_start_time = None

    async def _periodic_afk_check(self) -> None:
        """Periodically check for AFK users."""
        while self._running:
            try:
                await asyncio.sleep(60)  # Check every minute

                now = datetime.now(UTC)
                for session in self._sessions.values():
                    if not session.is_afk:
                        idle_seconds = (now - session.last_activity).total_seconds()
                        if idle_seconds >= self.afk_threshold:
                            session.is_afk = True
                            self.logger.debug(
                                f"User {session.username} marked as AFK after {idle_seconds}s idle"
                            )

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in AFK check: {e}", exc_info=True)

    def get_active_sessions(self) -> dict[tuple[str, str], list[str]]:
        """Get all active sessions grouped by (domain, channel).
        
        Returns:
            Dict mapping (domain, channel) to list of active usernames
        """
        result: dict[tuple[str, str], list[str]] = {}

        for (domain, channel, username), session in self._sessions.items():
            key = (domain, channel)
            if key not in result:
                result[key] = []
            result[key].append(username)

        return result
