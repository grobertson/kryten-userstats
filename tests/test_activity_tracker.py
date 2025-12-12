"""Tests for ActivityTracker."""

import logging
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from userstats.activity_tracker import ActivityTracker, UserSession


@pytest.fixture
def logger():
    """Create a test logger."""
    return logging.getLogger("test")


@pytest.fixture
def tracker(logger):
    """Create an ActivityTracker instance."""
    return ActivityTracker(logger)


class TestUserSession:
    """Tests for UserSession dataclass."""

    def test_default_values(self):
        """Test default values are set correctly."""
        now = datetime.now(timezone.utc)
        session = UserSession(username="alice", join_time=now, last_activity=now)

        assert session.username == "alice"
        assert session.join_time == now
        assert session.last_activity == now
        assert session.is_afk is False
        assert session.afk_start_time is None
        assert session.total_afk_seconds == 0


class TestActivityTracker:
    """Tests for ActivityTracker."""

    def test_initial_state(self, tracker):
        """Test initial state is empty."""
        assert tracker.get_active_session_count() == 0
        assert tracker.get_active_sessions() == {}

    def test_user_joined(self, tracker):
        """Test tracking a user join."""
        tracker.user_joined("cytu.be", "lounge", "alice")

        assert tracker.get_active_session_count() == 1
        sessions = tracker.get_active_sessions()
        assert ("cytu.be", "lounge") in sessions
        assert "alice" in sessions[("cytu.be", "lounge")]

    def test_user_joined_multiple_channels(self, tracker):
        """Test user joining multiple channels."""
        tracker.user_joined("cytu.be", "lounge", "alice")
        tracker.user_joined("cytu.be", "games", "alice")

        assert tracker.get_active_session_count() == 2
        sessions = tracker.get_active_sessions()
        assert "alice" in sessions[("cytu.be", "lounge")]
        assert "alice" in sessions[("cytu.be", "games")]

    def test_user_left_returns_time(self, tracker):
        """Test user leaving returns time spent."""
        with patch("userstats.activity_tracker.datetime") as mock_dt:
            # Set up time mocking
            join_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            leave_time = datetime(2024, 1, 1, 12, 30, 0, tzinfo=timezone.utc)

            mock_dt.now.return_value = join_time
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            tracker.user_joined("cytu.be", "lounge", "alice")

            mock_dt.now.return_value = leave_time
            result = tracker.user_left("cytu.be", "lounge", "alice")

        assert result is not None
        total_seconds, not_afk_seconds = result
        assert total_seconds == 1800  # 30 minutes
        assert not_afk_seconds == 1800  # All active

    def test_user_left_not_tracked(self, tracker):
        """Test leaving when user not tracked returns None."""
        result = tracker.user_left("cytu.be", "lounge", "unknown")
        assert result is None

    def test_user_left_removes_session(self, tracker):
        """Test that user_left removes the session."""
        tracker.user_joined("cytu.be", "lounge", "alice")
        assert tracker.get_active_session_count() == 1

        tracker.user_left("cytu.be", "lounge", "alice")
        assert tracker.get_active_session_count() == 0

    def test_user_activity_updates_last_activity(self, tracker):
        """Test that user_activity updates last_activity time."""
        tracker.user_joined("cytu.be", "lounge", "alice")

        # Get the session
        session = tracker._sessions[("cytu.be", "lounge", "alice")]
        original_activity = session.last_activity

        # Small delay then activity
        import time

        time.sleep(0.01)
        tracker.user_activity("cytu.be", "lounge", "alice")

        assert session.last_activity >= original_activity

    def test_user_activity_creates_session_if_not_exists(self, tracker):
        """Test that user_activity creates session if user not tracked."""
        assert tracker.get_active_session_count() == 0

        tracker.user_activity("cytu.be", "lounge", "alice")

        assert tracker.get_active_session_count() == 1

    def test_set_afk_status_going_afk(self, tracker):
        """Test setting AFK status to True."""
        tracker.user_joined("cytu.be", "lounge", "alice")
        tracker.set_afk_status("cytu.be", "lounge", "alice", True)

        session = tracker._sessions[("cytu.be", "lounge", "alice")]
        assert session.is_afk is True
        assert session.afk_start_time is not None

    def test_set_afk_status_returning_from_afk(self, tracker):
        """Test returning from AFK."""
        with patch("userstats.activity_tracker.datetime") as mock_dt:
            join_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            afk_start = datetime(2024, 1, 1, 12, 10, 0, tzinfo=timezone.utc)
            afk_end = datetime(2024, 1, 1, 12, 15, 0, tzinfo=timezone.utc)

            mock_dt.now.return_value = join_time
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            tracker.user_joined("cytu.be", "lounge", "alice")

            mock_dt.now.return_value = afk_start
            tracker.set_afk_status("cytu.be", "lounge", "alice", True)

            mock_dt.now.return_value = afk_end
            tracker.set_afk_status("cytu.be", "lounge", "alice", False)

        session = tracker._sessions[("cytu.be", "lounge", "alice")]
        assert session.is_afk is False
        assert session.afk_start_time is None
        assert session.total_afk_seconds == 300  # 5 minutes AFK

    def test_set_afk_creates_session_if_not_exists(self, tracker):
        """Test set_afk_status creates session if user not tracked."""
        tracker.set_afk_status("cytu.be", "lounge", "alice", True)

        assert tracker.get_active_session_count() == 1

    def test_user_left_with_current_afk(self, tracker):
        """Test leaving while AFK includes current AFK time."""
        with patch("userstats.activity_tracker.datetime") as mock_dt:
            join_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            afk_start = datetime(2024, 1, 1, 12, 10, 0, tzinfo=timezone.utc)
            leave_time = datetime(2024, 1, 1, 12, 30, 0, tzinfo=timezone.utc)

            mock_dt.now.return_value = join_time
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            tracker.user_joined("cytu.be", "lounge", "alice")

            mock_dt.now.return_value = afk_start
            tracker.set_afk_status("cytu.be", "lounge", "alice", True)

            mock_dt.now.return_value = leave_time
            result = tracker.user_left("cytu.be", "lounge", "alice")

        assert result is not None
        total_seconds, not_afk_seconds = result
        assert total_seconds == 1800  # 30 minutes total
        # AFK from 12:10 to 12:30 = 20 minutes = 1200 seconds
        # Not AFK = 1800 - 1200 = 600 seconds = 10 minutes
        assert not_afk_seconds == 600


class TestActivityTrackerLifecycle:
    """Tests for ActivityTracker start/stop."""

    @pytest.mark.asyncio
    async def test_start(self, tracker):
        """Test starting the tracker."""
        assert tracker._running is False

        await tracker.start()
        assert tracker._running is True

    @pytest.mark.asyncio
    async def test_stop(self, tracker):
        """Test stopping the tracker."""
        await tracker.start()
        assert tracker._running is True

        await tracker.stop()
        assert tracker._running is False

    @pytest.mark.asyncio
    async def test_start_idempotent(self, tracker):
        """Test that starting multiple times is safe."""
        await tracker.start()
        await tracker.start()  # Should not raise
        assert tracker._running is True
