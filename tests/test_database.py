"""Tests for StatsDatabase."""

import logging
import tempfile
from pathlib import Path

import pytest
import pytest_asyncio

from userstats.database import StatsDatabase


@pytest.fixture
def logger():
    """Create a test logger."""
    return logging.getLogger("test")


@pytest_asyncio.fixture
async def temp_db(logger):
    """Create a temporary database for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        db = StatsDatabase(db_path, logger)
        await db.initialize()
        yield db


class TestDatabaseInitialization:
    """Tests for database initialization."""

    @pytest.mark.asyncio
    async def test_creates_database_file(self, logger):
        """Test that initialization creates the database file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            db = StatsDatabase(db_path, logger)

            assert not db_path.exists()
            await db.initialize()
            assert db_path.exists()

    @pytest.mark.asyncio
    async def test_creates_parent_directories(self, logger):
        """Test that initialization creates parent directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "nested" / "path" / "test.db"
            db = StatsDatabase(db_path, logger)

            await db.initialize()
            assert db_path.exists()

    @pytest.mark.asyncio
    async def test_initialize_idempotent(self, temp_db):
        """Test that initialization can be called multiple times."""
        # Should not raise
        await temp_db.initialize()
        await temp_db.initialize()


class TestUserTracking:
    """Tests for user tracking."""

    @pytest.mark.asyncio
    async def test_track_user(self, temp_db):
        """Test tracking a user creates user record."""
        await temp_db.track_user("alice")

        # Verify user exists by getting total users
        total = await temp_db.get_total_users()
        assert total == 1

    @pytest.mark.asyncio
    async def test_track_user_idempotent(self, temp_db):
        """Test that tracking same user multiple times is safe."""
        await temp_db.track_user("alice")
        await temp_db.track_user("alice")
        await temp_db.track_user("alice")

        # Should still be just 1 user
        total = await temp_db.get_total_users()
        assert total == 1


class TestMessageCounts:
    """Tests for message count tracking."""

    @pytest.mark.asyncio
    async def test_increment_message_count(self, temp_db):
        """Test incrementing message count."""
        await temp_db.increment_message_count("alice", "lounge", "cytu.be")

        count = await temp_db.get_user_message_count("alice", "lounge", "cytu.be")
        assert count == 1

    @pytest.mark.asyncio
    async def test_increment_message_count_multiple(self, temp_db):
        """Test incrementing message count multiple times."""
        await temp_db.increment_message_count("alice", "lounge", "cytu.be")
        await temp_db.increment_message_count("alice", "lounge", "cytu.be")
        await temp_db.increment_message_count("alice", "lounge", "cytu.be")

        count = await temp_db.get_user_message_count("alice", "lounge", "cytu.be")
        assert count == 3

    @pytest.mark.asyncio
    async def test_message_count_per_channel(self, temp_db):
        """Test that message counts are per-channel."""
        await temp_db.increment_message_count("alice", "lounge", "cytu.be")
        await temp_db.increment_message_count("alice", "lounge", "cytu.be")
        await temp_db.increment_message_count("alice", "games", "cytu.be")

        lounge_count = await temp_db.get_user_message_count("alice", "lounge", "cytu.be")
        games_count = await temp_db.get_user_message_count("alice", "games", "cytu.be")

        assert lounge_count == 2
        assert games_count == 1

    @pytest.mark.asyncio
    async def test_message_count_zero(self, temp_db):
        """Test getting count for user with no messages."""
        count = await temp_db.get_user_message_count("alice", "lounge", "cytu.be")
        assert count == 0


class TestEmoteUsage:
    """Tests for emote usage tracking."""

    @pytest.mark.asyncio
    async def test_increment_emote_usage(self, temp_db):
        """Test incrementing emote usage."""
        await temp_db.increment_emote_usage("alice", "lounge", "cytu.be", "poggers")

        # Get via user emote usage - returns 'count' (aliased from usage_count)
        stats = await temp_db.get_user_emote_usage("alice", "cytu.be")
        assert len(stats) == 1
        assert stats[0]["emote"] == "poggers"
        assert stats[0]["count"] == 1

    @pytest.mark.asyncio
    async def test_increment_emote_usage_multiple(self, temp_db):
        """Test incrementing same emote multiple times."""
        await temp_db.increment_emote_usage("alice", "lounge", "cytu.be", "poggers")
        await temp_db.increment_emote_usage("alice", "lounge", "cytu.be", "poggers")

        stats = await temp_db.get_user_emote_usage("alice", "cytu.be")
        assert stats[0]["count"] == 2

    @pytest.mark.asyncio
    async def test_emote_usage_per_emote(self, temp_db):
        """Test that usage is tracked per emote."""
        await temp_db.increment_emote_usage("alice", "lounge", "cytu.be", "poggers")
        await temp_db.increment_emote_usage("alice", "lounge", "cytu.be", "poggers")
        await temp_db.increment_emote_usage("alice", "lounge", "cytu.be", "kappa")

        stats = await temp_db.get_user_emote_usage("alice", "cytu.be")
        # Should have 2 emotes
        assert len(stats) == 2

        # Find each emote's count
        poggers_count = next((s["count"] for s in stats if s["emote"] == "poggers"), 0)
        kappa_count = next((s["count"] for s in stats if s["emote"] == "kappa"), 0)

        assert poggers_count == 2
        assert kappa_count == 1


class TestPopulationSnapshots:
    """Tests for population snapshot tracking."""

    @pytest.mark.asyncio
    async def test_save_population_snapshot(self, temp_db):
        """Test saving a population snapshot."""
        await temp_db.save_population_snapshot("lounge", "cytu.be", 50, 30)

        # Verify by getting recent snapshots
        snapshots = await temp_db.get_recent_population_snapshots("lounge", "cytu.be")
        assert len(snapshots) >= 1
        assert snapshots[0]["connected_count"] == 50
        assert snapshots[0]["chat_count"] == 30

    @pytest.mark.asyncio
    async def test_population_high_water_mark(self, temp_db):
        """Test that high water mark is tracked."""
        await temp_db.save_population_snapshot("lounge", "cytu.be", 50, 30)
        await temp_db.save_population_snapshot("lounge", "cytu.be", 100, 60)  # New high
        await temp_db.save_population_snapshot("lounge", "cytu.be", 75, 50)

        watermarks = await temp_db.get_water_marks("lounge", "cytu.be")
        assert watermarks["high"] is not None
        assert watermarks["high"]["total_users"] == 100

    @pytest.mark.asyncio
    async def test_population_low_water_mark(self, temp_db):
        """Test that low water mark is tracked."""
        await temp_db.save_population_snapshot("lounge", "cytu.be", 50, 30)
        await temp_db.save_population_snapshot("lounge", "cytu.be", 10, 5)  # New low
        await temp_db.save_population_snapshot("lounge", "cytu.be", 75, 50)

        watermarks = await temp_db.get_water_marks("lounge", "cytu.be")
        assert watermarks["low"] is not None
        assert watermarks["low"]["total_users"] == 10


class TestKudosTracking:
    """Tests for kudos tracking."""

    @pytest.mark.asyncio
    async def test_increment_plusplus_kudos(self, temp_db):
        """Test incrementing ++ kudos."""
        await temp_db.increment_kudos_plusplus("alice", "lounge", "cytu.be")

        kudos = await temp_db.get_user_kudos_plusplus("alice", "cytu.be")
        assert kudos == 1

    @pytest.mark.asyncio
    async def test_increment_plusplus_kudos_multiple(self, temp_db):
        """Test incrementing ++ kudos multiple times."""
        await temp_db.increment_kudos_plusplus("alice", "lounge", "cytu.be")
        await temp_db.increment_kudos_plusplus("alice", "lounge", "cytu.be")
        await temp_db.increment_kudos_plusplus("alice", "lounge", "cytu.be")

        kudos = await temp_db.get_user_kudos_plusplus("alice", "cytu.be")
        assert kudos == 3


class TestUserActivity:
    """Tests for user activity time tracking."""

    @pytest.mark.asyncio
    async def test_update_user_activity(self, temp_db):
        """Test updating user activity time."""
        await temp_db.update_user_activity("alice", "lounge", "cytu.be", 3600, 3000)

        activity = await temp_db.get_user_activity_stats("alice", "lounge", "cytu.be")
        assert activity is not None
        assert activity["total_time_seconds"] == 3600
        assert activity["not_afk_time_seconds"] == 3000

    @pytest.mark.asyncio
    async def test_update_user_activity_accumulates(self, temp_db):
        """Test that activity time accumulates."""
        await temp_db.update_user_activity("alice", "lounge", "cytu.be", 3600, 3000)
        await temp_db.update_user_activity("alice", "lounge", "cytu.be", 1800, 1500)

        activity = await temp_db.get_user_activity_stats("alice", "lounge", "cytu.be")
        assert activity["total_time_seconds"] == 5400  # 3600 + 1800
        assert activity["not_afk_time_seconds"] == 4500  # 3000 + 1500


class TestMediaChanges:
    """Tests for media change logging."""

    @pytest.mark.asyncio
    async def test_log_media_change(self, temp_db):
        """Test logging a media change."""
        await temp_db.log_media_change(
            "lounge", "cytu.be", "Cool Video", "yt", "abc123"
        )

        recent = await temp_db.get_recent_media_changes("lounge", "cytu.be", limit=1)
        assert len(recent) == 1
        assert recent[0]["media_title"] == "Cool Video"
        assert recent[0]["media_type"] == "yt"
        assert recent[0]["media_id"] == "abc123"

    @pytest.mark.asyncio
    async def test_get_recent_media_order(self, temp_db):
        """Test that recent media is in reverse chronological order."""
        await temp_db.log_media_change("lounge", "cytu.be", "First", "yt", "1")
        await temp_db.log_media_change("lounge", "cytu.be", "Second", "yt", "2")
        await temp_db.log_media_change("lounge", "cytu.be", "Third", "yt", "3")

        recent = await temp_db.get_recent_media_changes("lounge", "cytu.be", limit=3)
        assert len(recent) == 3
        assert recent[0]["media_title"] == "Third"
        assert recent[1]["media_title"] == "Second"
        assert recent[2]["media_title"] == "First"
