"""Tests for EmoteDetector."""

import logging

import pytest

from userstats.emote_detector import EmoteDetector


@pytest.fixture
def logger():
    """Create a test logger."""
    return logging.getLogger("test")


@pytest.fixture
def detector(logger):
    """Create an EmoteDetector instance."""
    return EmoteDetector(logger)


class TestEmoteDetector:
    """Tests for EmoteDetector."""

    def test_initial_state_empty(self, detector):
        """Test that no emotes are loaded initially."""
        assert detector._emotes == set()

    def test_set_emote_list(self, detector):
        """Test setting emote list."""
        detector.set_emote_list(["poggers", "kappa", "lul"])

        assert len(detector._emotes) == 3
        assert "poggers" in detector._emotes
        assert "kappa" in detector._emotes
        assert "lul" in detector._emotes

    def test_set_emote_list_normalizes_case(self, detector):
        """Test that emote list is normalized to lowercase."""
        detector.set_emote_list(["Poggers", "KAPPA", "LuL"])

        assert "poggers" in detector._emotes
        assert "kappa" in detector._emotes
        assert "lul" in detector._emotes

    def test_detect_emotes_no_emotes_loaded(self, detector):
        """Test detecting with no emotes loaded returns empty list."""
        result = detector.detect_emotes("hello #poggers")
        assert result == []

    def test_detect_emotes_single(self, detector):
        """Test detecting a single emote."""
        detector.set_emote_list(["poggers", "kappa"])

        result = detector.detect_emotes("nice play #poggers")
        assert result == ["poggers"]

    def test_detect_emotes_multiple(self, detector):
        """Test detecting multiple emotes."""
        detector.set_emote_list(["poggers", "kappa", "lul"])

        result = detector.detect_emotes("#kappa that was #lul")
        assert "kappa" in result
        assert "lul" in result
        assert len(result) == 2

    def test_detect_emotes_case_insensitive(self, detector):
        """Test that detection is case insensitive."""
        detector.set_emote_list(["poggers"])

        result = detector.detect_emotes("nice #POGGERS")
        assert result == ["poggers"]

    def test_detect_emotes_no_match(self, detector):
        """Test detecting when no emotes match."""
        detector.set_emote_list(["poggers", "kappa"])

        result = detector.detect_emotes("hello world #unknown")
        assert result == []

    def test_detect_emotes_no_hashtags(self, detector):
        """Test detecting when message has no hashtags."""
        detector.set_emote_list(["poggers", "kappa"])

        result = detector.detect_emotes("hello world")
        assert result == []

    def test_detect_emotes_duplicate_usage(self, detector):
        """Test that duplicate emote usage is counted."""
        detector.set_emote_list(["poggers"])

        result = detector.detect_emotes("#poggers #poggers #poggers")
        assert result == ["poggers", "poggers", "poggers"]

    def test_detect_emotes_partial_match_not_counted(self, detector):
        """Test that partial matches are not counted."""
        detector.set_emote_list(["pog"])

        # #poggers should not match #pog
        result = detector.detect_emotes("#poggers")
        assert result == []

    def test_detect_emotes_in_sentence(self, detector):
        """Test detecting emotes embedded in sentences."""
        detector.set_emote_list(["kappa", "lul"])

        result = detector.detect_emotes("that joke was #lul and then he said #kappa")
        assert "lul" in result
        assert "kappa" in result

    def test_detect_emotes_at_boundaries(self, detector):
        """Test detecting emotes at message boundaries."""
        detector.set_emote_list(["kappa"])

        # At start
        result = detector.detect_emotes("#kappa hello")
        assert result == ["kappa"]

        # At end
        result = detector.detect_emotes("hello #kappa")
        assert result == ["kappa"]

    def test_set_emote_list_replaces_previous(self, detector):
        """Test that setting emote list replaces previous list."""
        detector.set_emote_list(["poggers", "kappa"])
        assert "poggers" in detector._emotes

        detector.set_emote_list(["lul", "omegalul"])
        assert "lul" in detector._emotes
        assert "poggers" not in detector._emotes
