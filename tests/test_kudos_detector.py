"""Tests for KudosDetector."""

import logging

import pytest

from userstats.kudos_detector import KudosDetector


@pytest.fixture
def logger():
    """Create a test logger."""
    return logging.getLogger("test")


@pytest.fixture
def detector(logger):
    """Create a KudosDetector instance."""
    return KudosDetector(logger)


class TestKudosDetectorPlusPlus:
    """Tests for ++ kudos detection."""

    def test_plusplus_prefix(self, detector):
        """Test detecting ++username pattern."""
        result = detector.detect_plusplus_kudos("++alice for the help")
        assert result == ["alice"]

    def test_plusplus_suffix(self, detector):
        """Test detecting username++ pattern."""
        result = detector.detect_plusplus_kudos("thanks alice++")
        assert result == ["alice"]

    def test_plusplus_multiple(self, detector):
        """Test detecting multiple ++ kudos."""
        result = detector.detect_plusplus_kudos("++alice ++bob thanks charlie++")
        assert "alice" in result
        assert "bob" in result
        assert "charlie" in result
        assert len(result) == 3

    def test_plusplus_case_insensitive(self, detector):
        """Test that ++ detection is case insensitive."""
        result = detector.detect_plusplus_kudos("++Alice")
        assert result == ["Alice"]

    def test_plusplus_none(self, detector):
        """Test message with no ++ kudos."""
        result = detector.detect_plusplus_kudos("hello world")
        assert result == []

    def test_plusplus_at_boundaries(self, detector):
        """Test ++ at message boundaries."""
        # At start
        result = detector.detect_plusplus_kudos("++alice")
        assert result == ["alice"]

        # At end
        result = detector.detect_plusplus_kudos("alice++")
        assert result == ["alice"]


class TestKudosDetectorPhrases:
    """Tests for phrase-based kudos detection."""

    def test_no_phrases_set(self, detector):
        """Test with no trigger phrases returns empty."""
        result = detector.detect_phrase_kudos("lol alice")
        assert result == []

    def test_phrase_before_username(self, detector):
        """Test 'phrase username' pattern."""
        detector.set_trigger_phrases(["lol", "rofl"])

        result = detector.detect_phrase_kudos("lol alice")
        assert ("alice", "lol") in result

    def test_phrase_after_username(self, detector):
        """Test 'username phrase' pattern."""
        detector.set_trigger_phrases(["lol", "rofl"])

        result = detector.detect_phrase_kudos("alice lol")
        assert ("alice", "lol") in result

    def test_phrase_case_insensitive(self, detector):
        """Test phrase detection is case insensitive."""
        detector.set_trigger_phrases(["lol"])

        result = detector.detect_phrase_kudos("LOL alice")
        assert ("alice", "lol") in result

    def test_phrase_with_punctuation(self, detector):
        """Test phrases with punctuation are stripped."""
        detector.set_trigger_phrases(["lol"])

        result = detector.detect_phrase_kudos("lol, alice!")
        assert ("alice", "lol") in result

    def test_haha_pattern(self, detector):
        """Test automatic haha/hehe pattern detection."""
        # Need trigger phrases set to enter the detection loop
        detector.set_trigger_phrases(["lol"])

        result = detector.detect_phrase_kudos("hahaha alice")
        assert ("alice", "haha") in result

    def test_hehe_pattern(self, detector):
        """Test hehe pattern detection."""
        detector.set_trigger_phrases(["lol"])

        result = detector.detect_phrase_kudos("hehehe alice")
        assert ("alice", "haha") in result

    def test_hoho_pattern(self, detector):
        """Test hoho pattern detection."""
        detector.set_trigger_phrases(["lol"])

        result = detector.detect_phrase_kudos("hohoho alice")
        assert ("alice", "haha") in result

    def test_phrase_no_match(self, detector):
        """Test message with no phrase kudos."""
        detector.set_trigger_phrases(["lol"])

        result = detector.detect_phrase_kudos("hello world")
        assert result == []

    def test_phrase_too_short_username_ignored(self, detector):
        """Test that single-char usernames are ignored."""
        detector.set_trigger_phrases(["lol"])

        result = detector.detect_phrase_kudos("lol a")
        # 'a' should be ignored as too short
        assert result == []


class TestKudosDetectorTriggerPhrases:
    """Tests for trigger phrase management."""

    def test_set_trigger_phrases(self, detector):
        """Test setting trigger phrases."""
        detector.set_trigger_phrases(["lol", "rofl", "lmao"])

        assert len(detector._trigger_phrases) == 3
        assert "lol" in detector._trigger_phrases
        assert "rofl" in detector._trigger_phrases
        assert "lmao" in detector._trigger_phrases

    def test_trigger_phrases_normalized(self, detector):
        """Test that trigger phrases are normalized to lowercase."""
        detector.set_trigger_phrases(["LOL", "ROFL"])

        assert "lol" in detector._trigger_phrases
        assert "rofl" in detector._trigger_phrases

    def test_trigger_phrases_replaced(self, detector):
        """Test that setting phrases replaces previous list."""
        detector.set_trigger_phrases(["lol"])
        assert "lol" in detector._trigger_phrases

        detector.set_trigger_phrases(["rofl"])
        assert "rofl" in detector._trigger_phrases
        assert "lol" not in detector._trigger_phrases
