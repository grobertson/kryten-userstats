"""Emote detector for tracking emote usage."""

import logging
import re


class EmoteDetector:
    """Detects emote usage in messages.

    Emotes are identified by hashtags matching the master emote list.
    """

    def __init__(self, logger: logging.Logger):
        """Initialize emote detector.

        Args:
            logger: Logger instance
        """
        self.logger = logger
        self._emotes: set[str] = set()

    def set_emote_list(self, emotes: list[str]) -> None:
        """Set the master list of available emotes.

        Args:
            emotes: List of emote names (without # prefix)
        """
        self._emotes = {e.lower() for e in emotes}
        self.logger.info(f"Loaded {len(self._emotes)} emotes")

    def detect_emotes(self, message: str) -> list[str]:
        """Detect emotes in message.

        Args:
            message: Message text to scan

        Returns:
            List of emote names (without # prefix) found in message
        """
        if not self._emotes:
            self.logger.debug("Emote detection called but emote list is empty")
            return []

        found = []

        # Find all hashtags
        hashtags = re.findall(r"#(\w+)", message, re.IGNORECASE)

        if hashtags:
            self.logger.debug(f"Found {len(hashtags)} hashtags in message: {hashtags[:5]}")

        for tag in hashtags:
            tag_lower = tag.lower()
            if tag_lower in self._emotes:
                found.append(tag_lower)
                self.logger.debug(f"Matched emote: #{tag}")
            else:
                # Show a few examples of what we're looking for
                if len(self._emotes) > 0:
                    sample_emotes = list(self._emotes)[:3]
                    self.logger.debug(
                        f"No match for '#{tag}' (lowercase: '{tag_lower}'). "
                        f"Emote list sample: {sample_emotes}"
                    )

        return found
