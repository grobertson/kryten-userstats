"""Kudos detection and tracking."""

import re
import logging
from typing import List, Optional, Set


class KudosDetector:
    """Detects kudos patterns in messages.
    
    Supports:
    - ++ kudos: "++username" or "username++"
    - Phrase kudos: "phrase username" or "username phrase"
    """
    
    def __init__(self, logger: logging.Logger):
        """Initialize kudos detector.
        
        Args:
            logger: Logger instance
        """
        self.logger = logger
        self._trigger_phrases: Set[str] = set()
        
        # Regex patterns for ++ kudos
        self._plusplus_prefix = re.compile(r'\+\+(\w+)', re.IGNORECASE)
        self._plusplus_suffix = re.compile(r'(\w+)\+\+', re.IGNORECASE)
        
    def set_trigger_phrases(self, phrases: List[str]) -> None:
        """Set the list of trigger phrases for phrase-based kudos.
        
        Args:
            phrases: List of trigger phrases (e.g., ["lol", "rofl", "haha"])
        """
        self._trigger_phrases = {p.lower() for p in phrases}
        self.logger.info(f"Loaded {len(self._trigger_phrases)} kudos trigger phrases")
        
    def detect_plusplus_kudos(self, message: str) -> List[str]:
        """Detect ++ kudos in message.
        
        Args:
            message: Message text to scan
            
        Returns:
            List of usernames receiving ++ kudos
        """
        usernames = []
        
        # Check for ++username
        for match in self._plusplus_prefix.finditer(message):
            usernames.append(match.group(1))
            
        # Check for username++
        for match in self._plusplus_suffix.finditer(message):
            usernames.append(match.group(1))
            
        return usernames
        
    def detect_phrase_kudos(self, message: str) -> List[tuple[str, str]]:
        """Detect phrase-based kudos in message.
        
        Args:
            message: Message text to scan
            
        Returns:
            List of (username, phrase) tuples
        """
        if not self._trigger_phrases:
            return []
            
        results = []
        words = message.split()
        
        for i, word in enumerate(words):
            word_lower = word.lower().strip('.,!?;:')
            
            # Check if this word is a trigger phrase
            if word_lower in self._trigger_phrases:
                # Check if there's a word before it (username phrase)
                if i > 0:
                    username = words[i - 1].strip('.,!?;:@')
                    if username and len(username) > 1:
                        results.append((username, word_lower))
                        
                # Check if there's a word after it (phrase username)
                if i < len(words) - 1:
                    username = words[i + 1].strip('.,!?;:@')
                    if username and len(username) > 1:
                        results.append((username, word_lower))
                        
            # Check for repeating "haha", "hehe", etc.
            if re.match(r'^(ha|he|ho){2,}$', word_lower, re.IGNORECASE):
                # Check surrounding words
                if i > 0:
                    username = words[i - 1].strip('.,!?;:@')
                    if username and len(username) > 1:
                        results.append((username, 'haha'))
                        
                if i < len(words) - 1:
                    username = words[i + 1].strip('.,!?;:@')
                    if username and len(username) > 1:
                        results.append((username, 'haha'))
                        
        return results
