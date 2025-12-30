"""Main user statistics tracking application."""

import asyncio
import json
import logging
import re
import signal
import time
from pathlib import Path
from typing import Any

from kryten import (
    ChangeMediaEvent,
    ChatMessageEvent,
    KrytenClient,
    UserJoinEvent,
    UserLeaveEvent,
)

from .activity_tracker import ActivityTracker
from .database import StatsDatabase
from .emote_detector import EmoteDetector
from .kudos_detector import KudosDetector
from .metrics_server import MetricsServer
from .nats_publisher import StatsPublisher


class UserStatsApp:
    """User statistics tracking microservice.

    Tracks:
    - User message counts (public and PM)
    - Channel population snapshots every 5 minutes
    - Media title changes
    - User activity time (total and not-AFK)
    - Emote usage
    - Kudos system (++ and phrase-based)
    """

    def __init__(self, config_path: str):
        """Initialize the application.

        Args:
            config_path: Path to configuration JSON file
        """
        self.config_path = Path(config_path)
        self.logger = logging.getLogger(__name__)

        # Components
        self.client: KrytenClient | None = None
        self.db: StatsDatabase | None = None
        self.activity_tracker: ActivityTracker | None = None
        self.kudos_detector: KudosDetector | None = None
        self.emote_detector: EmoteDetector | None = None
        self.metrics_server: MetricsServer | None = None
        self.nats_publisher: StatsPublisher | None = None

        # State
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._snapshot_task: asyncio.Task | None = None
        self._current_media: dict[str, dict[str, str]] = {}  # Track current media by channel
        self._start_time: float | None = None
        
        # Statistics counters
        self._events_processed = 0
        self._commands_processed = 0

        # Load configuration
        self._load_config()

    def _load_config(self) -> None:
        """Load configuration from file."""
        with open(self.config_path) as f:
            self.config = json.load(f)

        # Override version from package to ensure it stays in sync
        from . import __version__
        if "service" not in self.config:
            self.config["service"] = {}
        self.config["service"]["version"] = __version__

        self.logger.info(f"Configuration loaded from {self.config_path}")
        self.logger.info(f"Service version: {__version__}")

    @staticmethod
    def _clean_media_title(title: str) -> str:
        """Clean media title by removing file extensions and formatting.

        Args:
            title: Raw media title

        Returns:
            Cleaned title with file extensions removed and improved formatting
        """
        if not title:
            return "Unknown"

        # Remove common video file extensions
        extensions = [".mp4", ".mkv", ".avi", ".webm", ".flv", ".mov", ".wmv", ".m4v", ".mpg", ".mpeg"]
        cleaned = title
        for ext in extensions:
            if cleaned.lower().endswith(ext):
                cleaned = cleaned[: -len(ext)]
                break

        # Replace periods and underscores with spaces (common in filenames)
        cleaned = cleaned.replace(".", " ").replace("_", " ")

        # Remove multiple spaces
        cleaned = " ".join(cleaned.split())

        return cleaned if cleaned else "Unknown"

    async def _load_initial_state(self, domain: str, channel: str) -> None:
        """Load initial channel state from NATS KV stores.

        Args:
            domain: CyTube domain (e.g., 'cytu.be')
            channel: CyTube channel name
        """
        if not self.client:
            self.logger.warning("Kryten client not available, skipping initial state load")
            return

        try:
            # Construct bucket prefix to match Kryten-Robot naming
            # Format: kryten_{channel}_{type}
            # Channel name is case-sensitive
            bucket_prefix = f"kryten_{channel}"

            # Load userlist
            try:
                userlist_json = await self.client.kv_get(
                    f"{bucket_prefix}_userlist", "users", default=[], parse_json=True
                )

                if isinstance(userlist_json, list) and userlist_json:
                    self.logger.info(f"Loaded {len(userlist_json)} users from KV store")

                    # Track all users in database
                    for user in userlist_json:
                        username = user.get("name") or user.get("username")
                        if username:
                            await self.db.track_user(username)
                            # Add to activity tracker as already joined
                            self.activity_tracker.user_joined(domain, channel, username)
                            self.logger.debug(f"Initialized user: {username}")
                else:
                    self.logger.info("No initial userlist found in KV store")

            except Exception as e:
                self.logger.warning(f"Could not load userlist from KV store: {e}")

            # Load emote list
            try:
                emotes_json = await self.client.kv_get(f"{bucket_prefix}_emotes", "list", default=[], parse_json=True)

                if isinstance(emotes_json, list) and emotes_json:
                    emote_names: list[str] = []
                    for emote_entry in emotes_json:
                        if isinstance(emote_entry, dict):
                            name = emote_entry.get("name")
                            if isinstance(name, str):
                                # Strip leading # if present - emotes are stored with # in KV
                                emote_names.append(name.lstrip("#"))
                    if emote_names:
                        self.emote_detector.set_emote_list(emote_names)
                        self.logger.info(f"Loaded {len(emote_names)} emotes from KV store")
                    else:
                        self.logger.info("No emote names found in KV store data")
                else:
                    self.logger.info("No initial emote list found in KV store")

            except Exception as err:
                self.logger.warning(f"Could not load emotes from KV store: {err}")

            # Load playlist (for media tracking context)
            # If playlist is available in KV store, load it
            try:
                playlist_json = await self.client.kv_get(
                    f"{bucket_prefix}_playlist", "items", default=[], parse_json=True
                )

                if isinstance(playlist_json, list) and playlist_json:
                    self.logger.info(f"Loaded {len(playlist_json)} playlist items from KV store")

                    # If there's a current media, track it
                    # Find the first item (current playing)
                    if playlist_json:
                        current = playlist_json[0]
                        # Log the raw data for debugging
                        self.logger.debug(f"First playlist item raw data: {current}")
                        self.logger.info(f"Playlist item keys: {list(current.keys())}")

                        # Get title, explicitly check for None (not using 'or' which treats empty string as falsy)
                        # Check if media info is nested in a 'media' field (common in CyTube protocol)
                        if "media" in current and isinstance(current["media"], dict):
                            self.logger.info("Found nested 'media' field in playlist item")
                            media_obj = current["media"]
                            raw_title = media_obj.get("title")
                            media_type = media_obj.get("type", "")
                            media_id = media_obj.get("id", "")
                        else:
                            raw_title = current.get("title")
                            media_type = current.get("type", "")
                            media_id = current.get("id", "")

                        self.logger.debug(f"Raw title from playlist: '{raw_title}' (type: {type(raw_title).__name__})")

                        if raw_title is None or raw_title == "":
                            raw_title = "Unknown"
                            self.logger.warning(
                                f"Playlist item has no title, using 'Unknown'. "
                                f"Available fields: {list(current.keys())}"
                            )

                        # Clean the title (remove file extensions, improve formatting)
                        media_title = self._clean_media_title(raw_title)
                        media_type = current.get("type", "")
                        media_id = current.get("id", "")

                        self._current_media[channel] = {"title": media_title, "type": media_type, "id": media_id}
                        self.logger.info(f"Initialized current media: {media_title} (type={media_type}, id={media_id})")
                else:
                    self.logger.info("No initial playlist found in KV store")

            except Exception as e:
                self.logger.warning(f"Could not load playlist from KV store: {e}")

        except Exception as e:
            self.logger.error(f"Error loading initial state from KV stores: {e}", exc_info=True)

    async def start(self) -> None:
        """Start the application."""
        self.logger.info("Starting User Statistics Tracker")

        # Initialize database
        db_path = self.config.get("database", {}).get("path", "data/userstats.db")
        self.db = StatsDatabase(db_path, self.logger)
        await self.db.initialize()

        # Initialize activity tracker (no longer needs AFK threshold)
        self.activity_tracker = ActivityTracker(self.logger)
        await self.activity_tracker.start()

        # Initialize kudos detector
        self.kudos_detector = KudosDetector(self.logger)
        trigger_phrases = await self.db.get_trigger_phrases()
        if not trigger_phrases:
            # Load default phrases if none configured
            trigger_phrases = self.config.get("kudos", {}).get("default_phrases", ["lol", "rofl", "haha"])
            for phrase in trigger_phrases:
                await self.db.add_trigger_phrase(phrase)
        self.kudos_detector.set_trigger_phrases(trigger_phrases)

        # Initialize emote detector
        self.emote_detector = EmoteDetector(self.logger)
        # Emote list will be populated from emoteList events

        # Initialize Kryten client
        self.client = KrytenClient(self.config)

        # Register event handlers
        self.logger.info("Registering event handlers...")

        @self.client.on("chatmsg")
        async def handle_chat(event: ChatMessageEvent):
            await self._handle_chat_message(event)

        @self.client.on("pm")
        async def handle_pm(event):
            await self._handle_pm(event)

        @self.client.on("adduser")
        async def handle_user_join(event: UserJoinEvent):
            await self._handle_user_join(event)

        @self.client.on("userleave")
        async def handle_user_leave(event: UserLeaveEvent):
            await self._handle_user_leave(event)

        @self.client.on("changemedia")
        async def handle_media_change(event: ChangeMediaEvent):
            await self._handle_media_change(event)

        @self.client.on("emotelist")
        async def handle_emote_list(event):
            await self._handle_emote_list(event)

        @self.client.on("setafk")
        async def handle_set_afk(event):
            await self._handle_set_afk(event)

        self.logger.info(f"Registered {len(self.client._handlers)} event types with handlers")

        # Connect to NATS (lifecycle events handled automatically via ServiceConfig)
        await self.client.connect()

        # Track start time for uptime
        self._start_time = time.time()

        # Lifecycle is now managed by KrytenClient - log confirmation
        if self.client.lifecycle:
            self.logger.info("Lifecycle publisher initialized via KrytenClient")

        # Subscribe to robot startup - re-announce when robot starts
        await self.client.subscribe("kryten.lifecycle.robot.startup", self._handle_robot_startup)
        self.logger.info("Subscribed to kryten.lifecycle.robot.startup")

        # Load initial state from KV stores after connection
        # This gives us the full channel state, not just deltas
        for channel_config in self.config.get("channels", []):
            domain = channel_config["domain"]
            channel = channel_config["channel"]
            self.logger.info(f"Loading initial state for {domain}/{channel}...")
            await self._load_initial_state(domain, channel)

        # Initialize metrics server
        metrics_port = self.config.get("metrics", {}).get("port", 28282)
        self.metrics_server = MetricsServer(self, metrics_port)
        await self.metrics_server.start()

        # Initialize NATS publisher for stats queries using existing KrytenClient
        # NO separate NATS connection - reuses self.client
        self.nats_publisher = StatsPublisher(self, self.client)
        await self.nats_publisher.connect()

        # Start population snapshot task
        snapshot_interval = self.config.get("snapshots", {}).get("interval_seconds", 300)
        self._snapshot_task = asyncio.create_task(self._periodic_snapshots(snapshot_interval))

        # Start event processing
        self._running = True
        await self.client.run()

    async def stop(self) -> None:
        """Stop the application gracefully."""
        if not self._running:
            self.logger.debug("App not running, skip stop")
            return

        self.logger.info("Stopping User Statistics Tracker")
        self._running = False

        # Lifecycle shutdown is handled automatically by client.disconnect()
        # Stop client event loop first
        if self.client:
            self.logger.debug("Stopping Kryten client...")
            await self.client.stop()

        # Stop snapshot task
        if self._snapshot_task and not self._snapshot_task.done():
            self.logger.debug("Cancelling snapshot task...")
            self._snapshot_task.cancel()
            try:
                await self._snapshot_task
            except asyncio.CancelledError:
                pass

        # Stop NATS publisher
        if self.nats_publisher:
            self.logger.debug("Disconnecting NATS publisher...")
            await self.nats_publisher.disconnect()

        # Stop metrics server
        if self.metrics_server:
            self.logger.debug("Stopping metrics server...")
            await self.metrics_server.stop()

        # Stop activity tracker
        if self.activity_tracker:
            self.logger.debug("Stopping activity tracker...")
            await self.activity_tracker.stop()

        # Disconnect from NATS
        if self.client:
            self.logger.debug("Disconnecting from NATS...")
            await self.client.disconnect()

        self.logger.info("User Statistics Tracker stopped cleanly")

    async def _handle_chat_message(self, event: ChatMessageEvent) -> None:
        """Handle chat message event."""
        self._events_processed += 1
        try:
            # Safe message preview for logging
            msg_preview = (event.message or "")[:50] if event.message else "(no message)"
            self.logger.debug(f"Chat message from {event.username}: {msg_preview}")

            # Track user
            await self.db.track_user(event.username)

            # Increment message count
            await self.db.increment_message_count(event.username, event.channel, event.domain)

            # Record activity
            self.activity_tracker.user_activity(event.domain, event.channel, event.username)

            # Check for ++ kudos
            plusplus_users = self.kudos_detector.detect_plusplus_kudos(event.message)
            for username in plusplus_users:
                resolved = await self.db.resolve_username(username)
                if resolved != username:
                    self.logger.info(f"[KUDOS] Resolved alias '{username}' -> '{resolved}'")
                # Prevent self-kudos
                if resolved.lower() == event.username.lower():
                    self.logger.debug(f"[KUDOS] Ignored self-kudos attempt by '{event.username}'")
                    continue
                # Get canonical username (correct case) from users table
                canonical = await self.db.get_canonical_username(resolved)
                if canonical:
                    await self.db.increment_kudos_plusplus(canonical, event.channel, event.domain)
                    self.logger.info(f"[KUDOS] ++ for '{canonical}' from '{event.username}' in {event.channel}")
                else:
                    self.logger.debug(f"[KUDOS] Ignored ++ for unknown user '{resolved}' from '{event.username}'")

            # Check for phrase kudos
            phrase_kudos = self.kudos_detector.detect_phrase_kudos(event.message)
            for username, phrase in phrase_kudos:
                resolved = await self.db.resolve_username(username)
                if resolved != username:
                    self.logger.info(f"[KUDOS] Resolved alias '{username}' -> '{resolved}'")
                # Prevent self-kudos
                if resolved.lower() == event.username.lower():
                    self.logger.debug(f"[KUDOS] Ignored self-kudos attempt by '{event.username}'")
                    continue
                # Get canonical username (correct case) from users table
                canonical = await self.db.get_canonical_username(resolved)
                if canonical:
                    await self.db.increment_kudos_phrase(canonical, event.channel, event.domain, phrase)
                    self.logger.info(f"[KUDOS] '{phrase}' for '{canonical}' from '{event.username}' in {event.channel}")
                else:
                    self.logger.debug(
                        f"[KUDOS] Ignored '{phrase}' for unknown user '{resolved}' from '{event.username}'"
                    )

            # Check for emotes
            emotes = self.emote_detector.detect_emotes(event.message)
            if emotes:
                self.logger.debug(f"Detected {len(emotes)} emote(s) from {event.username}: {emotes}")
                for emote in emotes:
                    await self.db.increment_emote_usage(event.username, event.channel, event.domain, emote)

            # Check for movie voting (movie++ or movie--)
            vote_pattern = r"(?:^|\s)(movie|film|vid|video)([+-]{2}|[+-])\s*$"
            match = re.search(vote_pattern, event.message.lower())
            if match:
                vote_str = match.group(2)
                vote = 1 if "+" in vote_str else -1

                # Get current media for this channel
                current_media = self._current_media.get(event.channel)
                if current_media:
                    await self.db.record_movie_vote(
                        event.channel,
                        event.domain,
                        current_media["title"],
                        current_media.get("type", ""),
                        current_media.get("id", ""),
                        event.username,
                        vote,
                    )
                    self.logger.debug(f"Movie vote {vote} from {event.username} for '{current_media['title']}'")

        except Exception as e:
            self.logger.error(f"Error handling chat message: {e}", exc_info=True)

    async def _handle_pm(self, event) -> None:
        """Handle private message event."""
        self._events_processed += 1
        try:
            # PM events come as ChatMessageEvent from kryten-py
            # If it's a ChatMessageEvent, use username attribute
            if hasattr(event, "username"):
                username = event.username
            # Fallback to payload for RawEvent
            elif hasattr(event, "payload"):
                username = event.payload.get("from") or event.payload.get("username")
            else:
                return

            if not username:
                return

            # Track user
            await self.db.track_user(username)

            # Increment PM count
            await self.db.increment_pm_count(username)

        except Exception as e:
            self.logger.error(f"Error handling PM: {e}", exc_info=True)

    async def _handle_user_join(self, event: UserJoinEvent) -> None:
        """Handle user join event."""
        self._events_processed += 1
        try:
            # Track user
            await self.db.track_user(event.username)

            # Start activity tracking
            self.activity_tracker.user_joined(event.domain, event.channel, event.username)

        except Exception as e:
            self.logger.error(f"Error handling user join: {e}", exc_info=True)

    async def _handle_user_leave(self, event: UserLeaveEvent) -> None:
        """Handle user leave event."""
        self._events_processed += 1
        try:
            # Calculate activity time
            times = self.activity_tracker.user_left(event.domain, event.channel, event.username)

            if times:
                total_seconds, not_afk_seconds = times
                await self.db.update_user_activity(
                    event.username, event.channel, event.domain, total_seconds, not_afk_seconds
                )
                self.logger.debug(
                    f"User {event.username} left {event.channel}: " f"{total_seconds}s total, {not_afk_seconds}s active"
                )

        except Exception as e:
            self.logger.error(f"Error handling user leave: {e}", exc_info=True)

    async def _handle_media_change(self, event: ChangeMediaEvent) -> None:
        """Handle media change event."""
        self._events_processed += 1
        try:
            self.logger.debug(
                f"Media change event: title='{event.title}', type={event.media_type}, "
                f"id={event.media_id}, channel={event.channel}"
            )

            # Clean the media title (remove file extensions, improve formatting)
            cleaned_title = self._clean_media_title(event.title)

            self.logger.info(
                f"Media changed in {event.channel}: {cleaned_title} " f"(type={event.media_type}, id={event.media_id})"
            )

            # Track current media for movie voting (use cleaned title)
            self._current_media[event.channel] = {
                "title": cleaned_title,
                "type": event.media_type,
                "id": event.media_id,
            }

            # Log to database with cleaned title
            await self.db.log_media_change(event.channel, event.domain, cleaned_title, event.media_type, event.media_id)

        except Exception as e:
            self.logger.error(f"Error handling media change: {e}", exc_info=True)

    async def _handle_emote_list(self, event) -> None:
        """Handle emote list event."""
        self._events_processed += 1
        try:
            self.logger.info("Received emote list event")
            # emotelist events come as RawEvent (no typed conversion in kryten-py)
            # Extract emote names from payload
            emote_list = getattr(event, "payload", None) or event
            if emote_list is None:
                self.logger.warning("Received empty emote list event")
                return

            self.logger.debug(f"Emote list type: {type(emote_list)}")
            if isinstance(emote_list, list):
                self.logger.debug(f"Emote list has {len(emote_list)} items")
                emote_names: list[str] = []
                for e in emote_list:
                    if isinstance(e, dict):
                        name = e.get("name")
                        if isinstance(name, str):
                            # Strip leading # if present - emotes are stored with # in KV
                            emote_names.append(name.lstrip("#"))
                if emote_names:
                    self.logger.info(f"Setting emote list with {len(emote_names)} emotes")
                    self.emote_detector.set_emote_list(emote_names)
                else:
                    self.logger.warning("No valid emote names found in list")
            elif isinstance(emote_list, dict):
                # Sometimes emotes are in a dict format - strip # from keys
                emote_names = [k.lstrip("#") for k in emote_list.keys() if isinstance(k, str)]
                if emote_names:
                    self.logger.info(f"Setting emote list with {len(emote_names)} emotes from dict")
                    self.emote_detector.set_emote_list(emote_names)
            else:
                self.logger.warning(f"Unexpected emote list format: {type(emote_list)}")

        except Exception as e:
            self.logger.error(f"Error handling emote list: {e}", exc_info=True)

    async def _handle_set_afk(self, event) -> None:
        """Handle setAFK event from CyTube."""
        self._events_processed += 1
        try:
            # setafk events come as RawEvent (no typed conversion in kryten-py)
            # Extract username and AFK status from payload
            # Payload format: {"name": "username", "afk": true/false}
            if not hasattr(event, "payload"):
                return

            username = event.payload.get("name")
            is_afk = event.payload.get("afk", False)

            if not username:
                return

            # Update activity tracker with AFK status
            self.activity_tracker.set_afk_status(event.domain, event.channel, username, is_afk)

        except Exception as e:
            self.logger.error(f"Error handling setAFK: {e}", exc_info=True)

    async def _handle_robot_startup(self, msg: Any) -> None:
        """Handle robot startup notification.

        Re-announces the service when Kryten-Robot starts up.
        This ensures the robot knows about all running services.

        Args:
            msg: NATS message (ignored, just triggers re-announcement)
        """
        try:
            self.logger.info("Robot startup detected, re-announcing service")

            if self.client and self.client.lifecycle:
                await self.client.lifecycle.publish_startup(
                    channels_configured=len(self.config.get("channels", [])),
                    metrics_port=self.config.get("metrics", {}).get("port", 28282),
                    re_announcement=True,
                )
        except Exception as e:
            self.logger.error(f"Error handling robot startup: {e}", exc_info=True)

    async def _periodic_snapshots(self, interval: int) -> None:
        """Periodically save population snapshots."""
        while self._running:
            try:
                await asyncio.sleep(interval)

                # Get active sessions from activity tracker
                sessions = self.activity_tracker.get_active_sessions()

                for (domain, channel), usernames in sessions.items():
                    # Count connected users (all in session)
                    connected_count = len(usernames)

                    # Count chat users (not AFK)
                    # For simplicity, we'll use connected count for both
                    # A more sophisticated approach would track AFK status
                    chat_count = connected_count

                    await self.db.save_population_snapshot(channel, domain, connected_count, chat_count)

                    self.logger.debug(
                        f"Population snapshot for {channel}: " f"{connected_count} connected, {chat_count} in chat"
                    )

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in periodic snapshots: {e}", exc_info=True)


async def main():
    """Main entry point."""
    import argparse
    import platform
    import sys

    # Parse arguments
    parser = argparse.ArgumentParser(description="User Statistics Tracker for CyTube")
    parser.add_argument(
        "--config", help="Configuration file path (default: /etc/kryten/kryten-userstats/config.json or ./config.json)"
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    args = parser.parse_args()

    # Setup logging first so we can log errors during config validation
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()), format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)

    # Determine config file path
    if args.config:
        config_path = Path(args.config)
    else:
        # Try default locations in order
        default_paths = [Path("/etc/kryten/kryten-userstats/config.json"), Path("config.json")]

        config_path = None
        for path in default_paths:
            if path.exists() and path.is_file():
                config_path = path
                break

        if not config_path:
            logger.error("No configuration file found.")
            logger.error("  Searched:")
            for path in default_paths:
                logger.error(f"    - {path}")
            logger.error("  Use --config to specify a custom path.")
            sys.exit(1)

    # Validate config file exists
    if not config_path.exists():
        logger.error(f"Configuration file not found: {config_path}")
        sys.exit(1)

    if not config_path.is_file():
        logger.error(f"Configuration path is not a file: {config_path}")
        sys.exit(1)

    # Create application
    app = UserStatsApp(str(config_path))

    # Setup signal handlers for graceful shutdown
    shutdown_event = asyncio.Event()

    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, initiating shutdown...")
        shutdown_event.set()

    # Register signal handlers (platform-specific)
    if platform.system() != "Windows":
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: signal_handler(s, None))
    else:
        # Windows uses traditional signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    # Run application
    try:
        # Start app in background task
        app_task = asyncio.create_task(app.start())

        # Wait for shutdown signal or KeyboardInterrupt
        try:
            await shutdown_event.wait()
        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt, initiating shutdown...")

        # Stop the app
        await app.stop()

        # Cancel and wait for app task
        app_task.cancel()
        try:
            await app_task
        except asyncio.CancelledError:
            pass

        logger.info("Shutdown complete")

    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt during startup, shutting down...")
        await app.stop()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        await app.stop()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
