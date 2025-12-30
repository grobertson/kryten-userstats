"""Statistics query responder using KrytenClient.

This module handles publishing statistics data on userstats-owned NATS subjects.
It uses KrytenClient instead of direct NATS access, following the architectural
rule that all NATS operations must go through kryten-py.
"""

import logging
import time
from typing import Any

from kryten import KrytenClient


class StatsPublisher:
    """Publishes statistics data on NATS subjects owned by userstats."""

    def __init__(self, app_reference, client: KrytenClient):
        """Initialize stats publisher using existing KrytenClient.

        Args:
            app_reference: Reference to UserStatsApp for accessing database
            client: KrytenClient instance (already connected)
        """
        self.app = app_reference
        self.client = client
        self.logger = logging.getLogger(__name__)

        self._subscriptions: list[Any] = []

    async def connect(self) -> None:
        """Subscribe to unified command subject using KrytenClient.

        Single subject: kryten.userstats.command
        Commands are routed via 'command' field in message payload.
        """
        try:
            subject = "kryten.userstats.command"
            await self._subscribe(subject, self._handle_command)

            self.logger.info(f"Subscribed to {subject}")

        except Exception as e:
            self.logger.error(f"Failed to subscribe to query subjects: {e}", exc_info=True)
            raise

    async def disconnect(self) -> None:
        """Disconnect is handled by KrytenClient.

        No need to manually unsubscribe - KrytenClient manages all subscriptions.
        """
        self.logger.info("Stats publisher cleanup (managed by KrytenClient)")
        self._subscriptions.clear()

    async def _subscribe(self, subject: str, handler) -> None:
        """Subscribe to a query subject using KrytenClient's request-reply mechanism."""
        sub = await self.client.subscribe_request_reply(subject, handler)
        self._subscriptions.append(sub)
        self.logger.debug(f"Subscribed to {subject}")

    async def _handle_command(self, request: dict) -> dict:
        """Dispatch commands based on 'command' field in request.

        Request format:
            {
                "command": "user.stats" | "leaderboard.messages" | etc,
                "service": "userstats",  # For routing/filtering (optional)
                ... command-specific parameters ...
            }

        Response format:
            {
                "service": "userstats",
                "command": "user.stats",
                "success": true,
                "data": { ... } | "error": "message"
            }
        """
        # Increment commands counter
        self.app._commands_processed += 1
        
        command = request.get("command")

        if not command:
            return {"service": "userstats", "success": False, "error": "Missing 'command' field"}

        # Check service field for routing (other services can ignore)
        service = request.get("service")
        if service and service != "userstats":
            return {
                "service": "userstats",
                "success": False,
                "error": f"Command intended for '{service}', not 'userstats'",
            }

        # Dispatch to handler
        handler_map = {
            "user.stats": self._handle_user_stats,
            "user.messages": self._handle_user_messages,
            "user.activity": self._handle_user_activity,
            "user.kudos": self._handle_user_kudos,
            "channel.top_users": self._handle_channel_top_users,
            "channel.population": self._handle_channel_population,
            "channel.media_history": self._handle_channel_media_history,
            "leaderboard.messages": self._handle_leaderboard_messages,
            "leaderboard.kudos": self._handle_leaderboard_kudos,
            "leaderboard.emotes": self._handle_leaderboard_emotes,
            "system.ping": self._handle_system_ping,
            "system.health": self._handle_system_health,
            "system.stats": self._handle_system_stats,
            "channel.watermarks": self._handle_channel_watermarks,
            "channel.movie_votes": self._handle_movie_votes,
            "timeseries.messages": self._handle_timeseries_messages,
            "timeseries.kudos": self._handle_timeseries_kudos,
            "channel.all_stats": self._handle_channel_all_stats,
            # Alias management commands
            "alias.list": self._handle_alias_list,
            "alias.get": self._handle_alias_get,
            "alias.add": self._handle_alias_add,
            "alias.delete": self._handle_alias_delete,
            "alias.update": self._handle_alias_update,
            "alias.find": self._handle_alias_find,
        }

        handler = handler_map.get(command)
        if not handler:
            return {
                "service": "userstats",
                "command": command,
                "success": False,
                "error": f"Unknown command: {command}",
            }

        try:
            result = await handler(request)
            return {"service": "userstats", "command": command, "success": True, "data": result}
        except Exception as e:  # noqa: BLE001
            self.logger.error(f"Error executing command '{command}': {e}", exc_info=True)
            return {"service": "userstats", "command": command, "success": False, "error": str(e)}

    # Query handlers - all return dicts with query results (wrapped by _handle_command)

    async def _handle_user_stats(self, request: dict) -> dict:
        """Handle user.stats query - Get comprehensive user statistics."""
        username = request.get("username")
        channel = request.get("channel")
        domain = request.get("domain") or (self.client.config.channels[0].domain if self.client.config.channels else "cytu.be")

        if not username:
            raise ValueError("username required")

        # Build comprehensive stats from multiple sources
        stats = {
            "username": username,
            "messages": await self.app.db.get_user_message_count(username, channel, domain) if channel else 0,
            "all_messages": await self.app.db.get_user_all_message_counts(username, domain),
            "pms": await self.app.db.get_user_pm_count(username),
            "kudos_plusplus": await self.app.db.get_user_kudos_plusplus(username, domain),
            "kudos_phrases": await self.app.db.get_user_kudos_phrases(username, domain),
            "emotes": await self.app.db.get_user_emote_usage(username, domain),
        }

        if channel:
            activity = await self.app.db.get_user_activity_stats(username, channel, domain)
            stats["activity"] = activity
        else:
            stats["all_activity"] = await self.app.db.get_user_all_activity(username, domain)

        return stats

    async def _handle_user_messages(self, request: dict) -> dict:
        """Handle user.messages query - Get user message history."""
        username = request.get("username")
        channel = request.get("channel")
        domain = request.get("domain") or (self.client.config.channels[0].domain if self.client.config.channels else "cytu.be")

        if not username:
            raise ValueError("username required")

        if channel:
            messages = await self.app.db.get_user_message_count(username, channel, domain)
            return {"username": username, "channel": channel, "message_count": messages}
        else:
            all_counts = await self.app.db.get_user_all_message_counts(username, domain)
            return {"username": username, "channels": all_counts}

    async def _handle_user_activity(self, request: dict) -> dict:
        """Handle user.activity query - Get user activity time."""
        username = request.get("username")
        channel = request.get("channel")
        domain = request.get("domain") or (self.client.config.channels[0].domain if self.client.config.channels else "cytu.be")

        if not username:
            raise ValueError("username required")

        if channel:
            activity = await self.app.db.get_user_activity_stats(username, channel, domain)
            return {"username": username, "channel": channel, "activity": activity}
        else:
            all_activity = await self.app.db.get_user_all_activity(username, domain)
            return {"username": username, "all_activity": all_activity}

    async def _handle_user_kudos(self, request: dict) -> dict:
        """Handle user.kudos query - Get user kudos received."""
        username = request.get("username")
        domain = request.get("domain") or (self.client.config.channels[0].domain if self.client.config.channels else "cytu.be")

        if not username:
            raise ValueError("username required")

        kudos = {
            "username": username,
            "plusplus": await self.app.db.get_user_kudos_plusplus(username, domain),
            "phrases": await self.app.db.get_user_kudos_phrases(username, domain),
        }
        return kudos

    async def _handle_channel_top_users(self, request: dict) -> list[dict[str, Any]]:
        """Handle channel.top_users query - Get most active users."""
        channel = request.get("channel")
        domain = request.get("domain") or (self.client.config.channels[0].domain if self.client.config.channels else "cytu.be")
        limit = request.get("limit", 10)

        if not channel:
            raise ValueError("channel required")

        top_users = await self.app.db.get_top_message_senders(channel, domain, limit)
        return {"channel": channel, "top_users": top_users}

    async def _handle_channel_population(self, request: dict) -> dict:
        """Handle channel.population query - Get current/historical population."""
        channel = request.get("channel")
        domain = request.get("domain") or (self.client.config.channels[0].domain if self.client.config.channels else "cytu.be")
        hours = request.get("hours", 24)

        if not channel:
            raise ValueError("channel required")

        snapshots = await self.app.db.get_recent_population_snapshots(channel, domain, hours)
        # Return latest as "current" plus history
        current = snapshots[0] if snapshots else None
        return {"channel": channel, "current": current, "history": snapshots}

    async def _handle_channel_media_history(self, request: dict) -> dict:
        """Handle channel.media_history query - Get media change history."""
        channel = request.get("channel")
        domain = request.get("domain") or (self.client.config.channels[0].domain if self.client.config.channels else "cytu.be")
        limit = request.get("limit", 50)

        if not channel:
            raise ValueError("channel required")

        history = await self.app.db.get_recent_media_changes(channel, domain, limit)
        return {"channel": channel, "history": history}

    async def _handle_leaderboard_messages(self, request: dict) -> dict:
        """Handle leaderboard.messages query - Get message leaderboard."""
        channel = request.get("channel")
        domain = request.get("domain") or (self.client.config.channels[0].domain if self.client.config.channels else "cytu.be")
        limit = request.get("limit", 10)

        if not channel:
            raise ValueError("channel required")

        leaderboard = await self.app.db.get_top_message_senders(channel, domain, limit)
        return {"channel": channel, "leaderboard": leaderboard}

    async def _handle_leaderboard_kudos(self, request: dict) -> dict:
        """Handle leaderboard.kudos query - Get kudos leaderboard."""
        domain = request.get("domain") or (self.client.config.channels[0].domain if self.client.config.channels else "cytu.be")
        limit = request.get("limit", 10)

        leaderboard = await self.app.db.get_global_kudos_leaderboard(domain, limit)
        return {"leaderboard": leaderboard}

    async def _handle_leaderboard_emotes(self, request: dict) -> dict:
        """Handle leaderboard.emotes query - Get emote usage leaderboard."""
        domain = request.get("domain") or (self.client.config.channels[0].domain if self.client.config.channels else "cytu.be")
        limit = request.get("limit", 10)

        leaderboard = await self.app.db.get_top_emotes(domain, limit)
        return {"leaderboard": leaderboard}

    async def _handle_system_ping(self, request: dict) -> dict:
        """Handle system.ping query - Simple ping response for service discovery."""
        from datetime import datetime
        from userstats import __version__

        uptime_seconds = time.time() - self.app._start_time if hasattr(self.app, '_start_time') else 0

        # Get metrics port from config
        metrics_port = self.app.config.get("metrics", {}).get("port", 28282)

        return {
            "pong": True,
            "service": "userstats",
            "version": __version__,
            "uptime_seconds": uptime_seconds,
            "timestamp": datetime.now().isoformat(),
            "metrics_endpoint": f"http://localhost:{metrics_port}/metrics",
        }

    async def _handle_system_health(self, request: dict) -> dict:
        """Handle system.health query - Get service health status."""
        uptime_seconds = time.time() - self.app._start_time if hasattr(self.app, '_start_time') else 0
        health = {
            "service": "userstats",
            "status": "healthy" if self.app._running else "unhealthy",
            "uptime_seconds": uptime_seconds,
            "database_connected": bool(self.app.db),
            "nats_connected": self.client._connected,
        }
        return health

    async def _handle_system_stats(self, request: dict) -> dict:
        """Handle system.stats query - Get aggregate statistics."""
        stats = {
            "total_users": await self.app.db.get_total_users(),
            "total_messages": await self.app.db.get_total_messages(),
            "total_pms": await self.app.db.get_total_pms(),
            "total_kudos": await self.app.db.get_total_kudos_plusplus(),
            "total_emotes": await self.app.db.get_total_emote_usage(),
            "total_media_changes": await self.app.db.get_total_media_changes(),
            "events_processed": self.app._events_processed,
            "commands_processed": self.app._commands_processed,
        }

        if self.app.activity_tracker:
            stats["active_sessions"] = self.app.activity_tracker.get_active_session_count()

        return stats

    async def _handle_channel_watermarks(self, request: dict) -> dict[str, Any]:
        """Handle channel.watermarks query - Get high/low user population marks."""
        channel = request.get("channel")
        domain = request.get("domain") or (self.client.config.channels[0].domain if self.client.config.channels else "cytu.be")
        days = request.get("days")

        if not channel:
            raise ValueError("channel required")

        watermarks: dict[str, Any] = await self.app.db.get_water_marks(channel, domain, days)
        return watermarks

    async def _handle_movie_votes(self, request: dict) -> dict[str, Any]:
        """Handle channel.movie_votes query - Get movie voting statistics."""
        channel = request.get("channel")
        domain = request.get("domain") or (self.client.config.channels[0].domain if self.client.config.channels else "cytu.be")
        media_title = request.get("media_title")

        if not channel:
            raise ValueError("channel required")

        votes: dict[str, Any] = await self.app.db.get_movie_votes(channel, domain, media_title)
        return votes

    async def _handle_timeseries_messages(self, request: dict) -> dict[str, Any]:
        """Handle timeseries.messages query - Get message activity over time."""
        channel = request.get("channel")
        domain = request.get("domain") or (self.client.config.channels[0].domain if self.client.config.channels else "cytu.be")
        start_time = request.get("start_time")
        end_time = request.get("end_time")

        if not channel:
            raise ValueError("channel required")

        data = await self.app.db.get_time_series_messages(channel, domain, start_time, end_time)
        return {"channel": channel, "data": data}

    async def _handle_timeseries_kudos(self, request: dict) -> dict[str, Any]:
        """Handle timeseries.kudos query - Get kudos activity over time."""
        # TODO: implement time-series tracking for kudos
        return {"data": []}

    async def _handle_channel_all_stats(self, request: dict) -> dict[str, Any]:
        """Handle channel.all_stats query - Get all available statistics for a channel.
        
        Returns comprehensive statistics in a single response with sections for:
        - system: health and aggregate stats
        - leaderboards: messages, kudos, emotes
        - channel: top users, population, watermarks, media history, movie votes
        
        Timeseries data is excluded as it requires time range parameters.
        """
        channel = request.get("channel")
        domain = request.get("domain")
        
        # Use first configured channel as default if not specified
        if not domain:
            domain = self.client.config.channels[0].domain if self.client.config.channels else "cytu.be"
        
        if not channel:
            # Use first configured channel as default if not specified
            channel = self.client.config.channels[0].channel if self.client.config.channels else None
            if not channel:
                raise ValueError("channel required")
        
        # Get limits from request (CLI sends them in a 'limits' dict)
        limits = request.get("limits", {})
        top_users_limit = limits.get("top_users", 20)
        media_history_limit = limits.get("media_history", 15)
        leaderboard_limit = limits.get("leaderboards", 10)
        
        # Gather all stats in parallel for efficiency
        import asyncio
        
        results = await asyncio.gather(
            # System
            self._handle_system_health({}),
            self._handle_system_stats({}),
            
            # Channel-specific
            self.app.db.get_top_message_senders(channel, domain, top_users_limit),
            self.app.db.get_recent_population_snapshots(channel, domain, 24),
            self.app.db.get_water_marks(channel, domain, None),
            self.app.db.get_recent_media_changes(channel, domain, media_history_limit),
            self.app.db.get_movie_votes(channel, domain, None),
            
            # Global leaderboards
            self.app.db.get_top_message_senders(channel, domain, leaderboard_limit),
            self.app.db.get_global_kudos_leaderboard(domain, leaderboard_limit),
            self.app.db.get_top_emotes(domain, leaderboard_limit),
        )
        
        # Unpack results
        (system_health, system_stats, 
         top_users, population_snapshots, watermarks, media_history, movie_votes,
         messages_leaderboard, kudos_leaderboard, emotes_leaderboard) = results
        
        # Build comprehensive response
        return {
            "channel": channel,
            "domain": domain,
            "system": {
                "health": system_health,
                "stats": system_stats,
            },
            "leaderboards": {
                "messages": messages_leaderboard,
                "kudos": kudos_leaderboard,
                "emotes": emotes_leaderboard,
            },
            "channel": {
                "top_users": top_users,
                "population": {
                    "current": population_snapshots[0] if population_snapshots else None,
                    "history_24h": population_snapshots,
                },
                "watermarks": watermarks,
                "media_history": media_history,
                "movie_votes": movie_votes,
            },
        }

    # ==================== Alias Management Commands ====================

    async def _handle_alias_list(self, request: dict) -> dict[str, Any]:
        """Handle alias.list command - List all username aliases.

        Request format:
            {"command": "alias.list"}

        Returns:
            Dict mapping usernames to their list of aliases
        """
        aliases = await self.app.db.get_all_aliases()
        return {
            "aliases": aliases,
            "total_users": len(aliases),
            "total_aliases": sum(len(v) for v in aliases.values()),
        }

    async def _handle_alias_get(self, request: dict) -> dict[str, Any]:
        """Handle alias.get command - Get aliases for a specific user.

        Request format:
            {"command": "alias.get", "username": "kevinchrist"}

        Returns:
            List of aliases for the user
        """
        username = request.get("username")
        if not username:
            raise ValueError("username required")

        aliases = await self.app.db.get_user_aliases(username)
        return {
            "username": username,
            "aliases": aliases,
            "count": len(aliases),
        }

    async def _handle_alias_add(self, request: dict) -> dict[str, Any]:
        """Handle alias.add command - Add an alias for a user.

        Request format:
            {"command": "alias.add", "username": "kevinchrist", "alias": "kc"}

        Returns:
            Success status and message
        """
        username = request.get("username")
        alias = request.get("alias")

        if not username:
            raise ValueError("username required")
        if not alias:
            raise ValueError("alias required")

        success, message = await self.app.db.add_user_alias_checked(username, alias)
        return {
            "username": username,
            "alias": alias,
            "added": success,
            "message": message,
        }

    async def _handle_alias_delete(self, request: dict) -> dict[str, Any]:
        """Handle alias.delete command - Delete an alias for a user.

        Request format:
            {"command": "alias.delete", "username": "kevinchrist", "alias": "kc"}

        Returns:
            Success status and message
        """
        username = request.get("username")
        alias = request.get("alias")

        if not username:
            raise ValueError("username required")
        if not alias:
            raise ValueError("alias required")

        success, message = await self.app.db.delete_user_alias(username, alias)
        return {
            "username": username,
            "alias": alias,
            "deleted": success,
            "message": message,
        }

    async def _handle_alias_update(self, request: dict) -> dict[str, Any]:
        """Handle alias.update command - Update an existing alias.

        Request format:
            {"command": "alias.update", "old_alias": "kc", "new_alias": "kevin"}

        Returns:
            Success status and message
        """
        old_alias = request.get("old_alias")
        new_alias = request.get("new_alias")

        if not old_alias:
            raise ValueError("old_alias required")
        if not new_alias:
            raise ValueError("new_alias required")

        success, message = await self.app.db.update_user_alias(old_alias, new_alias)
        return {
            "old_alias": old_alias,
            "new_alias": new_alias,
            "updated": success,
            "message": message,
        }

    async def _handle_alias_find(self, request: dict) -> dict[str, Any]:
        """Handle alias.find command - Find which user owns an alias.

        Request format:
            {"command": "alias.find", "alias": "kc"}

        Returns:
            Username that owns the alias, or null if not found
        """
        alias = request.get("alias")

        if not alias:
            raise ValueError("alias required")

        username = await self.app.db.find_alias_owner(alias)
        return {
            "alias": alias,
            "username": username,
            "found": username is not None,
        }

