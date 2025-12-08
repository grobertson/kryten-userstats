"""Direct NATS publisher for statistics query responses.

This module handles publishing statistics data on userstats-owned NATS subjects.
It uses direct NATS client access since this data is owned by userstats, not CyTube.
"""

import asyncio
import json
import logging
from typing import Any, Optional

import nats
from nats.aio.client import Client as NATSClient


class StatsPublisher:
    """Publishes statistics data on NATS subjects owned by userstats."""
    
    def __init__(self, app_reference, domain: str, nats_config: dict):
        """Initialize NATS publisher.
        
        Args:
            app_reference: Reference to UserStatsApp for accessing database
            domain: CyTube domain (e.g., "cytu.be")
            nats_config: NATS connection configuration
        """
        self.app = app_reference
        self.domain = domain
        self.nats_config = nats_config
        self.logger = logging.getLogger(__name__)
        
        self._nats: Optional[NATSClient] = None
        self._connected = False
        self._subscriptions = []
        
    async def connect(self) -> None:
        """Connect to NATS and subscribe to query subjects."""
        if self._connected:
            self.logger.warning("Already connected to NATS")
            return
            
        try:
            self._nats = await nats.connect(
                servers=self.nats_config.get("servers", ["nats://localhost:4222"]),
                user=self.nats_config.get("user"),
                password=self.nats_config.get("password"),
                token=self.nats_config.get("token"),
            )
            
            self._connected = True
            self.logger.info("Stats publisher connected to NATS")
            
            # Subscribe to query request subjects
            # Pattern: userstats.query.{domain}.{query_type}
            base = f"userstats.query.{self.domain}"
            
            # User queries
            await self._subscribe(f"{base}.user.stats", self._handle_user_stats)
            await self._subscribe(f"{base}.user.messages", self._handle_user_messages)
            await self._subscribe(f"{base}.user.activity", self._handle_user_activity)
            await self._subscribe(f"{base}.user.kudos", self._handle_user_kudos)
            
            # Channel queries
            await self._subscribe(f"{base}.channel.top_users", self._handle_channel_top_users)
            await self._subscribe(f"{base}.channel.population", self._handle_channel_population)
            await self._subscribe(f"{base}.channel.media_history", self._handle_channel_media_history)
            
            # Leaderboard queries
            await self._subscribe(f"{base}.leaderboard.messages", self._handle_leaderboard_messages)
            await self._subscribe(f"{base}.leaderboard.kudos", self._handle_leaderboard_kudos)
            await self._subscribe(f"{base}.leaderboard.emotes", self._handle_leaderboard_emotes)
            
            # System queries
            await self._subscribe(f"{base}.system.health", self._handle_system_health)
            await self._subscribe(f"{base}.system.stats", self._handle_system_stats)
            
            # Water mark queries
            await self._subscribe(f"{base}.channel.watermarks", self._handle_channel_watermarks)
            
            # Movie voting queries
            await self._subscribe(f"{base}.channel.movie_votes", self._handle_movie_votes)
            
            # Time-series data queries
            await self._subscribe(f"{base}.timeseries.messages", self._handle_timeseries_messages)
            await self._subscribe(f"{base}.timeseries.kudos", self._handle_timeseries_kudos)
            
            self.logger.info(f"Subscribed to {len(self._subscriptions)} query subjects")
            
        except Exception as e:
            self.logger.error(f"Failed to connect stats publisher to NATS: {e}", exc_info=True)
            raise
            
    async def disconnect(self) -> None:
        """Disconnect from NATS."""
        if not self._connected or not self._nats:
            return
            
        try:
            for sub in self._subscriptions:
                try:
                    await sub.unsubscribe()
                except Exception as e:
                    self.logger.warning(f"Error unsubscribing: {e}")
                    
            self._subscriptions.clear()
            
            await self._nats.drain()
            await self._nats.close()
            
            self._connected = False
            self._nats = None
            
            self.logger.info("Stats publisher disconnected from NATS")
            
        except Exception as e:
            self.logger.error(f"Error disconnecting stats publisher: {e}", exc_info=True)
            
    async def _subscribe(self, subject: str, handler) -> None:
        """Subscribe to a query subject."""
        if not self._nats:
            raise RuntimeError("NATS client not connected")
            
        sub = await self._nats.subscribe(subject, cb=handler)
        self._subscriptions.append(sub)
        self.logger.debug(f"Subscribed to {subject}")
        
    async def _publish_response(self, reply_subject: str, data: dict) -> None:
        """Publish response on reply subject."""
        if not self._nats:
            self.logger.error("Cannot publish: NATS not connected")
            return
            
        try:
            payload = json.dumps(data).encode('utf-8')
            await self._nats.publish(reply_subject, payload)
            self.logger.debug(f"Published response to {reply_subject}")
        except Exception as e:
            self.logger.error(f"Error publishing response: {e}", exc_info=True)
            
    # Query handlers
    
    async def _handle_user_stats(self, msg) -> None:
        """Handle user.stats query - Get comprehensive user statistics."""
        try:
            request = json.loads(msg.data.decode('utf-8'))
            username = request.get('username')
            channel = request.get('channel')
            
            if not username:
                await self._publish_response(msg.reply, {"error": "username required"})
                return
                
            stats = await self.app.db.get_user_stats(username, channel)
            await self._publish_response(msg.reply, {"success": True, "data": stats})
            
        except Exception as e:
            self.logger.error(f"Error handling user.stats: {e}", exc_info=True)
            await self._publish_response(msg.reply, {"error": str(e)})
            
    async def _handle_user_messages(self, msg) -> None:
        """Handle user.messages query - Get user message history."""
        try:
            request = json.loads(msg.data.decode('utf-8'))
            username = request.get('username')
            channel = request.get('channel')
            limit = request.get('limit', 100)
            
            if not username:
                await self._publish_response(msg.reply, {"error": "username required"})
                return
                
            messages = await self.app.db.get_user_message_count(username, channel)
            await self._publish_response(msg.reply, {
                "success": True, 
                "data": {"username": username, "message_count": messages}
            })
            
        except Exception as e:
            self.logger.error(f"Error handling user.messages: {e}", exc_info=True)
            await self._publish_response(msg.reply, {"error": str(e)})
            
    async def _handle_user_activity(self, msg) -> None:
        """Handle user.activity query - Get user activity time."""
        try:
            request = json.loads(msg.data.decode('utf-8'))
            username = request.get('username')
            channel = request.get('channel')
            
            if not username:
                await self._publish_response(msg.reply, {"error": "username required"})
                return
                
            activity = await self.app.db.get_user_activity_time(username, channel)
            await self._publish_response(msg.reply, {"success": True, "data": activity})
            
        except Exception as e:
            self.logger.error(f"Error handling user.activity: {e}", exc_info=True)
            await self._publish_response(msg.reply, {"error": str(e)})
            
    async def _handle_user_kudos(self, msg) -> None:
        """Handle user.kudos query - Get user kudos received."""
        try:
            request = json.loads(msg.data.decode('utf-8'))
            username = request.get('username')
            channel = request.get('channel')
            
            if not username:
                await self._publish_response(msg.reply, {"error": "username required"})
                return
                
            kudos = await self.app.db.get_user_kudos(username, channel)
            await self._publish_response(msg.reply, {"success": True, "data": kudos})
            
        except Exception as e:
            self.logger.error(f"Error handling user.kudos: {e}", exc_info=True)
            await self._publish_response(msg.reply, {"error": str(e)})
            
    async def _handle_channel_top_users(self, msg) -> None:
        """Handle channel.top_users query - Get most active users."""
        try:
            request = json.loads(msg.data.decode('utf-8'))
            channel = request.get('channel')
            limit = request.get('limit', 10)
            
            top_users = await self.app.db.get_top_users_by_messages(channel, limit)
            await self._publish_response(msg.reply, {"success": True, "data": top_users})
            
        except Exception as e:
            self.logger.error(f"Error handling channel.top_users: {e}", exc_info=True)
            await self._publish_response(msg.reply, {"error": str(e)})
            
    async def _handle_channel_population(self, msg) -> None:
        """Handle channel.population query - Get current/historical population."""
        try:
            request = json.loads(msg.data.decode('utf-8'))
            channel = request.get('channel')
            
            population = await self.app.db.get_latest_population_snapshot(channel)
            await self._publish_response(msg.reply, {"success": True, "data": population})
            
        except Exception as e:
            self.logger.error(f"Error handling channel.population: {e}", exc_info=True)
            await self._publish_response(msg.reply, {"error": str(e)})
            
    async def _handle_channel_media_history(self, msg) -> None:
        """Handle channel.media_history query - Get media change history."""
        try:
            request = json.loads(msg.data.decode('utf-8'))
            channel = request.get('channel')
            limit = request.get('limit', 50)
            
            history = await self.app.db.get_recent_media_changes(channel, limit)
            await self._publish_response(msg.reply, {"success": True, "data": history})
            
        except Exception as e:
            self.logger.error(f"Error handling channel.media_history: {e}", exc_info=True)
            await self._publish_response(msg.reply, {"error": str(e)})
            
    async def _handle_leaderboard_messages(self, msg) -> None:
        """Handle leaderboard.messages query - Get message leaderboard."""
        try:
            request = json.loads(msg.data.decode('utf-8'))
            channel = request.get('channel')
            limit = request.get('limit', 10)
            
            leaderboard = await self.app.db.get_top_users_by_messages(channel, limit)
            await self._publish_response(msg.reply, {"success": True, "data": leaderboard})
            
        except Exception as e:
            self.logger.error(f"Error handling leaderboard.messages: {e}", exc_info=True)
            await self._publish_response(msg.reply, {"error": str(e)})
            
    async def _handle_leaderboard_kudos(self, msg) -> None:
        """Handle leaderboard.kudos query - Get kudos leaderboard."""
        try:
            request = json.loads(msg.data.decode('utf-8'))
            channel = request.get('channel')
            limit = request.get('limit', 10)
            
            leaderboard = await self.app.db.get_top_users_by_kudos(channel, limit)
            await self._publish_response(msg.reply, {"success": True, "data": leaderboard})
            
        except Exception as e:
            self.logger.error(f"Error handling leaderboard.kudos: {e}", exc_info=True)
            await self._publish_response(msg.reply, {"error": str(e)})
            
    async def _handle_leaderboard_emotes(self, msg) -> None:
        """Handle leaderboard.emotes query - Get emote usage leaderboard."""
        try:
            request = json.loads(msg.data.decode('utf-8'))
            channel = request.get('channel')
            limit = request.get('limit', 10)
            
            leaderboard = await self.app.db.get_top_emotes(channel, limit)
            await self._publish_response(msg.reply, {"success": True, "data": leaderboard})
            
        except Exception as e:
            self.logger.error(f"Error handling leaderboard.emotes: {e}", exc_info=True)
            await self._publish_response(msg.reply, {"error": str(e)})
            
    async def _handle_system_health(self, msg) -> None:
        """Handle system.health query - Get service health status."""
        try:
            health = {
                "service": "userstats",
                "status": "healthy" if self.app._running else "unhealthy",
                "uptime_seconds": 0,  # TODO: track uptime
                "database_connected": bool(self.app.db),
                "nats_connected": self._connected,
            }
            await self._publish_response(msg.reply, {"success": True, "data": health})
            
        except Exception as e:
            self.logger.error(f"Error handling system.health: {e}", exc_info=True)
            await self._publish_response(msg.reply, {"error": str(e)})
            
    async def _handle_system_stats(self, msg) -> None:
        """Handle system.stats query - Get aggregate statistics."""
        try:
            stats = {
                "total_users": await self.app.db.get_total_users(),
                "total_messages": await self.app.db.get_total_messages(),
                "total_pms": await self.app.db.get_total_pms(),
                "total_kudos": await self.app.db.get_total_kudos_plusplus(),
                "total_emotes": await self.app.db.get_total_emote_usage(),
                "total_media_changes": await self.app.db.get_total_media_changes(),
            }
            
            if self.app.activity_tracker:
                stats["active_sessions"] = self.app.activity_tracker.get_active_session_count()
                
            await self._publish_response(msg.reply, {"success": True, "data": stats})
            
        except Exception as e:
            self.logger.error(f"Error handling system.stats: {e}", exc_info=True)
            await self._publish_response(msg.reply, {"error": str(e)})

    async def _handle_channel_watermarks(self, msg) -> None:
        """Handle channel.watermarks query - Get high/low user population marks."""
        try:
            request = json.loads(msg.data.decode('utf-8'))
            channel = request.get('channel')
            days = request.get('days')  # None for all time, or 1,3,7,30,90
            
            if not channel:
                await self._publish_response(msg.reply, {"error": "channel required"})
                return
                
            watermarks = await self.app.db.get_water_marks(channel, self.domain, days)
            await self._publish_response(msg.reply, {"success": True, "data": watermarks})
            
        except Exception as e:
            self.logger.error(f"Error handling channel.watermarks: {e}", exc_info=True)
            await self._publish_response(msg.reply, {"error": str(e)})
    
    async def _handle_movie_votes(self, msg) -> None:
        """Handle channel.movie_votes query - Get movie voting statistics."""
        try:
            request = json.loads(msg.data.decode('utf-8'))
            channel = request.get('channel')
            media_title = request.get('media_title')  # Optional: specific movie
            
            if not channel:
                await self._publish_response(msg.reply, {"error": "channel required"})
                return
                
            votes = await self.app.db.get_movie_votes(channel, self.domain, media_title)
            await self._publish_response(msg.reply, {"success": True, "data": votes})
            
        except Exception as e:
            self.logger.error(f"Error handling movie_votes: {e}", exc_info=True)
            await self._publish_response(msg.reply, {"error": str(e)})
    
    async def _handle_timeseries_messages(self, msg) -> None:
        """Handle timeseries.messages query - Get message activity over time."""
        try:
            request = json.loads(msg.data.decode('utf-8'))
            channel = request.get('channel')
            start_time = request.get('start_time')
            end_time = request.get('end_time')
            
            if not channel:
                await self._publish_response(msg.reply, {"error": "channel required"})
                return
                
            data = await self.app.db.get_time_series_messages(channel, self.domain, start_time, end_time)
            await self._publish_response(msg.reply, {"success": True, "data": data})
            
        except Exception as e:
            self.logger.error(f"Error handling timeseries.messages: {e}", exc_info=True)
            await self._publish_response(msg.reply, {"error": str(e)})
    
    async def _handle_timeseries_kudos(self, msg) -> None:
        """Handle timeseries.kudos query - Get kudos activity over time."""
        try:
            request = json.loads(msg.data.decode('utf-8'))
            channel = request.get('channel')
            # TODO: implement time-series tracking for kudos
            await self._publish_response(msg.reply, {"success": True, "data": []})
            
        except Exception as e:
            self.logger.error(f"Error handling timeseries.kudos: {e}", exc_info=True)
            await self._publish_response(msg.reply, {"error": str(e)})
