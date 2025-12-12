"""Prometheus metrics HTTP server for user statistics."""

import logging
import time

from aiohttp import web


class MetricsServer:
    """HTTP server exposing Prometheus metrics on port 28282."""

    def __init__(self, app_reference, port: int = 28282):
        """Initialize metrics server.
        
        Args:
            app_reference: Reference to UserStatsApp for accessing components
            port: HTTP port to listen on (default 28282)
        """
        self.app = app_reference
        self.port = port
        self.logger = logging.getLogger(__name__)

        self._web_app: web.Application | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None

        # Metrics tracking
        self.start_time = time.time()

    async def start(self) -> None:
        """Start HTTP server."""
        self._web_app = web.Application()
        self._web_app.router.add_get("/metrics", self._handle_metrics)

        self._runner = web.AppRunner(self._web_app)
        await self._runner.setup()

        self._site = web.TCPSite(self._runner, "0.0.0.0", self.port)
        await self._site.start()

        self.logger.info(f"Prometheus metrics server listening on port {self.port}")

    async def stop(self) -> None:
        """Stop HTTP server."""
        if self._site:
            await self._site.stop()
        if self._runner:
            await self._runner.cleanup()

        self.logger.info("Metrics server stopped")

    async def _handle_metrics(self, request: web.Request) -> web.Response:
        """Handle GET /metrics request."""
        try:
            metrics = await self._collect_metrics()
            return web.Response(text=metrics, content_type="text/plain; version=0.0.4")
        except Exception as e:
            self.logger.error(f"Error collecting metrics: {e}", exc_info=True)
            return web.Response(text="# Error collecting metrics\n", status=500)

    async def _collect_metrics(self) -> str:
        """Collect and format Prometheus metrics."""
        lines = []

        # Service health metrics
        uptime_seconds = time.time() - self.start_time
        lines.append("# HELP userstats_uptime_seconds Service uptime in seconds")
        lines.append("# TYPE userstats_uptime_seconds gauge")
        lines.append(f"userstats_uptime_seconds {uptime_seconds:.2f}")
        lines.append("")

        lines.append("# HELP userstats_service_status Service health status (1=healthy, 0=unhealthy)")
        lines.append("# TYPE userstats_service_status gauge")
        service_healthy = 1 if self.app._running else 0
        lines.append(f"userstats_service_status {service_healthy}")
        lines.append("")

        # Database connection status
        lines.append("# HELP userstats_database_connected Database connection status (1=connected, 0=disconnected)")
        lines.append("# TYPE userstats_database_connected gauge")
        db_connected = 1 if self.app.db and self.app.db.db_path else 0
        lines.append(f"userstats_database_connected {db_connected}")
        lines.append("")

        # NATS connection status
        lines.append("# HELP userstats_nats_connected NATS connection status (1=connected, 0=disconnected)")
        lines.append("# TYPE userstats_nats_connected gauge")
        nats_connected = 1 if self.app.client and self.app.client._running else 0
        lines.append(f"userstats_nats_connected {nats_connected}")
        lines.append("")

        # Application metrics from database
        if self.app.db:
            try:
                # Total users tracked
                total_users = await self.app.db.get_total_users()
                lines.append("# HELP userstats_total_users_tracked Total number of unique users tracked")
                lines.append("# TYPE userstats_total_users_tracked gauge")
                lines.append(f"userstats_total_users_tracked {total_users}")
                lines.append("")

                # Total messages across all channels
                total_messages = await self.app.db.get_total_messages()
                lines.append("# HELP userstats_total_messages Total messages across all channels")
                lines.append("# TYPE userstats_total_messages counter")
                lines.append(f"userstats_total_messages {total_messages}")
                lines.append("")

                # Total PMs
                total_pms = await self.app.db.get_total_pms()
                lines.append("# HELP userstats_total_pms Total private messages sent")
                lines.append("# TYPE userstats_total_pms counter")
                lines.append(f"userstats_total_pms {total_pms}")
                lines.append("")

                # Total kudos (++ only)
                total_kudos = await self.app.db.get_total_kudos_plusplus()
                lines.append("# HELP userstats_total_kudos_plusplus Total ++ kudos given")
                lines.append("# TYPE userstats_total_kudos_plusplus counter")
                lines.append(f"userstats_total_kudos_plusplus {total_kudos}")
                lines.append("")

                # Total emote usage
                total_emotes = await self.app.db.get_total_emote_usage()
                lines.append("# HELP userstats_total_emote_usage Total emote uses")
                lines.append("# TYPE userstats_total_emote_usage counter")
                lines.append(f"userstats_total_emote_usage {total_emotes}")
                lines.append("")

                # Media changes
                total_media = await self.app.db.get_total_media_changes()
                lines.append("# HELP userstats_total_media_changes Total media changes logged")
                lines.append("# TYPE userstats_total_media_changes counter")
                lines.append(f"userstats_total_media_changes {total_media}")
                lines.append("")

                # Active sessions (currently online users)
                if self.app.activity_tracker:
                    active_sessions = self.app.activity_tracker.get_active_session_count()
                    lines.append("# HELP userstats_active_sessions Currently active user sessions")
                    lines.append("# TYPE userstats_active_sessions gauge")
                    lines.append(f"userstats_active_sessions {active_sessions}")
                    lines.append("")

            except Exception as e:
                self.logger.error(f"Error collecting database metrics: {e}", exc_info=True)

        return "\n".join(lines)
