"""Prometheus metrics HTTP server for user statistics.

Uses BaseMetricsServer from kryten-py for the HTTP server infrastructure.
"""

from kryten import BaseMetricsServer


class MetricsServer(BaseMetricsServer):
    """HTTP server exposing Prometheus metrics on port 28282.

    Extends kryten-py's BaseMetricsServer with userstats-specific metrics.
    """

    def __init__(self, app_reference, port: int = 28282):
        """Initialize metrics server.

        Args:
            app_reference: Reference to UserStatsApp for accessing components
            port: HTTP port to listen on (default 28282)
        """
        super().__init__(
            service_name="userstats",
            port=port,
            client=app_reference.client,
        )
        self.app = app_reference

    async def _collect_custom_metrics(self) -> list[str]:
        """Collect userstats-specific metrics."""
        lines = []

        # Database connection status
        db_connected = 1 if self.app.db and self.app.db.db_path else 0
        lines.append("# HELP userstats_database_connected Database connection status (1=connected, 0=disconnected)")
        lines.append("# TYPE userstats_database_connected gauge")
        lines.append(f"userstats_database_connected {db_connected}")
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

        return lines

    async def _get_health_details(self) -> dict:
        """Get userstats-specific health details."""
        details = {}

        # Database status
        if self.app.db:
            details["database"] = "connected" if self.app.db.db_path else "disconnected"
            try:
                total_users = await self.app.db.get_total_users()
                details["total_users_tracked"] = total_users
            except Exception:
                details["database_query_error"] = True
        else:
            details["database"] = "not_initialized"

        # Activity tracker status
        if self.app.activity_tracker:
            details["active_sessions"] = self.app.activity_tracker.get_active_session_count()
        else:
            details["activity_tracker"] = "not_initialized"

        # Channel configuration
        details["channels_configured"] = len(self.app.config.get("channels", []))

        return details
