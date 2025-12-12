"""User statistics tracking microservice for CyTube via Kryten bridge.

kryten-userstats is a microservice that tracks comprehensive statistics for CyTube
channels through the Kryten bridge, including:

- User message counts and activity time
- Kudos system (++ and phrase-based)
- Emote usage tracking
- Channel population snapshots
- Media change logging
- Username aliases

It exposes data via:
- Prometheus HTTP metrics (port 28282)
- NATS request/reply endpoints (userstats.query.* subjects)

For more information, see:
- README.md for setup and usage
- METRICS_IMPLEMENTATION.md for technical details
"""

__version__ = "0.2.4"
__author__ = "Kryten Contributors"
__license__ = "MIT"

from .main import UserStatsApp

__all__ = ["UserStatsApp"]
