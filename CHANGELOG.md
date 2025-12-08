# Changelog

All notable changes to kryten-userstats will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.4] - 2025-12-05

### Fixed
- **SQL Query Errors** - Fixed incorrect column names in aggregate queries
  - `kudos_plusplus`: Changed `SUM(count)` to `SUM(kudos_count)`
  - `emote_usage`: Changed `SUM(count)` to `SUM(usage_count)`
- **Graceful Shutdown** - Fixed clean exit on Ctrl+C
  - Proper signal handling for Windows and Unix
  - Sequential shutdown of components in correct order
  - No more hanging on exit
  - Added detailed shutdown logging

### Improved
- Better error handling in database initialization
- More detailed logging during component shutdown

## [0.2.3] - 2025-12-05

### Added
- **Direct NATS Publisher** - Separate NATS connection for publishing statistics data
  - 12 query endpoints: user stats, messages, activity, kudos, channel info, leaderboards, system health
  - Request/reply pattern on `userstats.query.{domain}.{query_type}` subjects
  - kryten-userstats owns and publishes its statistics data directly

### Changed
- **Separation of Duties** - Clear architectural boundary:
  - Consumes CyTube events via kryten-py (no direct NATS access for events)
  - Publishes statistics data via direct NATS connection (owns its data)
  - Two separate NATS connections with distinct responsibilities

### Technical Details
- Added `nats_publisher.py` with StatsPublisher class
- Dedicated NATS connection for query responses
- JSON request/reply protocol
- All query handlers async with error handling

## [0.2.2] - 2025-12-05

### Removed
- **NATS Query Endpoints** - Removed all query endpoints that accessed NATS directly
  - kryten-py does not expose NATS client and direct access violates architecture
  - Only HTTP Prometheus metrics server remains for data exposure

### Changed
- Simplified architecture to use only kryten-py's public API
- All statistics now exposed via HTTP metrics endpoint only (port 28282)

### Note
- This version properly respects kryten-py's abstraction boundaries
- Future request/response functionality will be added to kryten-py first

## [0.2.1] - 2025-01-29

### Fixed
- **CLI Entry Point** - Fixed `kryten-userstats` command failing with "coroutine 'main' was never awaited"
  - Added synchronous `main()` wrapper function in `__main__.py`
  - Renamed async main to `async_main()` and wrapped it with `asyncio.run()`
  - CLI command now works properly with Poetry scripts entry point

### Note
- Version 0.2.0 has a broken CLI command; users should upgrade to 0.2.1

## [0.2.0] - 2025-01-29

### Added
- **Prometheus HTTP Metrics Server** on port 28282
  - Health metrics (uptime, service status, database/NATS connection)
  - Application metrics (users, messages, PMs, kudos, emotes, media changes, active sessions)
- **NATS Query Endpoints** - 12 endpoints for programmatic data access
  - User queries (stats, messages, activity, kudos)
  - Channel queries (top users, population, media history)
  - Leaderboard queries (messages, kudos, emotes)
  - System queries (health, stats)
- **CLI Tool** (`query_cli.py`) for easy querying
- **Poetry Package Management** with proper pyproject.toml
- **Systemd Service Configuration**
  - Single instance service file
  - Template service for multiple instances
  - Comprehensive systemd documentation
- **Comprehensive Documentation**
  - QUERY_ENDPOINTS.md - Complete API documentation
  - METRICS_IMPLEMENTATION.md - Technical implementation details
  - INSTALL.md - Installation and troubleshooting guide
  - systemd/README.md - Systemd service management
- **Example Scripts**
  - examples/query_example.py - NATS query examples
  - examples/metrics_example.py - HTTP metrics example
- **Database Query Methods** - 20+ new methods for metrics and endpoints
- **Activity Tracker Methods** for session counting

### Changed
- **AFK Tracking** now uses CyTube's native `setAFK` events instead of inactivity detection
- **Package Structure** reorganized for proper Python packaging
- **Dependencies** now properly managed via Poetry
- Removed direct NATS client import from query_endpoints.py (uses kryten-py only)
- Updated README.md with query examples and CLI usage

### Fixed
- Improved accuracy of AFK time tracking
- Better error handling in query endpoints
- Proper async database operations

### Technical Details
- Python 3.11+ required
- Uses kryten-py >= 0.2.3
- Added aiohttp >= 3.9.0 for HTTP metrics
- All NATS operations now through kryten-py client
- Proper async/await throughout

## [0.1.0] - 2025-12-04

### Added
- Initial release
- Core statistics tracking
  - User message counts per channel
  - PM counts
  - Channel population snapshots (5-minute intervals)
  - Media change logging
  - User activity time (total and not-AFK)
- Emote tracking via hashtag detection
- Kudos system
  - ++ kudos detection
  - Phrase-based kudos (configurable trigger phrases)
- Username aliases system (N+1 aliases per username)
- SQLite database with 10+ tables
- Configuration via JSON
- Startup scripts for Windows (PowerShell) and Linux/macOS (Bash)
- Alias management utility (manage_aliases.py)
- Basic documentation (README.md)

### Technical Details
- Python 3.11+ support
- AsyncIO-based architecture
- kryten-py client library integration
- SQLite3 for persistent storage
- Automatic database schema creation

[0.2.0]: https://github.com/yourusername/kryten-userstats/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/yourusername/kryten-userstats/releases/tag/v0.1.0
