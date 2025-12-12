# Changelog

All notable changes to kryten-userstats will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.11] - 2025-12-12

### Changed

- **Updated kryten-py to v0.8.1** - Major version update from 0.5.9
  - Includes lifecycle integration, service discovery
  - Fixed event dispatching and handler invocation
  - Better NATS subject pattern matching

### Fixed

- **Chat message handler logging** - Made message preview safe for None values
  - Prevents potential exception when event.message is empty

## [0.4.10] - 2025-12-12

### Added

- **Enhanced Emote Detection Debugging** - Added comprehensive debug logging for emote detection
  - Shows sample of loaded emotes during initialization
  - Logs when hashtags are found but don't match any emotes
  - DEBUG logging in emote detector showing what's being compared
  - Shows emote list samples when no match is found
  - Helps diagnose why emote counting may not be working

## [0.4.9] - 2025-12-12

### Fixed

- **Self-Kudos Prevention** - Prevents users from awarding kudos to themselves
  - Added check to block self-kudos for both ++ and phrase-based kudos
  - Logs debug message when self-kudos is attempted

### Added

- **Enhanced Emote Detection Logging** - Added comprehensive logging for emote list handling
  - INFO level logging when emote list is received and loaded
  - DEBUG level logging showing emote list structure and item count
  - WARNING level logging when emote list is empty or malformed
  - Helps diagnose emote detection and counting issues

## [0.4.8] - 2025-12-12

### Fixed

- **Media Title Extraction** - Fixed extraction from nested media objects in playlist data
  - CyTube playlist items store title/type/id in a nested `media` object
  - Now checks for both nested `media.title` and direct `title` fields
  - Added logging to show available fields and structure when title is missing
  - This fixes "Initialized current media: Unknown" issue

## [0.4.7] - 2025-12-12

### Added

- **Enhanced Media Title Logging** - Added comprehensive debug logging for media title tracking
  - Added INFO level logging for media change events showing title, type, and ID
  - Added DEBUG level logging showing raw playlist data during initialization
  - Added WARNING when playlist items have missing/empty titles
  - Helps diagnose media title extraction issues and event reception

## [0.4.6] - 2025-12-12

### Added

- **Enhanced Kudos Metrics** - Added comprehensive kudos tracking in Prometheus metrics
  - New `userstats_total_kudos` metric tracks ALL kudos (both ++ and phrase-based)
  - Kept `userstats_total_kudos_plusplus` for ++ style kudos specifically
  - Added `userstats_total_kudos_phrases` for phrase-based kudos specifically
  - Added `get_total_kudos_phrases()` and `get_total_kudos()` database methods

### Changed

- **Metrics Clarity** - Renamed metric description to clarify coverage
  - "Total ++ kudos given" remains for `userstats_total_kudos_plusplus`
  - "Total kudos given (all types)" for new `userstats_total_kudos` metric

## [0.4.5] - 2025-12-12

### Fixed

- **Python 3.10 Compatibility** - Reverted `datetime.UTC` to `timezone.utc` for Python 3.10 support
  - `datetime.UTC` was added in Python 3.11 and broke Python 3.10 builds
  - Updated ruff configuration to ignore UP017 rule and set target to py310
  - All datetime references now use `timezone.utc` for broad compatibility

## [0.4.4] - 2025-01-17

### Fixed

- **Media Title Extraction** - Fixed initial state loading bug where empty title strings would fall back to username (queueby)
  - Changed from `or` operator to explicit `None` check to handle empty strings correctly
  - Now properly returns "Unknown" instead of username when title is missing/empty
  
### Added

- **Media Title Cleaning** - Added `_clean_media_title()` method to improve title formatting
  - Removes common video file extensions (.mp4, .mkv, .avi, .webm, .flv, .mov, .wmv, .m4v, .mpg, .mpeg)
  - Replaces periods and underscores with spaces (common in filenames)
  - Removes multiple consecutive spaces
  - Applied to both initial state loading and media change events

## [0.4.2] - 2025-06-16

### Fixed

- **NATS Publisher** - Fixed 9 database method calls with incorrect names/signatures
  - `get_user_stats()` → inline aggregation from multiple sources
  - `get_user_activity_time()` → `get_user_activity_stats()`
  - `get_user_kudos()` → `get_user_kudos_plusplus()` + `get_user_kudos_phrases()`
  - `get_top_users_by_messages()` → `get_top_message_senders()`
  - `get_latest_population_snapshot()` → `get_recent_population_snapshots()`
  - `get_top_users_by_kudos()` → `get_global_kudos_leaderboard()`
  - Added missing `domain` parameter to all handlers

### Changed

- **Logging** - More verbose logging for emotes and kudos
  - ++ kudos now logs at INFO level with sender attribution
  - Phrase kudos now logs at INFO level with phrase and sender
  - Emote detection now logs summary at INFO level

### Removed

- Unused `self._conn` member from `StatsDatabase`
- Unused `_get_connection()` async context manager from `StatsDatabase`
- Unused `asynccontextmanager` import

### Technical Details

- Moved `import re` and `import time` to module level in main.py
- Consistent use of `self.app._domain` for default domain in all handlers
- All 70 unit tests still passing

## [0.4.1] - 2025-12-12

### Changed

- **MetricsServer** - Refactored to use kryten-py's BaseMetricsServer
  - Inherits from `kryten.BaseMetricsServer` for shared HTTP infrastructure
  - Now exposes `/health` endpoint (JSON) in addition to `/metrics`
  - Custom metrics and health details in `_collect_custom_metrics()` and `_get_health_details()`
  - Reduced code duplication with shared library

### Added

- `/health` endpoint returns JSON with service status, database info, and session counts

### Technical Details

- Now requires kryten-py>=0.8.1
- MetricsServer now subclasses BaseMetricsServer from kryten-py

## [0.4.0] - 2025-06-16

### Changed

- **Lifecycle Integration** - Migrated to kryten-py 0.8.0's integrated lifecycle management
  - ServiceConfig now defined in config.json instead of code
  - Automatic startup/shutdown events via KrytenClient
  - Automatic heartbeat publishing (configurable interval)
  - Automatic discovery poll response
  - Removed ~50 lines of manual lifecycle code

### Added

- **ServiceConfig fields in config.json**:
  - `enable_lifecycle`: Enable lifecycle event publishing (default: true)
  - `enable_heartbeat`: Enable heartbeat publishing (default: true)
  - `heartbeat_interval`: Heartbeat interval in seconds (default: 30)
  - `enable_discovery`: Respond to discovery polls (default: true)

### Fixed

- Fixed logger being used before initialization in main()

### Technical Details

- Now requires kryten-py>=0.8.0
- `_handle_robot_startup` now uses `self.client.lifecycle.publish_startup()`
- Lifecycle shutdown automatically handled by `client.disconnect()`
- Simplified stop() method

## [0.2.5] - 2025-12-09

### Changed

- **Compatibility**: Lowered minimum Python version requirement from 3.11 to 3.10
  - No Python 3.11+ specific features are used in the codebase
  - Dependencies support Python 3.10

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
