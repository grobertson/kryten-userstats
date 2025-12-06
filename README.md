# Kryten User Statistics Tracker

A microservice for tracking comprehensive user statistics in CyTube channels via the Kryten bridge.

## Features

### Core Statistics
- **User Tracking**: Records all seen usernames with first/last seen timestamps
- **Message Counts**: Tracks public messages per user per channel
- **PM Counts**: Tracks private messages from each user
- **Channel Population**: Snapshots every 5 minutes (connected users and active chatters)
- **Media Changes**: Logs all media title changes with timestamps
- **User Activity Time**: 
  - Total time in channel
  - Active (not-AFK) time in channel
  - Uses CyTube's native `setAFK` events for accurate tracking

### Emote Tracking
- Tracks emote usage per user per channel
- Detects hashtags matching the master emote list
- Example: `#PogChamp` or `#Kappa`

### Kudos System
Two types of kudos tracking:

1. **++ Kudos**: Detects `++username` or `username++` patterns
2. **Phrase Kudos**: Detects username near trigger phrases
   - Configurable phrases (e.g., "lol", "rofl", "haha", emojis)
   - Supports both `"phrase username"` and `"username phrase"` patterns
   - Includes repeating laughter detection (haha, hehe, hoho, etc.)

### Username Aliases
- Configurable N+1 aliases per username
- Example: "kevinchrist" → ["kc", "kchrist", "kevin"]
- Kudos automatically resolved to canonical username

### Data Exposure
- **Prometheus Metrics**: HTTP endpoint on port 28282 (configurable)
  - Service health metrics
  - Application statistics (users, messages, kudos, emotes, etc.)
  - Real-time active session count
- **NATS Query Endpoints**: Request/reply pattern for programmatic access
  - User statistics (messages, activity, kudos, emotes)
  - Channel leaderboards and population data
  - Media history
  - Global leaderboards
  - System health and stats

See [QUERY_ENDPOINTS.md](QUERY_ENDPOINTS.md) for complete API documentation.

## Installation

### Prerequisites
- Python 3.11+
- NATS server running
- Kryten-Robot bridge connected to target channel(s)
- kryten-py library

### Quick Start

1. **Clone or copy the repository**
   ```bash
   cd d:\Devel\kryten-userstats
   ```

2. **Copy configuration template**
   ```powershell
   # Windows
   Copy-Item config.example.json config.json
   ```
   ```bash
   # Linux/macOS
   cp config.example.json config.json
   ```

3. **Edit configuration**
   - Set your NATS connection details
   - Configure channels to monitor
   - Adjust snapshot interval (default 5 minutes)

4. **Configure aliases (optional)**
   Edit `aliases.json` to add username aliases

5. **Start the tracker**
   ```powershell
   # Windows
   .\start-userstats.ps1
   ```
   ```bash
   # Linux/macOS
   chmod +x start-userstats.sh
   ./start-userstats.sh
   ```

## Configuration

### config.json

```json
{
  "nats": {
    "servers": ["nats://localhost:4222"],
    "user": "${NATS_USER}",
    "password": "${NATS_PASSWORD}"
  },
  "channels": [
    {
      "domain": "cytu.be",
      "channel": "420grindhouse"
    }
  ],
  "database": {
    "path": "data/userstats.db"
  },
  "snapshots": {
    "interval_seconds": 300
  },
  "kudos": {
    "default_phrases": [
      "lol",
      "rofl",
      "lmao",
      "haha",
      "😂",
      "🤣",
      "😆"
    ]
  }
}
```

### aliases.json

```json
{
  "aliases": {
    "kevinchrist": ["kc", "kchrist", "kevin"],
    "exampleuser": ["example", "ex"]
  },
  "kudos_phrases": [
    "lol",
    "rofl",
    "lmao",
    "haha",
    "nice",
    "great",
    "awesome"
  ]
}
```

## Database Schema

The tracker uses SQLite3 for persistent storage with the following tables:

### Core Tables
- `users` - All seen usernames with timestamps
- `user_aliases` - Username → alias mappings
- `message_counts` - Public message counts per user/channel
- `pm_counts` - PM counts per user
- `population_snapshots` - Channel population every N minutes
- `media_changes` - Media title change log
- `user_activity` - Total and not-AFK time per user/channel

### Emote & Kudos Tables
- `emote_usage` - Emote usage counts per user/channel/emote
- `kudos_plusplus` - ++ kudos counts per user/channel
- `kudos_phrases` - Phrase kudos counts per user/channel/phrase
- `kudos_trigger_phrases` - Configured trigger phrases

## Usage Examples

### Running in Background

```powershell
# Windows
.\start-userstats.ps1 -Background

# Stop
Stop-Process -Id (Get-Content userstats.pid)
```

### Custom Configuration

```powershell
# Windows
.\start-userstats.ps1 -ConfigFile custom-config.json
```

```bash
# Linux/macOS
./start-userstats.sh custom-config.json
```

### Monitoring Logs

```powershell
# Windows
Get-Content -Path userstats.log -Wait
```

```bash
# Linux/macOS
tail -f userstats.log
```

## Querying Statistics

### CLI Tool (Recommended)

Use the included CLI tool for easy querying:

```bash
# System statistics
python query_cli.py system stats

# System health
python query_cli.py system health

# User statistics
python query_cli.py user alice
python query_cli.py user alice --channel 420grindhouse

# Leaderboards
python query_cli.py leaderboard messages --limit 10
python query_cli.py leaderboard kudos --limit 10
python query_cli.py leaderboard emotes --limit 20

# Channel statistics
python query_cli.py channel top 420grindhouse --limit 10
python query_cli.py channel media 420grindhouse --limit 20
python query_cli.py channel population 420grindhouse --hours 24
```

### HTTP Metrics (Prometheus)

```bash
# View metrics
curl http://localhost:28282/metrics

# Or use the example script
python examples/metrics_example.py
```

### NATS Queries (Advanced)

See [QUERY_ENDPOINTS.md](QUERY_ENDPOINTS.md) for complete API documentation and examples.

```bash
# Using nats-cli
nats request cytube.query.userstats.cytu.be.system.stats '{}'

# Using Python
python examples/query_example.py
```

## Database Queries

Example SQL queries for common statistics:

### Top Message Senders
```sql
SELECT username, channel, message_count 
FROM message_counts 
ORDER BY message_count DESC 
LIMIT 10;
```

### Top Kudos Recipients (++)
```sql
SELECT username, channel, kudos_count 
FROM kudos_plusplus 
ORDER BY kudos_count DESC 
LIMIT 10;
```

### Most Used Emotes
```sql
SELECT emote, SUM(usage_count) as total 
FROM emote_usage 
GROUP BY emote 
ORDER BY total DESC 
LIMIT 10;
```

### Recent Media Changes
```sql
SELECT channel, timestamp, media_title 
FROM media_changes 
ORDER BY timestamp DESC 
LIMIT 20;
```

### User Activity Report
```sql
SELECT 
    username, 
    channel,
    total_time_seconds / 3600.0 as hours_total,
    not_afk_time_seconds / 3600.0 as hours_active
FROM user_activity 
ORDER BY not_afk_time_seconds DESC;
```

## Architecture

The tracker follows the kryten-misc pattern:

- **Database Layer** (`database.py`): SQLite3 operations with async executor
- **Activity Tracker** (`activity_tracker.py`): Monitors user sessions and AFK status
- **Kudos Detector** (`kudos_detector.py`): Regex-based ++ and phrase detection
- **Emote Detector** (`emote_detector.py`): Hashtag matching against emote list
- **Main Application** (`main.py`): Event handlers and coordination

## Requirements

- Python 3.11+
- kryten-py >= 0.2.3
- NATS server
- Kryten-Robot bridge (connected to monitored channels)

## Troubleshooting

### Tracker Not Receiving Events
1. Check NATS connection: `nats-cli sub "cytube.events.>"`
2. Verify Kryten-Robot bridge is running and connected
3. Ensure channel names match between config and bridge

### Database Locked Errors
- SQLite only allows one writer at a time
- Tracker uses async executor to prevent blocking
- If issues persist, check for other processes accessing the DB

### Kudos Not Detected
1. Check trigger phrases in database: `SELECT * FROM kudos_trigger_phrases`
2. Verify username aliases: `SELECT * FROM user_aliases`
3. Test regex patterns with sample messages

## License

MIT License - See kryten-py for details

## Credits

Built using:
- [kryten-py](https://github.com/yourusername/kryten-py) - CyTube microservices library
- [NATS](https://nats.io/) - Message bus
- [SQLite3](https://www.sqlite.org/) - Database
"# kryten-userstats" 
