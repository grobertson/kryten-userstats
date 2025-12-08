# kryten-userstats v0.2.0 - Release Summary

## Package Information

- **Name**: kryten-userstats
- **Version**: 0.2.0
- **License**: MIT
- **Python**: 3.11+
- **Build Date**: 2025-12-05

## What's New in 0.2.0

### Major Features
1. **Prometheus HTTP Metrics Server** (Port 28282)
   - Health monitoring (uptime, connections, status)
   - Application metrics (users, messages, kudos, emotes, etc.)
   - Prometheus-compatible text format

2. **NATS Query Endpoints** (12 endpoints)
   - User statistics queries
   - Channel leaderboards and population data
   - Global leaderboards
   - System health and statistics

3. **CLI Query Tool** (`query_cli.py`)
   - Easy command-line interface for querying statistics
   - User, channel, leaderboard, and system commands

4. **Professional Package Structure**
   - Poetry for dependency management
   - Proper Python packaging with pyproject.toml
   - Comprehensive documentation (INSTALL.md, QUERY_ENDPOINTS.md, etc.)
   - Systemd service configurations

5. **Enhanced AFK Tracking**
   - Now uses CyTube's native `setAFK` events
   - More accurate activity time calculations

## Installation

### From PyPI (After Publishing)
```bash
pip install kryten-userstats
```

### From Source
```bash
git clone https://github.com/yourusername/kryten-userstats.git
cd kryten-userstats
poetry install
```

## Quick Start

1. **Install**:
   ```bash
   pip install kryten-userstats
   ```

2. **Configure**:
   ```bash
   cp config.example.json config.json
   # Edit config.json with your settings
   ```

3. **Run**:
   ```bash
   kryten-userstats --config config.json
   ```

4. **Query Statistics**:
   ```bash
   # Using CLI tool
   python query_cli.py system stats
   
   # Using HTTP
   curl http://localhost:28282/metrics
   
   # Using NATS
   nats request cytube.query.userstats.cytu.be.system.stats '{}'
   ```

## Documentation

- **README.md**: Overview and usage examples
- **INSTALL.md**: Comprehensive installation guide
- **QUERY_ENDPOINTS.md**: Complete API documentation
- **METRICS_IMPLEMENTATION.md**: Technical implementation details
- **PUBLISHING.md**: Guide for publishing to PyPI
- **CHANGELOG.md**: Version history
- **systemd/README.md**: Systemd service configuration

## Dependencies

### Runtime
- **kryten-py** >= 0.2.3 (CyTube client library)
- **aiohttp** ^3.9.0 (HTTP server for metrics)

### Development
- **pytest** ^7.4.0
- **pytest-asyncio** ^0.21.0
- **black** ^23.7.0
- **ruff** ^0.0.285
- **mypy** ^1.5.0

## File Structure

```
kryten-userstats/
├── userstats/                 # Main package
│   ├── __init__.py           # Package metadata
│   ├── __main__.py           # Entry point
│   ├── main.py               # Application core
│   ├── database.py           # SQLite operations
│   ├── activity_tracker.py  # Activity tracking
│   ├── kudos_detector.py    # Kudos detection
│   ├── emote_detector.py    # Emote detection
│   ├── metrics_server.py    # Prometheus metrics
│   └── query_endpoints.py   # NATS query handlers
├── systemd/                  # Systemd service files
│   ├── kryten-userstats.service
│   ├── kryten-userstats@.service
│   └── README.md
├── examples/                 # Example scripts
│   ├── query_example.py
│   └── metrics_example.py
├── pyproject.toml           # Poetry configuration
├── README.md                # Main documentation
├── INSTALL.md               # Installation guide
├── QUERY_ENDPOINTS.md       # API documentation
├── METRICS_IMPLEMENTATION.md # Implementation details
├── PUBLISHING.md            # Publishing guide
├── CHANGELOG.md             # Version history
├── LICENSE                  # MIT License
├── config.example.json      # Configuration template
├── aliases.json             # Username aliases
├── query_cli.py            # CLI tool
├── manage_aliases.py       # Alias management
├── start-userstats.ps1     # Windows startup script
└── start-userstats.sh      # Linux startup script
```

## Publishing to PyPI

### Prerequisites
1. PyPI account and API token
2. Set environment variable: `export PYPI_TOKEN=pypi-your-token`

### Build
```bash
poetry build
```

### Publish
```bash
poetry publish
```

Or with token:
```bash
poetry publish --username __token__ --password pypi-your-token
```

### Verify
```bash
pip install kryten-userstats
python -c "import userstats; print(userstats.__version__)"
```

## Testing the Package

### Import Test
```python
import userstats
print(userstats.__version__)  # Should print: 0.2.0
```

### CLI Test
```bash
kryten-userstats --help
```

### Functionality Test
```bash
# Start with test config
kryten-userstats --config config.json

# In another terminal, test metrics
curl http://localhost:28282/metrics

# Test query endpoints
python query_cli.py system stats
```

## Metrics Exposed

### HTTP Metrics (Prometheus)
- `userstats_uptime_seconds` - Service uptime
- `userstats_service_status` - Health status
- `userstats_database_connected` - Database connection
- `userstats_nats_connected` - NATS connection
- `userstats_total_users_tracked` - Total users
- `userstats_total_messages` - Total messages
- `userstats_total_pms` - Total PMs
- `userstats_total_kudos_plusplus` - Total kudos
- `userstats_total_emote_usage` - Total emotes
- `userstats_total_media_changes` - Media changes
- `userstats_active_sessions` - Active sessions

### NATS Query Endpoints
All under `cytube.query.userstats.{domain}.*`:

**User Queries:**
- `user.stats` - Comprehensive user statistics
- `user.messages` - Message counts
- `user.activity` - Activity time
- `user.kudos` - Kudos received

**Channel Queries:**
- `channel.top_users` - Top message senders
- `channel.population` - Population snapshots
- `channel.media_history` - Media history

**Leaderboard Queries:**
- `leaderboard.messages` - Message leaderboard
- `leaderboard.kudos` - Kudos leaderboard
- `leaderboard.emotes` - Emote leaderboard

**System Queries:**
- `system.health` - Service health
- `system.stats` - Overall statistics

## Use Cases

1. **Monitoring**: Track channel activity with Prometheus/Grafana
2. **Dashboards**: Build web dashboards using query endpoints
3. **Discord Bots**: Query stats and post to Discord
4. **Analytics**: Analyze user behavior and trends
5. **Leaderboards**: Display top users, kudos, emotes
6. **Moderation**: Track user activity and engagement

## Support

- **Issues**: https://github.com/yourusername/kryten-userstats/issues
- **Documentation**: https://github.com/yourusername/kryten-userstats
- **PyPI**: https://pypi.org/project/kryten-userstats/

## License

MIT License - see LICENSE file for details

## Credits

Built with:
- [kryten-py](https://pypi.org/project/kryten-py/) - CyTube client library
- [aiohttp](https://github.com/aio-libs/aiohttp) - Async HTTP server
- [Poetry](https://python-poetry.org/) - Dependency management

## What's Next

Future enhancements may include:
- PostgreSQL/MySQL support
- Real-time WebSocket API
- Built-in web dashboard
- Graphical configuration tool
- Docker container
- Kubernetes deployment examples
- More detailed analytics
