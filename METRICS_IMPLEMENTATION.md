# Metrics and Query Endpoints - Implementation Summary

## Overview

kryten-userstats now exposes comprehensive statistics through two interfaces:

1. **Prometheus HTTP Metrics** - Port 28282
2. **NATS Query Endpoints** - Request/reply pattern

## Changes Made

### New Files Created

1. **userstats/metrics_server.py**
   - HTTP server using aiohttp
   - Exposes Prometheus-compatible metrics on port 28282
   - Includes health metrics and application statistics

2. **userstats/query_endpoints.py**
   - NATS request/reply subscribers
   - 12 query endpoints for user, channel, leaderboard, and system queries
   - JSON-based request/response protocol

3. **QUERY_ENDPOINTS.md**
   - Complete API documentation
   - Examples for all endpoints
   - Usage with nats-cli and Python

4. **examples/query_example.py**
   - Demonstrates NATS query usage
   - Shows 6 different query types

5. **examples/metrics_example.py**
   - Demonstrates HTTP metrics fetching
   - Parses and displays Prometheus metrics

### Modified Files

1. **userstats/main.py**
   - Added imports for MetricsServer and QueryEndpoints
   - Initialize and start both servers in start()
   - Stop both servers in stop()
   - Extract domain from config for query endpoints

2. **userstats/database.py**
   - Added 20+ query methods for metrics and endpoints:
     - get_total_users()
     - get_total_messages()
     - get_total_pms()
     - get_total_kudos_plusplus()
     - get_total_emote_usage()
     - get_total_media_changes()
     - get_user_message_count()
     - get_user_all_message_counts()
     - get_user_pm_count()
     - get_user_activity_stats()
     - get_user_all_activity()
     - get_user_kudos_plusplus()
     - get_user_kudos_phrases()
     - get_user_emote_usage()
     - get_top_message_senders()
     - get_recent_population_snapshots()
     - get_recent_media_changes()
     - get_global_message_leaderboard()
     - get_global_kudos_leaderboard()
     - get_top_emotes()

3. **userstats/activity_tracker.py**
   - Added get_active_sessions() - returns dict of (domain, channel) -> [usernames]
   - Added get_active_session_count() - returns total active session count

4. **config.json & config.example.json**
   - Added metrics.port configuration (default: 28282)

5. **requirements.txt**
   - Added aiohttp>=3.9.0 dependency

6. **README.md**
   - Added "Data Exposure" section
   - References QUERY_ENDPOINTS.md

## Prometheus Metrics Exposed

### Health Metrics
- `userstats_uptime_seconds` - Service uptime
- `userstats_service_status` - Health status (1=healthy, 0=unhealthy)
- `userstats_database_connected` - Database connection status
- `userstats_nats_connected` - NATS connection status

### Application Metrics
- `userstats_total_users_tracked` - Total unique users
- `userstats_total_messages` - Total messages across all channels
- `userstats_total_pms` - Total private messages
- `userstats_total_kudos_plusplus` - Total ++ kudos given
- `userstats_total_emote_usage` - Total emote uses
- `userstats_total_media_changes` - Total media changes logged
- `userstats_active_sessions` - Currently active user sessions

## NATS Query Endpoints

All endpoints use subject pattern: `cytube.query.userstats.{domain}.{query_type}`

### User Queries (4 endpoints)
- `user.stats` - Comprehensive user statistics
- `user.messages` - Message counts
- `user.activity` - Time spent in channel
- `user.kudos` - Kudos received

### Channel Queries (3 endpoints)
- `channel.top_users` - Top message senders
- `channel.population` - Recent population snapshots
- `channel.media_history` - Recent media changes

### Leaderboard Queries (3 endpoints)
- `leaderboard.messages` - Global message leaderboard
- `leaderboard.kudos` - Global kudos leaderboard
- `leaderboard.emotes` - Most used emotes

### System Queries (2 endpoints)
- `system.health` - Service health status
- `system.stats` - Overall statistics

## Usage Examples

### Prometheus Metrics (HTTP)
```bash
curl http://localhost:28282/metrics
```

```python
import requests
response = requests.get("http://localhost:28282/metrics")
print(response.text)
```

### NATS Queries (Request/Reply)
```bash
# Get system stats
nats request cytube.query.userstats.cytu.be.system.stats '{}'

# Get user stats
nats request cytube.query.userstats.cytu.be.user.stats '{"username":"foo"}'

# Get channel leaderboard
nats request cytube.query.userstats.cytu.be.channel.top_users '{"channel":"420grindhouse","limit":10}'
```

```python
import asyncio
import json
from nats.aio.client import Client as NATS

async def query_stats():
    nc = NATS()
    await nc.connect("nats://localhost:4222")
    
    response = await nc.request(
        "cytube.query.userstats.cytu.be.system.stats",
        json.dumps({}).encode(),
        timeout=5.0
    )
    
    stats = json.loads(response.data.decode())
    print(json.dumps(stats, indent=2))
    
    await nc.close()

asyncio.run(query_stats())
```

## Integration with Dashboard

A dashboard microservice can use these endpoints to build visualizations:

```python
async def dashboard_loop():
    while True:
        # HTTP: Fetch Prometheus metrics for health monitoring
        health_response = requests.get("http://localhost:28282/metrics")
        
        # NATS: Query user statistics
        user_stats = await query_user_stats("alice")
        
        # NATS: Query leaderboards
        leaderboard = await query_leaderboard(limit=10)
        
        # NATS: Query population trends
        population = await query_population("420grindhouse", hours=24)
        
        # Update dashboard UI
        update_dashboard(user_stats, leaderboard, population)
        
        await asyncio.sleep(10)
```

## Configuration

Add to `config.json`:

```json
{
  "metrics": {
    "port": 28282
  }
}
```

## Testing

1. **Start kryten-userstats:**
   ```powershell
   .\start-userstats.ps1
   ```

2. **Test Prometheus metrics:**
   ```bash
   curl http://localhost:28282/metrics
   ```
   Or run:
   ```bash
   python examples/metrics_example.py
   ```

3. **Test NATS queries:**
   ```bash
   nats request cytube.query.userstats.cytu.be.system.stats '{}'
   ```
   Or run:
   ```bash
   python examples/query_example.py
   ```

## Benefits

1. **Monitoring**: Prometheus metrics for alerting and dashboards
2. **Programmatic Access**: NATS queries for other microservices
3. **Real-time Data**: Both interfaces provide current statistics
4. **Flexible Queries**: NATS endpoints support filtering and limits
5. **Standard Format**: Prometheus-compatible metrics for easy integration

## Next Steps

Potential uses for these endpoints:

1. **Web Dashboard**: Build React/Vue dashboard consuming these APIs
2. **Discord Bot**: Query stats and post to Discord channels
3. **Alerting**: Set up Prometheus alerts for channel activity
4. **Analytics**: Collect metrics over time for trend analysis
5. **Grafana**: Create beautiful dashboards using Prometheus metrics

## Dependencies

- **aiohttp>=3.9.0**: HTTP server for Prometheus metrics
- **kryten-py>=0.2.3**: NATS client and event handling
