# Query Endpoints Documentation

## Overview

kryten-userstats exposes statistics data via two interfaces:

1. **Prometheus HTTP Metrics** - Port 28282 (configurable)
2. **NATS Query Endpoints** - Request/reply pattern

## Prometheus Metrics (HTTP)

Access metrics at `http://localhost:28282/metrics`

### Health Metrics

```
userstats_uptime_seconds        - Service uptime in seconds
userstats_service_status        - Service health (1=healthy, 0=unhealthy)
userstats_database_connected    - Database connection status
userstats_nats_connected        - NATS connection status
```

### Application Metrics

```
userstats_total_users_tracked   - Total unique users tracked
userstats_total_messages        - Total messages across all channels
userstats_total_pms             - Total private messages
userstats_total_kudos_plusplus  - Total ++ kudos given
userstats_total_emote_usage     - Total emote uses
userstats_total_media_changes   - Total media changes logged
userstats_active_sessions       - Currently active user sessions
```

### Example

```bash
curl http://localhost:28282/metrics
```

## NATS Query Endpoints

All query subjects follow the pattern:
```
cytube.query.userstats.{domain}.{query_type}
```

For domain `cytu.be`, queries are:
```
cytube.query.userstats.cytu.be.*
```

### User Queries

#### user.stats - Comprehensive User Statistics

Get all statistics for a user.

**Subject:** `cytube.query.userstats.cytu.be.user.stats`

**Request:**
```json
{
  "username": "foo",
  "channel": "420grindhouse"  // optional, omit for all channels
}
```

**Response:**
```json
{
  "username": "foo",
  "aliases": ["foo_alt", "foo2"],
  "messages": {
    "420grindhouse": 1234
  },
  "pms": 56,
  "activity": {
    "420grindhouse": {
      "total_seconds": 7200,
      "active_seconds": 6500
    }
  },
  "kudos_plusplus": 42,
  "kudos_phrases": [
    {"phrase": "lol", "count": 15},
    {"phrase": "haha", "count": 8}
  ],
  "emotes": [
    {"emote": "Kappa", "count": 89},
    {"emote": "PogChamp", "count": 67}
  ]
}
```

#### user.messages - Message Counts

**Subject:** `cytube.query.userstats.cytu.be.user.messages`

**Request:**
```json
{
  "username": "foo",
  "channel": "420grindhouse"  // optional
}
```

**Response (single channel):**
```json
{
  "username": "foo",
  "channel": "420grindhouse",
  "count": 1234
}
```

**Response (all channels):**
```json
{
  "username": "foo",
  "channels": {
    "420grindhouse": 1234,
    "otherChannel": 567
  }
}
```

#### user.activity - Time Spent in Channel

**Subject:** `cytube.query.userstats.cytu.be.user.activity`

**Request:**
```json
{
  "username": "foo",
  "channel": "420grindhouse"
}
```

**Response:**
```json
{
  "username": "foo",
  "channel": "420grindhouse",
  "total_seconds": 7200,
  "active_seconds": 6500
}
```

#### user.kudos - Kudos Received

**Subject:** `cytube.query.userstats.cytu.be.user.kudos`

**Request:**
```json
{
  "username": "foo"
}
```

**Response:**
```json
{
  "username": "foo",
  "plusplus": 42,
  "phrases": [
    {"phrase": "lol", "count": 15},
    {"phrase": "haha", "count": 8}
  ]
}
```

### Channel Queries

#### channel.top_users - Top Message Senders

**Subject:** `cytube.query.userstats.cytu.be.channel.top_users`

**Request:**
```json
{
  "channel": "420grindhouse",
  "limit": 10
}
```

**Response:**
```json
{
  "channel": "420grindhouse",
  "top_users": [
    {"username": "alice", "count": 5678},
    {"username": "bob", "count": 4321}
  ]
}
```

#### channel.population - Population Snapshots

**Subject:** `cytube.query.userstats.cytu.be.channel.population`

**Request:**
```json
{
  "channel": "420grindhouse",
  "hours": 24
}
```

**Response:**
```json
{
  "channel": "420grindhouse",
  "hours": 24,
  "snapshots": [
    {
      "timestamp": "2025-12-05T10:00:00Z",
      "connected": 15,
      "chatting": 12
    }
  ]
}
```

#### channel.media_history - Recent Media Changes

**Subject:** `cytube.query.userstats.cytu.be.channel.media_history`

**Request:**
```json
{
  "channel": "420grindhouse",
  "limit": 20
}
```

**Response:**
```json
{
  "channel": "420grindhouse",
  "media_history": [
    {
      "timestamp": "2025-12-05T10:15:30Z",
      "title": "Cool Video",
      "type": "yt",
      "id": "dQw4w9WgXcQ"
    }
  ]
}
```

### Leaderboard Queries

#### leaderboard.messages - Global Message Leaderboard

**Subject:** `cytube.query.userstats.cytu.be.leaderboard.messages`

**Request:**
```json
{
  "limit": 20
}
```

**Response:**
```json
{
  "leaderboard": [
    {"username": "alice", "count": 12345},
    {"username": "bob", "count": 9876}
  ]
}
```

#### leaderboard.kudos - Global Kudos Leaderboard

**Subject:** `cytube.query.userstats.cytu.be.leaderboard.kudos`

**Request:**
```json
{
  "limit": 20
}
```

**Response:**
```json
{
  "leaderboard": [
    {"username": "alice", "count": 150},
    {"username": "bob", "count": 98}
  ]
}
```

#### leaderboard.emotes - Most Used Emotes

**Subject:** `cytube.query.userstats.cytu.be.leaderboard.emotes`

**Request:**
```json
{
  "limit": 20
}
```

**Response:**
```json
{
  "leaderboard": [
    {"emote": "Kappa", "count": 5678},
    {"emote": "PogChamp", "count": 4321}
  ]
}
```

### System Queries

#### system.health - Service Health Status

**Subject:** `cytube.query.userstats.cytu.be.system.health`

**Request:** `{}` (empty)

**Response:**
```json
{
  "service": "userstats",
  "status": "healthy",
  "database_connected": true,
  "nats_connected": true,
  "uptime_seconds": 3600
}
```

#### system.stats - Overall Statistics

**Subject:** `cytube.query.userstats.cytu.be.system.stats`

**Request:** `{}` (empty)

**Response:**
```json
{
  "total_users": 1234,
  "total_messages": 56789,
  "total_pms": 890,
  "total_kudos": 456,
  "total_emotes": 7890,
  "total_media_changes": 234,
  "active_sessions": 12
}
```

## Usage Examples

### Using nats-cli

```bash
# Get user stats
nats request cytube.query.userstats.cytu.be.user.stats '{"username":"foo"}'

# Get channel leaderboard
nats request cytube.query.userstats.cytu.be.channel.top_users '{"channel":"420grindhouse","limit":10}'

# Get system health
nats request cytube.query.userstats.cytu.be.system.health '{}'

# Get global message leaderboard
nats request cytube.query.userstats.cytu.be.leaderboard.messages '{"limit":20}'
```

### Using Python

```python
import asyncio
import json
from nats.aio.client import Client as NATS

async def query_user_stats(username: str):
    nc = NATS()
    await nc.connect("nats://localhost:4222")
    
    request = json.dumps({"username": username})
    response = await nc.request(
        "cytube.query.userstats.cytu.be.user.stats",
        request.encode(),
        timeout=5.0
    )
    
    stats = json.loads(response.data.decode())
    print(json.dumps(stats, indent=2))
    
    await nc.close()

asyncio.run(query_user_stats("foo"))
```

### Using in Dashboard

A dashboard microservice could subscribe to these endpoints to build a web UI:

```python
# Get real-time stats every 10 seconds
async def update_dashboard():
    while True:
        stats = await query_system_stats()
        leaderboard = await query_message_leaderboard(limit=10)
        population = await query_channel_population("420grindhouse", hours=24)
        
        # Update dashboard display
        render_dashboard(stats, leaderboard, population)
        
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

The port is configurable but defaults to 28282 if not specified.
