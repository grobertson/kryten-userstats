# Initial State Loading from NATS KV Stores

## Overview

kryten-userstats now loads the complete initial channel state (userlist, emotes, playlist) from NATS KeyValue stores maintained by Kryten-Robot. This provides a full picture of the channel at startup, rather than only tracking changes that occur after connection.

## Benefits

1. **Complete Channel Context**: Know all users, emotes, and media from the moment the service starts
2. **No Cold Start Gap**: Previously, the service only knew about users/emotes/media that appeared after it connected
3. **Accurate Statistics**: User tracking and activity monitoring includes users already in the channel
4. **Emote Detection**: Emote usage tracking works immediately with the full emote list
5. **Media Context**: Movie voting works for media that was playing before connection

## Implementation

### Architecture

```
Kryten-Robot                    kryten-userstats
    |                                 |
    | Maintains KV Stores             | 
    | - userlist                      |
    | - emotes                        |
    | - playlist                      |
    |                                 |
    |                                 | On Startup:
    |                                 | 1. Connect to NATS
    |                                 | 2. Fetch from KV stores
    |                                 | 3. Populate database
    |                                 | 4. Initialize trackers
    |                                 | 5. Start event handling
```

### Code Changes

#### 1. Imports (main.py)
```python
import nats
from kryten import get_kv_store, kv_get
```

#### 2. NATS Client for KV Access
```python
# Separate NATS connection for KV store access
self.nats_client: Optional[nats.NATS] = None
```

#### 3. Initial State Loading Method
```python
async def _load_initial_state(self, domain: str, channel: str) -> None:
    """Load initial channel state from NATS KV stores."""
    
    # Construct bucket names to match Kryten-Robot
    # Format: kryten_{channel}_<type>
    # Channel name must be lowercase
    bucket_prefix = f"kryten_{channel.lower()}"
    
    # Load userlist
    kv_userlist = await get_kv_store(self.nats_client, f"{bucket_prefix}_userlist")
    userlist_json = await kv_get(kv_userlist, "users", default="[]", json_decode=True)
    
    # Track all users in database and activity tracker
    for user in userlist_json:
        username = user.get("name") or user.get("username")
        await self.db.track_user(username)
        self.activity_tracker.user_joined(domain, channel, username)
    
    # Load emotes
    kv_emotes = await get_kv_store(self.nats_client, f"{bucket_prefix}_emotes")
    emotes_json = await kv_get(kv_emotes, "emotes", default="[]", json_decode=True)
    emote_names = [e.get("name") for e in emotes_json]
    self.emote_detector.set_emote_list(emote_names)
    
    # Load playlist (for current media context)
    kv_playlist = await get_kv_store(self.nats_client, f"{bucket_prefix}_playlist")
    playlist_json = await kv_get(kv_playlist, "playlist", default="[]", json_decode=True)
    
    if playlist_json:
        current = playlist_json[0]  # First item is currently playing
        self._current_media[channel] = {
            'title': current.get("title"),
            'type': current.get("type"),
            'id': current.get("id")
        }
```

#### 4. Startup Sequence
```python
async def start(self) -> None:
    # 1. Initialize database
    # 2. Initialize trackers (activity, kudos, emotes)
    # 3. Initialize Kryten client
    
    # 4. Connect to NATS for KV access
    self.nats_client = await nats.connect(servers=nats_servers)
    
    # 5. Load initial state BEFORE registering event handlers
    for channel_config in self.config.get("channels", []):
        await self._load_initial_state(channel_config["domain"], channel_config["channel"])
    
    # 6. Register event handlers (deltas)
    # 7. Start event processing
```

### KV Store Bucket Naming Convention

Kryten-Robot creates three KV buckets per channel:

- **Userlist**: `cytube_{domain}_{channel}_userlist`
  - Key: `users`
  - Value: JSON array of user objects `[{name, rank, profile, ...}, ...]`

- **Emotes**: `cytube_{domain}_{channel}_emotes`
  - Key: `emotes`
  - Value: JSON array of emote objects `[{name, image, ...}, ...]`

- **Playlist**: `cytube_{domain}_{channel}_playlist`
  - Key: `playlist`
  - Value: JSON array of media objects `[{title, type, id, duration, ...}, ...]`

Example for channel `cytu.be/420grindhouse`:
- `kryten_420grindhouse_userlist`
- `kryten_420grindhouse_emotes`
- `kryten_420grindhouse_playlist`

## Synchronization Strategy

### Initial State (Startup)
1. **Load from KV stores**: Get complete userlist, emotes, playlist
2. **Populate database**: Track all users seen
3. **Initialize trackers**: Mark users as "joined" in activity tracker
4. **Set emote list**: Enable immediate emote detection

### Delta Updates (Runtime)
Event handlers keep state synchronized:

- **addUser**: New user joins
- **userLeave**: User leaves
- **emoteList**: Emote list changes
- **playlist/queue/delete**: Playlist modifications
- **changeMedia**: Current media changes
- **chatmsg**: User activity, kudos, emotes

### Result
Complete, accurate tracking from the moment the service starts, with continuous updates via CyTube events.

## Dependencies

Requires **kryten-py >= 0.3.1** for KV store helpers:
- `get_kv_store(nc, bucket_name)` - Get/create KV bucket
- `kv_get(kv, key, default, json_decode)` - Retrieve value with JSON parsing

## Error Handling

The implementation is resilient to KV store issues:

```python
try:
    kv_userlist = await get_kv_store(self.nats_client, bucket_name)
    userlist_json = await kv_get(kv_userlist, "users", default="[]", json_decode=True)
    # Process data...
except Exception as e:
    self.logger.warning(f"Could not load userlist from KV store: {e}")
    # Service continues with delta-only tracking
```

If KV stores are unavailable:
- Service logs warnings but continues
- Falls back to delta-only tracking (original behavior)
- No fatal errors or startup failures

## Testing

### Prerequisites
1. NATS server running with JetStream enabled
2. Kryten-Robot connected and populating KV stores
3. At least one active CyTube channel

### Verification Steps

1. **Start Kryten-Robot** (to populate KV stores):
   ```powershell
   cd d:\Devel\Kryten-Robot
   python -m kryten config.json
   ```

2. **Start kryten-userstats**:
   ```powershell
   cd d:\Devel\kryten-userstats
   python -m userstats --config config.json --log-level DEBUG
   ```

3. **Check logs** for initial state loading:
   ```
   INFO - Connected to NATS for KV store access
   INFO - Loading initial state for cytu.be/420grindhouse...
   INFO - Loaded 15 users from KV store
   DEBUG - Initialized user: alice
   DEBUG - Initialized user: bob
   ...
   INFO - Loaded 142 emotes from KV store
   INFO - Loaded 23 playlist items from KV store
   INFO - Initialized current media: Big Buck Bunny
   INFO - Registering event handlers...
   ```

4. **Query statistics** immediately:
   ```powershell
   python query_cli.py user alice
   ```
   
   Should show alice in the database even though she was already in the channel before userstats started.

## Migration Notes

### Upgrading from Previous Versions

**No database migration required**. The changes are purely in the startup logic.

**Steps**:
1. Update requirements: `kryten-py>=0.3.1`
2. Restart kryten-userstats
3. Verify initial state logs

### Backward Compatibility

Fully backward compatible:
- If KV stores don't exist, falls back to delta-only tracking
- No config changes required
- Existing database schema unchanged

## Future Enhancements

Potential improvements:

1. **Periodic State Refresh**: Re-sync from KV stores every N minutes to catch any missed events
2. **State Validation**: Compare delta-tracked state vs KV state for drift detection
3. **Historical Backfill**: On startup, backfill statistics for users already in the database
4. **State Persistence**: Cache loaded state to avoid repeated KV fetches on restart

## Related Documentation

- [STATE_QUERY_IMPLEMENTATION.md](../Kryten-Robot/STATE_QUERY_IMPLEMENTATION.md) - Kryten-Robot's state management
- [kryten-py README](../kryten-py/README.md) - Library documentation
- [METRICS_IMPLEMENTATION.md](METRICS_IMPLEMENTATION.md) - Statistics tracking details
