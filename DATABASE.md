# Database Initialization

## Automatic Setup

The kryten-userstats database is automatically initialized on first run. No manual setup is required.

### What Happens on First Run

1. **Directory Creation**: If the database directory doesn't exist (default: `data/`), it will be created automatically
2. **Database File**: SQLite database file is created (default: `data/userstats.db`)
3. **Schema Creation**: All 11 tables are created with proper indices:
   - `users` - Tracked usernames with first/last seen timestamps
   - `user_aliases` - Username alias mappings
   - `message_counts` - Public message counts per user/channel
   - `pm_counts` - Private message counts per user
   - `population_snapshots` - Channel population data (every 5 minutes)
   - `media_changes` - Media change event log
   - `user_activity` - Time tracking (total and not-AFK)
   - `emote_usage` - Emote usage statistics
   - `kudos_plusplus` - ++ kudos received
   - `kudos_phrases` - Phrase-based kudos
   - `kudos_trigger_phrases` - Configured trigger phrases

4. **Default Fixtures**: Default kudos trigger phrases are inserted:
   - lol, rofl, lmao, haha, 😂, 🤣, 😆

## Configuration

Database path is configured in `config.json`:

```json
{
  "database": {
    "path": "data/userstats.db"
  }
}
```

You can change the path to any location. The parent directory will be created automatically.

## Manual Database Reset

To reset the database and start fresh:

```bash
# Stop kryten-userstats
systemctl stop kryten-userstats

# Remove database file
rm data/userstats.db

# Restart - database will be recreated
systemctl start kryten-userstats
```

## Backup

To backup statistics:

```bash
# Create backup
cp data/userstats.db data/userstats.db.backup

# Or with timestamp
cp data/userstats.db "data/userstats.db.$(date +%Y%m%d)"
```

## Schema Migrations

The database uses SQLite with `CREATE TABLE IF NOT EXISTS`, so:
- **New installations**: All tables created fresh
- **Upgrades**: Existing tables preserved, new tables added
- **No automatic migrations**: Schema changes require manual migration scripts

## Customizing Default Kudos Phrases

Edit `config.json` to customize the default kudos trigger phrases:

```json
{
  "kudos": {
    "default_phrases": [
      "lol",
      "rofl",
      "custom_phrase"
    ]
  }
}
```

These are only used if no phrases exist in the database. To update phrases on an existing database, use the query endpoints or SQL:

```sql
-- Add new phrase
INSERT INTO kudos_trigger_phrases (phrase) VALUES ('new_phrase');

-- Remove phrase
DELETE FROM kudos_trigger_phrases WHERE phrase = 'old_phrase';

-- List all phrases
SELECT phrase FROM kudos_trigger_phrases;
```

## File Permissions

Ensure the user running kryten-userstats has write permissions to the database directory:

```bash
# If using systemd with User=kryten
chown -R kryten:kryten /path/to/data/
chmod 755 /path/to/data/
```

## Database Location Best Practices

- **Development**: `./data/userstats.db` (relative path)
- **Production**: `/var/lib/kryten-userstats/userstats.db` (absolute path)
- **Multi-instance**: `/var/lib/kryten-userstats/{channel}/userstats.db` (per-channel)

## Troubleshooting

### "OperationalError: unable to open database file"

- Check parent directory exists and is writable
- Verify file permissions
- Check disk space

### "DatabaseError: database is locked"

- Multiple processes accessing same database
- Use separate database per instance
- Check for stale lock files

### Database corruption

```bash
# Check integrity
sqlite3 data/userstats.db "PRAGMA integrity_check;"

# If corrupted, restore from backup or reset
rm data/userstats.db  # Will be recreated on restart
```
