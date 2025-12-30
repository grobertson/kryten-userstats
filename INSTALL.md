# Installation Guide

## Requirements

- Python 3.11 or higher
- NATS server (for message bus)
- Kryten-Robot bridge (for CyTube connectivity)

## Installation Methods

### 1. Install from PyPI (Recommended)

```bash
pip install kryten-userstats
```

### 2. Install from Source

```bash
# Clone repository
git clone https://github.com/yourusername/kryten-userstats.git
cd kryten-userstats

# Install with Poetry
uv sync

# Or with pip
pip install -e .
```

### 3. Install with Poetry for Development

```bash
# Clone repository
git clone https://github.com/yourusername/kryten-userstats.git
cd kryten-userstats

# Install with dev dependencies
uv sync --with dev

# Activate virtual environment
poetry shell
```

## Configuration

### 1. Create Configuration File

Copy the example configuration:

```bash
cp config.example.json config.json
```

### 2. Edit Configuration

Edit `config.json` with your settings:

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
      "channel": "your_channel_here"
    }
  ],
  "database": {
    "path": "data/userstats.db"
  },
  "metrics": {
    "port": 28282
  },
  "snapshots": {
    "interval_seconds": 300
  },
  "kudos": {
    "default_phrases": [
      "lol",
      "rofl",
      "lmao",
      "haha"
    ]
  }
}
```

### 3. Set Environment Variables

Set NATS credentials:

```bash
# Linux/macOS
export NATS_USER=your_username
export NATS_PASSWORD=your_password

# Windows PowerShell
$env:NATS_USER = "your_username"
$env:NATS_PASSWORD = "your_password"
```

### 4. Configure Username Aliases (Optional)

Edit `aliases.json` to map aliases to canonical usernames:

```json
{
  "aliases": {
    "kevinchrist": ["kc", "kchrist", "kevin"],
    "alice": ["alice2", "alice_alt"]
  }
}
```

Load aliases:

```bash
python manage_aliases.py --load aliases.json
```

## Running the Service

### Command Line

```bash
# Using module
python -m userstats --config config.json --log-level INFO

# If installed via pip/poetry
kryten-userstats --config config.json --log-level INFO
```

### With Scripts

**Windows:**
```powershell
.\start-userstats.ps1
```

**Linux/macOS:**
```bash
chmod +x start-userstats.sh
./start-userstats.sh
```

### As Systemd Service (Linux)

1. **Install service file:**
   ```bash
   sudo cp systemd/kryten-userstats.service /etc/systemd/system/
   ```

2. **Edit service file:**
   ```bash
   sudo nano /etc/systemd/system/kryten-userstats.service
   ```
   
   Update paths and credentials.

3. **Enable and start:**
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable kryten-userstats
   sudo systemctl start kryten-userstats
   ```

4. **Check status:**
   ```bash
   sudo systemctl status kryten-userstats
   sudo journalctl -u kryten-userstats -f
   ```

See [systemd/README.md](systemd/README.md) for detailed systemd configuration.

## Verifying Installation

### 1. Check Service Status

The service should connect to NATS and start tracking events:

```
2025-12-05 10:00:00 - INFO - Starting User Statistics Tracker
2025-12-05 10:00:01 - INFO - Database initialized at data/userstats.db
2025-12-05 10:00:02 - INFO - Connected to NATS
2025-12-05 10:00:03 - INFO - Prometheus metrics server listening on port 28282
2025-12-05 10:00:04 - INFO - NATS query endpoints registered
```

### 2. Test Prometheus Metrics

```bash
curl http://localhost:28282/metrics
```

You should see Prometheus-formatted metrics.

### 3. Test NATS Query Endpoints

```bash
# Install nats-cli
go install github.com/nats-io/natscli/nats@latest

# Query system stats
nats request cytube.query.userstats.cytu.be.system.stats '{}'
```

### 4. Use CLI Tool

```bash
# System statistics
python query_cli.py system stats

# System health
python query_cli.py system health

# User statistics
python query_cli.py user your_username
```

## Troubleshooting

### Service Won't Start

**Check NATS connection:**
```bash
nats-cli sub "cytube.events.>"
```

**Check configuration:**
```bash
python -m json.tool config.json
```

**Check logs:**
```bash
# If running as service
sudo journalctl -u kryten-userstats -n 50

# If running manually
# Logs will be in console output
```

### No Events Received

1. **Verify Kryten-Robot bridge is running**
2. **Check channel configuration matches bridge**
3. **Verify NATS subjects:**
   ```bash
   nats-cli sub "cytube.events.cytu.be.your_channel.>"
   ```

### Database Errors

**Check permissions:**
```bash
ls -la data/
# Should be writable by service user
```

**Check disk space:**
```bash
df -h
```

### Metrics Server Not Accessible

**Check if port is in use:**
```bash
# Linux
netstat -tuln | grep 28282

# Windows
netstat -an | findstr 28282
```

**Check firewall:**
```bash
# Linux
sudo ufw status
sudo ufw allow 28282/tcp
```

### Query Endpoints Not Responding

**Verify NATS connection:**
```bash
nats-cli server info
```

**Check if endpoints are registered:**
```bash
nats-cli sub "cytube.query.userstats.>"
```

## Updating

### From PyPI

```bash
pip install --upgrade kryten-userstats
```

### From Source

```bash
cd kryten-userstats
git pull
uv sync
```

## Uninstallation

```bash
# If installed via pip
pip uninstall kryten-userstats

# If installed via poetry
uv remove kryten-userstats

# Remove systemd service
sudo systemctl stop kryten-userstats
sudo systemctl disable kryten-userstats
sudo rm /etc/systemd/system/kryten-userstats.service
sudo systemctl daemon-reload
```

## Next Steps

- Read [QUERY_ENDPOINTS.md](QUERY_ENDPOINTS.md) for API documentation
- See [METRICS_IMPLEMENTATION.md](METRICS_IMPLEMENTATION.md) for technical details
- Check [README.md](README.md) for usage examples
- Configure Prometheus/Grafana for monitoring
- Build a dashboard using the query endpoints
