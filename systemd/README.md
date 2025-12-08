# Systemd Service Configuration for kryten-userstats

This directory contains systemd service files for running kryten-userstats as a system service.

## Files

- `kryten-userstats.service` - Main systemd service unit
- `kryten-userstats@.service` - Template for running multiple instances

## Installation

### Single Instance

1. **Copy service file to systemd directory:**
   ```bash
   sudo cp systemd/kryten-userstats.service /etc/systemd/system/
   ```

2. **Edit the service file to match your setup:**
   ```bash
   sudo nano /etc/systemd/system/kryten-userstats.service
   ```
   
   Update these values:
   - `WorkingDirectory` - Path to your installation
   - `ExecStart` - Path to Python and config file
   - `User` - User to run service as
   - `Group` - Group to run service as
   - Environment variables (NATS_USER, NATS_PASSWORD)

3. **Reload systemd and enable service:**
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

### Multiple Instances

Use the template service for running multiple instances with different configs:

1. **Copy template service file:**
   ```bash
   sudo cp systemd/kryten-userstats@.service /etc/systemd/system/
   ```

2. **Create config files:**
   ```bash
   # Example: config-channel1.json, config-channel2.json
   cp config.example.json /etc/kryten-userstats/config-channel1.json
   cp config.example.json /etc/kryten-userstats/config-channel2.json
   ```

3. **Edit and customize each config file**

4. **Start instances:**
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable kryten-userstats@channel1
   sudo systemctl enable kryten-userstats@channel2
   sudo systemctl start kryten-userstats@channel1
   sudo systemctl start kryten-userstats@channel2
   ```

5. **Check status:**
   ```bash
   sudo systemctl status kryten-userstats@channel1
   sudo journalctl -u kryten-userstats@channel1 -f
   ```

## Service Management

```bash
# Start service
sudo systemctl start kryten-userstats

# Stop service
sudo systemctl stop kryten-userstats

# Restart service
sudo systemctl restart kryten-userstats

# Check status
sudo systemctl status kryten-userstats

# View logs
sudo journalctl -u kryten-userstats -f

# View last 100 lines
sudo journalctl -u kryten-userstats -n 100

# Enable auto-start on boot
sudo systemctl enable kryten-userstats

# Disable auto-start
sudo systemctl disable kryten-userstats
```

## Troubleshooting

### Service fails to start

Check logs:
```bash
sudo journalctl -u kryten-userstats -n 50
```

Common issues:
- Wrong path in `WorkingDirectory` or `ExecStart`
- Missing dependencies (run `pip install -r requirements.txt`)
- Config file not found or invalid JSON
- NATS server not running
- Permission issues (wrong User/Group)

### Service crashes

Check if it's related to:
- Database file permissions
- NATS connection issues
- Invalid configuration

View detailed logs:
```bash
sudo journalctl -u kryten-userstats --since "10 minutes ago"
```

### Environment Variables

You can set environment variables in the service file or use a separate file:

**In service file:**
```ini
[Service]
Environment="NATS_USER=myuser"
Environment="NATS_PASSWORD=mypass"
```

**Using environment file:**
```bash
# Create /etc/kryten-userstats/env
NATS_USER=myuser
NATS_PASSWORD=mypass
```

Then in service file:
```ini
[Service]
EnvironmentFile=/etc/kryten-userstats/env
```

## Security Considerations

1. **Run as non-root user:**
   - Create dedicated user: `sudo useradd -r -s /bin/false kryten`
   - Update service file: `User=kryten` and `Group=kryten`

2. **Protect credentials:**
   - Use environment files with restricted permissions
   - `sudo chmod 600 /etc/kryten-userstats/env`
   - `sudo chown kryten:kryten /etc/kryten-userstats/env`

3. **Database permissions:**
   - Ensure kryten user can write to database directory
   - `sudo chown -R kryten:kryten /var/lib/kryten-userstats`

4. **Firewall:**
   - Restrict metrics port (28282) to trusted networks
   - Use systemd socket activation if needed
