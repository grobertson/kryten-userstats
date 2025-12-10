#!/bin/bash
# Bash startup script for User Statistics Tracker (Linux/macOS)
# Usage: ./start-userstats.sh [config.json]

set -e

CONFIG_FILE="${1:-config.json}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_PATH="$SCRIPT_DIR/$CONFIG_FILE"
VENV_DIR="$SCRIPT_DIR/.venv"
LOG_FILE="$SCRIPT_DIR/userstats.log"
PID_FILE="$SCRIPT_DIR/userstats.pid"

# Logging functions
log_info() {
    echo -e "\033[0;32m[INFO]\033[0m $1"
}

log_warn() {
    echo -e "\033[0;33m[WARN]\033[0m $1"
}

log_error() {
    echo -e "\033[0;31m[ERROR]\033[0m $1"
}

# Check if already running
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        log_error "User Statistics Tracker is already running (PID: $PID)"
        exit 1
    else
        log_warn "Stale PID file found, removing..."
        rm -f "$PID_FILE"
    fi
fi

# Build config argument
if [ -f "$CONFIG_PATH" ]; then
    CONFIG_ARG="--config $CONFIG_PATH"
    log_info "Using config: $CONFIG_PATH"
else
    CONFIG_ARG=""
    log_info "Using default config paths"
fi

# Check for virtual environment
if [ ! -d "$VENV_DIR" ]; then
    log_info "Virtual environment not found, creating..."
    python3 -m venv "$VENV_DIR"
fi

# Activate virtual environment
log_info "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

# Install dependencies
log_info "Installing/updating dependencies..."
pip install --quiet --upgrade pip
pip install --quiet kryten-py

# Clear PYTHONPATH to avoid loading development versions
export PYTHONPATH=""

# Check NATS connectivity
NATS_HOST="localhost"
NATS_PORT=4222

if timeout 3 bash -c "</dev/tcp/$NATS_HOST/$NATS_PORT" 2>/dev/null; then
    log_info "NATS server reachable at $NATS_HOST:$NATS_PORT"
else
    log_warn "NATS server not reachable at $NATS_HOST:$NATS_PORT"
    log_warn "Tracker will attempt to connect anyway..."
fi

# Start the tracker
log_info "Starting User Statistics Tracker..."
log_info "Log file: $LOG_FILE"
log_info "Press Ctrl+C to stop"

cd "$SCRIPT_DIR"
python -m userstats $CONFIG_ARG 2>&1 | tee -a "$LOG_FILE"
