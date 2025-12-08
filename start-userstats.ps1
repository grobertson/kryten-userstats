# PowerShell startup script for User Statistics Tracker (Windows)
# Usage: .\start-userstats.ps1 [-ConfigFile config.json]

param(
    [string]$ConfigFile = "config.json",
    [switch]$Background
)

$ErrorActionPreference = "Stop"

# Configuration
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ConfigPath = Join-Path $ScriptDir $ConfigFile
$VenvDir = Join-Path $ScriptDir ".venv"
$LogFile = Join-Path $ScriptDir "userstats.log"
$PidFile = Join-Path $ScriptDir "userstats.pid"

# Logging functions
function Write-Info {
    param([string]$Message)
    Write-Host "[INFO] $Message" -ForegroundColor Green
}

function Write-Warn {
    param([string]$Message)
    Write-Host "[WARN] $Message" -ForegroundColor Yellow
}

function Write-Error-Custom {
    param([string]$Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red
}

# Check if already running
if (Test-Path $PidFile) {
    $Pid = Get-Content $PidFile
    if (Get-Process -Id $Pid -ErrorAction SilentlyContinue) {
        Write-Error-Custom "User Statistics Tracker is already running (PID: $Pid)"
        exit 1
    } else {
        Write-Warn "Stale PID file found, removing..."
        Remove-Item $PidFile -Force
    }
}

# Check if config file exists
if (-not (Test-Path $ConfigPath)) {
    Write-Error-Custom "Configuration file not found: $ConfigPath"
    Write-Info "Copy config.example.json to config.json and configure it"
    exit 1
}

# Check for virtual environment
if (-not (Test-Path $VenvDir)) {
    Write-Info "Virtual environment not found, creating..."
    python -m venv $VenvDir
    if ($LASTEXITCODE -ne 0) {
        Write-Error-Custom "Failed to create virtual environment"
        exit 1
    }
}

# Activate virtual environment and install dependencies
Write-Info "Activating virtual environment..."
& "$VenvDir\Scripts\Activate.ps1"

Write-Info "Installing/updating dependencies..."
pip install --quiet --upgrade pip
pip install --quiet kryten-py

if ($LASTEXITCODE -ne 0) {
    Write-Error-Custom "Failed to install dependencies"
    exit 1
}

# Clear PYTHONPATH to avoid loading development versions
$env:PYTHONPATH = ""

# Check NATS connectivity (optional)
$NatsHost = "localhost"
$NatsPort = 4222

try {
    $TcpClient = New-Object System.Net.Sockets.TcpClient
    $Connect = $TcpClient.BeginConnect($NatsHost, $NatsPort, $null, $null)
    $Wait = $Connect.AsyncWaitHandle.WaitOne(3000, $false)
    
    if ($Wait) {
        $TcpClient.EndConnect($Connect)
        Write-Info "NATS server reachable at ${NatsHost}:${NatsPort}"
        $TcpClient.Close()
    } else {
        Write-Warn "NATS server not reachable at ${NatsHost}:${NatsPort}"
        Write-Warn "Tracker will attempt to connect anyway..."
    }
} catch {
    Write-Warn "Could not check NATS connectivity: $_"
}

# Start the tracker
Write-Info "Starting User Statistics Tracker..."
Write-Info "Config: $ConfigPath"
Write-Info "Log file: $LogFile"

Push-Location $ScriptDir

if ($Background) {
    # Start in background
    $Process = Start-Process -FilePath "python" `
        -ArgumentList "-m", "userstats.main", "--config", $ConfigPath `
        -RedirectStandardOutput $LogFile `
        -RedirectStandardError $LogFile `
        -NoNewWindow `
        -PassThru
    
    $Process.Id | Out-File $PidFile -Encoding ASCII
    
    Start-Sleep -Seconds 2
    
    if (Get-Process -Id $Process.Id -ErrorAction SilentlyContinue) {
        Write-Info "User Statistics Tracker started successfully (PID: $($Process.Id))"
        Write-Info "Monitor logs with: Get-Content -Path $LogFile -Wait"
        Write-Info "Stop with: Stop-Process -Id $($Process.Id)"
    } else {
        Write-Error-Custom "Tracker failed to start. Check $LogFile for details"
        Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
        exit 1
    }
} else {
    # Run in foreground
    Write-Info "Running in foreground (Ctrl+C to stop)..."
    python -m userstats.main --config $ConfigPath
}

Pop-Location
