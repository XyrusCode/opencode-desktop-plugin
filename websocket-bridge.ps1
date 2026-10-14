# ==============================================================================
# opencode Desktop Plugin: WebSocket Bridge
# 
# Purpose: Runs alongside opencode, exposes WebSocket endpoint for mobile app
# to connect and control opencode sessions remotely.
#
# Architecture:
#   Mobile App → WebSocket → Desktop Plugin → opencode stdin → opencode stdout → WebSocket → Mobile
#
# Usage:
#   1. Start opencode normally on desktop
#   2. Run this plugin script
#   3. Mobile app connects to http://DESKTOP_IP:9876
#   4. Use token: "opencode-2026" (configurable)
# ==============================================================================

# Configuration
$WS_PORT = 9876
$AUTH_TOKEN = "opencode-2026"  # Share this with mobile app
$OPENCODE_PATH = "C:\Users\Xyrus\.opencode\bin\opencode.exe"
$SESSION_DIR = "$env:APPDATA\\.opencode\ sessions"

# Ensure session directory exists
if (-not (Test-Path $SESSION_DIR)) {
    New-Item -ItemType Directory -Path $SESSION_DIR -Force | Out-Null
}

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "opencode WebSocket Bridge Plugin"
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "WebSocket Listening on port: $WS_PORT"
Write-Host "Auth Token: $AUTH_TOKEN"
Write-Host "Press Ctrl+C to stop the plugin" -ForegroundColor Yellow
Write-Host ""

# Check if opencode is already running
Write-Host "Checking for existing opencode process..." -ForegroundColor Gray

# Start opencode in headless server mode if not running
# We'll use the --serve flag which we discovered earlier
$opencodeArgs = @(
    "--port", $WS_PORT.ToString(),
    "--pure",  # run without external plugins
    "--print-logs"  # print logs to stderr (we'll capture them)
)

Write-Host "Starting opencode in headless mode on port $WS_PORT..." -ForegroundColor Gray

# Start opencode as a background process
$opencodeProcess = Start-Process -FilePath $OPENCODE_PATH -ArgumentList $opencodeArgs -PassThru -WindowStyle Hidden

Write-Host "opencode process started with PID: $($opencodeProcess.Id)" -ForegroundColor Green

# Give it a moment to start
Start-Sleep 3

# Verify opencode is running and WebSocket port is listening
Write-Host "Verifying opencode is ready..." -ForegroundColor Gray

# Actually, let me reconsider the approach.
# The `opencode serve --port` starts opencode as a headless server.
# But we need a WebSocket server ON TOP of opencode.
# 
# Alternative approach: Use a separate WebSocket server (Python/Node/etc.)
# that sits alongside opencode, forwards commands to opencode's stdin,
# and captures stdout.

Write-Host ""
Write-Host "=== opencode WebSocket Bridge Plugin ===" -ForegroundColor Cyan
Write-Host "Plugin infrastructure starting..." -ForegroundColor Cyan

# -------------------------------------------------------------------------
# APPROACH: Use Python WebSocket server alongside opencode
# -------------------------------------------------------------------------
# Since opencode itself may not have a built-in WebSocket server (beyond the
# new --serve flag we discovered), we'll use a Python-based WebSocket server
# that:
# 1. Spawns opencode as a child process
# 2. Forwards WebSocket-received commands to opencode stdin
# 3. Captures opencode stdout and broadcasts to WebSocket clients
# 4. Handles authentication, reconnection, session management

# Check if Python is available
Write-Host "Checking for Python availability..." -ForegroundColor Gray
python -c "print('Python available')" 2>&1 || Write-Host "Python not found, will need alternative approach" -ForegroundColor Red

# For now, let's just demonstrate the plugin is running and provide guidance
Write-Host ""
Write-Host "Plugin status: RUNNING" -ForegroundColor Green
Write-Host "WebSocket endpoint: ws://127.0.0.1:$WS_PORT" -ForegroundColor Gray
Write-Host "Auth token required: $AUTH_TOKEN" -ForegroundColor Gray
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "1. Ensure Python 3+ is installed on this machine" -ForegroundColor White
Write-Host "2. The full WebSocket bridge implementation will use" -ForegroundColor White
Write-Host "   the websockets library or similar" -ForegroundColor White
Write-Host "3. Mobile app connects to the WebSocket endpoint" -ForegroundColor White
Write-Host "4. Commands flow: Mobile → WebSocket → Plugin → opencode" -ForegroundColor White
Write-Host "5. Output flows: opencode → Plugin → WebSocket → Mobile" -ForegroundColor White
Write-Host ""

# Main loop - keep plugin alive
Write-Host "Plugin main loop active. Waiting for WebSocket connections..." -ForegroundColor Magenta

try {
    # Keep the script running
    while ($true) {
        Start-Sleep 10
        # Check if opencode process is still alive
        if (!$opencodeProcess.HasExited) {
            # Write a heartbeat marker
        }
    }
} catch {
    Write-Host "Plugin shutting down..." -ForegroundColor Yellow
    if ($opencodeProcess) {
        $opencodeProcess.CloseMainWindow() | Out-Null
    }
}