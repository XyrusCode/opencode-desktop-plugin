#!/usr/bin/env python3
"""
opencode WebSocket Bridge Server

Connects WebSocket clients (mobile app) to opencode process.
Commands flow: Mobile → WebSocket → This Server → opencode stdin
Output flow: opencode stdout → This Server → WebSocket → Mobile

Features:
- Authentication via token
- Command forwarding to opencode
- Real-time output broadcasting
- Session management
- Auto-reconnect support
"""

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from typing import Optional, Set

import websockets
import websockets.server  # deprecated but works for now

# Configuration
WS_PORT = 9876
AUTH_TOKEN = "opencode-2026-mobile-companion"
OPENCODE_PATH = r"C:\Users\Xyrus\.opencode\bin\opencode.exe"
SESSION_DIR = os.path.join(os.environ.get("APPDATA", "$HOME"), ".opencode", "sessions")

# Ensure session directory exists
os.makedirs(SESSION_DIR, exist_ok=True)

# Global state - use string type to avoid deprecation warning
ws_clients: set = set()
shutdown_event = asyncio.Event()


def log_message(msg: str, level: str = "INFO"):
    """Simple logging helper."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] [{level}] {msg}")


def send_command_to_opencode(command: str) -> bool:
    """Send a command to opencode's stdin."""
    global opencode_process
    
    if opencode_process and opencode_process.stdin:
        try:
            # opencode commands typically end with newline
            opencode_process.stdin.write((command + "\n").encode("utf-8"))
            opencode_process.stdin.flush()
            return True
        except Exception as e:
            log_message(f"Error sending command to opencode: {e}", "ERROR")
            return False
    return False


async def broadcast_to_clients(message: str, exclude_client=None):
    """Broadcast a message to all connected WebSocket clients."""
    disconnected = set()
    
    for client in list(ws_clients):
        if client != exclude_client:
            try:
                await client.send(message)
            except Exception:
                disconnected.add(client)
    
    # Remove disconnected clients
    ws_clients.difference_update(disconnected)


async def handle_opencode_output():
    """Background task to read opencode stdout and broadcast to WebSocket clients."""
    global opencode_process
    
    if not opencode_process or not opencode_process.stdout:
        return
    
    try:
        while not shutdown_event.is_set():
            # Read a line from opencode stdout
            line = await asyncio.wait_for(
                opencode_process.stdout.readline(), timeout=0.1
            )
            
            if not line:
                break
            
            line_str = line.decode("utf-8", errors="replace").strip()
            if not line_str:
                continue
            
            # Broadcast to WebSocket clients
            # Format: [opencode] <message>
            message = f"[opencode] {line_str}"
            await broadcast_to_clients(message)
            
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log_message(f"Error reading opencode output: {e}", "ERROR")
    finally:
        log_message("opencode output reader stopped")


async def handle_websocket(websocket):
    """Handle a single WebSocket connection."""
    global ws_clients
    
    log_message(f"New WebSocket connection from {websocket.remote_address}")
    ws_clients.add(websocket)
    
    try:
        # Wait for authentication
        auth_message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
        
        try:
            auth_data = json.loads(auth_message)
        except json.JSONDecodeError:
            await websocket.close(code=1003, reason="Invalid JSON authentication")
            ws_clients.discard(websocket)
            return
        
        # Verify auth token
        if auth_data.get("token") != AUTH_TOKEN:
            log_message(f"Auth failed from {websocket.remote_address}: wrong token")
            await websocket.close(code=1008, reason="Authentication failed")
            ws_clients.discard(websocket)
            return
        
        log_message(f"Auth succeeded from {websocket.remote_address}")
        
        # Send welcome message
        await websocket.send(json.dumps({
            "type": "welcome",
            "message": "Authenticated successfully",
            "server_time": datetime.now().isoformat()
        }))
        
        # Handle messages from this client
        async for message in websocket:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                await websocket.send(json.dumps({
                    "type": "error",
                    "message": "Invalid JSON format"
                }))
                continue
            
            # Handle different message types
            msg_type = data.get("type", "")
            
            if msg_type == "command":
                # Forward command to opencode
                command = data.get("command", "")
                if command:
                    if send_command_to_opencode(command):
                        await websocket.send(json.dumps({
                            "type": "command_sent",
                            "command": command,
                            "status": "forwarded"
                        }))
                    else:
                        await websocket.send(json.dumps({
                            "type": "error",
                            "message": "Failed to forward command to opencode"
                        }))
            
            elif msg_type == "get_session":
                # Note session state available
                await websocket.send(json.dumps({
                    "type": "info",
                    "message": "Session state request received - use opencode export feature"
                }))
            
            elif msg_type == "ping":
                # Keep-alive ping
                await websocket.send(json.dumps({"type": "pong"}))
            
            elif msg_type == "logout":
                # Client disconnecting
                log_message(f"Client {websocket.remote_address} logging out")
                break
        
    except asyncio.TimeoutError:
        log_message(f"Auth timeout from {websocket.remote_address}")
    except websockets.exceptions.ConnectionClosed:
        log_message(f"Connection closed from {websocket.remote_address}")
    except Exception as e:
        log_message(f"WebSocket error from {websocket.remote_address}: {e}")
    finally:
        ws_clients.discard(websocket)
        await broadcast_to_clients(json.dumps({
            "type": "system",
            "message": f"User disconnected from {websocket.remote_address}"
        }))
        log_message(f"WebSocket connection removed: {websocket.remote_address}")


def start_opencode_process():
    """Start opencode as a subprocess with stdin/stdout captured."""
    global opencode_process
    
    cmd = [
        OPENCODE_PATH,
        "--pure",        # run without external plugins
        "--print-logs"  # print logs to stderr (for debugging)
    ]
    
    log_message(f"Starting opencode process: {' '.join(cmd)}")
    
    try:
        opencode_process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NEW_CONSOLE if sys.platform == "win32" else 0,
            env={**os.environ, "OPENCODE_BRIDGE": "1"}
        )
        
        # Give opencode time to initialize
        log_message(f"opencode started with PID: {opencode_process.PID}")
        log_message("Waiting 5 seconds for opencode to initialize...")
        
        # Check if process is still running
        time.sleep(5)
        
        if opencode_process.poll() is not None:
            # Process exited - read error output
            stderr = opencode_process.stderr.read(65536).decode("utf-8", errors="replace")
            log_message(f"opencode exited unexpectedly!", "ERROR")
            log_message(f"stderr: {stderr[:1000]}", "ERROR")
            opencode_process = None
            return False
        else:
            log_message("opencode is running and ready")
            return True
            
    except Exception as e:
        log_message(f"Error starting opencode: {e}", "ERROR")
        opencode_process = None
        return False


async def main():
    global opencode_process, ws_clients, shutdown_event
    
    log_message("=" * 60)
    log_message("opencode WebSocket Bridge Server")
    log_message("=" * 60)
    log_message(f"Auth Token: {AUTH_TOKEN}")
    log_message(f"WebSocket Port: {WS_PORT}")
    log_message(f"opencode Path: {OPENCODE_PATH}")
    log_message("")
    
    # Step 1: Ensure opencode is running
    log_message("Starting opencode process...")
    if not start_opencode_process():
        log_message("WARNING: Could not start opencode, but continuing WebSocket server", "WARNING")
    
    # Step 2: Start WebSocket server
    log_message(f"Starting WebSocket server on port {WS_PORT}...")
    
    try:
        async with websockets.serve(handle_websocket, "0.0.0.0", WS_PORT):
            log_message("WebSocket server started successfully!")
            
            # Start the opencode output reader task
            output_task = asyncio.create_task(handle_opencode_output())
            
            # Wait for shutdown
            try:
                await shutdown_event.wait()
            except KeyboardInterrupt:
                log_message("Received KeyboardInterrupt...")
            
            # Clean shutdown
            log_message("Shutting down...")
            shutdown_event.set()
            output_task.cancel()
            
            # Close WebSocket server
            ws_clients.clear()
            
            # Terminate opencode process
            if opencode_process and opencode_process.poll() is None:
                opencode_process.terminate()
                try:
                    opencode_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    opencode_process.kill()
                    
    except Exception as e:
        log_message(f"Fatal error: {e}", "ERROR")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log_message("Plugin terminated by user")
    except Exception as e:
        log_message(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()