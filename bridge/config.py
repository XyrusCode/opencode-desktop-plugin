"""Runtime configuration, sourced from the environment.

Every value has a default that works on a developer machine; CI and the
packaged launcher override via environment variables so nothing is hardcoded.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_AUTH_TOKEN = "opencode-2026-mobile-companion"
DEFAULT_WS_PORT = 9876
DEFAULT_OPENCODE_PORT = 4096


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Config:
    """Everything the bridge needs to run."""

    ws_port: int = DEFAULT_WS_PORT
    """Port the phone connects to."""

    ws_host: str = "0.0.0.0"
    """Bind address for the WebSocket server. 0.0.0.0 so the phone can reach it."""

    auth_token: str = DEFAULT_AUTH_TOKEN
    """Shared secret the phone must present before it can do anything."""

    opencode_url: str = ""
    """Base URL of an already-running opencode server. Empty means spawn one."""

    opencode_host: str = "127.0.0.1"
    """Bind address for the opencode server we spawn. Loopback only: the phone
    never talks to opencode directly, it goes through this bridge."""

    opencode_port: int = DEFAULT_OPENCODE_PORT
    """Port for the opencode server we spawn."""

    opencode_command: str = "opencode"
    """Executable to spawn. Overridable so tests can inject a stub."""

    workdir: str = ""
    """Project directory opencode operates in. Empty means the process cwd."""

    spawn_opencode: bool = True
    """Whether to start opencode ourselves, or attach to an existing server."""

    opencode_token: str = ""
    """Bearer token for opencode, if OPENCODE_SERVER_PASSWORD is set on it."""

    @property
    def resolved_opencode_url(self) -> str:
        if self.opencode_url:
            return self.opencode_url.rstrip("/")
        return f"http://{self.opencode_host}:{self.opencode_port}"

    @property
    def resolved_workdir(self) -> str:
        return self.workdir or os.getcwd()


def load_config() -> Config:
    """Build a Config from the current environment."""
    return Config(
        ws_port=_env_int("BRIDGE_WS_PORT", DEFAULT_WS_PORT),
        ws_host=os.environ.get("BRIDGE_WS_HOST", "0.0.0.0"),
        auth_token=os.environ.get("BRIDGE_AUTH_TOKEN", DEFAULT_AUTH_TOKEN),
        opencode_url=os.environ.get("BRIDGE_OPENCODE_URL", ""),
        opencode_host=os.environ.get("BRIDGE_OPENCODE_HOST", "127.0.0.1"),
        opencode_port=_env_int("BRIDGE_OPENCODE_PORT", DEFAULT_OPENCODE_PORT),
        opencode_command=os.environ.get("BRIDGE_OPENCODE_COMMAND", "opencode"),
        workdir=os.environ.get("BRIDGE_WORKDIR", ""),
        spawn_opencode=_env_flag("BRIDGE_SPAWN_OPENCODE", True),
        opencode_token=os.environ.get("BRIDGE_OPENCODE_TOKEN", ""),
    )
