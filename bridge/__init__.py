"""opencode <-> phone bridge.

Run `python -m bridge` to start everything: it spawns `opencode serve` on
loopback, waits for it to be ready, then serves the phone-facing WebSocket API.
"""

from .config import Config, load_config
from .opencode_client import OpencodeClient, OpencodeError
from .server import BridgeServer

__all__ = ["Config", "load_config", "OpencodeClient", "OpencodeError", "BridgeServer"]
