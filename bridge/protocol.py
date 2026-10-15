"""The wire protocol between the phone and this bridge.

Two directions, both JSON text frames over WebSocket:

  phone -> bridge  {type: auth|list_sessions|create_session|attach|prompt|abort|ping, ...}
  bridge -> phone  {type: welcome|sessions|session|output_delta|message_done|status|error|pong, ...}

The bridge also subscribes to opencode's own event stream and translates a small
subset of those events into `output_delta` frames. Everything the phone renders
comes from here, so this module is deliberately pure and side-effect free: it is
the part worth unit testing hardest.
"""

from __future__ import annotations

import json
from typing import Any

# --- frames the phone sends -------------------------------------------------

AUTH = "auth"
LIST_SESSIONS = "list_sessions"
CREATE_SESSION = "create_session"
ATTACH = "attach"
PROMPT = "prompt"
ABORT = "abort"
PING = "ping"

# --- frames the bridge sends ------------------------------------------------

WELCOME = "welcome"
SESSIONS = "sessions"
SESSION = "session"
OUTPUT_DELTA = "output_delta"
MESSAGE_DONE = "message_done"
STATUS = "status"
ERROR = "error"
PONG = "pong"


class ProtocolError(ValueError):
    """Raised when a frame from the phone cannot be understood."""


def encode(frame: dict[str, Any]) -> str:
    """Serialise a bridge -> phone frame."""
    return json.dumps(frame, separators=(",", ":"))


def decode(raw: str) -> dict[str, Any]:
    """Parse a phone -> bridge frame.

    Raises ProtocolError on anything that is not a JSON object, so the caller
    can answer with an error frame instead of dropping the connection.
    """
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise ProtocolError(f"invalid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ProtocolError("frame must be a JSON object")
    frame_type = parsed.get("type")
    if not isinstance(frame_type, str) or not frame_type:
        raise ProtocolError("frame is missing a string 'type'")
    return parsed


def require(frame: dict[str, Any], key: str) -> Any:
    """Pull a required field out of a frame, or raise ProtocolError."""
    value = frame.get(key)
    if value is None or value == "":
        raise ProtocolError(f"frame is missing required field {key!r}")
    return value


def error(message: str, *, fatal: bool = False) -> dict[str, Any]:
    """Build an error frame."""
    return {"type": ERROR, "message": message, "fatal": fatal}


# --- opencode event translation --------------------------------------------

# opencode's event types we care about. Everything else on its stream is noise
# from the phone's point of view (tool calls, LSP diagnostics, diffs) and is
# dropped rather than forwarded.
_OUTPUT_DELTA = "message.part.delta"
_PART_UPDATED = "message.part.updated"
_STATUS = "session.status"
_IDLE = "session.idle"

# Only assistant text is streamed back. Tool-call parts would otherwise dump
# raw JSON into the phone's transcript.
_STREAMED_PART_TYPES = {"text", "reasoning"}


def translate_opencode_event(event: dict[str, Any]) -> dict[str, Any] | None:
    """Convert one opencode SSE event into a bridge -> phone frame.

    Returns None for events the phone does not need. Pure function, no I/O.
    """
    event_type = event.get("type")
    properties = event.get("properties") or {}

    if event_type == _OUTPUT_DELTA:
        field = properties.get("field")
        delta = properties.get("delta")
        if field != "text" or not isinstance(delta, str) or not delta:
            return None
        return {
            "type": OUTPUT_DELTA,
            "sessionID": properties.get("sessionID"),
            "messageID": properties.get("messageID"),
            "delta": delta,
        }

    if event_type == _PART_UPDATED:
        part = properties.get("part") or {}
        # A finished text part is the authoritative version of the stream we
        # already forwarded as deltas. Re-sending it would duplicate the text,
        # so only the completion signal is useful.
        if part.get("type") not in _STREAMED_PART_TYPES:
            return None
        if not part.get("time", {}).get("end"):
            return None
        return {
            "type": MESSAGE_DONE,
            "sessionID": properties.get("sessionID"),
            "messageID": part.get("messageID"),
        }

    if event_type == _STATUS:
        status = properties.get("status") or {}
        return {
            "type": STATUS,
            "sessionID": properties.get("sessionID"),
            "status": status.get("type", "unknown"),
        }

    if event_type == _IDLE:
        return {
            "type": MESSAGE_DONE,
            "sessionID": properties.get("sessionID"),
            "messageID": None,
            "reason": "idle",
        }

    return None


def session_summary(session: dict[str, Any]) -> dict[str, Any]:
    """Trim a full opencode session object down to what the phone renders."""
    model = session.get("model") or {}
    return {
        "id": session.get("id"),
        "title": session.get("title") or "(untitled)",
        "directory": session.get("directory"),
        "updated": (session.get("time") or {}).get("updated"),
        "model": model.get("id"),
        "provider": model.get("providerID"),
    }


def history_frame(session_id: str, messages: list[dict[str, Any]]) -> dict[str, Any]:
    """Flatten opencode message history into transcript lines for the phone.

    opencode returns [{info, parts}] where parts carry the actual text. The
    phone shows a flat list of lines, so roles are prefixed inline.
    """
    lines: list[str] = []
    for entry in messages:
        info = entry.get("info") or {}
        role = info.get("role")
        text = "".join(
            part.get("text", "")
            for part in entry.get("parts") or []
            if part.get("type") in _STREAMED_PART_TYPES and part.get("text")
        )
        if not text:
            continue
        prefix = "you" if role == "user" else "opencode"
        lines.append(f"{prefix}: {text.strip()}")
    return {
        "type": SESSIONS,
        "kind": "history",
        "sessionID": session_id,
        "lines": lines,
    }
