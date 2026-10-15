"""The wire protocol between the phone and this bridge.

Two directions, both JSON text frames over WebSocket:

  phone -> bridge  {type: auth|list_sessions|create_session|attach|prompt|abort|ping|
                    select_model|select_agent|pick_project|reply_permission|reply_question, ...}
  bridge -> phone  {type: welcome|sessions|session|output_delta|part|message_done|status|
                    error|pong|providers|agents|projects|permission_prompt|question_prompt|
                    history|model, ...}

The bridge also subscribes to opencode's own event stream and translates the
subset of those events the phone needs. Everything the phone renders comes from
here, so this module is deliberately pure and side-effect free: it is the part
worth unit testing hardest.
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

# New: model/agent/project selection and permission/question replies.
SELECT_MODEL = "select_model"
SELECT_AGENT = "select_agent"
PICK_PROJECT = "pick_project"
REPLY_PERMISSION = "reply_permission"
REPLY_QUESTION = "reply_question"

# --- frames the bridge sends ------------------------------------------------

WELCOME = "welcome"
SESSIONS = "sessions"
SESSION = "session"
OUTPUT_DELTA = "output_delta"
PART = "part"
MESSAGE_DONE = "message_done"
STATUS = "status"
ERROR = "error"
PONG = "pong"

# Catalog data the phone needs to build its pickers.
PROVIDERS = "providers"
AGENTS = "agents"
PROJECTS = "projects"
MODEL = "model"

# Interactive prompts that opencode raises and the phone must answer.
PERMISSION_PROMPT = "permission_prompt"
QUESTION_PROMPT = "question_prompt"

# Structured transcript (replaces the old flat `lines` history).
HISTORY = "history"


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

# opencode's event types that carry streamed assistant text.
_OUTPUT_DELTA = "message.part.delta"
_PART_UPDATED = "message.part.updated"
_STATUS = "session.status"
_IDLE = "session.idle"

# Text that is streamed token by token as it is generated.
_STREAMED_PART_TYPES = {"text", "reasoning"}


def translate_opencode_event(event: dict[str, Any]) -> dict[str, Any] | None:
    """Convert one opencode SSE event into a bridge -> phone frame.

    Returns None for events the phone does not need. Pure function, no I/O.

    Text and reasoning parts stream live as deltas; every other part (tool
    calls, file uploads, snapshot diffs, errors) is forwarded whole the moment
    it is finalized so the phone can render it as a structured card.
    """
    event_type = event.get("type")
    properties = event.get("properties") or {}

    if event_type == _OUTPUT_DELTA:
        field = properties.get("field")
        delta = properties.get("delta")
        if field not in _STREAMED_PART_TYPES or not isinstance(delta, str) or not delta:
            return None
        return {
            "type": OUTPUT_DELTA,
            "sessionID": properties.get("sessionID"),
            "messageID": properties.get("messageID"),
            "delta": delta,
        }

    if event_type == _PART_UPDATED:
        part = properties.get("part") or {}
        ended = bool((part.get("time") or {}).get("end"))
        part_type = part.get("type")

        if part_type in _STREAMED_PART_TYPES:
            # A finished text/reasoning part only signals end-of-turn; its text
            # already arrived as deltas, so re-sending it would duplicate.
            if not ended:
                return None
            return {
                "type": MESSAGE_DONE,
                "sessionID": properties.get("sessionID"),
                "messageID": part.get("messageID"),
            }

        # Non-text parts are handed to the phone as whole objects to render.
        if not ended:
            return None
        return {
            "type": PART,
            "sessionID": properties.get("sessionID"),
            "messageID": part.get("messageID"),
            "part": part,
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

    # Push fast-path for interactive prompts. The poller below is the reliable
    # floor, but a prompt delivered over the stream is forwarded immediately.
    if isinstance(event_type, str) and event_type.startswith("permission."):
        return _prompt_frame(PERMISSION_PROMPT, properties)
    if isinstance(event_type, str) and event_type.startswith("question."):
        return _prompt_frame(QUESTION_PROMPT, properties)

    return None


def _prompt_frame(kind: str, properties: dict[str, Any]) -> dict[str, Any]:
    """Shape a pushed permission/question event into the matching phone frame."""
    tool = properties.get("tool") or {}
    frame: dict[str, Any] = {
        "type": kind,
        "requestID": properties.get("id") or properties.get("requestID"),
        "sessionID": properties.get("sessionID"),
        "permission" if kind == PERMISSION_PROMPT else "questions": (
            properties.get("permission") if kind == PERMISSION_PROMPT
            else properties.get("questions") or []
        ),
        "tool": {"callID": tool.get("callID"), "messageID": tool.get("messageID")},
    }
    return {k: v for k, v in frame.items() if v is not None}


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
        "agent": session.get("agent"),
    }


def history_frame(session_id: str, messages: list[dict[str, Any]]) -> dict[str, Any]:
    """Forward opencode message history as structured parts for the phone.

    opencode returns [{info, parts}] where parts carry the actual content. The
    phone renders richly (text, tool calls, diffs, errors), so the bridge hands
    each part through whole instead of flattening to plain lines.
    """
    rendered: list[dict[str, Any]] = []
    for entry in messages:
        info = entry.get("info") or {}
        parts = entry.get("parts") or []
        if not parts:
            continue
        rendered.append(
            {
                "role": info.get("role"),
                "parts": parts,
            }
        )
    return {
        "type": HISTORY,
        "sessionID": session_id,
        "messages": rendered,
    }


# --- catalog builders -------------------------------------------------------


def providers_frame(providers: list[dict[str, Any]]) -> dict[str, Any]:
    """Trim provider/model blobs down to what the model picker needs."""
    trimmed = []
    for provider in providers:
        models = []
        for model in (provider.get("models") or {}).values():
            models.append(
                {
                    "id": model.get("id"),
                    "name": model.get("name"),
                    "providerID": model.get("providerID") or provider.get("id"),
                    "cost": model.get("cost"),
                    "limit": model.get("limit"),
                }
            )
        trimmed.append(
            {
                "id": provider.get("id"),
                "name": provider.get("name"),
                "models": models,
            }
        )
    return {"type": PROVIDERS, "providers": trimmed}


def agents_frame(agents: list[dict[str, Any]]) -> dict[str, Any]:
    trimmed = [
        {"name": a.get("name"), "description": a.get("description")}
        for a in agents
    ]
    return {"type": AGENTS, "agents": trimmed}


def projects_frame(projects: list[dict[str, Any]]) -> dict[str, Any]:
    trimmed = [
        {
            "id": p.get("id"),
            "name": p.get("name") or p.get("id"),
            "worktree": p.get("worktree"),
            "vcs": p.get("vcs"),
            "icon": p.get("icon"),
        }
        for p in projects
    ]
    return {"type": PROJECTS, "projects": trimmed}


# --- prompt builders --------------------------------------------------------


def permission_prompt_frame(request: dict[str, Any]) -> dict[str, Any]:
    tool = request.get("tool") or {}
    return {
        "type": PERMISSION_PROMPT,
        "requestID": request.get("id"),
        "sessionID": request.get("sessionID"),
        "permission": request.get("permission"),
        "patterns": request.get("patterns") or [],
        "always": request.get("always") or [],
        "tool": {"callID": tool.get("callID"), "messageID": tool.get("messageID")},
    }


def question_prompt_frame(request: dict[str, Any]) -> dict[str, Any]:
    tool = request.get("tool") or {}
    questions = []
    for q in request.get("questions") or []:
        questions.append(
            {
                "header": q.get("header"),
                "question": q.get("question"),
                "multiple": q.get("multiple"),
                "options": [
                    {"label": for_option(o)} for o in (q.get("options") or [])
                ],
            }
        )
    return {
        "type": QUESTION_PROMPT,
        "requestID": request.get("id"),
        "sessionID": request.get("sessionID"),
        "questions": questions,
        "tool": {"callID": tool.get("callID"), "messageID": tool.get("messageID")},
    }


def for_option(option: dict[str, Any]) -> str:
    """A question option may carry more than just a label; the phone needs the label."""
    return option.get("label") or option.get("value") or ""


def model_frame(session_id: str, *, model_id: str, provider_id: str) -> dict[str, Any]:
    return {
        "type": MODEL,
        "sessionID": session_id,
        "modelID": model_id,
        "providerID": provider_id,
    }
