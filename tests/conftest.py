"""Shared fixtures.

`fake_opencode` is a real aiohttp HTTP server speaking the subset of opencode's
API that the bridge uses. The bridge under test talks to it over real sockets,
so these are integration tests rather than unit tests with mocked internals:
a bug in the bridge's HTTP handling, SSE parsing or WebSocket routing shows up
here, and no opencode install is needed.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import aiohttp
import pytest
import pytest_asyncio
from aiohttp import web

TOKEN = "test-token"


class FakeOpencode:
    """Minimal stand-in for `opencode serve`."""

    def __init__(self) -> None:
        self.sessions: dict[str, dict[str, Any]] = {}
        self.messages: dict[str, list[dict[str, Any]]] = {}
        self.prompts: list[dict[str, Any]] = []
        self.aborted: list[str] = []
        self._subscribers: set[asyncio.Queue[str]] = set()
        self._closing = False
        self._next_session = 0
        self._runner: web.AppRunner | None = None
        self.port: int = 0

        # Catalog data the bridge can serve to the phone.
        self.providers = [
            {
                "id": "openai",
                "name": "OpenAI",
                "models": {
                    "gpt-4o": {
                        "id": "gpt-4o",
                        "name": "GPT-4o",
                        "providerID": "openai",
                    }
                },
            }
        ]
        self.agents = [{"name": "build", "description": "default agent", "native": True}]
        self.projects = [
            {
                "id": "proj_1",
                "name": "marcuscassel.org",
                "worktree": "/srv/marcuscassel.org",
                "vcs": "git",
                "icon": {"url": "data:image/png;base64,ZmFrZQ=="},
            }
        ]
        # Pending interactive prompts the poller should surface.
        self.permissions: list[dict[str, Any]] = []
        self.questions: list[dict[str, Any]] = []

        # When True, the fake emits a finalized tool part in the prompt stream so
        # the bridge's part-forwarding can be exercised end to end.
        self.emit_tool = False

    # --- helpers used by tests -------------------------------------------

    def add_session(self, title: str, *, session_id: str | None = None) -> str:
        self._next_session += 1
        sid = session_id or f"ses_fake{self._next_session:04d}"
        self.sessions[sid] = {
            "id": sid,
            "title": title,
            "directory": "C:\\work",
            "time": {"created": 1000 + self._next_session, "updated": 2000},
            "model": {"id": "test-model", "providerID": "test-provider"},
        }
        self.messages.setdefault(sid, [])
        return sid

    def emit(self, event: dict[str, Any]) -> None:
        """Push an event to every open /event subscriber."""
        payload = json.dumps(event)
        for queue in list(self._subscribers):
            queue.put_nowait(payload)

    def add_permission(self, request: dict[str, Any]) -> None:
        self.permissions.append(request)

    def add_question(self, request: dict[str, Any]) -> None:
        self.questions.append(request)

    # --- routes ----------------------------------------------------------

    async def _health(self, _request: web.Request) -> web.Response:
        return web.json_response({"healthy": True})

    async def _list_sessions(self, request: web.Request) -> web.Response:
        return web.json_response(list(self.sessions.values()))

    async def _create_session(self, request: web.Request) -> web.Response:
        body = await request.json() if request.can_read_body else {}
        directory = request.query.get("directory")
        sid = self.add_session(body.get("title", ""))
        if directory:
            self.sessions[sid]["directory"] = directory
        return web.json_response(self.sessions[sid])

    async def _get_messages(self, request: web.Request) -> web.Response:
        sid = request.match_info["sessionID"]
        return web.json_response(self.messages.get(sid, []))

    async def _prompt_async(self, request: web.Request) -> web.Response:
        sid = request.match_info["sessionID"]
        body = await request.json()
        text = "".join(p.get("text", "") for p in body.get("parts", []))
        self.prompts.append(
            {
                "sid": sid,
                "text": text,
                "model": body.get("model"),
                "agent": body.get("agent"),
            }
        )

        message_id = f"msg_fake{len(self.prompts):04d}"
        self.messages.setdefault(sid, []).append(
            {"info": {"id": message_id, "role": "user"}, "parts": [{"type": "text", "text": text}]}
        )

        # Echo the text back as deltas, then finish, mirroring opencode's shape.
        self.emit(
            {
                "type": "message.updated",
                "properties": {
                    "sessionID": sid,
                    "info": {"id": message_id, "role": "assistant", "sessionID": sid},
                },
            }
        )
        for piece in text.split():
            self.emit(
                {
                    "type": "message.part.delta",
                    "properties": {
                        "sessionID": sid,
                        "messageID": message_id,
                        "field": "text",
                        "delta": piece + " ",
                    },
                }
            )
        self.emit(
            {
                "type": "message.part.updated",
                "properties": {
                    "sessionID": sid,
                    "part": {
                        "id": "prt_fake",
                        "type": "text",
                        "messageID": message_id,
                        "text": text,
                        "time": {"start": 1, "end": 2},
                    },
                },
            }
        )

        if self.emit_tool:
            self.emit(
                {
                    "type": "message.part.updated",
                    "properties": {
                        "sessionID": sid,
                        "part": {
                            "id": "prt_tool",
                            "type": "tool",
                            "messageID": message_id,
                            "callID": "call_fake",
                            "tool": "bash",
                            "state": {"result": "ran clean"},
                            "time": {"start": 1, "end": 2},
                        },
                    },
                }
            )

        self.emit({"type": "session.idle", "properties": {"sessionID": sid}})
        return web.Response(status=204)

    async def _abort(self, request: web.Request) -> web.Response:
        sid = request.match_info["sessionID"]
        self.aborted.append(sid)
        self.emit({"type": "session.idle", "properties": {"sessionID": sid}})
        return web.json_response(True)

    async def _list(self, data_attr: str, request: web.Request) -> web.Response:
        return web.json_response(getattr(self, data_attr))

    async def _reply_permission(self, request: web.Request) -> web.Response:
        request_id = request.match_info["requestID"]
        body = await request.json()
        self.permissions = [p for p in self.permissions if p.get("id") != request_id]
        return web.json_response(True)

    async def _reply_question(self, request: web.Request) -> web.Response:
        request_id = request.match_info["requestID"]
        body = await request.json()
        self.questions = [q for q in self.questions if q.get("id") != request_id]
        return web.json_response(True)

    async def _events(self, request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(
            status=200, headers={"Content-Type": "text/event-stream"}
        )
        await response.prepare(request)
        queue: asyncio.Queue[str | None] = asyncio.Queue()
        self._subscribers.add(queue)
        try:
            while True:
                # A None sentinel ends the stream at shutdown. Without this the
                # handler blocks forever on queue.get() and AppRunner.cleanup()
                # waits out its full shutdown timeout.
                payload = await queue.get()
                if payload is None:
                    break
                await response.write(f"data: {payload}\n\n".encode())
        except (asyncio.CancelledError, ConnectionResetError):
            pass
        finally:
            self._subscribers.discard(queue)
        return response

    # --- lifecycle -------------------------------------------------------

    async def start(self) -> str:
        app = web.Application()
        app.router.add_get("/global/health", self._health)
        app.router.add_get("/session", self._list_sessions)
        app.router.add_post("/session", self._create_session)
        app.router.add_get("/session/{sessionID}/message", self._get_messages)
        app.router.add_post("/session/{sessionID}/prompt_async", self._prompt_async)
        app.router.add_post("/session/{sessionID}/abort", self._abort)
        app.router.add_get("/event", self._events)
        app.router.add_get("/provider", lambda r: self._list("providers", r))
        app.router.add_get("/agent", lambda r: self._list("agents", r))
        app.router.add_get("/project", lambda r: self._list("projects", r))
        app.router.add_get("/permission", lambda r: self._list("permissions", r))
        app.router.add_get("/question", lambda r: self._list("questions", r))
        app.router.add_post("/permission/{requestID}/reply", self._reply_permission)
        app.router.add_post("/question/{requestID}/reply", self._reply_question)

        self._runner = web.AppRunner(app, shutdown_timeout=1.0)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        self.port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
        return f"http://127.0.0.1:{self.port}"

    async def stop(self) -> None:
        if self._runner is None:
            return
        # End the SSE handlers before cleanup, or it blocks for shutdown_timeout.
        self._closing = True
        for queue in list(self._subscribers):
            queue.put_nowait(None)
        self._subscribers.clear()
        await self._runner.cleanup()
        self._runner = None


@pytest_asyncio.fixture
async def fake_opencode():
    server = FakeOpencode()
    url = await server.start()
    try:
        yield server, url
    finally:
        await server.stop()
