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
        self.prompts: list[tuple[str, str]] = []
        self.aborted: list[str] = []
        self._subscribers: set[asyncio.Queue[str]] = set()
        self._closing = False
        self._next_session = 0
        self._runner: web.AppRunner | None = None
        self.port: int = 0

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

    # --- routes ----------------------------------------------------------

    async def _health(self, _request: web.Request) -> web.Response:
        return web.json_response({"healthy": True})

    async def _list_sessions(self, request: web.Request) -> web.Response:
        return web.json_response(list(self.sessions.values()))

    async def _create_session(self, request: web.Request) -> web.Response:
        body = await request.json() if request.can_read_body else {}
        sid = self.add_session(body.get("title", ""))
        return web.json_response(self.sessions[sid])

    async def _get_messages(self, request: web.Request) -> web.Response:
        sid = request.match_info["sessionID"]
        return web.json_response(self.messages.get(sid, []))

    async def _prompt_async(self, request: web.Request) -> web.Response:
        sid = request.match_info["sessionID"]
        body = await request.json()
        text = "".join(p.get("text", "") for p in body.get("parts", []))
        self.prompts.append((sid, text))

        message_id = f"msg_fake{len(self.prompts):04d}"
        self.messages.setdefault(sid, []).append(
            {"info": {"id": message_id, "role": "user"}, "parts": [{"type": "text", "text": text}]}
        )

        # Echo the text back as deltas, then finish. Mirrors opencode's shape.
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
        self.emit({"type": "session.idle", "properties": {"sessionID": sid}})
        return web.Response(status=204)

    async def _abort(self, request: web.Request) -> web.Response:
        sid = request.match_info["sessionID"]
        self.aborted.append(sid)
        self.emit({"type": "session.idle", "properties": {"sessionID": sid}})
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
