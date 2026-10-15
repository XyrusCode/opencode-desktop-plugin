"""WebSocket front end. Phones connect here; opencode is reached via OpencodeClient.

Connection lifecycle:
    connect -> auth frame -> welcome -> commands until disconnect

Only authenticated sockets receive opencode events. The opencode event stream
is a single shared subscription; every client gets a filtered copy of it.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed

from . import protocol as p
from .opencode_client import OpencodeClient, OpencodeError

log = logging.getLogger(__name__)

AUTH_TIMEOUT_SECONDS = 10.0
# opencode sends a heartbeat; these are noise once translated to phone frames.
_IGNORED_EVENT_TYPES = {"server.connected", "server.heartbeat"}


class BridgeServer:
    """Serves the phone-facing WebSocket API."""

    def __init__(self, client: OpencodeClient, *, auth_token: str) -> None:
        self._client = client
        self._auth_token = auth_token
        self._clients: set[Any] = set()
        self._attached: dict[Any, str] = {}  # socket -> session id
        self._pump: asyncio.Task[None] | None = None
        self._server: Any = None

    # --- lifecycle -------------------------------------------------------

    async def serve(self, host: str, port: int) -> None:
        """Start listening and run until cancelled."""
        async with websockets.serve(self._handle, host, port):
            log.info("bridge listening on ws://%s:%d", host, port)
            self._pump = asyncio.create_task(self._pump_events())
            try:
                await asyncio.Future()
            finally:
                await self.shutdown()

    async def shutdown(self) -> None:
        if self._pump is not None:
            self._pump.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._pump
            self._pump = None
        for socket in list(self._clients):
            with contextlib.suppress(Exception):
                await socket.close()

    # --- outbound --------------------------------------------------------

    async def _send(self, socket: Any, frame: dict[str, Any]) -> None:
        try:
            await socket.send(p.encode(frame))
        except ConnectionClosed:
            self._clients.discard(socket)
            self._attached.pop(socket, None)

    async def _broadcast(self, frame: dict[str, Any]) -> None:
        """Fan a frame out to every authenticated client."""
        dead: list[Any] = []
        for socket in list(self._clients):
            try:
                await socket.send(p.encode(frame))
            except ConnectionClosed:
                dead.append(socket)
        for socket in dead:
            self._clients.discard(socket)
            self._attached.pop(socket, None)

    # --- inbound ---------------------------------------------------------

    async def _handle(self, socket: Any) -> None:
        peer = getattr(socket, "remote_address", None)
        log.info("phone connected: %s", peer)

        try:
            raw = await asyncio.wait_for(socket.recv(), timeout=AUTH_TIMEOUT_SECONDS)
        except (asyncio.TimeoutError, ConnectionClosed):
            log.warning("dropping %s: no auth within timeout", peer)
            return

        try:
            frame = p.decode(raw)
            token = p.require(frame, "token")
        except p.ProtocolError as exc:
            await self._send(socket, p.error(str(exc), fatal=True))
            await socket.close(code=1003)
            return

        if token != self._auth_token:
            log.warning("auth rejected for %s", peer)
            await self._send(socket, p.error("authentication failed", fatal=True))
            await socket.close(code=1008)
            return

        self._clients.add(socket)
        log.info("auth ok for %s", peer)
        await self._send(socket, {"type": p.WELCOME, "message": "authenticated"})

        try:
            async for raw in socket:
                await self._dispatch(socket, raw)
        except ConnectionClosed:
            pass
        finally:
            self._clients.discard(socket)
            self._attached.pop(socket, None)
            log.info("phone disconnected: %s", peer)

    async def _dispatch(self, socket: Any, raw: str) -> None:
        try:
            frame = p.decode(raw)
        except p.ProtocolError as exc:
            await self._send(socket, p.error(str(exc)))
            return

        kind = frame["type"]
        try:
            if kind == p.PING:
                await self._send(socket, {"type": p.PONG})
            elif kind == p.LIST_SESSIONS:
                sessions = await self._client.list_sessions()
                await self._send(
                    socket,
                    {
                        "type": p.SESSIONS,
                        "kind": "list",
                        "sessions": [p.session_summary(s) for s in sessions],
                    },
                )
            elif kind == p.CREATE_SESSION:
                title = frame.get("title") or ""
                session = await self._client.create_session(title=title)
                self._attached[socket] = session["id"]
                await self._send(
                    socket,
                    {
                        "type": p.SESSION,
                        "kind": "created",
                        "session": p.session_summary(session),
                    },
                )
            elif kind == p.ATTACH:
                session_id = p.require(frame, "sessionID")
                messages = await self._client.get_messages(session_id)
                self._attached[socket] = session_id
                await self._send(socket, p.history_frame(session_id, messages))
                await self._send(
                    socket,
                    {"type": p.SESSION, "kind": "attached", "sessionID": session_id},
                )
            elif kind == p.PROMPT:
                text = p.require(frame, "text")
                session_id = frame.get("sessionID") or self._attached.get(socket)
                if not session_id:
                    await self._send(
                        socket, p.error("attach to a session before sending a prompt")
                    )
                    return
                self._attached[socket] = session_id
                await self._client.prompt(session_id, text)
            elif kind == p.ABORT:
                session_id = frame.get("sessionID") or self._attached.get(socket)
                if not session_id:
                    await self._send(socket, p.error("no session to abort"))
                    return
                await self._client.abort(session_id)
            else:
                await self._send(socket, p.error(f"unsupported frame type {kind!r}"))
        except p.ProtocolError as exc:
            await self._send(socket, p.error(str(exc)))
        except OpencodeError as exc:
            await self._send(socket, p.error(str(exc)))
        except Exception as exc:  # noqa: BLE001 - never let one frame kill the socket
            log.exception("dispatch failed for %s", kind)
            await self._send(socket, p.error(f"internal error: {exc}"))

    # --- opencode event pump ---------------------------------------------

    async def _pump_events(self) -> None:
        """Translate opencode's event stream into phone frames, forever.

        Reconnects on failure: opencode can restart underneath us, and the
        phone should recover without re-pairing.
        """
        backoff = 1.0
        while True:
            try:
                async for event in self._client.events():
                    backoff = 1.0
                    event_type = event.get("type")
                    if event_type in _IGNORED_EVENT_TYPES:
                        continue
                    translated = p.translate_opencode_event(event)
                    if translated is not None:
                        await self._broadcast(translated)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("opencode event stream dropped (%s); retry in %.0fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
