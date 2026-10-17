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
# How often the bridge polls opencode for pending permission/question prompts.
# A poll is the reliable floor; push over the event stream is a fast path.
PROMPT_POLL_SECONDS = 2.0
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
        self._poll: asyncio.Task[None] | None = None
        self._server: Any = None
        # Per-socket pending selection. These ride along on the next prompt so
        # model/agent/project act like a session-level override from the phone.
        self._model: dict[Any, dict[str, Any]] = {}
        self._agent: dict[Any, str | None] = {}
        self._project_dir: dict[Any, str | None] = {}
        # Prompt IDs already surfaced to the phone, so each is delivered once.
        self._seen_permissions: set[str] = set()
        self._seen_questions: set[str] = set()

    # --- lifecycle -------------------------------------------------------

    async def serve(self, host: str, port: int) -> None:
        """Start listening and run until cancelled."""
        async with websockets.serve(self._handle, host, port):
            log.info("bridge listening on ws://%s:%d", host, port)
            self._pump = asyncio.create_task(self._pump_events())
            self._poll = asyncio.create_task(self._poll_prompts())
            try:
                await asyncio.Future()
            finally:
                await self.shutdown()

    async def shutdown(self) -> None:
        for task in (self._pump, self._poll):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                # Only _pump needs resetting; _poll is re-checked via the loop.
        self._pump = None
        self._poll = None
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
        # Push the model/agent catalogs up front so the phone can populate its
        # pickers before the user starts a session.
        await self._send_catalogs(socket)

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
                directory = frame.get("directory") or self._project_dir.get(socket)
                session = await self._client.create_session(title=title, directory=directory)
                self._attached[socket] = session["id"]
                await self._send(
                    socket,
                    {
                        "type": p.SESSION,
                        "kind": "created",
                        "session": p.session_summary(session),
                    },
                )
                await self._push_projects(socket)
            elif kind == p.ATTACH:
                session_id = p.require(frame, "sessionID")
                messages = await self._client.get_messages(session_id)
                self._attached[socket] = session_id
                await self._send(socket, p.history_frame(session_id, messages))
                await self._send(
                    socket,
                    {"type": p.SESSION, "kind": "attached", "sessionID": session_id},
                )
                await self._push_projects(socket)
            elif kind == p.SELECT_MODEL:
                session_id = frame.get("sessionID") or self._attached.get(socket)
                if not session_id:
                    await self._send(socket, p.error("attach before selecting a model"))
                    return
                model = p.require(frame, "modelID")
                provider = p.require(frame, "providerID")
                self._model[socket] = {"modelID": model, "providerID": provider}
                await self._send(
                    socket, p.model_frame(session_id, model_id=model, provider_id=provider)
                )
            elif kind == p.SELECT_AGENT:
                self._agent[socket] = p.require(frame, "agent")
            elif kind == p.PICK_PROJECT:
                # Remembered per-socket; the next CREATE_SESSION lands here.
                self._project_dir[socket] = p.require(frame, "worktree")
            elif kind == p.REPLY_PERMISSION:
                request_id = p.require(frame, "requestID")
                reply = p.require(frame, "reply")
                await self._client.reply_permission(request_id, reply, frame.get("message"))
            elif kind == p.REPLY_QUESTION:
                request_id = p.require(frame, "requestID")
                answers = p.require(frame, "answers")
                await self._client.reply_question(request_id, answers)
            elif kind == p.PROMPT:
                text = p.require(frame, "text")
                session_id = frame.get("sessionID") or self._attached.get(socket)
                if not session_id:
                    await self._send(
                        socket, p.error("attach to a session before sending a prompt")
                    )
                    return
                self._attached[socket] = session_id
                # Apply any model/agent the phone selected; per-message override
                # is the only setter that works on current opencode builds.
                prompt_model = self._model.pop(socket, None)
                prompt_agent = self._agent.pop(socket, None)
                # An existing session carries its own model/agent. When the phone
                # has nothing selected (old session, nothing picked yet), carry the
                # session's so a model-less prompt can never wedge it busy forever.
                if prompt_model is None or prompt_agent is None:
                    try:
                        info = await self._client.get_session(session_id)
                    except Exception:
                        info = {}
                    session_model = info.get("model") or {}
                    if (
                        prompt_model is None
                        and isinstance(session_model, dict)
                        and session_model.get("id")
                    ):
                        prompt_model = {
                            "providerID": session_model.get("providerID", ""),
                            "modelID": session_model["id"],
                        }
                    if prompt_agent is None and info.get("agent"):
                        prompt_agent = info["agent"]
                await self._client.prompt(
                    session_id, text, model=prompt_model, agent=prompt_agent
                )
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

    # --- prompts: poll + stream push ---------------------------------------

    async def _poll_prompts(self) -> None:
        """Surface interactive prompts by polling opencode.

        A poll is the reliable floor: `POST /permission` and `/question` list
        pending prompts, and each is delivered once per session. If opencode
        also pushes them as events on the stream (see translate_opencode_event),
        they arrive faster via _pump_events instead -- but the poller guarantees
        correctness regardless of whether push is implemented.
        """
        while True:
            try:
                await asyncio.sleep(PROMPT_POLL_SECONDS)
                perms = await self._client.list_permissions()
                qs = await self._client.list_questions()
                for request in perms:
                    rid = request.get("id")
                    if rid in self._seen_permissions:
                        continue
                    self._seen_permissions.add(rid)
                    await self._broadcast(p.permission_prompt_frame(request))
                for request in qs:
                    rid = request.get("id")
                    if rid in self._seen_questions:
                        continue
                    self._seen_questions.add(rid)
                    await self._broadcast(p.question_prompt_frame(request))
                # Forget IDs the phone has answered so a re-prompt can re-fire.
                self._seen_permissions &= {r.get("id") for r in perms}
                self._seen_questions &= {r.get("id") for r in qs}
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("prompt poll dropped (%s)", exc)
                await asyncio.sleep(PROMPT_POLL_SECONDS)

    # --- catalog helpers ----------------------------------------------------

    async def _send_catalogs(self, socket: Any) -> None:
        await self._send(socket, p.providers_frame(await self._client.list_providers()))
        await self._send(socket, p.agents_frame(await self._client.list_agents()))

    async def _push_projects(self, socket: Any) -> None:
        await self._send(socket, p.projects_frame(await self._client.list_projects()))
