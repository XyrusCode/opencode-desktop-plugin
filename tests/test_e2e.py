"""End-to-end tests: real WebSocket client -> real bridge -> fake opencode.

Every socket in these tests is real. Only opencode itself is replaced, so auth,
frame routing, HTTP calls to opencode, SSE parsing and the broadcast fan-out are
all exercised for real.
"""

from __future__ import annotations

import asyncio
import json

import pytest
import pytest_asyncio
import websockets

from bridge import protocol as p
from bridge.opencode_client import OpencodeClient, OpencodeError
from bridge.server import BridgeServer

TOKEN = "test-token"
FREE_PORT_BASE = 49100


def _free_port() -> int:
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Harness:
    """A running bridge wired to a fake opencode, plus a connect helper."""

    def __init__(self, server: BridgeServer, ws_url: str, client: OpencodeClient) -> None:
        self.server = server
        self.ws_url = ws_url
        self.client = client

    async def connect(self, *, token: str = TOKEN):
        socket = await websockets.connect(self.ws_url)
        await socket.send(p.encode({"type": p.AUTH, "token": token}))
        return socket

    async def send(self, socket, frame: dict) -> None:
        await socket.send(p.encode(frame))


@pytest_asyncio.fixture
async def bridge(fake_opencode):
    server_obj, url = fake_opencode
    async with OpencodeClient(url, workdir="C:\\work") as client:
        bridge_server = BridgeServer(client, auth_token=TOKEN)
        ws_port = _free_port()
        ws_url = f"ws://127.0.0.1:{ws_port}"
        serve_task = asyncio.create_task(bridge_server.serve("127.0.0.1", ws_port))
        harness = Harness(bridge_server, ws_url, client)

        # Wait for the listener to actually accept before handing out the URL.
        for _ in range(100):
            try:
                probe = await websockets.connect(ws_url)
                await probe.close()
                break
            except OSError:
                await asyncio.sleep(0.02)
        else:
            serve_task.cancel()
            raise RuntimeError("bridge did not start listening")

        try:
            yield harness, server_obj
        finally:
            serve_task.cancel()
            try:
                await serve_task
            except asyncio.CancelledError:
                pass


async def _recv_until(socket, predicate, timeout: float = 5.0):
    """Collect frames until one satisfies `predicate`, or fail loudly."""
    deadline = asyncio.get_running_loop().time() + timeout
    seen = []
    while asyncio.get_running_loop().time() < deadline:
        try:
            raw = await asyncio.wait_for(socket.recv(), timeout=deadline - asyncio.get_running_loop().time())
        except asyncio.TimeoutError:
            break
        frame = json.loads(raw)
        seen.append(frame)
        if predicate(frame):
            return frame, seen
    raise AssertionError(f"never saw expected frame; got {seen}")


# --- auth ------------------------------------------------------------------


class TestAuth:
    async def test_correct_token_gets_welcome(self, bridge):
        harness, _ = bridge
        socket = await harness.connect()
        frame, _ = await _recv_until(socket, lambda f: f["type"] == p.WELCOME)
        assert frame["message"] == "authenticated"
        await socket.close()

    async def test_wrong_token_is_rejected(self, bridge):
        harness, _ = bridge
        socket = await harness.connect(token="nope")
        frame, _ = await _recv_until(socket, lambda f: f["type"] == p.ERROR)
        assert frame["fatal"] is True
        await socket.close()

    async def test_missing_token_is_rejected(self, bridge):
        harness, _ = bridge
        socket = await websockets.connect(harness.ws_url)
        await socket.send(p.encode({"type": p.AUTH}))
        frame, _ = await _recv_until(socket, lambda f: f["type"] == p.ERROR)
        assert "token" in frame["message"]
        await socket.close()

    async def test_non_json_first_frame_is_rejected(self, bridge):
        harness, _ = bridge
        socket = await websockets.connect(harness.ws_url)
        await socket.send("garbage")
        frame, _ = await _recv_until(socket, lambda f: f["type"] == p.ERROR)
        assert frame["fatal"] is True
        await socket.close()

    async def test_unauthenticated_socket_gets_no_events(self, bridge):
        """A socket that never authenticates must not receive session output."""
        harness, fake = bridge
        legit = await harness.connect()
        await _recv_until(legit, lambda f: f["type"] == p.WELCOME)

        lurker = await websockets.connect(harness.ws_url)
        sid = fake.add_session("busy")
        await harness.send(legit, {"type": p.ATTACH, "sessionID": sid})
        await _recv_until(legit, lambda f: f.get("kind") == "attached")
        await harness.send(legit, {"type": p.PROMPT, "text": "hello there"})

        # Wait for the legit socket to receive streamed output...
        await _recv_until(legit, lambda f: f["type"] == p.OUTPUT_DELTA)
        # ...then confirm the lurker saw nothing at all.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(lurker.recv(), timeout=0.5)
        await lurker.close()
        await legit.close()


# --- commands --------------------------------------------------------------


class TestCommands:
    async def test_ping_pong(self, bridge):
        harness, _ = bridge
        socket = await harness.connect()
        await _recv_until(socket, lambda f: f["type"] == p.WELCOME)
        await harness.send(socket, {"type": p.PING})
        frame, _ = await _recv_until(socket, lambda f: f["type"] == p.PONG)
        assert frame["type"] == p.PONG
        await socket.close()

    async def test_unknown_frame_type_errors_without_dropping_socket(self, bridge):
        harness, _ = bridge
        socket = await harness.connect()
        await _recv_until(socket, lambda f: f["type"] == p.WELCOME)
        await harness.send(socket, {"type": "teleport"})
        frame, _ = await _recv_until(socket, lambda f: f["type"] == p.ERROR)
        assert "teleport" in frame["message"]
        # Socket must still work.
        await harness.send(socket, {"type": p.PING})
        await _recv_until(socket, lambda f: f["type"] == p.PONG)
        await socket.close()

    async def test_list_sessions(self, bridge):
        harness, fake = bridge
        fake.add_session("Fix the build", session_id="ses_alpha")
        fake.add_session("Write tests", session_id="ses_beta")

        socket = await harness.connect()
        await _recv_until(socket, lambda f: f["type"] == p.WELCOME)
        await harness.send(socket, {"type": p.LIST_SESSIONS})
        frame, _ = await _recv_until(socket, lambda f: f.get("kind") == "list")
        ids = [s["id"] for s in frame["sessions"]]
        assert ids == ["ses_alpha", "ses_beta"]
        await socket.close()

    async def test_create_session(self, bridge):
        harness, fake = bridge
        socket = await harness.connect()
        await _recv_until(socket, lambda f: f["type"] == p.WELCOME)
        await harness.send(socket, {"type": p.CREATE_SESSION, "title": "From phone"})
        frame, _ = await _recv_until(socket, lambda f: f.get("kind") == "created")
        assert frame["session"]["title"] == "From phone"
        assert frame["session"]["id"] in fake.sessions
        await socket.close()

    async def test_attach_returns_history(self, bridge):
        harness, fake = bridge
        sid = fake.add_session("with history")
        fake.messages[sid] = [
            {"info": {"role": "user"}, "parts": [{"type": "text", "text": "earlier q"}]},
            {"info": {"role": "assistant"}, "parts": [{"type": "text", "text": "earlier a"}]},
        ]
        socket = await harness.connect()
        await _recv_until(socket, lambda f: f["type"] == p.WELCOME)
        await harness.send(socket, {"type": p.ATTACH, "sessionID": sid})

        frame, _ = await _recv_until(socket, lambda f: f.get("kind") == "history")
        assert frame["lines"] == ["you: earlier q", "opencode: earlier a"]
        await socket.close()

    async def test_attach_requires_session_id(self, bridge):
        harness, _ = bridge
        socket = await harness.connect()
        await _recv_until(socket, lambda f: f["type"] == p.WELCOME)
        await harness.send(socket, {"type": p.ATTACH})
        frame, _ = await _recv_until(socket, lambda f: f["type"] == p.ERROR)
        assert "sessionID" in frame["message"]
        await socket.close()

    async def test_prompt_before_attach_is_refused(self, bridge):
        harness, fake = bridge
        socket = await harness.connect()
        await _recv_until(socket, lambda f: f["type"] == p.WELCOME)
        await harness.send(socket, {"type": p.PROMPT, "text": "hi"})
        frame, _ = await _recv_until(socket, lambda f: f["type"] == p.ERROR)
        assert "attach" in frame["message"]
        assert fake.prompts == []
        await socket.close()

    async def test_abort_without_session_is_refused(self, bridge):
        harness, _ = bridge
        socket = await harness.connect()
        await _recv_until(socket, lambda f: f["type"] == p.WELCOME)
        await harness.send(socket, {"type": p.ABORT})
        frame, _ = await _recv_until(socket, lambda f: f["type"] == p.ERROR)
        assert "no session" in frame["message"]
        await socket.close()


# --- streaming -------------------------------------------------------------


class TestStreaming:
    async def test_prompt_streams_output_and_completes(self, bridge):
        harness, fake = bridge
        sid = fake.add_session("stream target")
        socket = await harness.connect()
        await _recv_until(socket, lambda f: f["type"] == p.WELCOME)
        await harness.send(socket, {"type": p.ATTACH, "sessionID": sid})
        await _recv_until(socket, lambda f: f.get("kind") == "attached")

        await harness.send(socket, {"type": p.PROMPT, "text": "say hello now"})

        done, seen = await _recv_until(
            socket, lambda f: f["type"] == p.MESSAGE_DONE and f.get("reason") == "idle"
        )
        deltas = [f["delta"] for f in seen if f["type"] == p.OUTPUT_DELTA]
        assert "".join(deltas).strip() == "say hello now"
        assert fake.prompts == [(sid, "say hello now")]
        assert done["sessionID"] == sid
        await socket.close()

    async def test_abort_reaches_opencode(self, bridge):
        harness, fake = bridge
        sid = fake.add_session("abort target")
        socket = await harness.connect()
        await _recv_until(socket, lambda f: f["type"] == p.WELCOME)
        await harness.send(socket, {"type": p.ATTACH, "sessionID": sid})
        await _recv_until(socket, lambda f: f.get("kind") == "attached")

        await harness.send(socket, {"type": p.ABORT})
        await _recv_until(socket, lambda f: f["type"] == p.MESSAGE_DONE)
        assert fake.aborted == [sid]
        await socket.close()

    async def test_two_clients_both_receive_stream(self, bridge):
        """Broadcast semantics: a second paired phone sees the same output."""
        harness, fake = bridge
        sid = fake.add_session("shared")
        first = await harness.connect()
        second = await harness.connect()
        await _recv_until(first, lambda f: f["type"] == p.WELCOME)
        await _recv_until(second, lambda f: f["type"] == p.WELCOME)

        await harness.send(first, {"type": p.ATTACH, "sessionID": sid})
        await _recv_until(first, lambda f: f.get("kind") == "attached")
        await harness.send(first, {"type": p.PROMPT, "text": "broadcast me"})

        for socket in (first, second):
            await _recv_until(
                socket, lambda f: f["type"] == p.MESSAGE_DONE and f.get("reason") == "idle"
            )
        await first.close()
        await second.close()


# --- client unit -----------------------------------------------------------


class TestOpencodeClient:
    async def test_wait_until_ready_raises_when_nothing_listening(self):
        async with OpencodeClient(f"http://127.0.0.1:{_free_port()}", workdir="C:\\w") as client:
            with pytest.raises(OpencodeError):
                await client.wait_until_ready(attempts=2, delay=0.01)

    async def test_prompt_async_sends_text_part(self, fake_opencode):
        server, url = fake_opencode
        sid = server.add_session("t")
        async with OpencodeClient(url, workdir="C:\\work") as client:
            await client.prompt(sid, "hello")
        assert server.prompts[0] == (sid, "hello")
