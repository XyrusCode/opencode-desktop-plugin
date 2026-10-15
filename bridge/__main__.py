"""Entry point: `python -m bridge`.

Starts opencode (unless told to attach to a running one) and then serves the
phone-facing WebSocket API until interrupted.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shutil
import signal
import subprocess
import sys

from .config import load_config
from .opencode_client import OpencodeClient, OpencodeError
from .server import BridgeServer

log = logging.getLogger("bridge")


def _resolve_command(command: str) -> str | None:
    """Find the opencode executable.

    opencode installs as a bare command on PATH, but Windows installs land in
    %USERPROFILE%\\.opencode\\bin which is not always on PATH for GUI processes.
    """
    found = shutil.which(command)
    if found:
        return found
    fallback = os.path.join(os.path.expanduser("~"), ".opencode", "bin", f"{command}.exe")
    return fallback if os.path.exists(fallback) else None


async def _spawn_opencode(config) -> subprocess.Popen[bytes] | None:
    """Start `opencode serve` on loopback and return the process.

    Returns None when the executable is missing; the caller decides whether
    that is fatal (it usually is, since there is nothing to bridge to).
    """
    exe = _resolve_command(config.opencode_command)
    if exe is None:
        log.error(
            "could not find %r on PATH; install opencode or set BRIDGE_OPENCODE_COMMAND",
            config.opencode_command,
        )
        return None

    cmd = [
        exe,
        "serve",
        "--hostname",
        config.opencode_host,
        "--port",
        str(config.opencode_port),
    ]
    log.info("spawning: %s", " ".join(cmd))
    return subprocess.Popen(
        cmd,
        cwd=config.resolved_workdir,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )


def _terminate(proc: subprocess.Popen[bytes] | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


async def amain() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    config = load_config()

    proc: subprocess.Popen[bytes] | None = None
    if config.spawn_opencode:
        proc = await _spawn_opencode(config)
        if proc is None:
            return 1

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    try:
        async with OpencodeClient(
            config.resolved_opencode_url,
            workdir=config.resolved_workdir,
            token=config.opencode_token,
        ) as client:
            try:
                await client.wait_until_ready()
            except OpencodeError as exc:
                log.error("opencode never became ready: %s", exc)
                if proc is not None:
                    stderr = proc.stderr.read() if proc.stderr else b""
                    if stderr:
                        log.error("opencode stderr: %s", stderr.decode(errors="replace")[:800])
                return 1

            log.info("opencode ready at %s", config.resolved_opencode_url)
            server = BridgeServer(client, auth_token=config.auth_token)

            serve_task = asyncio.create_task(
                server.serve(config.ws_host, config.ws_port)
            )
            stop_task = asyncio.create_task(stop.wait())

            done, pending = await asyncio.wait(
                {serve_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            for task in done:
                exc = task.exception()
                if exc is not None:
                    log.error("bridge stopped: %s", exc)
                    return 1
            return 0
    finally:
        _terminate(proc)


def main() -> int:
    try:
        return asyncio.run(amain())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
