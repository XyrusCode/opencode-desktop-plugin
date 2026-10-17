"""Async client for the opencode headless server.

Only the endpoints the bridge actually uses are wrapped. opencode's own OpenAPI
document lives at ``GET /doc`` on a running server if anything needs adding.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, AsyncIterator

import aiohttp

log = logging.getLogger(__name__)


class OpencodeError(RuntimeError):
    """opencode was reachable but answered with an error or not at all."""


class OpencodeClient:
    """Thin async wrapper over the opencode HTTP + SSE API."""

    def __init__(
        self,
        base_url: str,
        *,
        workdir: str,
        token: str = "",
        timeout: float = 30.0,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.workdir = workdir
        self._token = token
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session = session
        self._owns_session = session is None

    async def __aenter__(self) -> "OpencodeClient":
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._session is not None and self._owns_session:
            await self._session.close()
            self._session = None

    @property
    def _http(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise OpencodeError("client is not started; use `async with`")
        return self._session

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        directory: str | None = None,
        ignore_workdir: bool = False,
    ) -> Any:
        query = dict(params or {})
        # A per-call directory overrides the client's default workdir; this is
        # how the phone creates a session in a project it picked. ignore_workdir
        # suppresses the default entirely, so a listing call spans every project
        # instead of only the bridge's working directory.
        if directory is not None:
            query.setdefault("directory", directory)
        elif self.workdir and not ignore_workdir:
            query.setdefault("directory", self.workdir)
        try:
            async with self._http.request(
                method,
                f"{self.base_url}{path}",
                params=query,
                json=json_body,
                headers=self._headers(),
            ) as resp:
                if resp.status >= 400:
                    detail = (await resp.text())[:400]
                    raise OpencodeError(
                        f"{method} {path} -> {resp.status}: {detail}"
                    )
                if resp.status == 204 or resp.content_length == 0:
                    return None
                return await resp.json(content_type=None)
        except aiohttp.ClientError as exc:
            raise OpencodeError(f"{method} {path} failed: {exc}") from exc

    # --- health ----------------------------------------------------------

    async def wait_until_ready(self, *, attempts: int = 60, delay: float = 0.5) -> None:
        """Block until opencode answers /global/health.

        opencode prints its listen line before the HTTP server is actually
        accepting, so the first probe can fail; retry rather than assume.
        """
        last: Exception | None = None
        for _ in range(attempts):
            try:
                await self._request("GET", "/global/health")
                return
            except OpencodeError as exc:
                last = exc
                await asyncio.sleep(delay)
        raise OpencodeError(f"opencode never became ready at {self.base_url}: {last}")

    # --- sessions --------------------------------------------------------

    async def list_sessions(self, *, limit: int = 500) -> list[dict[str, Any]]:
        """Sessions across every project, newest first.

        One unscoped call spans all projects, and opencode caps the response at
        whatever exists (a few hundred), so a high limit returns everything in a
        single round trip. The cap is what used to hide projects: at 50 only 8
        project folders came back, at 500 all 29 do.
        Do NOT fan out into a per-project request per worktree: opencode serve
        registers hundreds of worktrees and that many round trips saturates it
        until /project times out.
        The phone groups these by their real directory into projects.
        """
        data = await self._request("GET", "/session", params={"limit": limit}, ignore_workdir=True)
        return data if isinstance(data, list) else []

    async def get_session(self, session_id: str) -> dict[str, Any]:
        data = await self._request("GET", f"/session/{session_id}", ignore_workdir=True)
        return data if isinstance(data, dict) else {}

    async def create_session(self, *, title: str = "", directory: str | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if title:
            body["title"] = title
        data = await self._request("POST", "/session", json_body=body, directory=directory)
        if not isinstance(data, dict) or not data.get("id"):
            raise OpencodeError(f"opencode returned an unusable session: {data!r}")
        return data

    async def get_messages(self, session_id: str) -> list[dict[str, Any]]:
        data = await self._request("GET", f"/session/{session_id}/message")
        return data if isinstance(data, list) else []

    async def prompt(
        self,
        session_id: str,
        text: str,
        *,
        model: dict[str, Any] | None = None,
        agent: str | None = None,
    ) -> None:
        """Send a prompt, optionally over-riding the model or agent.

        prompt_async is deliberate: the synchronous /message variant holds the
        request open for the whole generation, which would block the phone's
        command and make streaming impossible.

        The /api/session/{id}/model and /agent setters return 500 on current
        builds, so per-message model/agent selection is done in the body here;
        opencode latches it onto the resulting session after the turn.
        """
        body: dict[str, Any] = {"parts": [{"type": "text", "text": text}]}
        if model:
            body["model"] = model  # {providerID, modelID}
        if agent:
            body["agent"] = agent
        await self._request(
            "POST",
            f"/session/{session_id}/prompt_async",
            json_body=body,
        )

    async def abort(self, session_id: str) -> None:
        await self._request("POST", f"/session/{session_id}/abort")

    # --- catalog + prompts --------------------------------------------------

    async def list_providers(self) -> list[dict[str, Any]]:
        """Configured providers with their models. opencode wraps them in `all`."""
        data = await self._request("GET", "/provider")
        if isinstance(data, dict) and isinstance(data.get("all"), list):
            return data["all"]
        return data if isinstance(data, list) else []

    async def list_agents(self) -> list[dict[str, Any]]:
        data = await self._request("GET", "/agent")
        return data if isinstance(data, list) else []

    async def list_projects(self) -> list[dict[str, Any]]:
        data = await self._request("GET", "/project")
        return data if isinstance(data, list) else []

    async def list_permissions(self) -> list[dict[str, Any]]:
        data = await self._request("GET", "/permission")
        return data if isinstance(data, list) else []

    async def list_questions(self) -> list[dict[str, Any]]:
        data = await self._request("GET", "/question")
        return data if isinstance(data, list) else []

    async def reply_permission(
        self, request_id: str, reply: str, message: str | None = None
    ) -> bool:
        body: dict[str, Any] = {"reply": reply}
        if message:
            body["message"] = message
        result = await self._request("POST", f"/permission/{request_id}/reply", json_body=body)
        return bool(result)

    async def reply_question(self, request_id: str, answers: list[list[str]]) -> bool:
        result = await self._request(
            "POST", f"/question/{request_id}/reply", json_body={"answers": answers}
        )
        return bool(result)

    # --- events ----------------------------------------------------------

    async def events(self) -> AsyncIterator[dict[str, Any]]:
        """Yield opencode SSE events forever.

        Uses a session without a total timeout: the stream is long lived by
        nature, so a request-level timeout would tear it down on its own.
        """
        timeout = aiohttp.ClientTimeout(total=None, sock_read=None)
        headers = self._headers()
        headers["Accept"] = "text/event-stream"
        async with self._http.get(
            f"{self.base_url}/event", headers=headers, timeout=timeout
        ) as resp:
            if resp.status != 200:
                raise OpencodeError(f"GET /event -> {resp.status}")
            async for raw in resp.content:
                line = raw.decode("utf-8", errors="replace").strip()
                if not line.startswith("data:"):
                    continue
                payload = line[5:].strip()
                if not payload:
                    continue
                try:
                    yield json.loads(payload)
                except ValueError:
                    log.debug("skipping unparseable SSE payload: %s", payload[:120])
