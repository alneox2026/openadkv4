"""Agent Runtime session lifecycle client for the persistence worker."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

import google.auth
import httpx
from google.auth.credentials import Credentials
from google.auth.transport.requests import Request as GoogleAuthRequest

from common.schemas import ThreadDeleteRequestedEvent
from services.agent_persistence_worker_v3.app.core.config import get_settings
from services.agent_persistence_worker_v3.app.core.errors import RetryableWorkerError


AUTH_SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)


class AgentRuntimeSessionNotFoundError(Exception):
    """Signals that the runtime session has already been removed."""


class AgentRuntimeSessionsClient:
    def __init__(self, http_client: httpx.AsyncClient | None = None) -> None:
        settings = get_settings()
        self._settings = settings
        self._http_client = http_client or httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=10.0,
                read=settings.runtime_delete_timeout_seconds,
                write=settings.runtime_delete_timeout_seconds,
                pool=10.0,
            )
        )
        self._credentials: Credentials | None = None
        self._auth_lock = asyncio.Lock()

    async def delete_session(self, event: ThreadDeleteRequestedEvent) -> None:
        if not event.agent_resource_name:
            raise RetryableWorkerError(
                f"Missing Agent Runtime resource name for delete event {event.event_id}."
            )
        headers = await self._authorized_headers()
        url = f"https://aiplatform.googleapis.com/v1beta1/{event.agent_resource_name}/sessions/{event.session_id}"

        try:
            response = await self._http_client.delete(url, headers=headers)
        except httpx.RequestError as exc:
            raise RetryableWorkerError(
                f"Failed to reach Agent Runtime for session delete {event.session_id}: {exc}"
            ) from exc

        if response.status_code in {200, 204}:
            return
        if response.status_code == 404:
            raise AgentRuntimeSessionNotFoundError(event.session_id)
        if response.status_code >= 500:
            raise RetryableWorkerError(
                f"Agent Runtime session delete failed with {response.status_code}: {response.text}"
            )
        raise RetryableWorkerError(
            f"Agent Runtime session delete returned unexpected status {response.status_code}: {response.text}"
        )

    async def get_session_events(
        self,
        *,
        resource_name: str,
        region: str,
        session_id: str,
        started_at: str,
        completed_at: str,
    ) -> list[dict[str, Any]]:
        """Read a bounded time window of persisted events for usage recovery."""
        start = _parse_timestamp(started_at) - timedelta(seconds=30)
        end = _parse_timestamp(completed_at) + timedelta(seconds=30)
        resource_path = quote(resource_name.strip().strip("/"), safe="/")
        encoded_session = quote(session_id.strip(), safe="")
        url = (
            f"https://{region.strip()}-aiplatform.googleapis.com/v1beta1/"
            f"{resource_path}/sessions/{encoded_session}/events"
        )
        params: dict[str, Any] = {
            "pageSize": 100,
            "filter": (
                f'timestamp>="{_format_timestamp(start)}" AND '
                f'timestamp<="{_format_timestamp(end)}"'
            ),
        }
        events: list[dict[str, Any]] = []
        headers = await self._authorized_headers()
        for page_number in range(2):
            try:
                response = await self._http_client.get(
                    url, headers=headers, params=params
                )
            except httpx.RequestError as exc:
                raise RetryableWorkerError(
                    f"Failed to read Agent Runtime session events: {type(exc).__name__}."
                ) from exc
            if response.status_code != 200:
                raise RetryableWorkerError(
                    "Agent Runtime session events lookup failed with "
                    f"HTTP {response.status_code}."
                )
            try:
                payload = response.json()
            except ValueError as exc:
                raise RetryableWorkerError(
                    "Agent Runtime session events response was not valid JSON."
                ) from exc
            if not isinstance(payload, dict):
                raise RetryableWorkerError(
                    "Agent Runtime session events response was not an object."
                )
            page = payload.get("sessionEvents", payload.get("session_events", []))
            if isinstance(page, list):
                events.extend(item for item in page if isinstance(item, dict))
            page_token = payload.get("nextPageToken") or payload.get("next_page_token")
            if not page_token:
                break
            if page_number == 1:
                raise RetryableWorkerError(
                    "Agent Runtime usage recovery exceeded its bounded event-page limit."
                )
            params["pageToken"] = page_token
        return events

    async def close(self) -> None:
        await self._http_client.aclose()

    async def _authorized_headers(self) -> dict[str, str]:
        token = await self._access_token()
        return {"Authorization": f"Bearer {token}"}

    async def _access_token(self) -> str:
        async with self._auth_lock:
            if self._credentials is None:
                self._credentials = await asyncio.to_thread(self._default_credentials)
            if not self._credentials.valid or not self._credentials.token:
                await asyncio.to_thread(self._credentials.refresh, GoogleAuthRequest())
            token = self._credentials.token
            if not token:
                raise RetryableWorkerError(
                    "Unable to obtain a Google access token for Agent Runtime session deletion."
                )
            return token

    def _default_credentials(self) -> Credentials:
        credentials, _ = google.auth.default(scopes=AUTH_SCOPES)
        return credentials


def _parse_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RetryableWorkerError("Agent Runtime usage recovery timestamp is invalid.") from exc
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _format_timestamp(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


_client_singleton: AgentRuntimeSessionsClient | None = None
_client_lock = asyncio.Lock()


async def get_agent_runtime_sessions_client() -> AgentRuntimeSessionsClient:
    global _client_singleton
    if _client_singleton is None:
        async with _client_lock:
            if _client_singleton is None:
                _client_singleton = AgentRuntimeSessionsClient()
    return _client_singleton


async def close_agent_runtime_sessions_client() -> None:
    global _client_singleton
    if _client_singleton is not None:
        await _client_singleton.close()
    _client_singleton = None
