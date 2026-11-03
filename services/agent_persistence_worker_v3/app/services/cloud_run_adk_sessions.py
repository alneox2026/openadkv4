"""Cloud Run ADK session lifecycle client for the persistence worker."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from urllib.parse import quote

import httpx
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2 import id_token

from common.diagnostics import (
    is_retryable_http_status,
    safe_http_response_details,
    safe_truncate,
    safe_url_host,
)
from common.schemas import ThreadDeleteRequestedEvent
from services.agent_persistence_worker_v3.app.core.config import get_settings
from services.agent_persistence_worker_v3.app.core.errors import RetryableWorkerError
from services.agent_persistence_worker_v3.app.core.logging import log_structured


LOGGER = logging.getLogger(__name__)


class CloudRunAdkSessionNotFoundError(RuntimeError):
    """Signals that a Cloud Run ADK session has already been removed."""


@dataclass
class _TokenCacheEntry:
    audience: str
    token: str
    expires_at_monotonic: float


class CloudRunAdkSessionsClient:
    def __init__(
        self,
        *,
        timeout_seconds: float = 30.0,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self._http_client = http_client or httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=10.0,
                read=timeout_seconds,
                write=timeout_seconds,
                pool=10.0,
            )
        )
        self._token_cache: _TokenCacheEntry | None = None
        self._auth_lock = asyncio.Lock()

    async def close(self) -> None:
        await self._http_client.aclose()

    async def delete_session(self, event: ThreadDeleteRequestedEvent) -> None:
        base_url = (event.agent_base_url or "").rstrip("/")
        app_name = event.agent_app_name or ""
        if not base_url or not app_name:
            raise RetryableWorkerError(
                "Cloud Run ADK delete event is missing base URL or app name."
            )

        path = (
            f"/apps/{quote(app_name, safe='')}"
            f"/users/{quote(event.user_id, safe='')}"
            f"/sessions/{quote(event.session_id, safe='')}"
        )
        headers = await self._authorized_headers(
            audience=event.agent_audience or base_url,
        )
        started_at = time.monotonic()
        try:
            response = await self._http_client.delete(
                f"{base_url}{path}",
                headers=headers,
            )
        except httpx.ConnectTimeout as exc:
            details = self._diagnostic_context(
                event=event,
                base_url=base_url,
                app_name=app_name,
                started_at=started_at,
                extra={
                    "reason": safe_truncate(exc),
                    "timeout_seconds": 10.0,
                    "timeout_type": "connect",
                    "retryable": True,
                },
            )
            log_structured(
                LOGGER,
                logging.WARNING,
                "cloud_run_adk_session_delete_timeout",
                **details,
            )
            raise RetryableWorkerError(
                "Cloud Run ADK session delete connect timeout: "
                f"{details['reason']}"
            ) from exc
        except httpx.ReadTimeout as exc:
            details = self._diagnostic_context(
                event=event,
                base_url=base_url,
                app_name=app_name,
                started_at=started_at,
                extra={
                    "reason": safe_truncate(exc),
                    "timeout_seconds": self.timeout_seconds,
                    "timeout_type": "read",
                    "retryable": True,
                },
            )
            log_structured(
                LOGGER,
                logging.WARNING,
                "cloud_run_adk_session_delete_timeout",
                **details,
            )
            raise RetryableWorkerError(
                "Cloud Run ADK session delete read timeout: "
                f"{details['reason']}"
            ) from exc
        except httpx.RequestError as exc:
            details = self._diagnostic_context(
                event=event,
                base_url=base_url,
                app_name=app_name,
                started_at=started_at,
                extra={"reason": safe_truncate(exc), "retryable": True},
            )
            log_structured(
                LOGGER,
                logging.WARNING,
                "cloud_run_adk_session_delete_transport_error",
                **details,
            )
            raise RetryableWorkerError(
                "Failed to reach Cloud Run ADK for session delete "
                f"{event.session_id}: {details['reason']}"
            ) from exc

        if response.status_code in {200, 202, 204}:
            log_structured(
                LOGGER,
                logging.INFO,
                "cloud_run_adk_session_delete_completed",
                **self._diagnostic_context(
                    event=event,
                    base_url=base_url,
                    app_name=app_name,
                    started_at=started_at,
                    extra={
                        "status_code": response.status_code,
                        "response_body_length": len(response.text or ""),
                    },
                ),
            )
            return
        if response.status_code == 404:
            log_structured(
                LOGGER,
                logging.INFO,
                "cloud_run_adk_session_delete_not_found",
                **self._diagnostic_context(
                    event=event,
                    base_url=base_url,
                    app_name=app_name,
                    started_at=started_at,
                    extra={
                        **safe_http_response_details(response),
                        "retryable": False,
                    },
                ),
            )
            raise CloudRunAdkSessionNotFoundError(event.session_id)
        retryable = is_retryable_http_status(response.status_code)
        details = self._diagnostic_context(
            event=event,
            base_url=base_url,
            app_name=app_name,
            started_at=started_at,
            extra={
                **safe_http_response_details(response),
                "retryable": retryable,
            },
        )
        log_structured(
            LOGGER,
            logging.WARNING if retryable else logging.ERROR,
            "cloud_run_adk_session_delete_non_success",
            **details,
        )
        raise RetryableWorkerError(
            "Cloud Run ADK session delete failed with "
            f"{response.status_code}: {details.get('body_excerpt', '')}"
        )

    async def get_session_events(
        self,
        *,
        base_url: str,
        app_name: str,
        user_id: str,
        session_id: str,
        audience: str | None = None,
    ) -> list[dict[str, object]]:
        """Read ADK session history for an exact invocation's late usage."""
        base_url = base_url.rstrip("/")
        path = (
            f"/apps/{quote(app_name, safe='')}"
            f"/users/{quote(user_id, safe='')}"
            f"/sessions/{quote(session_id, safe='')}"
        )
        headers = await self._authorized_headers(audience=audience or base_url)
        try:
            response = await self._http_client.get(f"{base_url}{path}", headers=headers)
        except httpx.RequestError as exc:
            raise RetryableWorkerError(
                "Failed to read Cloud Run ADK session history: "
                f"{safe_truncate(exc)}"
            ) from exc
        if response.status_code != 200:
            details = safe_http_response_details(response)
            raise RetryableWorkerError(
                "Cloud Run ADK session history lookup failed with "
                f"{response.status_code}: {details.get('body_excerpt', '')}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise RetryableWorkerError(
                "Cloud Run ADK session history response was not valid JSON."
            ) from exc
        session = payload.get("session", payload) if isinstance(payload, dict) else None
        events = session.get("events") if isinstance(session, dict) else None
        if not isinstance(events, list):
            raise RetryableWorkerError(
                "Cloud Run ADK session history response did not contain an events list."
            )
        return [event for event in events if isinstance(event, dict)]

    async def _authorized_headers(self, *, audience: str) -> dict[str, str]:
        token = await self._id_token(audience=audience)
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    async def _id_token(self, *, audience: str) -> str:
        now = time.monotonic()
        async with self._auth_lock:
            if (
                self._token_cache is not None
                and self._token_cache.audience == audience
                and self._token_cache.expires_at_monotonic > now
            ):
                return self._token_cache.token
            token = await asyncio.to_thread(
                id_token.fetch_id_token,
                GoogleAuthRequest(),
                audience,
            )
            if not token:
                raise RetryableWorkerError(
                    "Unable to obtain an identity token for Cloud Run ADK session deletion."
                )
            self._token_cache = _TokenCacheEntry(
                audience=audience,
                token=token,
                expires_at_monotonic=now + 2700,
            )
            return token

    def _diagnostic_context(
        self,
        *,
        event: ThreadDeleteRequestedEvent,
        base_url: str,
        app_name: str,
        started_at: float,
        extra: dict[str, object] | None = None,
    ) -> dict[str, object]:
        context: dict[str, object] = {
            "event_id": event.event_id,
            "agent_id": event.agent_id,
            "operation": "session_delete",
            "thread_id": event.thread_id,
            "session_id": event.session_id,
            "app_name": app_name,
            "base_url_host": safe_url_host(base_url) or "",
            "upstream_elapsed_ms": int((time.monotonic() - started_at) * 1000),
        }
        if extra:
            context.update(extra)
        return context


_client_singleton: CloudRunAdkSessionsClient | None = None
_client_lock = asyncio.Lock()


async def get_cloud_run_adk_sessions_client() -> CloudRunAdkSessionsClient:
    global _client_singleton
    if _client_singleton is None:
        async with _client_lock:
            if _client_singleton is None:
                settings = get_settings()
                _client_singleton = CloudRunAdkSessionsClient(
                    timeout_seconds=settings.runtime_delete_timeout_seconds,
                )
    return _client_singleton


async def close_cloud_run_adk_sessions_client() -> None:
    global _client_singleton
    if _client_singleton is not None:
        await _client_singleton.close()
        _client_singleton = None
