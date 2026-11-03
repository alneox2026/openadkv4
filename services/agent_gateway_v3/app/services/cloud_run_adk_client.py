"""Cloud Run-hosted ADK agent client for the gateway."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2 import id_token

from common.diagnostics import (
    is_retryable_http_status,
    safe_http_response_details,
    safe_response_body_details,
    safe_truncate,
    safe_url_host,
)
from common.ids import new_session_id, new_thread_id
from common.schemas import AgentConfig, ChatRequest
from services.agent_gateway_v3.app.core.errors import ApiError
from services.agent_gateway_v3.app.core.logging import log_structured
from services.agent_gateway_v3.app.services.agent_runtime_client import (
    BufferedAgentResponse,
    SessionResult,
    UpstreamStreamEvent,
)
from services.agent_gateway_v3.app.services.turn_assembler import TurnAssembler


LOGGER = logging.getLogger(__name__)
_client_singleton: "CloudRunAdkClient | None" = None
_client_lock = asyncio.Lock()
EVENT_SUMMARY_LIMIT = 5
RESOURCE_EXHAUSTED_TERMS = (
    "resource_exhausted",
    "resource exhausted",
    "quota",
    "rate_limit_exceeded",
    "insufficient_quota",
    "tokens per min",
    "requests per min",
    "tpm",
    "rpm",
    "rate limit",
)

try:
    import h2  # noqa: F401

    _HTTP2_AVAILABLE = True
except ImportError:
    _HTTP2_AVAILABLE = False


@dataclass
class _TokenCacheEntry:
    audience: str
    token: str
    expires_at_monotonic: float


class CloudRunAdkClient:
    def __init__(
        self,
        *,
        connect_timeout_seconds: float = 10.0,
        read_timeout_seconds: float = 60.0,
        http_client: httpx.AsyncClient | None = None,
        limits: httpx.Limits | None = None,
    ) -> None:
        self.connect_timeout_seconds = connect_timeout_seconds
        self.read_timeout_seconds = read_timeout_seconds
        self._http_client = http_client or httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=connect_timeout_seconds,
                read=read_timeout_seconds,
                write=read_timeout_seconds,
                pool=connect_timeout_seconds,
            ),
            limits=limits
            or httpx.Limits(
                max_connections=200,
                max_keepalive_connections=100,
                keepalive_expiry=60.0,
            ),
            http2=_HTTP2_AVAILABLE,
        )
        self._token_cache: dict[str, _TokenCacheEntry] = {}
        self._auth_lock = asyncio.Lock()


    async def close(self) -> None:
        await self._http_client.aclose()

    async def ensure_session(
        self,
        *,
        agent_config: AgentConfig,
        user_id: str,
        request: ChatRequest,
    ) -> SessionResult:
        if request.thread_id or request.session_id:
            raise ApiError(
                500,
                "invalid_session_resolution_state",
                "Existing thread sessions must be resolved before reaching Cloud Run ADK.",
            )
        session_id = new_session_id()
        await self._create_or_update_session(
            agent_config=agent_config,
            user_id=user_id,
            session_id=session_id,
        )
        return SessionResult(session_id=session_id, thread_id=new_thread_id())

    def _build_request_payload(
        self,
        *,
        agent_config: AgentConfig,
        user_id: str,
        session_id: str,
        message: str,
        streaming: bool,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "app_name": self._app_name(agent_config),
            "user_id": user_id,
            "session_id": session_id,
            "new_message": {
                "role": "user",
                "parts": [{"text": message}],
            },
            "streaming": streaming,
        }
        return payload

    async def chat_buffered_query(
        self,
        *,
        agent_config: AgentConfig,
        user_id: str,
        session_id: str,
        message: str,
    ) -> BufferedAgentResponse:
        response_payload = await self._post_run_sse(
            agent_config=agent_config,
            user_id=user_id,
            session_id=session_id,
            payload=self._build_request_payload(
                agent_config=agent_config,
                user_id=user_id,
                session_id=session_id,
                message=message,
                streaming=False,
            ),
        )
        raw_events = self._extract_event_payloads(response_payload)
        assembler = TurnAssembler(model_name=agent_config.model)
        for event in raw_events:
            assembler.add_event(event)
            for fragment in self._extract_text_fragments(event):
                assembler.add_text(fragment)
        reply_text = assembler.reply_text()
        log_structured(
            LOGGER,
            logging.INFO,
            "cloud_run_adk_buffered_response_assembled",
            agent_id=agent_config.agent_id,
            session_id=session_id,
            app_name=self._app_name(agent_config),
            base_url_host=safe_url_host(self._base_url(agent_config)),
            response_event_count=len(raw_events),
            reply_text_length=len(reply_text),
        )
        if not reply_text:
            error_event = self._find_error_event(raw_events)
            if error_event is not None:
                raise self._error_event_api_error(
                    error_event,
                    raw_events,
                    agent_config=agent_config,
                    session_id=session_id,
                )
            raise self._empty_response_error(
                raw_events,
                agent_config=agent_config,
                session_id=session_id,
            )
        return BufferedAgentResponse(
            reply_text=reply_text,
            raw_events=raw_events,
            usage=assembler.usage,
            raw_usage_reports=assembler.raw_usage_reports,
        )

    async def stream_chat_events(
        self,
        *,
        agent_config: AgentConfig,
        user_id: str,
        session_id: str,
        message: str,
    ):
        headers = await self._authorized_headers(agent_config)
        operation = "run_sse_stream"
        started_at = time.monotonic()
        url = f"{self._base_url(agent_config)}/run_sse"
        payload = self._build_request_payload(
            agent_config=agent_config,
            user_id=user_id,
            session_id=session_id,
            message=message,
            streaming=True,
        )
        try:
            async with self._http_client.stream(
                "POST",
                url,
                headers=headers,
                json=payload,
            ) as response:
                if response.status_code >= 400:
                    raise await self._stream_response_error(
                        response,
                        agent_config=agent_config,
                        operation=operation,
                        started_at=started_at,
                        user_id=user_id,
                        session_id=session_id,
                    )
                log_structured(
                    LOGGER,
                    logging.INFO,
                    "cloud_run_adk_stream_opened",
                    **self._diagnostic_context(
                        agent_config=agent_config,
                        operation=operation,
                        started_at=started_at,
                        user_id=user_id,
                        session_id=session_id,
                        extra={
                            "status_code": response.status_code,
                            "content_type": response.headers.get("content-type", ""),
                        },
                    ),
                )
                async for event_name, data in self._iter_sse_messages(response):
                    if not data or data == "[DONE]":
                        continue
                    for parsed in self._parse_json_messages(data):
                        yield UpstreamStreamEvent(event_name=event_name, payload=parsed)
        except ApiError:
            raise
        except httpx.ConnectTimeout as exc:
            raise self._timeout_error(
                "cloud_run_adk_stream_connect_timeout",
                "connect",
                exc,
                agent_config=agent_config,
                operation=operation,
                started_at=started_at,
                user_id=user_id,
                session_id=session_id,
            ) from exc
        except httpx.ReadTimeout as exc:
            raise self._timeout_error(
                "cloud_run_adk_stream_read_timeout",
                "read",
                exc,
                agent_config=agent_config,
                operation=operation,
                started_at=started_at,
                user_id=user_id,
                session_id=session_id,
            ) from exc
        except httpx.RequestError as exc:
            self._log_transport_error(
                agent_config=agent_config,
                operation=operation,
                exc=exc,
                started_at=started_at,
                user_id=user_id,
                session_id=session_id,
            )
            raise ApiError(
                502,
                "cloud_run_adk_stream_unreachable",
                "The gateway could not open a streaming connection to the Cloud Run ADK agent.",
                self._diagnostic_context(
                    agent_config=agent_config,
                    operation=operation,
                    started_at=started_at,
                    user_id=user_id,
                    session_id=session_id,
                    extra={"reason": safe_truncate(exc), "retryable": True},
                ),
            ) from exc

    async def _create_or_update_session(
        self,
        *,
        agent_config: AgentConfig,
        user_id: str,
        session_id: str,
    ) -> None:
        await self._post_json(
            agent_config=agent_config,
            path=(
                f"/apps/{quote(self._app_name(agent_config), safe='')}"
                f"/users/{quote(user_id, safe='')}"
                f"/sessions/{quote(session_id, safe='')}"
            ),
            payload={},
            error_code="cloud_run_adk_session_error",
            operation="session_create",
            user_id=user_id,
            session_id=session_id,
            conflict_ok=True,
        )

    def extract_text_fragments(self, event_payload: dict[str, Any]) -> list[str]:
        return self._extract_text_fragments(event_payload)

    async def _post_run_sse(
        self,
        *,
        agent_config: AgentConfig,
        user_id: str,
        session_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        headers = await self._authorized_headers(agent_config)
        operation = "run_sse"
        started_at = time.monotonic()
        url = f"{self._base_url(agent_config)}/run_sse"
        try:
            response = await self._http_client.post(
                url,
                headers=headers,
                json=payload,
            )
        except httpx.ConnectTimeout as exc:
            raise self._timeout_error(
                "cloud_run_adk_connect_timeout",
                "connect",
                exc,
                agent_config=agent_config,
                operation=operation,
                started_at=started_at,
                user_id=user_id,
                session_id=session_id,
            ) from exc
        except httpx.ReadTimeout as exc:
            raise self._timeout_error(
                "cloud_run_adk_read_timeout",
                "read",
                exc,
                agent_config=agent_config,
                operation=operation,
                started_at=started_at,
                user_id=user_id,
                session_id=session_id,
            ) from exc
        except httpx.RequestError as exc:
            self._log_transport_error(
                agent_config=agent_config,
                operation=operation,
                exc=exc,
                started_at=started_at,
                user_id=user_id,
                session_id=session_id,
            )
            raise ApiError(
                502,
                "cloud_run_adk_unreachable",
                "The gateway could not reach the Cloud Run ADK agent.",
                self._diagnostic_context(
                    agent_config=agent_config,
                    operation=operation,
                    started_at=started_at,
                    user_id=user_id,
                    session_id=session_id,
                    extra={"reason": safe_truncate(exc)},
                ),
            ) from exc

        if response.status_code >= 400:
            raise self._response_error(
                response,
                "cloud_run_adk_error",
                agent_config=agent_config,
                operation=operation,
                started_at=started_at,
                user_id=user_id,
                session_id=session_id,
            )
        log_structured(
            LOGGER,
            logging.INFO,
            "cloud_run_adk_request_completed",
            **self._diagnostic_context(
                agent_config=agent_config,
                operation=operation,
                started_at=started_at,
                user_id=user_id,
                session_id=session_id,
                extra={
                    "status_code": response.status_code,
                    "content_type": response.headers.get("content-type", ""),
                    "response_body_length": len(response.text or ""),
                },
            ),
        )
        return self._decode_run_response(
            response,
            agent_config=agent_config,
            operation=operation,
            started_at=started_at,
            user_id=user_id,
            session_id=session_id,
        )

    async def _post_json(
        self,
        *,
        agent_config: AgentConfig,
        path: str,
        payload: dict[str, Any],
        error_code: str,
        operation: str,
        user_id: str,
        session_id: str,
        conflict_ok: bool = False,
    ) -> dict[str, Any]:
        headers = await self._authorized_headers(agent_config)
        started_at = time.monotonic()
        url = f"{self._base_url(agent_config)}{path}"
        try:
            response = await self._http_client.post(
                url,
                headers=headers,
                json=payload,
            )
        except httpx.ConnectTimeout as exc:
            raise self._timeout_error(
                "cloud_run_adk_connect_timeout",
                "connect",
                exc,
                agent_config=agent_config,
                operation=operation,
                started_at=started_at,
                user_id=user_id,
                session_id=session_id,
            ) from exc
        except httpx.ReadTimeout as exc:
            raise self._timeout_error(
                "cloud_run_adk_read_timeout",
                "read",
                exc,
                agent_config=agent_config,
                operation=operation,
                started_at=started_at,
                user_id=user_id,
                session_id=session_id,
            ) from exc
        except httpx.RequestError as exc:
            self._log_transport_error(
                agent_config=agent_config,
                operation=operation,
                exc=exc,
                started_at=started_at,
                user_id=user_id,
                session_id=session_id,
            )
            raise ApiError(
                502,
                "cloud_run_adk_unreachable",
                "The gateway could not reach the Cloud Run ADK agent.",
                self._diagnostic_context(
                    agent_config=agent_config,
                    operation=operation,
                    started_at=started_at,
                    user_id=user_id,
                    session_id=session_id,
                    extra={"reason": safe_truncate(exc)},
                ),
            ) from exc

        if conflict_ok and response.status_code == 409:
            log_structured(
                LOGGER,
                logging.INFO,
                "cloud_run_adk_session_already_exists",
                **self._diagnostic_context(
                    agent_config=agent_config,
                    operation=operation,
                    started_at=started_at,
                    user_id=user_id,
                    session_id=session_id,
                    extra={"status_code": response.status_code},
                ),
            )
            return {}
        if response.status_code >= 400:
            raise self._response_error(
                response,
                error_code,
                agent_config=agent_config,
                operation=operation,
                started_at=started_at,
                user_id=user_id,
                session_id=session_id,
            )
        log_structured(
            LOGGER,
            logging.INFO,
            "cloud_run_adk_request_completed",
            **self._diagnostic_context(
                agent_config=agent_config,
                operation=operation,
                started_at=started_at,
                user_id=user_id,
                session_id=session_id,
                extra={
                    "status_code": response.status_code,
                    "content_type": response.headers.get("content-type", ""),
                    "response_body_length": len(response.text or ""),
                },
            ),
        )
        if not response.content:
            return {}
        try:
            return response.json()
        except ValueError as exc:
            details = self._invalid_json_details(
                response,
                agent_config=agent_config,
                operation=operation,
                started_at=started_at,
                user_id=user_id,
                session_id=session_id,
            )
            log_structured(
                LOGGER,
                logging.WARNING,
                "cloud_run_adk_invalid_json",
                **details,
            )
            raise ApiError(
                502,
                "invalid_cloud_run_adk_json",
                "Cloud Run ADK returned invalid JSON.",
                details,
            ) from exc

    def _decode_run_response(
        self,
        response: httpx.Response,
        *,
        agent_config: AgentConfig,
        operation: str,
        started_at: float,
        user_id: str,
        session_id: str,
    ) -> dict[str, Any]:
        content_type = response.headers.get("content-type", "")
        if "text/event-stream" in content_type:
            return {"output": self._parse_sse_text(response.text)}
        try:
            parsed = response.json()
        except ValueError as exc:
            details = self._invalid_json_details(
                response,
                agent_config=agent_config,
                operation=operation,
                started_at=started_at,
                user_id=user_id,
                session_id=session_id,
            )
            log_structured(
                LOGGER,
                logging.WARNING,
                "cloud_run_adk_invalid_json",
                **details,
            )
            raise ApiError(
                502,
                "invalid_cloud_run_adk_json",
                "Cloud Run ADK returned invalid JSON.",
                details,
            ) from exc
        if isinstance(parsed, dict):
            return parsed
        if isinstance(parsed, list):
            return {"output": parsed}
        return {"output": []}

    def _parse_sse_text(self, text: str) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        data_lines: list[str] = []
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                self._append_sse_payload(events, data_lines)
                data_lines = []
                continue
            if line.startswith(":") or line.startswith("event:"):
                continue
            if line.startswith("data:"):
                data_lines.append(line.partition(":")[2].strip())
                continue
            data_lines.append(line)
        self._append_sse_payload(events, data_lines)
        return events

    def _append_sse_payload(
        self,
        events: list[dict[str, Any]],
        data_lines: list[str],
    ) -> None:
        if not data_lines:
            return
        data = "\n".join(data_lines).strip()
        if not data or data == "[DONE]":
            return
        try:
            parsed = json.loads(data)
        except json.JSONDecodeError:
            return
        if isinstance(parsed, dict):
            events.append(parsed)
        elif isinstance(parsed, list):
            events.extend(item for item in parsed if isinstance(item, dict))

    async def _iter_sse_messages(self, response: httpx.Response):
        event_name: str | None = None
        data_lines: list[str] = []

        async for raw_line in response.aiter_lines():
            if raw_line is None:
                continue
            line = raw_line.strip()
            if not line:
                if data_lines:
                    yield event_name, "\n".join(data_lines)
                    event_name = None
                    data_lines = []
                continue
            if line.startswith(":"):
                continue
            if line.startswith("event:"):
                event_name = line.partition(":")[2].strip() or None
                continue
            if line.startswith("data:"):
                data_lines.append(line.partition(":")[2].strip())
                continue
            data_lines.append(line)

        if data_lines:
            yield event_name, "\n".join(data_lines)

    def _parse_json_messages(self, data: str) -> list[dict[str, Any]]:
        try:
            parsed = json.loads(data)
        except json.JSONDecodeError:
            parsed_messages: list[dict[str, Any]] = []
            for line in data.splitlines():
                stripped = line.strip()
                if not stripped or stripped == "[DONE]":
                    continue
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError:
                    continue
                if isinstance(parsed, dict):
                    parsed_messages.append(parsed)
            return parsed_messages

        if isinstance(parsed, dict):
            return [parsed]
        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]
        return []

    def _extract_event_payloads(self, response_payload: dict[str, Any]) -> list[dict[str, Any]]:
        output = response_payload.get("output", response_payload)
        if isinstance(output, list):
            return [item for item in output if isinstance(item, dict)]
        if isinstance(output, dict):
            events = output.get("events")
            if isinstance(events, list):
                return [item for item in events if isinstance(item, dict)]
            return [output]
        return []

    def _extract_text_fragments(self, event_payload: dict[str, Any]) -> list[str]:
        fragments: list[str] = []

        def collect_parts(container: dict[str, Any] | None) -> None:
            if not isinstance(container, dict):
                return
            parts = container.get("parts")
            if not isinstance(parts, list):
                return
            for part in parts:
                if isinstance(part, dict) and isinstance(part.get("text"), str):
                    fragments.append(part["text"])

        def walk(value: Any) -> None:
            if isinstance(value, list):
                for item in value:
                    walk(item)
                return
            if not isinstance(value, dict):
                return

            output = value.get("output")
            if isinstance(output, str):
                fragments.append(output)

            if value.get("role") == "model":
                collect_parts(value)

            content = value.get("content")
            if isinstance(content, dict):
                if content.get("role") == "model":
                    collect_parts(content)
                walk(content)

            for nested_key in (
                "result",
                "event",
                "message",
                "response",
                "data",
                "value",
                "payload",
                "output",
            ):
                nested_value = value.get(nested_key)
                if isinstance(nested_value, (dict, list)):
                    walk(nested_value)

        walk(event_payload)
        deduped: list[str] = []
        seen: set[str] = set()
        for fragment in fragments:
            if fragment not in seen:
                seen.add(fragment)
                deduped.append(fragment)
        return deduped

    def _empty_response_error(
        self,
        raw_events: list[dict[str, Any]],
        *,
        agent_config: AgentConfig,
        session_id: str,
    ) -> ApiError:
        details = {
            "agent_id": agent_config.agent_id,
            "operation": "run_sse",
            "app_name": self._app_name(agent_config),
            "base_url_host": safe_url_host(self._base_url(agent_config)),
            "session_id": session_id,
            "response_event_count": len(raw_events),
            "event_summaries": self._event_summaries(raw_events),
            "retryable": True,
        }
        log_structured(
            LOGGER,
            logging.WARNING,
            "cloud_run_adk_empty_response",
            **details,
        )
        return ApiError(
            502,
            "cloud_run_adk_empty_response",
            "Cloud Run ADK completed without an assistant reply.",
            details,
        )

    def _find_error_event(self, raw_events: list[dict[str, Any]]) -> dict[str, Any] | None:
        for event in raw_events:
            if isinstance(event, dict) and "error" in event:
                return event
        return None

    def _error_event_api_error(
        self,
        error_event: dict[str, Any],
        raw_events: list[dict[str, Any]],
        *,
        agent_config: AgentConfig,
        session_id: str,
    ) -> ApiError:
        error_payload = error_event.get("error")
        upstream_status_code = self._error_status_code(error_payload)
        upstream_status = self._error_status(error_payload)
        safe_reason = self._error_reason(error_payload)
        is_resource_exhausted = self._is_resource_exhausted_error(
            upstream_status_code=upstream_status_code,
            upstream_status=upstream_status,
            safe_reason=safe_reason,
        )
        code = (
            "cloud_run_adk_resource_exhausted"
            if is_resource_exhausted
            else "cloud_run_adk_error_event"
        )
        details = {
            "agent_id": agent_config.agent_id,
            "operation": "run_sse",
            "app_name": self._app_name(agent_config),
            "base_url_host": safe_url_host(self._base_url(agent_config)),
            "session_id": session_id,
            "response_event_count": len(raw_events),
            "event_summaries": self._event_summaries(raw_events),
            "upstream_status_code": upstream_status_code,
            "upstream_error_status": upstream_status,
            "safe_reason": safe_reason,
            "retryable": True,
        }
        log_structured(
            LOGGER,
            logging.WARNING,
            code,
            **details,
        )
        return ApiError(
            503 if is_resource_exhausted else 502,
            code,
            (
                "The Cloud Run ADK agent is temporarily capacity limited."
                if is_resource_exhausted
                else "Cloud Run ADK returned an error event."
            ),
            details,
        )

    def _error_status_code(self, error_payload: Any) -> int | None:
        if isinstance(error_payload, dict):
            value = error_payload.get("code") or error_payload.get("status_code")
            try:
                return int(value) if value is not None else None
            except (TypeError, ValueError):
                return None
        return None

    def _error_status(self, error_payload: Any) -> str | None:
        if isinstance(error_payload, dict):
            value = error_payload.get("status") or error_payload.get("statusText")
            if value is not None:
                return safe_truncate(value, limit=120)
        return None

    def _error_reason(self, error_payload: Any) -> str | None:
        if isinstance(error_payload, dict):
            value = error_payload.get("message") or error_payload.get("detail")
            if value is not None:
                return safe_truncate(value, limit=300)
        if error_payload is not None:
            return safe_truncate(error_payload, limit=300)
        return None

    def _is_resource_exhausted_error(
        self,
        *,
        upstream_status_code: int | None,
        upstream_status: str | None,
        safe_reason: str | None,
    ) -> bool:
        if upstream_status_code == 429:
            return True
        text = f"{upstream_status or ''} {safe_reason or ''}".lower()
        return any(term in text for term in RESOURCE_EXHAUSTED_TERMS)

    def _event_summaries(self, raw_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            self._event_summary(event)
            for event in raw_events[:EVENT_SUMMARY_LIMIT]
            if isinstance(event, dict)
        ]

    def _event_summary(self, event: dict[str, Any]) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "keys": sorted(str(key) for key in event.keys())[:20],
        }
        for key in ("author", "role", "finishReason", "finish_reason", "partial"):
            if key in event:
                summary[key] = event.get(key)

        content = event.get("content")
        if isinstance(content, dict):
            summary["content_keys"] = sorted(str(key) for key in content.keys())[:20]
            if "role" in content:
                summary["content_role"] = content.get("role")
            parts = content.get("parts")
            if isinstance(parts, list):
                summary["content_parts_count"] = len(parts)
                summary["content_part_keys"] = [
                    sorted(str(key) for key in part.keys())[:20]
                    for part in parts[:EVENT_SUMMARY_LIMIT]
                    if isinstance(part, dict)
                ]

        usage_metadata = event.get("usage_metadata")
        if isinstance(usage_metadata, dict):
            summary["usage_metadata_keys"] = sorted(
                str(key) for key in usage_metadata.keys()
            )[:20]

        error = event.get("error")
        if isinstance(error, dict):
            summary["error_keys"] = sorted(str(key) for key in error.keys())[:20]
            summary["error_code"] = self._error_status_code(error)
            summary["error_status"] = self._error_status(error)
            summary["error_message_preview"] = self._error_reason(error)
        elif error is not None:
            summary["error_type"] = type(error).__name__
            summary["error_message_preview"] = self._error_reason(error)
        return summary

    async def _authorized_headers(self, agent_config: AgentConfig) -> dict[str, str]:
        token = await self._id_token(agent_config)
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    async def _id_token(self, agent_config: AgentConfig) -> str:
        audience = agent_config.audience or self._base_url(agent_config)
        now = time.monotonic()
        cached = self._token_cache.get(audience)
        if cached is not None and cached.expires_at_monotonic > now:
            return cached.token

        async with self._auth_lock:
            cached = self._token_cache.get(audience)
            if cached is not None and cached.expires_at_monotonic > now:
                return cached.token
            token = await asyncio.to_thread(
                id_token.fetch_id_token,
                GoogleAuthRequest(),
                audience,
            )
            if not token:
                raise ApiError(
                    500,
                    "missing_cloud_run_identity_token",
                    "Unable to obtain an identity token for the Cloud Run ADK agent.",
                )
            self._token_cache[audience] = _TokenCacheEntry(
                audience=audience,
                token=token,
                expires_at_monotonic=now + 2700,
            )
            return token

    def _base_url(self, agent_config: AgentConfig) -> str:
        if not agent_config.base_url:
            raise ApiError(
                500,
                "missing_cloud_run_adk_base_url",
                "The Cloud Run ADK agent is missing base_url configuration.",
            )
        return agent_config.base_url.rstrip("/")

    def _app_name(self, agent_config: AgentConfig) -> str:
        if not agent_config.app_name:
            raise ApiError(
                500,
                "missing_cloud_run_adk_app_name",
                "The Cloud Run ADK agent is missing app_name configuration.",
            )
        return agent_config.app_name

    def _timeout_error(
        self,
        code: str,
        timeout_type: str,
        exc: Exception,
        *,
        agent_config: AgentConfig,
        operation: str,
        started_at: float,
        user_id: str,
        session_id: str,
    ) -> ApiError:
        timeout_seconds = (
            self.connect_timeout_seconds
            if timeout_type == "connect"
            else self.read_timeout_seconds
        )
        details = self._diagnostic_context(
            agent_config=agent_config,
            operation=operation,
            started_at=started_at,
            user_id=user_id,
            session_id=session_id,
            extra={
                "reason": safe_truncate(exc),
                "timeout_seconds": timeout_seconds,
                "timeout_type": timeout_type,
                "retryable": True,
            },
        )
        log_structured(
            LOGGER,
            logging.WARNING,
            "cloud_run_adk_request_timeout",
            **details,
        )
        return ApiError(
            504,
            code,
            "The gateway timed out while calling the Cloud Run ADK agent.",
            details,
        )

    def _response_error(
        self,
        response: httpx.Response,
        code: str,
        *,
        agent_config: AgentConfig,
        operation: str,
        started_at: float,
        user_id: str,
        session_id: str,
    ) -> ApiError:
        retryable = is_retryable_http_status(response.status_code)
        details = self._diagnostic_context(
            agent_config=agent_config,
            operation=operation,
            started_at=started_at,
            user_id=user_id,
            session_id=session_id,
            extra={
                **safe_http_response_details(response),
                "retryable": retryable,
            },
        )
        log_structured(
            LOGGER,
            logging.WARNING if retryable else logging.ERROR,
            "cloud_run_adk_non_success_response",
            **details,
        )
        return ApiError(
            502,
            code,
            "Cloud Run ADK returned a non-success response.",
            details,
        )

    async def _stream_response_error(
        self,
        response: httpx.Response,
        *,
        agent_config: AgentConfig,
        operation: str,
        started_at: float,
        user_id: str,
        session_id: str,
    ) -> ApiError:
        raw_body = await response.aread()
        text = raw_body.decode("utf-8", errors="replace")
        try:
            parsed_body = json.loads(text)
            body_details = safe_response_body_details(
                status_code=response.status_code,
                content_type=response.headers.get("content-type", ""),
                text=text,
                parsed_body=parsed_body,
            )
        except json.JSONDecodeError:
            parsed_body = None
            body_details = safe_response_body_details(
                status_code=response.status_code,
                content_type=response.headers.get("content-type", ""),
                text=text,
                parse_failed=True,
            )
        details = self._diagnostic_context(
            agent_config=agent_config,
            operation=operation,
            started_at=started_at,
            user_id=user_id,
            session_id=session_id,
            extra={
                **body_details,
                "retryable": is_retryable_http_status(response.status_code),
            },
        )
        log_structured(
            LOGGER,
            logging.WARNING if details["retryable"] else logging.ERROR,
            "cloud_run_adk_stream_non_success_response",
            **details,
        )
        return ApiError(
            502,
            "cloud_run_adk_stream_error",
            "Cloud Run ADK returned a non-success response for streaming.",
            details,
        )

    def _invalid_json_details(
        self,
        response: httpx.Response,
        *,
        agent_config: AgentConfig,
        operation: str,
        started_at: float,
        user_id: str,
        session_id: str,
    ) -> dict[str, Any]:
        return self._diagnostic_context(
            agent_config=agent_config,
            operation=operation,
            started_at=started_at,
            user_id=user_id,
            session_id=session_id,
            extra={
                **safe_http_response_details(response),
                "retryable": True,
            },
        )

    def _log_transport_error(
        self,
        *,
        agent_config: AgentConfig,
        operation: str,
        exc: Exception,
        started_at: float,
        user_id: str,
        session_id: str,
    ) -> None:
        log_structured(
            LOGGER,
            logging.WARNING,
            "cloud_run_adk_transport_error",
            **self._diagnostic_context(
                agent_config=agent_config,
                operation=operation,
                started_at=started_at,
                user_id=user_id,
                session_id=session_id,
                extra={"reason": safe_truncate(exc), "retryable": True},
            ),
        )

    def _diagnostic_context(
        self,
        *,
        agent_config: AgentConfig,
        operation: str,
        started_at: float,
        user_id: str,
        session_id: str,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        context = {
            "agent_id": agent_config.agent_id,
            "operation": operation,
            "app_name": self._app_name(agent_config),
            "base_url_host": safe_url_host(self._base_url(agent_config)),
            "user_id": user_id,
            "session_id": session_id,
            "upstream_elapsed_ms": int((time.monotonic() - started_at) * 1000),
        }
        if extra:
            context.update(extra)
        return context


async def get_cloud_run_adk_client() -> CloudRunAdkClient:
    global _client_singleton
    if _client_singleton is None:
        async with _client_lock:
            if _client_singleton is None:
                from services.agent_gateway_v3.app.core.config import get_settings

                settings = get_settings()
                _client_singleton = CloudRunAdkClient(
                    connect_timeout_seconds=settings.upstream_connect_timeout_seconds,
                    read_timeout_seconds=settings.upstream_read_timeout_seconds,
                    limits=httpx.Limits(
                        max_connections=settings.http_max_connections,
                        max_keepalive_connections=settings.http_max_keepalive_connections,
                        keepalive_expiry=settings.http_keepalive_expiry_seconds,
                    ),
                )
    return _client_singleton



async def close_cloud_run_adk_client() -> None:
    global _client_singleton
    if _client_singleton is not None:
        await _client_singleton.close()
        _client_singleton = None
