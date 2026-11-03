"""Async Agent Runtime client implementation for the gateway."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any

import google.auth
import httpx
from google.auth.credentials import Credentials
from google.auth.transport.requests import Request as GoogleAuthRequest

from common.ids import new_thread_id
from common.schemas import AgentConfig, ChatRequest
from services.agent_gateway_v3.app.core.errors import ApiError
from services.agent_gateway_v3.app.services.turn_assembler import TurnAssembler


AUTH_SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)
STREAM_RUN_CONFIG = {"streaming_mode": "sse"}
BUFFERED_RUN_CONFIG = {"streaming_mode": None}

_client_singleton: "AgentRuntimeClient | None" = None
_client_lock = asyncio.Lock()


@dataclass(frozen=True)
class SessionResult:
    session_id: str
    thread_id: str


@dataclass(frozen=True)
class BufferedAgentResponse:
    reply_text: str
    raw_events: list[dict[str, Any]]
    usage: dict[str, Any] = field(default_factory=dict)
    raw_usage_reports: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class UpstreamStreamEvent:
    event_name: str | None
    payload: dict[str, Any]


class AgentRuntimeClient:
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
                max_connections=100,
                max_keepalive_connections=50,
                keepalive_expiry=30.0,
            ),
        )
        self._credentials: Credentials | None = None
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
                "Existing thread sessions must be resolved before reaching Agent Runtime.",
            )
        thread_id = new_thread_id()

        session_id = None
        # 1. Try class_method: async_create_session via :query first
        try:
            response_payload = await self._post_json(
                url=self._build_query_url(agent_config),
                payload={
                    "class_method": "async_create_session",
                    "input": {"user_id": user_id},
                },
            )
            session_payload = response_payload.get("output", response_payload)
            session_id = str(session_payload.get("id", "")).strip()
        except ApiError as exc:
            # If :query returned 404/502 (reasoning engine uses native Agent Platform REST API),
            # fall back to native Agent Runtime session creation
            if exc.status_code in {404, 502}:
                session_id = await self._create_native_session(agent_config, user_id)
            else:
                raise

        if not session_id:
            session_id = await self._create_native_session(agent_config, user_id)

        if not session_id:
            raise ApiError(
                502,
                "missing_session_id",
                "Agent Runtime did not return a session id.",
            )
        return SessionResult(session_id=session_id, thread_id=thread_id)

    async def _create_native_session(
        self,
        agent_config: AgentConfig,
        user_id: str,
    ) -> str:
        url = self._build_sessions_url(agent_config)
        response_payload = await self._post_json(
            url=url,
            payload={"user_id": user_id},
        )
        session_info = response_payload.get("response", response_payload)
        session_name = str(session_info.get("name", "") or response_payload.get("name", "")).strip()
        if session_name:
            return session_name.rstrip("/").split("/")[-1]
        session_id = str(session_info.get("id", "") or response_payload.get("id", "")).strip()
        return session_id

    async def chat(
        self,
        *,
        agent_config: AgentConfig,
        user_id: str,
        session_id: str,
        message: str,
    ) -> BufferedAgentResponse:
        return await self.chat_buffered_query(
            agent_config=agent_config,
            user_id=user_id,
            session_id=session_id,
            message=message,
        )

    async def chat_buffered_query(
        self,
        *,
        agent_config: AgentConfig,
        user_id: str,
        session_id: str,
        message: str,
    ) -> BufferedAgentResponse:
        run_config: dict[str, Any] = {"streaming_mode": None}
        if agent_config.max_output_tokens is not None:
            run_config["max_output_tokens"] = agent_config.max_output_tokens
            run_config["generation_config"] = {"max_output_tokens": agent_config.max_output_tokens}

        try:
            response_payload = await self._post_json(
                url=self._build_query_url(agent_config),
                payload={
                    "class_method": "async_buffered_query",
                    "input": {
                        "user_id": user_id,
                        "session_id": session_id,
                        "message": message,
                        "run_config": run_config,
                    },
                },
            )
            raw_events = self._extract_event_payloads(response_payload)
            assembler = TurnAssembler(model_name=agent_config.model)
            for event in raw_events:
                assembler.add_event(event)
                for fragment in self._extract_text_fragments(event):
                    assembler.add_text(fragment)

            return BufferedAgentResponse(
                reply_text=assembler.reply_text(),
                raw_events=raw_events,
                usage=assembler.usage,
                raw_usage_reports=assembler.raw_usage_reports,
            )
        except ApiError as exc:
            if exc.status_code in {404, 502}:
                # Fall back to stream aggregation for streaming-first ADK agents
                assembler = TurnAssembler(model_name=agent_config.model)
                raw_events = []
                async for stream_event in self.stream_chat_events(
                    agent_config=agent_config,
                    user_id=user_id,
                    session_id=session_id,
                    message=message,
                ):
                    raw_events.append(stream_event.payload)
                    assembler.add_event(stream_event.payload)
                    for fragment in self._extract_text_fragments(stream_event.payload):
                        assembler.add_text(fragment)
                return BufferedAgentResponse(
                    reply_text=assembler.reply_text(),
                    raw_events=raw_events,
                    usage=assembler.usage,
                    raw_usage_reports=assembler.raw_usage_reports,
                )
            raise

    async def stream_chat_events(
        self,
        *,
        agent_config: AgentConfig,
        user_id: str,
        session_id: str,
        message: str,
    ):
        headers = await self._authorized_headers()
        run_config: dict[str, Any] = {"streaming_mode": "sse"}
        if agent_config.max_output_tokens is not None:
            run_config["max_output_tokens"] = agent_config.max_output_tokens
            run_config["generation_config"] = {"max_output_tokens": agent_config.max_output_tokens}

        payload = {
            "class_method": "async_stream_query",
            "input": {
                "user_id": user_id,
                "session_id": session_id,
                "message": message,
                "run_config": run_config,
            },
        }

        try:
            async with self._http_client.stream(
                "POST",
                self._build_stream_query_url(agent_config),
                headers=headers,
                json=payload,
            ) as response:
                if response.status_code >= 400:
                    # If async_stream_query fails, try standard stream_query fallback (without run_config for standard ADK compatibility)
                    fallback_payload = {
                        "class_method": "stream_query",
                        "input": {
                            "user_id": user_id,
                            "session_id": session_id,
                            "message": message,
                        },
                    }
                    async with self._http_client.stream(
                        "POST",
                        self._build_stream_query_url(agent_config),
                        headers=headers,
                        json=fallback_payload,
                    ) as fallback_response:
                        if fallback_response.status_code >= 400:
                            raise await self._stream_error(fallback_response)
                        async for event_name, data in self._iter_sse_messages(fallback_response):
                            if not data or data == "[DONE]":
                                continue
                            for parsed in self._parse_json_messages(data):
                                yield UpstreamStreamEvent(event_name=event_name, payload=parsed)
                        return

                async for event_name, data in self._iter_sse_messages(response):
                    if not data or data == "[DONE]":
                        continue
                    for parsed in self._parse_json_messages(data):
                        yield UpstreamStreamEvent(event_name=event_name, payload=parsed)
        except ApiError:
            raise
        except httpx.ConnectTimeout as exc:
            raise ApiError(
                504,
                "agent_runtime_stream_connect_timeout",
                "The gateway timed out while opening a streaming connection to Agent Runtime.",
                {
                    "reason": str(exc),
                    "timeout_seconds": self.connect_timeout_seconds,
                    "timeout_type": "connect",
                },
            ) from exc
        except httpx.ReadTimeout as exc:
            raise ApiError(
                504,
                "agent_runtime_stream_read_timeout",
                "The gateway timed out while waiting for Agent Runtime to send stream data.",
                {
                    "reason": str(exc),
                    "timeout_seconds": self.read_timeout_seconds,
                    "timeout_type": "read",
                },
            ) from exc
        except httpx.RequestError as exc:
            raise ApiError(
                502,
                "agent_runtime_stream_unreachable",
                "The gateway could not open a streaming connection to Agent Runtime.",
                {"reason": str(exc)},
            ) from exc

    def extract_text_fragments(self, event_payload: dict[str, Any]) -> list[str]:
        return self._extract_text_fragments(event_payload)

    async def _post_json(self, *, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        headers = await self._authorized_headers()
        try:
            response = await self._http_client.post(url, headers=headers, json=payload)
        except httpx.RequestError as exc:
            raise ApiError(
                502,
                "agent_runtime_unreachable",
                "The gateway could not reach Agent Runtime.",
                {"reason": str(exc)},
            ) from exc

        if response.status_code >= 400:
            raise self._response_error(response, "agent_runtime_error")

        try:
            return response.json()
        except ValueError as exc:
            raise ApiError(
                502,
                "invalid_agent_runtime_json",
                "Agent Runtime returned invalid JSON.",
                {"body": response.text},
            ) from exc

    async def _authorized_headers(self) -> dict[str, str]:
        token = await self._access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    async def _access_token(self) -> str:
        async with self._auth_lock:
            if self._credentials is None:
                self._credentials = await asyncio.to_thread(self._default_credentials)

            if not self._credentials.valid or not self._credentials.token:
                await asyncio.to_thread(self._credentials.refresh, GoogleAuthRequest())

            token = self._credentials.token
            if not token:
                raise ApiError(
                    500,
                    "missing_google_access_token",
                    "Unable to obtain a Google access token for Agent Runtime.",
                )
            return token

    def _default_credentials(self) -> Credentials:
        credentials, _ = google.auth.default(scopes=AUTH_SCOPES)
        return credentials

    def _build_query_url(self, agent_config: AgentConfig) -> str:
        if not agent_config.resource_name:
            raise ApiError(
                500,
                "missing_agent_runtime_resource_name",
                "The Agent Runtime agent is missing resource_name configuration.",
            )
        return (
            f"https://{agent_config.region}-aiplatform.googleapis.com/v1/"
            f"{agent_config.resource_name}:query"
        )

    def _build_stream_query_url(self, agent_config: AgentConfig) -> str:
        if not agent_config.resource_name:
            raise ApiError(
                500,
                "missing_agent_runtime_resource_name",
                "The Agent Runtime agent is missing resource_name configuration.",
            )
        return (
            f"https://{agent_config.region}-aiplatform.googleapis.com/v1/"
            f"{agent_config.resource_name}:streamQuery?alt=sse"
        )

    def _build_sessions_url(self, agent_config: AgentConfig) -> str:
        if not agent_config.resource_name:
            raise ApiError(
                500,
                "missing_agent_runtime_resource_name",
                "The Agent Runtime agent is missing resource_name configuration.",
            )
        return (
            f"https://{agent_config.region}-aiplatform.googleapis.com/v1/"
            f"{agent_config.resource_name}/sessions"
        )

    async def _iter_sse_messages(self, response: httpx.Response):
        event_name: str | None = None
        data_lines: list[str] = []

        async for line in response.aiter_lines():
            if line is None:
                continue
            if line == "":
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
            data_lines.append(line.strip())

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

            role = value.get("role")
            if role == "model":
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

        deduped_fragments: list[str] = []
        seen: set[str] = set()
        for fragment in fragments:
            if fragment not in seen:
                seen.add(fragment)
                deduped_fragments.append(fragment)
        return deduped_fragments

    def _response_error(self, response: httpx.Response, code: str) -> ApiError:
        try:
            body = response.json()
        except ValueError:
            body = {"raw": response.text}
        return ApiError(
            502,
            code,
            "Agent Runtime returned a non-success response.",
            {"status_code": response.status_code, "body": body},
        )

    async def _stream_error(self, response: httpx.Response) -> ApiError:
        raw_body = await response.aread()
        try:
            body = json.loads(raw_body.decode("utf-8"))
        except Exception:
            body = {"raw": raw_body.decode("utf-8", errors="replace")}
        return ApiError(
            502,
            "agent_runtime_stream_error",
            "Agent Runtime returned a non-success response for streamQuery.",
            {"status_code": response.status_code, "body": body},
        )


async def get_agent_runtime_client() -> AgentRuntimeClient:
    global _client_singleton
    if _client_singleton is None:
        async with _client_lock:
            if _client_singleton is None:
                from services.agent_gateway_v3.app.core.config import get_settings

                settings = get_settings()
                _client_singleton = AgentRuntimeClient(
                    connect_timeout_seconds=settings.upstream_connect_timeout_seconds,
                    read_timeout_seconds=settings.upstream_read_timeout_seconds,
                    limits=httpx.Limits(
                        max_connections=settings.http_max_connections,
                        max_keepalive_connections=settings.http_max_keepalive_connections,
                        keepalive_expiry=settings.http_keepalive_expiry_seconds,
                    ),
                )
    return _client_singleton



async def close_agent_runtime_client() -> None:
    global _client_singleton
    if _client_singleton is not None:
        await _client_singleton.close()
        _client_singleton = None
