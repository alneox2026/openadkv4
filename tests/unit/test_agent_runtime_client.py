from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import httpx
import pytest

from common.schemas import AgentConfig
from services.agent_gateway_v3.app.core.config import get_settings
from services.agent_gateway_v3.app.core.errors import ApiError
from services.agent_gateway_v3.app.services.agent_runtime_client import (
    AgentRuntimeClient,
    BUFFERED_RUN_CONFIG,
    STREAM_RUN_CONFIG,
    close_agent_runtime_client,
    get_agent_runtime_client,
)


class _FakeStreamResponse:
    def __init__(self, lines: list[str] | None = None) -> None:
        self._lines = lines or [
            "event: message",
            'data: {"content":{"role":"model","parts":[{"text":"hello"}]}}',
            "",
        ]

    status_code = 200

    async def __aenter__(self) -> "_FakeStreamResponse":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False

    async def aiter_lines(self) -> AsyncIterator[str]:
        for line in self._lines:
            yield line

    async def aread(self) -> bytes:
        return b""


class _FakeJsonResponse:
    def __init__(self, payload: dict | None = None, status_code: int = 200) -> None:
        self._payload = payload or {
            "output": [
                {"content": {"role": "model", "parts": [{"text": "echo:hello"}]}}
            ]
        }
        self.status_code = status_code
        self.text = ""

    def json(self) -> dict:
        return self._payload


class _RecordingAsyncClient:
    def __init__(
        self,
        response: _FakeStreamResponse | None = None,
        json_response: _FakeJsonResponse | None = None,
    ) -> None:
        self.stream_calls: list[dict[str, object]] = []
        self.post_calls: list[dict[str, object]] = []
        self._response = response or _FakeStreamResponse()
        self._json_response = json_response or _FakeJsonResponse()

    def stream(self, method: str, url: str, headers=None, json=None):
        self.stream_calls.append(
            {
                "method": method,
                "url": url,
                "headers": headers,
                "json": json,
            }
        )
        return self._response

    async def post(self, url: str, headers=None, json=None):
        self.post_calls.append(
            {
                "url": url,
                "headers": headers,
                "json": json,
            }
        )
        return self._json_response

    async def aclose(self) -> None:
        return None


class _FailingStreamResponse:
    def __init__(self, exc: httpx.RequestError, *, raise_on_enter: bool) -> None:
        self._exc = exc
        self._raise_on_enter = raise_on_enter

    status_code = 200

    async def __aenter__(self) -> "_FailingStreamResponse":
        if self._raise_on_enter:
            raise self._exc
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False

    async def aiter_lines(self) -> AsyncIterator[str]:
        raise self._exc
        yield ""  # pragma: no cover

    async def aread(self) -> bytes:
        return b""


class _FailingAsyncClient:
    def __init__(self, exc: httpx.RequestError, *, raise_on_enter: bool) -> None:
        self._exc = exc
        self._raise_on_enter = raise_on_enter

    def stream(self, method: str, url: str, headers=None, json=None):
        return _FailingStreamResponse(self._exc, raise_on_enter=self._raise_on_enter)

    async def aclose(self) -> None:
        return None


async def _fake_authorized_headers() -> dict[str, str]:
    return {
        "Authorization": "Bearer test-token",
        "Content-Type": "application/json",
    }


def _agent_config() -> AgentConfig:
    return AgentConfig(
        agent_id="maxima",
        resource_name="projects/test/locations/us-central1/reasoningEngines/123",
        region="us-central1",
    )


async def _collect_stream(runtime_client: AgentRuntimeClient) -> None:
    runtime_client._authorized_headers = _fake_authorized_headers  # type: ignore[method-assign]
    async for _ in runtime_client.stream_chat_events(
        agent_config=_agent_config(),
        user_id="user-1",
        session_id="session-1",
        message="hello",
    ):
        pass


def test_get_agent_runtime_client_uses_configured_timeouts(monkeypatch) -> None:
    async def _run() -> None:
        await close_agent_runtime_client()
        get_settings.cache_clear()
        monkeypatch.setenv("UPSTREAM_CONNECT_TIMEOUT_SECONDS", "3")
        monkeypatch.setenv("UPSTREAM_READ_TIMEOUT_SECONDS", "123")

        runtime_client = await get_agent_runtime_client()

        assert runtime_client.connect_timeout_seconds == 3
        assert runtime_client.read_timeout_seconds == 123

        await close_agent_runtime_client()
        get_settings.cache_clear()

    asyncio.run(_run())


def test_stream_chat_events_maps_connect_timeout() -> None:
    async def _run() -> None:
        request = httpx.Request("POST", "https://example.test")
        runtime_client = AgentRuntimeClient(
            connect_timeout_seconds=7,
            read_timeout_seconds=99,
            http_client=_FailingAsyncClient(
                httpx.ConnectTimeout("connect timed out", request=request),
                raise_on_enter=True,
            ),
        )

        with pytest.raises(ApiError) as exc_info:
            await _collect_stream(runtime_client)

        assert exc_info.value.status_code == 504
        assert exc_info.value.code == "agent_runtime_stream_connect_timeout"
        assert exc_info.value.details["timeout_type"] == "connect"
        assert exc_info.value.details["timeout_seconds"] == 7

    asyncio.run(_run())


def test_stream_chat_events_maps_read_timeout() -> None:
    async def _run() -> None:
        request = httpx.Request("POST", "https://example.test")
        runtime_client = AgentRuntimeClient(
            connect_timeout_seconds=7,
            read_timeout_seconds=99,
            http_client=_FailingAsyncClient(
                httpx.ReadTimeout("read timed out", request=request),
                raise_on_enter=False,
            ),
        )

        with pytest.raises(ApiError) as exc_info:
            await _collect_stream(runtime_client)

        assert exc_info.value.status_code == 504
        assert exc_info.value.code == "agent_runtime_stream_read_timeout"
        assert exc_info.value.details["timeout_type"] == "read"
        assert exc_info.value.details["timeout_seconds"] == 99

    asyncio.run(_run())


def test_stream_chat_events_maps_generic_request_error() -> None:
    async def _run() -> None:
        request = httpx.Request("POST", "https://example.test")
        runtime_client = AgentRuntimeClient(
            http_client=_FailingAsyncClient(
                httpx.ConnectError("network unavailable", request=request),
                raise_on_enter=True,
            ),
        )

        with pytest.raises(ApiError) as exc_info:
            await _collect_stream(runtime_client)

        assert exc_info.value.status_code == 502
        assert exc_info.value.code == "agent_runtime_stream_unreachable"

    asyncio.run(_run())


def test_stream_chat_events_requests_sse_run_config() -> None:
    async def _run() -> None:
        http_client = _RecordingAsyncClient()
        runtime_client = AgentRuntimeClient(http_client=http_client)

        runtime_client._authorized_headers = _fake_authorized_headers  # type: ignore[method-assign]

        events = [
            event
            async for event in runtime_client.stream_chat_events(
                agent_config=_agent_config(),
                user_id="user-1",
                session_id="session-1",
                message="hello",
            )
        ]

        assert len(events) == 1
        assert len(http_client.stream_calls) == 1

        stream_call = http_client.stream_calls[0]
        assert stream_call["method"] == "POST"
        assert stream_call["json"] == {
            "class_method": "async_stream_query",
            "input": {
                "user_id": "user-1",
                "session_id": "session-1",
                "message": "hello",
                "run_config": {
                    "streaming_mode": "sse",
                    "max_output_tokens": 8192,
                    "generation_config": {"max_output_tokens": 8192},
                },
            },
        }

    asyncio.run(_run())


def test_stream_chat_events_parses_multiple_json_objects_from_one_sse_message() -> None:
    async def _run() -> None:
        http_client = _RecordingAsyncClient(
            response=_FakeStreamResponse(
                lines=[
                    "event: message",
                    'data: {"content":{"role":"model","parts":[{"text":"echo:"}]}}',
                    'data: {"content":{"role":"model","parts":[{"text":"echo:hello"}]}}',
                    "",
                ]
            )
        )
        runtime_client = AgentRuntimeClient(http_client=http_client)

        runtime_client._authorized_headers = _fake_authorized_headers  # type: ignore[method-assign]

        events = [
            event
            async for event in runtime_client.stream_chat_events(
                agent_config=_agent_config(),
                user_id="user-1",
                session_id="session-1",
                message="hello",
            )
        ]

        assert [event.payload["content"]["parts"][0]["text"] for event in events] == [
            "echo:",
            "echo:hello",
        ]

    asyncio.run(_run())


def test_buffered_chat_requests_non_streaming_run_config() -> None:
    async def _run() -> None:
        http_client = _RecordingAsyncClient(
            json_response=_FakeJsonResponse(
                payload={
                    "output": [
                        {
                            "content": {
                                "role": "model",
                                "parts": [{"text": "echo:"}],
                            }
                        },
                        {
                            "content": {
                                "role": "model",
                                "parts": [{"text": "echo:hello"}],
                            },
                            "usage_metadata": {
                                "prompt_token_count": 10,
                                "candidates_token_count": 4,
                                "thoughts_token_count": 2,
                                "total_token_count": 16,
                            },
                        },
                    ]
                }
            )
        )
        runtime_client = AgentRuntimeClient(http_client=http_client)

        runtime_client._authorized_headers = _fake_authorized_headers  # type: ignore[method-assign]

        response = await runtime_client.chat(
            agent_config=_agent_config(),
            user_id="user-1",
            session_id="session-1",
            message="hello",
        )

        assert response.reply_text == "echo:hello"
        assert response.usage["total_token_count"] == 16
        assert "estimated_cost_usd" not in response.usage
        assert len(response.raw_usage_reports) == 1
        assert len(response.raw_events) == 2
        assert len(http_client.stream_calls) == 0
        assert len(http_client.post_calls) == 1
        post_call = http_client.post_calls[0]
        assert post_call["json"] == {
            "class_method": "async_buffered_query",
            "input": {
                "user_id": "user-1",
                "session_id": "session-1",
                "message": "hello",
                "run_config": {
                    "streaming_mode": None,
                    "max_output_tokens": 8192,
                    "generation_config": {"max_output_tokens": 8192},
                },
            },
        }

    asyncio.run(_run())
