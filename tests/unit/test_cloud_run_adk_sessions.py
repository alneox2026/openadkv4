import asyncio

import httpx
import pytest

from common.schemas import ThreadDeleteRequestedEvent
from services.agent_persistence_worker_v3.app.core.errors import RetryableWorkerError
from services.agent_persistence_worker_v3.app.services.cloud_run_adk_sessions import (
    CloudRunAdkSessionNotFoundError,
    CloudRunAdkSessionsClient,
)


def _event() -> ThreadDeleteRequestedEvent:
    return ThreadDeleteRequestedEvent(
        event_id="evt-delete",
        agent_id="maxima_cloudrun",
        agent_backend="cloud_run_adk",
        agent_region="us-central1",
        agent_base_url="https://maxima-cloudrun-canary.example.run.app",
        agent_app_name="app",
        runtime_session_cleanup="cloud_run_adk",
        user_id="user-1",
        thread_id="thread-1",
        session_id="session-1",
    )


def test_delete_session_calls_cloud_run_adk_session_endpoint(monkeypatch) -> None:
    calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(204)

    monkeypatch.setattr(
        "services.agent_persistence_worker_v3.app.services.cloud_run_adk_sessions.id_token.fetch_id_token",
        lambda request, audience: "identity-token",
    )
    client = CloudRunAdkSessionsClient(
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    asyncio.run(client.delete_session(_event()))

    assert str(calls[0].url) == (
        "https://maxima-cloudrun-canary.example.run.app"
        "/apps/app/users/user-1/sessions/session-1"
    )
    assert calls[0].headers["authorization"] == "Bearer identity-token"


def test_delete_session_treats_404_as_already_deleted(monkeypatch) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "not found"})

    monkeypatch.setattr(
        "services.agent_persistence_worker_v3.app.services.cloud_run_adk_sessions.id_token.fetch_id_token",
        lambda request, audience: "identity-token",
    )
    client = CloudRunAdkSessionsClient(
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    with pytest.raises(CloudRunAdkSessionNotFoundError):
        asyncio.run(client.delete_session(_event()))


def test_delete_session_non_success_is_retryable(monkeypatch) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="unavailable")

    monkeypatch.setattr(
        "services.agent_persistence_worker_v3.app.services.cloud_run_adk_sessions.id_token.fetch_id_token",
        lambda request, audience: "identity-token",
    )
    client = CloudRunAdkSessionsClient(
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    with pytest.raises(RetryableWorkerError):
        asyncio.run(client.delete_session(_event()))


def test_delete_session_non_success_redacts_sensitive_body(monkeypatch) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            500,
            json={
                "detail": {
                    "message": "secret user prompt",
                    "text": "secret assistant response",
                    "safe_reason": "delete failed",
                }
            },
        )

    monkeypatch.setattr(
        "services.agent_persistence_worker_v3.app.services.cloud_run_adk_sessions.id_token.fetch_id_token",
        lambda request, audience: "identity-token",
    )
    client = CloudRunAdkSessionsClient(
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    with pytest.raises(RetryableWorkerError) as exc_info:
        asyncio.run(client.delete_session(_event()))

    message = str(exc_info.value)
    assert "delete failed" in message
    assert "secret user prompt" not in message
    assert "secret assistant response" not in message
    assert "<redacted>" in message


def test_get_session_events_uses_authenticated_adk_session_endpoint(monkeypatch) -> None:
    calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(
            200,
            json={"id": "session-1", "events": [{"invocation_id": "invoke-1"}]},
        )

    monkeypatch.setattr(
        "services.agent_persistence_worker_v3.app.services.cloud_run_adk_sessions.id_token.fetch_id_token",
        lambda request, audience: "identity-token",
    )
    client = CloudRunAdkSessionsClient(
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    events = asyncio.run(
        client.get_session_events(
            base_url="https://maxima-cloudrun-canary.example.run.app/",
            app_name="my app",
            user_id="user-1",
            session_id="session-1",
            audience="https://agent.example.run.app",
        )
    )

    assert events == [{"invocation_id": "invoke-1"}]
    assert str(calls[0].url) == (
        "https://maxima-cloudrun-canary.example.run.app"
        "/apps/my%20app/users/user-1/sessions/session-1"
    )
    assert calls[0].headers["authorization"] == "Bearer identity-token"
