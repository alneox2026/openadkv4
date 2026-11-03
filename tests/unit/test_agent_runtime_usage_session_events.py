import asyncio

import httpx

from services.agent_persistence_worker_v3.app.services.agent_runtime_sessions import (
    AgentRuntimeSessionsClient,
)


class FakeCredentials:
    valid = True
    token = "access-token"


def test_agent_runtime_session_event_lookup_is_time_bounded_and_paged(monkeypatch):
    calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(
            200,
            json={"sessionEvents": [{"invocationId": "inv-1"}]},
        )

    monkeypatch.setattr(
        "services.agent_persistence_worker_v3.app.services.agent_runtime_sessions.google.auth.default",
        lambda scopes: (FakeCredentials(), "project-1"),
    )
    client = AgentRuntimeSessionsClient(
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    events = asyncio.run(
        client.get_session_events(
            resource_name="projects/project-1/locations/us-central1/reasoningEngines/123",
            region="us-central1",
            session_id="session-1",
            started_at="2026-10-22T12:00:00+00:00",
            completed_at="2026-10-22T12:00:10+00:00",
        )
    )

    assert events == [{"invocationId": "inv-1"}]
    assert str(calls[0].url).partition("?")[0] == (
        "https://us-central1-aiplatform.googleapis.com/v1beta1/"
        "projects/project-1/locations/us-central1/reasoningEngines/123/"
        "sessions/session-1/events"
    )
    query = calls[0].url.params
    assert query["pageSize"] == "100"
    assert 'timestamp>="2026-10-22T11:59:30.000000Z"' in query["filter"]
    assert 'timestamp<="2026-10-22T12:00:40.000000Z"' in query["filter"]
