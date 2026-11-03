import asyncio

from common.schemas import ThreadDeleteRequestedEvent
from services.agent_persistence_worker_v3.app.services.delete_thread import DeleteThreadService


class FakeIdempotencyStore:
    def exists(self, client, event_id):
        return True

    def add_create_to_batch(self, batch, client, event) -> None:
        batch.actions.append(("idempotency", event.event_id))


class FakeUnprocessedIdempotencyStore(FakeIdempotencyStore):
    def exists(self, client, event_id):
        return False


class FakeBatch:
    def __init__(self) -> None:
        self.actions = []

    def commit(self) -> None:
        return None


class FakeClient:
    def __init__(self) -> None:
        self.batch_instance = FakeBatch()

    def batch(self) -> FakeBatch:
        return self.batch_instance


class FakeThreadsRepository:
    def add_delete_completed_to_batch(
        self,
        batch,
        client,
        *,
        thread_id,
        completed_at,
        runtime_session_status="deleted",
        error_code=None,
    ) -> None:
        batch.actions.append(
            ("delete_completed", thread_id, runtime_session_status, error_code)
        )

    def add_delete_failed_to_batch(self, *args, **kwargs) -> None:
        raise AssertionError("Cloud Run cleanup none must not mark delete failed")


class FakeCloudRunSessionsClient:
    def __init__(self) -> None:
        self.deleted = []

    async def delete_session(self, event) -> None:
        self.deleted.append((event.agent_base_url, event.agent_app_name, event.session_id))


def _event() -> ThreadDeleteRequestedEvent:
    return ThreadDeleteRequestedEvent(
        event_id="evt-delete",
        agent_id="maxima",
        agent_region="us-central1",
        agent_resource_name="projects/test/locations/us-central1/reasoningEngines/123",
        user_id="user-1",
        thread_id="thread-1",
        session_id="session-1",
    )


def _cloud_run_event() -> ThreadDeleteRequestedEvent:
    return ThreadDeleteRequestedEvent(
        event_id="evt-cloudrun-delete",
        agent_id="maxima_cloudrun",
        agent_backend="cloud_run_adk",
        agent_region="us-central1",
        agent_resource_name=None,
        agent_base_url="https://maxima-cloudrun-canary.example.run.app",
        agent_app_name="app",
        runtime_session_cleanup="cloud_run_adk",
        user_id="user-1",
        thread_id="thread-1",
        session_id="session-1",
    )


def _cloud_run_event_cleanup_none() -> ThreadDeleteRequestedEvent:
    return _cloud_run_event().model_copy(update={"runtime_session_cleanup": "none"})


def test_duplicate_delete_event_does_not_call_agent_runtime() -> None:
    async def _runtime_client_factory():
        raise AssertionError("runtime delete must not run for duplicate events")

    service = DeleteThreadService(
        idempotency_store=FakeIdempotencyStore(),
        firestore_client_factory=lambda: object(),
        runtime_sessions_client_factory=_runtime_client_factory,
    )

    result = asyncio.run(service.delete_requested(_event()))

    assert result.event_id == "evt-delete"
    assert result.runtime_session_status == "deleted"


def test_cloud_run_delete_event_cleanup_none_does_not_call_runtime() -> None:
    async def _runtime_client_factory():
        raise AssertionError("Cloud Run cleanup none must not call Agent Runtime")

    client = FakeClient()
    service = DeleteThreadService(
        idempotency_store=FakeUnprocessedIdempotencyStore(),
        threads_repository=FakeThreadsRepository(),
        firestore_client_factory=lambda: client,
        runtime_sessions_client_factory=_runtime_client_factory,
    )

    result = asyncio.run(service.delete_requested(_cloud_run_event_cleanup_none()))

    assert result.event_id == "evt-cloudrun-delete"
    assert result.runtime_session_status == "not_applicable"
    assert client.batch_instance.actions == [
        ("idempotency", "evt-cloudrun-delete"),
        (
            "delete_completed",
            "thread-1",
            "not_applicable",
            "runtime_cleanup_not_applicable",
        ),
    ]


def test_cloud_run_delete_event_calls_cloud_run_session_delete() -> None:
    async def _runtime_client_factory():
        raise AssertionError("Cloud Run cleanup must not call Agent Runtime")

    cloud_run_client = FakeCloudRunSessionsClient()

    async def _cloud_run_client_factory():
        return cloud_run_client

    client = FakeClient()
    service = DeleteThreadService(
        idempotency_store=FakeUnprocessedIdempotencyStore(),
        threads_repository=FakeThreadsRepository(),
        firestore_client_factory=lambda: client,
        runtime_sessions_client_factory=_runtime_client_factory,
        cloud_run_sessions_client_factory=_cloud_run_client_factory,
    )

    result = asyncio.run(service.delete_requested(_cloud_run_event()))

    assert result.event_id == "evt-cloudrun-delete"
    assert result.runtime_session_status == "deleted"
    assert cloud_run_client.deleted == [
        ("https://maxima-cloudrun-canary.example.run.app", "app", "session-1")
    ]
    assert client.batch_instance.actions == [
        ("idempotency", "evt-cloudrun-delete"),
        ("delete_completed", "thread-1", "deleted", None),
    ]
