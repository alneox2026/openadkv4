"""Thread delete lifecycle handling for Pub/Sub-delivered events."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from common.constants import (
    RUNTIME_SESSION_STATUS_DELETE_FAILED,
    RUNTIME_SESSION_STATUS_DELETED,
    RUNTIME_SESSION_STATUS_NOT_APPLICABLE,
)
from common.schemas import ThreadDeleteRequestedEvent
from services.agent_persistence_worker_v3.app.core.errors import RetryableWorkerError
from services.agent_persistence_worker_v3.app.services.agent_runtime_sessions import (
    AgentRuntimeSessionNotFoundError,
    AgentRuntimeSessionsClient,
    get_agent_runtime_sessions_client,
)
from services.agent_persistence_worker_v3.app.services.cloud_run_adk_sessions import (
    CloudRunAdkSessionNotFoundError,
    CloudRunAdkSessionsClient,
    get_cloud_run_adk_sessions_client,
)
from services.agent_persistence_worker_v3.app.services.firestore_client import (
    get_firestore_client,
)
from services.agent_persistence_worker_v3.app.services.firestore_threads import (
    FirestoreThreadsRepository,
)
from services.agent_persistence_worker_v3.app.services.idempotency import IdempotencyStore


@dataclass(frozen=True)
class DeleteThreadResult:
    event_id: str
    thread_id: str
    runtime_session_status: str


class DeleteThreadService:
    def __init__(
        self,
        *,
        idempotency_store: IdempotencyStore | None = None,
        threads_repository: FirestoreThreadsRepository | None = None,
        firestore_client_factory: Callable[[], Any] | None = None,
        runtime_sessions_client_factory: Callable[[], Any] | None = None,
        cloud_run_sessions_client_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.idempotency_store = idempotency_store or IdempotencyStore()
        self.threads_repository = threads_repository or FirestoreThreadsRepository()
        self.firestore_client_factory = firestore_client_factory or get_firestore_client
        self.runtime_sessions_client_factory = (
            runtime_sessions_client_factory or get_agent_runtime_sessions_client
        )
        self.cloud_run_sessions_client_factory = (
            cloud_run_sessions_client_factory or get_cloud_run_adk_sessions_client
        )

    async def delete_requested(
        self,
        event: ThreadDeleteRequestedEvent,
    ) -> DeleteThreadResult:
        duplicate_result = await asyncio.to_thread(
            self._processed_duplicate_result_sync,
            event,
        )
        if duplicate_result is not None:
            return duplicate_result

        if event.runtime_session_cleanup == "none":
            outcome = (RUNTIME_SESSION_STATUS_NOT_APPLICABLE, "runtime_cleanup_not_applicable")
            return await asyncio.to_thread(self._persist_outcome_sync, event, outcome)

        try:
            if event.runtime_session_cleanup == "cloud_run_adk":
                cloud_run_client = await self.cloud_run_sessions_client_factory()
                outcome = await self._delete_cloud_run_session(cloud_run_client, event)
            else:
                runtime_client = await self.runtime_sessions_client_factory()
                outcome = await self._delete_runtime_session(runtime_client, event)
        except RetryableWorkerError as exc:
            await asyncio.to_thread(
                self._persist_delete_failed_sync,
                event,
                "delete_failed",
                str(exc),
            )
            raise
        return await asyncio.to_thread(self._persist_outcome_sync, event, outcome)

    def _processed_duplicate_result_sync(
        self,
        event: ThreadDeleteRequestedEvent,
    ) -> DeleteThreadResult | None:
        client = self.firestore_client_factory()
        if not self.idempotency_store.exists(client, event.event_id):
            return None
        return DeleteThreadResult(
            event_id=event.event_id,
            thread_id=event.thread_id,
            runtime_session_status=(
                RUNTIME_SESSION_STATUS_NOT_APPLICABLE
                if event.runtime_session_cleanup == "none"
                else RUNTIME_SESSION_STATUS_DELETED
            ),
        )

    async def _delete_runtime_session(
        self,
        runtime_client: AgentRuntimeSessionsClient,
        event: ThreadDeleteRequestedEvent,
    ) -> tuple[str, str | None]:
        try:
            await runtime_client.delete_session(event)
            return RUNTIME_SESSION_STATUS_DELETED, None
        except AgentRuntimeSessionNotFoundError:
            return RUNTIME_SESSION_STATUS_DELETED, "session_not_found"
        except RetryableWorkerError:
            raise
        except Exception as exc:
            raise RetryableWorkerError(
                f"Unexpected runtime session delete failure for {event.session_id}: {exc}"
            ) from exc

    async def _delete_cloud_run_session(
        self,
        cloud_run_client: CloudRunAdkSessionsClient,
        event: ThreadDeleteRequestedEvent,
    ) -> tuple[str, str | None]:
        try:
            await cloud_run_client.delete_session(event)
            return RUNTIME_SESSION_STATUS_DELETED, None
        except CloudRunAdkSessionNotFoundError:
            return RUNTIME_SESSION_STATUS_DELETED, "session_not_found"
        except RetryableWorkerError:
            raise
        except Exception as exc:
            raise RetryableWorkerError(
                f"Unexpected Cloud Run ADK session delete failure for {event.session_id}: {exc}"
            ) from exc

    def _persist_outcome_sync(
        self,
        event: ThreadDeleteRequestedEvent,
        outcome: tuple[str, str | None],
    ) -> DeleteThreadResult:
        runtime_status, error_code = outcome
        client = self.firestore_client_factory()
        batch = client.batch()
        self.idempotency_store.add_create_to_batch(batch, client, event)

        if runtime_status in {
            RUNTIME_SESSION_STATUS_DELETED,
            RUNTIME_SESSION_STATUS_NOT_APPLICABLE,
        }:
            self.threads_repository.add_delete_completed_to_batch(
                batch,
                client,
                thread_id=event.thread_id,
                completed_at=datetime.now(timezone.utc),
                runtime_session_status=runtime_status,
                error_code=error_code,
            )
        else:
            self.threads_repository.add_delete_failed_to_batch(
                batch,
                client,
                thread_id=event.thread_id,
                failed_at=datetime.now(timezone.utc),
                error_code=error_code or "delete_failed",
                error_message="Runtime session deletion failed.",
            )

        try:
            batch.commit()
        except Exception as exc:
            if self._is_conflict_error(exc):
                return DeleteThreadResult(
                    event_id=event.event_id,
                    thread_id=event.thread_id,
                    runtime_session_status=runtime_status,
                )
            raise RetryableWorkerError(
                f"Failed to persist delete lifecycle event {event.event_id}: {exc}"
            ) from exc

        return DeleteThreadResult(
            event_id=event.event_id,
            thread_id=event.thread_id,
            runtime_session_status=runtime_status,
        )

    def _persist_delete_failed_sync(
        self,
        event: ThreadDeleteRequestedEvent,
        error_code: str,
        error_message: str,
    ) -> None:
        client = self.firestore_client_factory()
        self.threads_repository.document(client, event.thread_id).set(
            {
                "runtime_session_status": RUNTIME_SESSION_STATUS_DELETE_FAILED,
                "last_runtime_error_code": error_code,
                "last_runtime_error_message": error_message,
                "updated_at": datetime.now(timezone.utc),
            },
            merge=True,
        )

    def _is_conflict_error(self, exc: Exception) -> bool:
        if exc.__class__.__name__ == "Conflict":
            return True
        try:
            from google.cloud import exceptions as cloud_exceptions

            return isinstance(exc, cloud_exceptions.Conflict)
        except ModuleNotFoundError:
            return False
