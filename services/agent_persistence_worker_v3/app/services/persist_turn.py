"""Turn persistence orchestration for Pub/Sub-delivered events."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable

from common.schemas import TurnCompletedEvent
from services.agent_persistence_worker_v3.app.core.errors import RetryableWorkerError
from services.agent_persistence_worker_v3.app.core.config import get_settings
from services.agent_persistence_worker_v3.app.services.firestore_client import (
    get_firestore_client,
)
from services.agent_persistence_worker_v3.app.services.billing_ledger import (
    BillingLedgerRepository,
)
from services.agent_persistence_worker_v3.app.services.billing_settlement import (
    BillingSettlementService,
)
from services.agent_persistence_worker_v3.app.services.billing_reservation_repository import (
    BillingReservationRepository,
)
from services.agent_persistence_worker_v3.app.services.billing_batch_settlement import (
    BillingBatchSettlementService,
)
from services.agent_persistence_worker_v3.app.services.firestore_messages import (
    FirestoreMessagesRepository,
)
from services.agent_persistence_worker_v3.app.services.firestore_threads import (
    FirestoreThreadsRepository,
)
from services.agent_persistence_worker_v3.app.services.idempotency import IdempotencyStore
from services.agent_gateway_v3.app.services.usage_metadata import price_usage_reports


@dataclass(frozen=True)
class PersistTurnResult:
    event_id: str
    thread_id: str
    persisted: bool
    ignored_reason: str | None = None
    billing_settlement_status: str | None = None


class PersistTurnService:
    def __init__(
        self,
        idempotency_store: IdempotencyStore | None = None,
        billing_ledger_repository: BillingLedgerRepository | None = None,
        billing_settlement_service: BillingSettlementService | None = None,
        billing_reservation_repository: BillingReservationRepository | None = None,
        billing_batch_settlement_service: BillingBatchSettlementService | None = None,
        threads_repository: FirestoreThreadsRepository | None = None,
        messages_repository: FirestoreMessagesRepository | None = None,
        firestore_client_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.idempotency_store = idempotency_store or IdempotencyStore()
        self.billing_ledger_repository = (
            billing_ledger_repository or BillingLedgerRepository()
        )
        self.billing_settlement_service = (
            billing_settlement_service or BillingSettlementService()
        )
        self.billing_reservation_repository = (
            billing_reservation_repository or BillingReservationRepository()
        )
        self.billing_batch_settlement_service = (
            billing_batch_settlement_service or BillingBatchSettlementService()
        )
        self.threads_repository = (
            threads_repository or FirestoreThreadsRepository()
        )
        self.messages_repository = (
            messages_repository or FirestoreMessagesRepository()
        )
        self.firestore_client_factory = firestore_client_factory or get_firestore_client
        self._settings = get_settings()

    async def persist(self, event: TurnCompletedEvent) -> PersistTurnResult:
        return await asyncio.to_thread(self._persist_sync, event)

    def _persist_sync(self, event: TurnCompletedEvent) -> PersistTurnResult:
        try:
            client = self.firestore_client_factory()
            existing_thread = self.threads_repository.load_existing(
                client,
                event.thread_id,
            )
            ignored_reason = self.threads_repository.validate_existing_for_turn(
                existing_thread,
                event,
            )
            if ignored_reason:
                return self._persist_idempotency_only(
                    client,
                    event,
                    ignored_reason=ignored_reason,
                )
            event = _price_turn_usage_in_worker(event)
            batch = client.batch()
            self.idempotency_store.add_create_to_batch(batch, client, event)
            self.billing_ledger_repository.add_create_to_batch(batch, client, event)
            self.billing_reservation_repository.add_create_to_batch(batch, client, event)
            self.threads_repository.add_upsert_to_batch(
                batch,
                client,
                event,
                existing_thread=existing_thread,
            )
            self.messages_repository.add_turn_messages_to_batch(
                batch,
                client,
                event,
            )
            batch.commit()
        except Exception as exc:
            if self._is_conflict_error(exc):
                persisted = False
            else:
                raise RetryableWorkerError(
                    f"Failed to persist turn event {event.event_id}: {exc}"
                ) from exc
        else:
            persisted = True

        try:
            if self._settings.billing_batch_settlement_enabled:
                queued = self.billing_batch_settlement_service.enqueue_turn_sync(event)
                settlement_status = "batch_pending" if queued else None
            else:
                # Kept as an explicit rollback switch; v4 deployments enable batching.
                settlement = self.billing_settlement_service.settle_if_required_sync(event)
                settlement_status = settlement.status if settlement else None
        except Exception as exc:
            if isinstance(exc, RetryableWorkerError):
                raise
            raise RetryableWorkerError(
                f"Failed to settle billing for turn {event.turn_id}: {exc}"
            ) from exc

        return PersistTurnResult(
            event_id=event.event_id,
            thread_id=event.thread_id,
            persisted=persisted,
            billing_settlement_status=settlement_status,
        )

    def _persist_idempotency_only(
        self,
        client: Any,
        event: TurnCompletedEvent,
        *,
        ignored_reason: str,
    ) -> PersistTurnResult:
        batch = client.batch()
        self.idempotency_store.add_create_to_batch(batch, client, event)
        try:
            batch.commit()
        except Exception as exc:
            if self._is_conflict_error(exc):
                return PersistTurnResult(
                    event_id=event.event_id,
                    thread_id=event.thread_id,
                    persisted=False,
                    ignored_reason=ignored_reason,
                )
            raise RetryableWorkerError(
                f"Failed to record ignored turn event {event.event_id}: {exc}"
            ) from exc
        return PersistTurnResult(
            event_id=event.event_id,
            thread_id=event.thread_id,
            persisted=False,
            ignored_reason=ignored_reason,
        )

    def _is_conflict_error(self, exc: Exception) -> bool:
        if exc.__class__.__name__ == "Conflict":
            return True
        try:
            from google.cloud import exceptions as cloud_exceptions

            return isinstance(exc, cloud_exceptions.Conflict)
        except ModuleNotFoundError:
            return False


def _price_turn_usage_in_worker(event: TurnCompletedEvent) -> TurnCompletedEvent:
    """Apply the configured rate card off the gateway request path."""
    metadata = event.metadata if isinstance(event.metadata, dict) else {}
    raw_reports = metadata.get("provider_usage_reports")
    if not isinstance(raw_reports, list) or not raw_reports:
        return event

    usage = price_usage_reports(raw_reports)
    if not usage:
        # Keep the gateway's token-only summary if the provider reports an
        # unsupported shape; reconciliation can still recover usage later.
        usage = event.usage if isinstance(event.usage, dict) else {}
    worker_metadata = dict(metadata)
    worker_metadata.pop("provider_usage_reports", None)
    return event.model_copy(update={"usage": usage, "metadata": worker_metadata})
