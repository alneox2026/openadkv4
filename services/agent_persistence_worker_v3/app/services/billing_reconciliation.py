"""Reconciliation for reservations whose completed-turn event never arrived."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from common.billing import customer_wallet_document_id, nonnegative_int
from common.firestore import get_transaction_document_snapshot
from common.schemas import TurnCompletedEvent
from services.agent_persistence_worker_v3.app.core.config import get_settings
from services.agent_persistence_worker_v3.app.core.errors import RetryableWorkerError
from services.agent_persistence_worker_v3.app.services.billing_settlement import (
    BillingSettlementService,
)
from services.agent_persistence_worker_v3.app.services.billing_batch_settlement import (
    BillingBatchSettlementService,
)
from services.agent_persistence_worker_v3.app.services.cloud_run_adk_usage_hydrator import (
    CloudRunAdkSessionUsageHydrator,
)
from services.agent_persistence_worker_v3.app.services.firestore_client import (
    get_firestore_client,
)

_USAGE_RECOVERY_STATUSES = (
    "late_usage_pending",
    "batch_pending",
    "late_batch_pending",
    "settled",
    "settled_shortfall",
    "late_settled",
    "late_settled_shortfall",
)
_FINANCIAL_RESERVATION_STATUSES = frozenset(
    {
        "completed_pending",
        "late_usage_pending",
        "batch_pending",
        "late_batch_pending",
        "settled",
        "settled_shortfall",
        "late_settled",
        "late_settled_shortfall",
    }
)


@dataclass(frozen=True)
class BillingReconciliationResult:
    scanned_reservations: int
    settled_reservations: int
    released_reservations: int
    skipped_reservations: int
    settled_batches: int = 0
    settled_batch_turns: int = 0


class BillingReconciliationService:
    """Recover late usage, close wallet batches, and clean up legacy holds."""

    def __init__(
        self,
        *,
        firestore_client_factory: Callable[[], Any] | None = None,
        settlement_service: BillingSettlementService | None = None,
        batch_settlement_service: BillingBatchSettlementService | None = None,
        usage_hydrator: CloudRunAdkSessionUsageHydrator | None = None,
    ) -> None:
        self._settings = get_settings()
        self._firestore_client_factory = firestore_client_factory or get_firestore_client
        self._settlement_service = settlement_service or BillingSettlementService(
            firestore_client_factory=self._firestore_client_factory,
        )
        self._batch_settlement_service = batch_settlement_service or BillingBatchSettlementService(
            firestore_client_factory=self._firestore_client_factory,
        )
        self._usage_hydrator = usage_hydrator or CloudRunAdkSessionUsageHydrator()

    async def reconcile_expired(self) -> BillingReconciliationResult:
        return await asyncio.to_thread(self.reconcile_expired_sync)

    def reconcile_expired_sync(self) -> BillingReconciliationResult:
        client = self._firestore_client_factory()
        now = datetime.now(timezone.utc)
        reservations = self._expired_reservations(client, now)
        pending_usage = self._pending_usage_reservations(client, now)
        scanned = settled = released = skipped = 0
        for reservation_snapshot in reservations:
            scanned += 1
            reservation = reservation_snapshot.to_dict() or {}
            if reservation.get("status") != "reserved":
                skipped += 1
                continue
            turn_id = _required_string(reservation.get("turn_id"), "turn_id")
            ledger_snapshot = client.collection(
                self._settings.billing_ledger_collection
            ).document(turn_id).get()
            if ledger_snapshot.exists:
                event = _event_from_ledger_and_reservation(
                    ledger_snapshot.to_dict() or {},
                    reservation,
                )
                self._settlement_service.settle_if_required_sync(event)
                settled += 1
                continue
            if self._release_expired_reservation_sync(client, reservation, now):
                released += 1
            else:
                skipped += 1
        for reservation_snapshot in pending_usage:
            scanned += 1
            reservation = reservation_snapshot.to_dict() or {}
            status = reservation.get("status")
            if status not in _USAGE_RECOVERY_STATUSES:
                skipped += 1
                continue
            if not self._usage_reconciliation_is_due(reservation, now):
                skipped += 1
                continue
            deadline = reservation.get("usage_reconciliation_deadline_at")
            if isinstance(deadline, datetime) and deadline <= now:
                self._mark_usage_manual_review(
                    client, reservation, now, "provider_usage_retention_expired"
                )
                skipped += 1
                continue
            turn_id = _required_string(reservation.get("turn_id"), "turn_id")
            ledger_snapshot = client.collection(
                self._settings.billing_ledger_collection
            ).document(turn_id).get()
            if not ledger_snapshot.exists:
                self._mark_usage_manual_review(client, reservation, now, "late_ledger_missing")
                skipped += 1
                continue
            ledger = ledger_snapshot.to_dict() or {}
            try:
                recovered_usage = self._usage_hydrator.recover(
                    ledger=ledger,
                    reservation=reservation,
                )
            except RetryableWorkerError:
                recovered_usage = None
            if not recovered_usage:
                self._schedule_usage_retry(client, reservation, now)
                skipped += 1
                continue
            event = _event_from_ledger_and_reservation(ledger, reservation)
            event = event.model_copy(update={"usage": recovered_usage})
            try:
                self._batch_settlement_service.enqueue_usage_correction_sync(event)
            except RetryableWorkerError:
                self._schedule_usage_retry(client, reservation, now)
                skipped += 1
            else:
                settled += 1
        batch_result = self._batch_settlement_service.reconcile_due_sync()
        return BillingReconciliationResult(
            scanned_reservations=scanned,
            settled_reservations=settled,
            released_reservations=released,
            skipped_reservations=skipped,
            settled_batches=batch_result.settled_batches,
            settled_batch_turns=batch_result.settled_turns,
        )

    def _pending_usage_reservations(self, client: Any, now: datetime):
        collection = client.collection(self._settings.billing_reservations_collection)
        batch_size = self._settings.billing_usage_reconciliation_batch_size
        statuses = list(_USAGE_RECOVERY_STATUSES)
        try:
            from google.cloud.firestore_v1.base_query import FieldFilter

            snapshots = (
                collection.where(filter=FieldFilter("status", "in", statuses))
                .where(filter=FieldFilter("usage_reconciliation_next_attempt_at", "<=", now))
                .order_by("usage_reconciliation_next_attempt_at")
                .limit(batch_size)
                .stream()
            )
            return list(snapshots)
        except (AttributeError, ModuleNotFoundError, ImportError):
            candidates = []
            for snapshot in collection.stream():
                reservation = snapshot.to_dict() or {}
                if reservation.get("status") in statuses and self._usage_reconciliation_is_due(
                    reservation, now
                ):
                    candidates.append((reservation.get("usage_reconciliation_next_attempt_at"), snapshot))
            candidates.sort(key=lambda item: item[0] or datetime.min.replace(tzinfo=timezone.utc))
            return [snapshot for _, snapshot in candidates[:batch_size]]

    def _usage_reconciliation_is_due(self, reservation: dict[str, Any], now: datetime) -> bool:
        next_attempt = reservation.get("usage_reconciliation_next_attempt_at")
        return isinstance(next_attempt, datetime) and next_attempt <= now

    def _schedule_usage_retry(
        self,
        client: Any,
        reservation: dict[str, Any],
        now: datetime,
    ) -> None:
        turn_id = _required_string(reservation.get("turn_id"), "turn_id")
        try:
            attempts = nonnegative_int(
                reservation.get("usage_reconciliation_attempt_count", 0),
                field_name="usage_reconciliation_attempt_count",
            )
        except ValueError as exc:
            raise RetryableWorkerError(
                f"Usage-recovery retry count is invalid for turn {turn_id}."
            ) from exc
        started_at = reservation.get("usage_reconciliation_started_at")
        started_at = started_at if isinstance(started_at, datetime) else now
        deadline = reservation.get("usage_reconciliation_deadline_at")
        if not isinstance(deadline, datetime):
            deadline = started_at + timedelta(
                seconds=max(1, self._settings.billing_usage_reconciliation_retention_seconds)
            )
        initial = max(1, self._settings.billing_usage_reconciliation_initial_backoff_seconds)
        maximum = max(initial, self._settings.billing_usage_reconciliation_max_backoff_seconds)
        delay = min(initial * (2 ** min(attempts, 20)), maximum)
        next_attempt = min(now + timedelta(seconds=delay), deadline)
        client.collection(self._settings.billing_reservations_collection).document(
            turn_id
        ).update(
            {
                "usage_reconciliation_status": "pending_collection"
                if reservation.get("status")
                in {"settled_shortfall", "late_settled_shortfall"}
                else "pending",
                "usage_reconciliation_attempt_count": attempts + 1,
                "usage_reconciliation_started_at": started_at,
                "usage_reconciliation_deadline_at": deadline,
                "usage_reconciliation_next_attempt_at": next_attempt,
                "updated_at": now,
            }
        )

    def _mark_usage_manual_review(
        self,
        client: Any,
        reservation: dict[str, Any],
        now: datetime,
        reason: str,
    ) -> None:
        turn_id = _required_string(reservation.get("turn_id"), "turn_id")
        client.collection(self._settings.billing_reservations_collection).document(
            turn_id
        ).update(
            {
                "status": (
                    reservation.get("status")
                    if reservation.get("status") in _FINANCIAL_RESERVATION_STATUSES
                    else "usage_recovery_manual_review"
                ),
                "usage_reconciliation_status": "manual_review",
                "usage_reconciliation_manual_review_reason": reason,
                "usage_reconciliation_manual_review_at": now,
                "usage_reconciliation_next_attempt_at": None,
                "updated_at": now,
            }
        )

    def _expired_reservations(self, client: Any, now: datetime):
        collection = client.collection(self._settings.billing_reservations_collection)
        batch_size = self._settings.billing_reconciliation_batch_size
        try:
            from google.cloud.firestore_v1.base_query import FieldFilter

            return (
                collection.where(filter=FieldFilter("status", "==", "reserved"))
                .where(filter=FieldFilter("expires_at", "<=", now))
                .order_by("expires_at")
                .limit(batch_size)
                .stream()
            )
        except (AttributeError, ModuleNotFoundError, ImportError):
            # Test doubles and older fakes may not implement Firestore queries.
            # Keep the same status filter before applying the bounded limit.
            candidates = []
            for snapshot in collection.stream():
                reservation = snapshot.to_dict() or {}
                expires_at = reservation.get("expires_at")
                if (
                    reservation.get("status") == "reserved"
                    and isinstance(expires_at, datetime)
                    and expires_at <= now
                ):
                    candidates.append((expires_at, snapshot))
            candidates.sort(key=lambda item: item[0])
            return [snapshot for _, snapshot in candidates[:batch_size]]

    def _release_expired_reservation_sync(
        self,
        client: Any,
        reservation: dict[str, Any],
        released_at: datetime,
    ) -> bool:
        turn_id = _required_string(reservation.get("turn_id"), "turn_id")
        billing_subject_id = _required_string(
            reservation.get("billing_subject_id"),
            "billing_subject_id",
        )
        owner_uid = _required_string(reservation.get("owner_uid"), "owner_uid")
        reservation_ref = client.collection(
            self._settings.billing_reservations_collection
        ).document(turn_id)
        wallet_ref = client.collection(self._settings.wallets_collection).document(
            customer_wallet_document_id(billing_subject_id)
        )
        transaction_ref = client.collection(
            self._settings.wallet_transactions_collection
        ).document(f"reservation_expired_{turn_id}")

        def operation(transaction: Any) -> bool:
            # Keep all reads before writes for Firestore transaction validity.
            current_reservation_snapshot = get_transaction_document_snapshot(transaction, reservation_ref)
            wallet_snapshot = get_transaction_document_snapshot(transaction, wallet_ref)
            transaction_snapshot = get_transaction_document_snapshot(transaction, transaction_ref)
            if transaction_snapshot.exists or not current_reservation_snapshot.exists:
                return False
            current_reservation = current_reservation_snapshot.to_dict() or {}
            if current_reservation.get("status") != "reserved":
                return False
            if not wallet_snapshot.exists:
                raise RetryableWorkerError(
                    f"Wallet is missing for expired reservation {turn_id}."
                )
            wallet = wallet_snapshot.to_dict() or {}
            if (
                wallet.get("status") != "active"
                or wallet.get("billing_subject_id") != billing_subject_id
                or wallet.get("owner_uid") != owner_uid
            ):
                raise RetryableWorkerError(
                    f"Wallet does not match expired reservation {turn_id}."
                )
            try:
                reserved_amount_nanos = nonnegative_int(
                    current_reservation.get("reserved_amount_nanos"),
                    field_name="reserved_amount_nanos",
                )
                available_credit_nanos = int(wallet.get("available_credit_nanos", 0))
                reserved_credit_nanos = nonnegative_int(
                    wallet.get("reserved_credit_nanos"),
                    field_name="reserved_credit_nanos",
                )
            except ValueError as exc:
                raise RetryableWorkerError(
                    f"Wallet data is invalid for expired reservation {turn_id}."
                ) from exc
            if reserved_credit_nanos < reserved_amount_nanos:
                raise RetryableWorkerError(
                    f"Wallet reserved credit is invalid for expired reservation {turn_id}."
                )
            transaction.create(
                transaction_ref,
                {
                    "schema_version": 1,
                    "transaction_id": f"reservation_expired_{turn_id}",
                    "transaction_type": "reservation_expiry_release",
                    "status": "released",
                    "billing_subject_id": billing_subject_id,
                    "owner_uid": owner_uid,
                    "wallet_document_id": customer_wallet_document_id(billing_subject_id),
                    "currency": current_reservation.get("currency", "USD"),
                    "turn_id": turn_id,
                    "request_id": current_reservation.get("request_id"),
                    "agent_id": current_reservation.get("agent_id"),
                    "reservation_id": turn_id,
                    "reserved_amount_nanos": reserved_amount_nanos,
                    "amount_nanos": 0,
                    "released_amount_nanos": reserved_amount_nanos,
                    "created_at": released_at,
                    "reason": "completed_turn_not_received_before_reservation_expiry",
                },
            )
            transaction.update(
                wallet_ref,
                {
                    "available_credit_nanos": available_credit_nanos + reserved_amount_nanos,
                    "reserved_credit_nanos": reserved_credit_nanos - reserved_amount_nanos,
                    "updated_at": released_at,
                    "last_reservation_release_at": released_at,
                },
            )
            transaction.update(
                reservation_ref,
                {
                    "status": "expired_released",
                    "released_at": released_at,
                    "released_amount_nanos": reserved_amount_nanos,
                    "release_reason": "completed_turn_not_received_before_reservation_expiry",
                },
            )
            return True

        return _run_firestore_transaction(client, operation)


def _event_from_ledger_and_reservation(
    ledger: dict[str, Any],
    reservation: dict[str, Any],
) -> TurnCompletedEvent:
    return TurnCompletedEvent(
        event_id=_required_string(ledger.get("event_id"), "event_id"),
        turn_id=_required_string(ledger.get("turn_id"), "turn_id"),
        agent_id=_required_string(ledger.get("agent_id"), "agent_id"),
        user_id=_required_string(ledger.get("uid"), "uid"),
        thread_id=_required_string(ledger.get("thread_id"), "thread_id"),
        session_id=_required_string(ledger.get("session_id"), "session_id"),
        user_message="",
        assistant_message="",
        created_at=_required_datetime(ledger.get("created_at"), "created_at"),
        metadata={
            "billing": {
                "reservation_id": reservation.get("reservation_id"),
                "billing_subject_id": reservation.get("billing_subject_id"),
            }
        },
    )


def _required_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RetryableWorkerError(f"Billing reconciliation field {field_name} is required.")
    return value.strip()


def _required_datetime(value: Any, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise RetryableWorkerError(f"Billing reconciliation field {field_name} is required.")
    return value


def _run_firestore_transaction(client: Any, operation: Callable[[Any], bool]) -> bool:
    from google.cloud import firestore

    transaction = client.transaction()

    @firestore.transactional
    def run(transaction: Any) -> bool:
        return operation(transaction)

    return run(transaction)
