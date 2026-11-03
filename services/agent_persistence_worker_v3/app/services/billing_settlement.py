"""Settlement of prepaid credit from immutable completed-turn ledger entries."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from common.billing import (
    customer_billing_period_document_id,
    customer_wallet_document_id,
    nonnegative_int,
)
from common.firestore import get_transaction_document_snapshot
from common.schemas import TurnCompletedEvent
from services.agent_persistence_worker_v3.app.core.config import get_settings
from services.agent_persistence_worker_v3.app.core.errors import RetryableWorkerError
from services.agent_persistence_worker_v3.app.services.firestore_client import (
    get_firestore_client,
)


@dataclass(frozen=True)
class BillingSettlementResult:
    """The result of settling one completed turn, including any review shortfall."""

    turn_id: str
    status: str
    estimated_cost_nanos: int | None
    settled_amount_nanos: int
    released_amount_nanos: int
    shortfall_nanos: int
    billing_period_id: str


TransactionRunner = Callable[
    [Any, Callable[[Any], BillingSettlementResult]],
    BillingSettlementResult,
]


class BillingSettlementService:
    """Legacy per-turn fallback for zero-hold markers and older reservations."""

    def __init__(
        self,
        *,
        firestore_client_factory: Callable[[], Any] | None = None,
        transaction_runner: TransactionRunner | None = None,
    ) -> None:
        self._settings = get_settings()
        self._firestore_client_factory = firestore_client_factory or get_firestore_client
        self._transaction_runner = transaction_runner or _run_firestore_transaction

    async def settle_if_required(
        self,
        event: TurnCompletedEvent,
    ) -> BillingSettlementResult | None:
        return await asyncio.to_thread(self.settle_if_required_sync, event)

    def settle_if_required_sync(
        self,
        event: TurnCompletedEvent,
    ) -> BillingSettlementResult | None:
        billing_metadata = self._billing_metadata(event)
        if billing_metadata is None:
            return None
        return self._settle_sync(event, billing_metadata)

    def _settle_sync(
        self,
        event: TurnCompletedEvent,
        billing_metadata: dict[str, Any],
    ) -> BillingSettlementResult:
        reservation_id = self._required_string(
            billing_metadata.get("reservation_id")
            or billing_metadata.get("billing_reservation_id"),
            "reservation_id",
        )
        if reservation_id != event.turn_id:
            raise RetryableWorkerError(
                f"Billing reservation {reservation_id} does not match turn {event.turn_id}."
            )
        billing_subject_id = self._required_string(
            billing_metadata.get("billing_subject_id"),
            "billing_subject_id",
        )
        if billing_subject_id != event.user_id:
            raise RetryableWorkerError(
                "Billing subject does not match the authenticated turn owner."
            )

        created_at = _as_utc(event.created_at)
        period_key, period_start, period_end = _billing_period(created_at)
        wallet_document_id = customer_wallet_document_id(billing_subject_id)
        billing_period_id = customer_billing_period_document_id(
            billing_subject_id,
            period_key,
        )
        client = self._firestore_client_factory()
        wallet_ref = client.collection(self._settings.wallets_collection).document(
            wallet_document_id
        )
        reservation_ref = client.collection(
            self._settings.billing_reservations_collection
        ).document(reservation_id)
        ledger_ref = client.collection(self._settings.billing_ledger_collection).document(
            event.turn_id
        )
        transaction_ref = client.collection(
            self._settings.wallet_transactions_collection
        ).document(f"usage_{event.turn_id}")
        period_ref = client.collection(
            self._settings.customer_billing_periods_collection
        ).document(billing_period_id)
        settled_at = datetime.now(timezone.utc)

        def operation(transaction: Any) -> BillingSettlementResult:
            # Firestore requires all reads to happen before writes in a transaction.
            reservation_snapshot = get_transaction_document_snapshot(transaction, reservation_ref)
            ledger_snapshot = get_transaction_document_snapshot(transaction, ledger_ref)
            wallet_snapshot = get_transaction_document_snapshot(transaction, wallet_ref)
            transaction_snapshot = get_transaction_document_snapshot(transaction, transaction_ref)
            period_snapshot = get_transaction_document_snapshot(transaction, period_ref)

            if transaction_snapshot.exists:
                return self._result_from_existing_transaction(
                    transaction_snapshot.to_dict() or {},
                    event.turn_id,
                    billing_period_id,
                )
            if not ledger_snapshot.exists:
                raise RetryableWorkerError(
                    f"Billing ledger for turn {event.turn_id} is not available for settlement."
                )
            if not reservation_snapshot.exists:
                raise RetryableWorkerError(
                    f"Billing reservation for turn {event.turn_id} is not available for settlement."
                )
            if not wallet_snapshot.exists:
                raise RetryableWorkerError(
                    f"Wallet for turn {event.turn_id} is not available for settlement."
                )

            reservation = reservation_snapshot.to_dict() or {}
            ledger = ledger_snapshot.to_dict() or {}
            wallet = wallet_snapshot.to_dict() or {}
            period = period_snapshot.to_dict() or {}
            self._validate_settlement_records(
                event=event,
                reservation=reservation,
                ledger=ledger,
                wallet=wallet,
                billing_subject_id=billing_subject_id,
                reservation_id=reservation_id,
            )

            try:
                reserved_amount_nanos = nonnegative_int(
                    reservation.get("reserved_amount_nanos", 0),
                    field_name="reserved_amount_nanos",
                )
                available_credit_nanos = _wallet_amount(
                    wallet.get("available_credit_nanos", 0)
                )
                reserved_credit_nanos = nonnegative_int(
                    wallet.get("reserved_credit_nanos", 0),
                    field_name="reserved_credit_nanos",
                )
                settled_usage_nanos = nonnegative_int(
                    wallet.get("settled_usage_nanos", 0),
                    field_name="settled_usage_nanos",
                )
                usage_debt_nanos = nonnegative_int(
                    wallet.get("usage_debt_nanos", 0),
                    field_name="usage_debt_nanos",
                )
            except ValueError as exc:
                raise RetryableWorkerError(
                    f"Wallet or reservation data for turn {event.turn_id} is invalid."
                ) from exc
            if reserved_credit_nanos < reserved_amount_nanos:
                raise RetryableWorkerError(
                    f"Wallet reserved credit is lower than the turn reservation for {event.turn_id}."
                )

            estimated_cost_nanos = _ledger_cost_nanos(ledger)
            recovery = ledger.get("provider_usage_recovery")
            recovery = recovery if isinstance(recovery, dict) else {}
            recovery_supported = _has_supported_usage_recovery(recovery)
            estimated_cost_complete = ledger.get("estimated_cost_complete") is not False
            usage_pending = recovery_supported and (
                estimated_cost_nanos is None or not estimated_cost_complete
            )
            unpriced_pending = estimated_cost_nanos is None and usage_pending
            (
                status,
                settled_amount_nanos,
                released_amount_nanos,
                shortfall_nanos,
            ) = _settlement_amounts(
                estimated_cost_nanos=estimated_cost_nanos,
                reserved_amount_nanos=reserved_amount_nanos,
                available_credit_nanos=available_credit_nanos,
            )
            wallet_updates = {
                "available_credit_nanos": (
                    available_credit_nanos
                    + released_amount_nanos
                    - max(settled_amount_nanos - reserved_amount_nanos, 0)
                ),
                "reserved_credit_nanos": reserved_credit_nanos - reserved_amount_nanos,
                "settled_usage_nanos": settled_usage_nanos + settled_amount_nanos,
                "usage_debt_nanos": usage_debt_nanos + shortfall_nanos,
                "lifetime_usage_debt_added_nanos": nonnegative_int(
                    wallet.get("lifetime_usage_debt_added_nanos", 0),
                    field_name="lifetime_usage_debt_added_nanos",
                ) + shortfall_nanos,
                "updated_at": settled_at,
                "last_settlement_at": settled_at,
            }
            transaction.create(
                transaction_ref,
                {
                    "schema_version": 1,
                    "transaction_id": f"usage_{event.turn_id}",
                    "transaction_type": "agent_usage_debit",
                    "status": status,
                    "billing_subject_id": billing_subject_id,
                    "owner_uid": event.user_id,
                    "wallet_document_id": wallet_document_id,
                    "currency": "USD",
                    "turn_id": event.turn_id,
                    "request_id": reservation.get("request_id"),
                    "agent_id": event.agent_id,
                    "ledger_document_id": event.turn_id,
                    "reservation_id": reservation_id,
                    "reserved_amount_nanos": reserved_amount_nanos,
                    "estimated_cost_nanos": estimated_cost_nanos,
                    "amount_nanos": settled_amount_nanos,
                    "released_amount_nanos": released_amount_nanos,
                    "shortfall_nanos": shortfall_nanos,
                    "usage_debt_added_nanos": shortfall_nanos,
                    "remaining_usage_debt_nanos": usage_debt_nanos + shortfall_nanos,
                    "pricing_model": ledger.get("pricing_model"),
                    "pricing_version": ledger.get("pricing_version"),
                    "created_at": settled_at,
                },
            )
            transaction.update(wallet_ref, wallet_updates)
            transaction.update(
                reservation_ref,
                {
                    "status": "late_usage_pending" if unpriced_pending else status,
                    "period_key": period_key,
                    "period_start": period_start,
                    "period_end": period_end,
                    "session_id": event.session_id,
                    "pricing_model": ledger.get("pricing_model"),
                    "pricing_version": ledger.get("pricing_version"),
                    "late_usage": False,
                    "settled_at": settled_at,
                    "settled_amount_nanos": settled_amount_nanos,
                    "released_amount_nanos": released_amount_nanos,
                    "estimated_cost_nanos": estimated_cost_nanos,
                    "accounted_cost_nanos": estimated_cost_nanos or 0,
                    "estimated_cost_complete": estimated_cost_complete,
                    "shortfall_nanos": shortfall_nanos,
                    "provider_usage_recovery": recovery,
                    "usage_reconciliation_status": (
                        "pending" if usage_pending else "manual_review"
                        if estimated_cost_nanos is None or not estimated_cost_complete
                        else "complete"
                    ),
                    "usage_reconciliation_started_at": (
                        reservation.get("usage_reconciliation_started_at")
                        or (settled_at if usage_pending else None)
                    ),
                    "usage_reconciliation_deadline_at": (
                        reservation.get("usage_reconciliation_deadline_at")
                        or (
                            settled_at
                            + timedelta(
                                seconds=max(
                                    1,
                                    self._settings.billing_usage_reconciliation_retention_seconds,
                                )
                            )
                            if usage_pending
                            else None
                        )
                    ),
                    "usage_reconciliation_attempt_count": reservation.get(
                        "usage_reconciliation_attempt_count", 0
                    ),
                    "usage_reconciliation_next_attempt_at": (
                        settled_at
                        + timedelta(
                            seconds=max(
                                1,
                                self._settings.billing_usage_reconciliation_initial_backoff_seconds,
                            )
                        )
                        if usage_pending
                        else None
                    ),
                    "usage_reconciliation_manual_review_reason": (
                        None
                        if usage_pending
                        or (estimated_cost_nanos is not None and estimated_cost_complete)
                        else "no_supported_provider_usage_recovery_source"
                    ),
                },
            )
            self._write_billing_period(
                transaction=transaction,
                period_ref=period_ref,
                period=period,
                period_exists=period_snapshot.exists,
                event=event,
                billing_subject_id=billing_subject_id,
                period_key=period_key,
                period_start=period_start,
                period_end=period_end,
                settled_at=settled_at,
                estimated_cost_nanos=estimated_cost_nanos,
                settled_amount_nanos=settled_amount_nanos,
                shortfall_nanos=shortfall_nanos,
            )
            return BillingSettlementResult(
                turn_id=event.turn_id,
                status=status,
                estimated_cost_nanos=estimated_cost_nanos,
                settled_amount_nanos=settled_amount_nanos,
                released_amount_nanos=released_amount_nanos,
                shortfall_nanos=shortfall_nanos,
                billing_period_id=billing_period_id,
            )

        return self._transaction_runner(client, operation)

    def settle_late_usage_sync(
        self,
        event: TurnCompletedEvent,
    ) -> BillingSettlementResult | None:
        """Debit recovered usage from current available credit, never a stale hold."""
        billing = self._billing_metadata(event)
        if billing is None:
            return None
        reservation_id = self._required_string(
            billing.get("reservation_id") or billing.get("billing_reservation_id"),
            "reservation_id",
        )
        billing_subject_id = self._required_string(
            billing.get("billing_subject_id"), "billing_subject_id"
        )
        estimated_cost_nanos = _usage_cost_nanos(event.usage)
        if estimated_cost_nanos is None:
            raise RetryableWorkerError(
                f"Recovered usage for turn {event.turn_id} has no complete priced cost."
            )
        period_key, period_start, period_end = _billing_period(_as_utc(event.created_at))
        wallet_document_id = customer_wallet_document_id(billing_subject_id)
        billing_period_id = customer_billing_period_document_id(
            billing_subject_id, period_key
        )
        client = self._firestore_client_factory()
        wallet_ref = client.collection(self._settings.wallets_collection).document(
            wallet_document_id
        )
        reservation_ref = client.collection(
            self._settings.billing_reservations_collection
        ).document(reservation_id)
        ledger_ref = client.collection(self._settings.billing_ledger_collection).document(
            event.turn_id
        )
        original_tx_ref = client.collection(
            self._settings.wallet_transactions_collection
        ).document(f"usage_{event.turn_id}")
        late_tx_ref = client.collection(
            self._settings.wallet_transactions_collection
        ).document(f"late_usage_{event.turn_id}")
        period_ref = client.collection(
            self._settings.customer_billing_periods_collection
        ).document(billing_period_id)
        settled_at = datetime.now(timezone.utc)

        def operation(transaction: Any) -> BillingSettlementResult:
            reservation_snapshot = get_transaction_document_snapshot(transaction, reservation_ref)
            ledger_snapshot = get_transaction_document_snapshot(transaction, ledger_ref)
            wallet_snapshot = get_transaction_document_snapshot(transaction, wallet_ref)
            original_tx_snapshot = get_transaction_document_snapshot(transaction, original_tx_ref)
            late_tx_snapshot = get_transaction_document_snapshot(transaction, late_tx_ref)
            period_snapshot = get_transaction_document_snapshot(transaction, period_ref)
            if late_tx_snapshot.exists:
                return self._result_from_existing_transaction(
                    late_tx_snapshot.to_dict() or {}, event.turn_id, billing_period_id
                )
            if not all(
                snapshot.exists
                for snapshot in (
                    reservation_snapshot,
                    ledger_snapshot,
                    wallet_snapshot,
                    original_tx_snapshot,
                    period_snapshot,
                )
            ):
                raise RetryableWorkerError(
                    f"Late usage records for turn {event.turn_id} are incomplete."
                )
            reservation = reservation_snapshot.to_dict() or {}
            ledger = ledger_snapshot.to_dict() or {}
            wallet = wallet_snapshot.to_dict() or {}
            original_tx = original_tx_snapshot.to_dict() or {}
            period = period_snapshot.to_dict() or {}
            if (
                reservation.get("status") != "late_usage_pending"
                or original_tx.get("status") != "unpriced_released"
                or reservation.get("billing_subject_id") != billing_subject_id
                or reservation.get("owner_uid") != event.user_id
                or reservation.get("agent_id") != event.agent_id
                or ledger.get("turn_id") != event.turn_id
                or ledger.get("uid") != event.user_id
                or wallet.get("billing_subject_id") != billing_subject_id
                or wallet.get("owner_uid") != event.user_id
            ):
                raise RetryableWorkerError(
                    f"Late usage records for turn {event.turn_id} do not match."
                )
            available = _wallet_amount(wallet.get("available_credit_nanos", 0))
            settled_usage = nonnegative_int(
                wallet.get("settled_usage_nanos", 0), field_name="settled_usage_nanos"
            )
            usage_debt = nonnegative_int(
                wallet.get("usage_debt_nanos", 0), field_name="usage_debt_nanos"
            )
            collected = min(estimated_cost_nanos, max(0, available))
            shortfall = estimated_cost_nanos - collected
            status = "late_settled" if shortfall == 0 else "late_settled_shortfall"
            transaction.create(
                late_tx_ref,
                {
                    "schema_version": 1,
                    "transaction_id": f"late_usage_{event.turn_id}",
                    "transaction_type": "agent_usage_late_debit",
                    "status": status,
                    "billing_subject_id": billing_subject_id,
                    "owner_uid": event.user_id,
                    "wallet_document_id": wallet_document_id,
                    "currency": "USD",
                    "turn_id": event.turn_id,
                    "request_id": reservation.get("request_id"),
                    "agent_id": event.agent_id,
                    "ledger_document_id": event.turn_id,
                    "reservation_id": reservation_id,
                    "reserved_amount_nanos": 0,
                    "estimated_cost_nanos": estimated_cost_nanos,
                    "amount_nanos": collected,
                    "released_amount_nanos": 0,
                    "shortfall_nanos": shortfall,
                    "usage_debt_added_nanos": shortfall,
                    "remaining_usage_debt_nanos": usage_debt + shortfall,
                    "pricing_model": event.usage.get("pricing_model"),
                    "pricing_version": event.usage.get("pricing_version"),
                    "usage": event.usage,
                    "created_at": settled_at,
                },
            )
            transaction.update(
                wallet_ref,
                {
                    "available_credit_nanos": available - collected,
                    "settled_usage_nanos": settled_usage + collected,
                    "usage_debt_nanos": usage_debt + shortfall,
                    "lifetime_usage_debt_added_nanos": nonnegative_int(
                        wallet.get("lifetime_usage_debt_added_nanos", 0),
                        field_name="lifetime_usage_debt_added_nanos",
                    ) + shortfall,
                    "updated_at": settled_at,
                    "last_settlement_at": settled_at,
                },
            )
            period_updates = _late_period_updates(
                period,
                estimated_cost_nanos=estimated_cost_nanos,
                collected_nanos=collected,
                shortfall_nanos=shortfall,
                settled_at=settled_at,
                decrement_unpriced=True,
            )
            transaction.update(period_ref, period_updates)
            transaction.update(
                reservation_ref,
                {
                    "status": status,
                    "estimated_cost_nanos": estimated_cost_nanos,
                    "accounted_cost_nanos": estimated_cost_nanos,
                    "settled_amount_nanos": collected,
                    "shortfall_nanos": shortfall,
                    "late_usage_shortfall_nanos": shortfall,
                    "recovered_usage": event.usage,
                    "usage_reconciliation_status": (
                        "pending_collection" if shortfall else "complete"
                    ),
                    "usage_reconciliation_next_attempt_at": (
                        settled_at
                        + timedelta(
                            seconds=max(1, self._settings.billing_usage_reconciliation_initial_backoff_seconds)
                        )
                        if shortfall
                        else None
                    ),
                    "usage_recovered_at": settled_at,
                    "updated_at": settled_at,
                },
            )
            return BillingSettlementResult(
                turn_id=event.turn_id,
                status=status,
                estimated_cost_nanos=estimated_cost_nanos,
                settled_amount_nanos=collected,
                released_amount_nanos=0,
                shortfall_nanos=shortfall,
                billing_period_id=billing_period_id,
            )

        return self._transaction_runner(client, operation)

    def _write_billing_period(
        self,
        *,
        transaction: Any,
        period_ref: Any,
        period: dict[str, Any],
        period_exists: bool,
        event: TurnCompletedEvent,
        billing_subject_id: str,
        period_key: str,
        period_start: datetime,
        period_end: datetime,
        settled_at: datetime,
        estimated_cost_nanos: int | None,
        settled_amount_nanos: int,
        shortfall_nanos: int,
    ) -> None:
        usage_estimated_nanos = estimated_cost_nanos or 0
        try:
            previous_usage = nonnegative_int(
                period.get("usage_estimated_nanos", 0),
                field_name="usage_estimated_nanos",
            )
            previous_collected = nonnegative_int(
                period.get("collected_usage_nanos", 0),
                field_name="collected_usage_nanos",
            )
            previous_shortfall = nonnegative_int(
                period.get("uncollected_usage_nanos", 0),
                field_name="uncollected_usage_nanos",
            )
            previous_turns = nonnegative_int(
                period.get("usage_turn_count", 0),
                field_name="usage_turn_count",
            )
            previous_unpriced_turns = nonnegative_int(
                period.get("unpriced_turn_count", 0),
                field_name="unpriced_turn_count",
            )
        except ValueError as exc:
            raise RetryableWorkerError(
                f"Billing-period data for turn {event.turn_id} is invalid."
            ) from exc

        common_updates = {
            "usage_estimated_nanos": previous_usage + usage_estimated_nanos,
            "collected_usage_nanos": previous_collected + settled_amount_nanos,
            "uncollected_usage_nanos": previous_shortfall + shortfall_nanos,
            "usage_turn_count": previous_turns + 1,
            "unpriced_turn_count": previous_unpriced_turns + int(estimated_cost_nanos is None),
            "last_turn_at": event.created_at,
            "updated_at": settled_at,
        }
        if period_exists:
            transaction.update(period_ref, common_updates)
            return
        transaction.create(
            period_ref,
            {
                "schema_version": 1,
                "billing_subject_id": billing_subject_id,
                "owner_uid": event.user_id,
                "currency": "USD",
                "period_key": period_key,
                "period_start": period_start,
                "period_end": period_end,
                "status": "open",
                "monthly_service_fee_nanos": self._settings.monthly_service_fee_nanos,
                "monthly_service_fee_status": "pending_collection",
                "created_at": settled_at,
                **common_updates,
            },
        )

    @staticmethod
    def _billing_metadata(event: TurnCompletedEvent) -> dict[str, Any] | None:
        metadata = event.metadata if isinstance(event.metadata, dict) else {}
        billing = metadata.get("billing")
        return dict(billing) if isinstance(billing, dict) else None

    @staticmethod
    def _required_string(value: Any, field_name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise RetryableWorkerError(f"Billing metadata field {field_name} is required.")
        return value.strip()

    @staticmethod
    def _validate_settlement_records(
        *,
        event: TurnCompletedEvent,
        reservation: dict[str, Any],
        ledger: dict[str, Any],
        wallet: dict[str, Any],
        billing_subject_id: str,
        reservation_id: str,
    ) -> None:
        if reservation.get("status") not in {
            "reserved",
            "completed_pending",
            "late_usage_pending",
            "unpriced_manual_review",
        }:
            raise RetryableWorkerError(
                f"Billing marker {reservation_id} is not pending settlement."
            )
        if (
            reservation.get("turn_id") != event.turn_id
            or reservation.get("reservation_id") != reservation_id
            or reservation.get("owner_uid") != event.user_id
            or reservation.get("billing_subject_id") != billing_subject_id
            or reservation.get("agent_id") != event.agent_id
        ):
            raise RetryableWorkerError(
                f"Billing reservation {reservation_id} does not match completed turn {event.turn_id}."
            )
        ledger_reservation_id = ledger.get("billing_reservation_id")
        if (
            ledger.get("turn_id") != event.turn_id
            or ledger.get("uid") != event.user_id
            or ledger.get("billing_subject_id") != billing_subject_id
            or (
                ledger_reservation_id is not None
                and ledger_reservation_id != reservation_id
            )
        ):
            raise RetryableWorkerError(
                f"Billing ledger for turn {event.turn_id} does not match its reservation."
            )
        if (
            wallet.get("status") != "active"
            or wallet.get("owner_uid") != event.user_id
            or wallet.get("billing_subject_id") != billing_subject_id
        ):
            raise RetryableWorkerError(
                f"Wallet for turn {event.turn_id} is inactive or does not match the turn owner."
            )

    @staticmethod
    def _result_from_existing_transaction(
        transaction_data: dict[str, Any],
        turn_id: str,
        billing_period_id: str,
    ) -> BillingSettlementResult:
        try:
            estimated_cost = transaction_data.get("estimated_cost_nanos")
            if estimated_cost is not None:
                estimated_cost = nonnegative_int(
                    estimated_cost,
                    field_name="estimated_cost_nanos",
                )
            return BillingSettlementResult(
                turn_id=turn_id,
                status=str(transaction_data.get("status", "settled")),
                estimated_cost_nanos=estimated_cost,
                settled_amount_nanos=nonnegative_int(
                    transaction_data.get("amount_nanos", 0),
                    field_name="amount_nanos",
                ),
                released_amount_nanos=nonnegative_int(
                    transaction_data.get("released_amount_nanos", 0),
                    field_name="released_amount_nanos",
                ),
                shortfall_nanos=nonnegative_int(
                    transaction_data.get("shortfall_nanos", 0),
                    field_name="shortfall_nanos",
                ),
                billing_period_id=billing_period_id,
            )
        except ValueError as exc:
            raise RetryableWorkerError(
                f"Existing wallet transaction for turn {turn_id} is invalid."
            ) from exc


def _ledger_cost_nanos(ledger: dict[str, Any]) -> int | None:
    cost = ledger.get("estimated_cost_usd_nanos")
    if cost is None:
        cost = ledger.get("provisional_estimated_cost_usd_nanos")
    if cost is None:
        return None
    try:
        return nonnegative_int(cost, field_name="estimated_cost_usd_nanos")
    except ValueError as exc:
        raise RetryableWorkerError("Billing ledger has an invalid estimated cost.") from exc


def _has_supported_usage_recovery(recovery: dict[str, Any]) -> bool:
    invocation_ids = recovery.get("invocation_ids")
    has_invocation = isinstance(invocation_ids, list) and any(
        isinstance(value, str) and value.strip() for value in invocation_ids
    )
    if recovery.get("source") == "cloud_run_adk_session_events":
        return (
            isinstance(recovery.get("base_url"), str)
            and bool(recovery.get("base_url", "").strip())
            and isinstance(recovery.get("app_name"), str)
            and bool(recovery.get("app_name", "").strip())
            and has_invocation
        )
    if recovery.get("source") == "agent_runtime_session_events":
        return all(
            isinstance(recovery.get(field), str) and bool(recovery[field].strip())
            for field in ("resource_name", "region", "started_at", "completed_at")
        ) and has_invocation
    return False


def _usage_cost_nanos(usage: dict[str, Any]) -> int | None:
    value = usage.get("estimated_cost_usd")
    if isinstance(value, bool) or value is None:
        return None
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not amount.is_finite() or amount < 0:
        return None
    return int((amount * Decimal("1000000000")).to_integral_value(rounding=ROUND_HALF_UP))


def _wallet_amount(value: Any) -> int:
    try:
        amount = int(value)
    except (TypeError, ValueError) as exc:
        raise RetryableWorkerError("Wallet available credit is invalid.") from exc
    if amount < 0:
        raise RetryableWorkerError("Wallet available credit cannot be negative.")
    return amount


def _late_period_updates(
    period: dict[str, Any],
    *,
    estimated_cost_nanos: int,
    collected_nanos: int,
    shortfall_nanos: int,
    settled_at: datetime,
    decrement_unpriced: bool,
) -> dict[str, Any]:
    try:
        estimated = nonnegative_int(
            period.get("usage_estimated_nanos", 0), field_name="usage_estimated_nanos"
        )
        collected = nonnegative_int(
            period.get("collected_usage_nanos", 0), field_name="collected_usage_nanos"
        )
        uncollected = nonnegative_int(
            period.get("uncollected_usage_nanos", 0), field_name="uncollected_usage_nanos"
        )
        unpriced = nonnegative_int(
            period.get("unpriced_turn_count", 0), field_name="unpriced_turn_count"
        )
    except ValueError as exc:
        raise RetryableWorkerError("Billing-period late-usage data is invalid.") from exc
    new_uncollected = uncollected + shortfall_nanos
    if new_uncollected < 0:
        raise RetryableWorkerError("Billing-period late-usage shortfall is inconsistent.")
    return {
        "usage_estimated_nanos": estimated + estimated_cost_nanos,
        "collected_usage_nanos": collected + collected_nanos,
        "uncollected_usage_nanos": new_uncollected,
        "unpriced_turn_count": max(0, unpriced - 1) if decrement_unpriced else unpriced,
        "updated_at": settled_at,
    }


def _settlement_amounts(
    *,
    estimated_cost_nanos: int | None,
    reserved_amount_nanos: int,
    available_credit_nanos: int,
) -> tuple[str, int, int, int]:
    """Return status, collected amount, released amount, and shortfall."""

    if estimated_cost_nanos is None:
        return "unpriced_released", 0, reserved_amount_nanos, 0
    if estimated_cost_nanos <= reserved_amount_nanos:
        return (
            "settled",
            estimated_cost_nanos,
            reserved_amount_nanos - estimated_cost_nanos,
            0,
        )

    additional_needed = estimated_cost_nanos - reserved_amount_nanos
    additional_collected = max(0, min(additional_needed, available_credit_nanos))
    settled_amount_nanos = reserved_amount_nanos + additional_collected
    shortfall_nanos = estimated_cost_nanos - settled_amount_nanos
    status = "settled" if shortfall_nanos == 0 else "settled_shortfall"
    return status, settled_amount_nanos, 0, shortfall_nanos


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _billing_period(value: datetime) -> tuple[str, datetime, datetime]:
    period_start = datetime(value.year, value.month, 1, tzinfo=timezone.utc)
    if value.month == 12:
        period_end = datetime(value.year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        period_end = datetime(value.year, value.month + 1, 1, tzinfo=timezone.utc)
    return period_start.strftime("%Y-%m"), period_start, period_end


def _run_firestore_transaction(
    client: Any,
    operation: Callable[[Any], BillingSettlementResult],
) -> BillingSettlementResult:
    """Run a Firestore transaction with retry-on-contention semantics."""

    from google.cloud import firestore

    transaction = client.transaction()

    @firestore.transactional
    def run(transaction: Any) -> BillingSettlementResult:
        return operation(transaction)

    return run(transaction)
