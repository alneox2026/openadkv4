"""Worker-owned, zero-hold lifecycle markers for completed turns."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from common.schemas import TurnCompletedEvent
from services.agent_persistence_worker_v3.app.core.config import get_settings
from services.agent_persistence_worker_v3.app.core.errors import RetryableWorkerError


class BillingReservationRepository:
    """Creates the turn lifecycle marker atomically with its immutable ledger.

    The legacy collection name is retained for schema compatibility, but new
    documents are not wallet holds: the gateway performs a read-only minimum
    balance check, and estimates are not subtracted from wallet balance.
    """

    def __init__(self, collection: str | None = None) -> None:
        self._collection = collection or get_settings().billing_reservations_collection
        self._settings = get_settings()

    def add_create_to_batch(
        self, batch: Any, client: Any, event: TurnCompletedEvent
    ) -> None:
        metadata = event.metadata if isinstance(event.metadata, dict) else {}
        billing = metadata.get("billing")
        if not isinstance(billing, dict):
            return
        turn_id = str(event.turn_id)
        reservation_id = str(
            billing.get("reservation_id") or billing.get("billing_reservation_id") or ""
        ).strip()
        subject_id = str(billing.get("billing_subject_id") or "").strip()
        if reservation_id != turn_id or subject_id != event.user_id:
            raise RetryableWorkerError(
                f"Billing metadata does not match completed turn {turn_id}."
            )

        now = datetime.now(timezone.utc)
        usage = event.usage if isinstance(event.usage, dict) else {}
        provider_recovery = metadata.get("provider_usage_recovery")
        provider_pending = _has_pending_provider_usage(
            usage,
            provider_recovery if isinstance(provider_recovery, dict) else None,
        )
        provisional_cost_nanos = _usage_cost_nanos(usage)
        data: dict[str, Any] = {
            "schema_version": 1,
            "reservation_id": turn_id,
            "turn_id": turn_id,
            "request_id": billing.get("request_id"),
            "agent_id": event.agent_id,
            "session_id": event.session_id,
            "thread_id": event.thread_id,
            "owner_uid": event.user_id,
            "billing_subject_id": subject_id,
            "currency": "USD",
            "reserved_amount_nanos": 0,
            "minimum_balance_nanos": _nonnegative_int(
                billing.get("minimum_balance_nanos")
            ),
            "authorization_mode": "minimum_balance_check",
            "status": (
                "completed_pending"
                if provisional_cost_nanos is not None
                else "late_usage_pending"
                if provider_pending
                else "unpriced_manual_review"
            ),
            "initial_estimated_cost_nanos": provisional_cost_nanos,
            "estimated_cost_nanos": provisional_cost_nanos,
            "estimated_cost_complete": usage.get("estimated_cost_complete") is not False,
            "provider_usage_recovery": (
                dict(provider_recovery) if isinstance(provider_recovery, dict) else {}
            ),
            "created_at": event.created_at or now,
            "turn_created_at": event.created_at or now,
            "updated_at": now,
        }
        if provider_pending:
            data.update(
                {
                    "usage_reconciliation_status": "pending",
                    "usage_reconciliation_attempt_count": 0,
                    "usage_reconciliation_started_at": now,
                    "usage_reconciliation_deadline_at": now
                    + timedelta(
                        seconds=max(
                            1,
                            self._settings.billing_usage_reconciliation_retention_seconds,
                        )
                    ),
                    "usage_reconciliation_next_attempt_at": now
                    + timedelta(
                        seconds=max(
                            1,
                            self._settings.billing_usage_reconciliation_initial_backoff_seconds,
                        )
                    ),
                }
            )
        batch.create(
            client.collection(self._collection).document(turn_id),
            data,
        )


def _has_pending_provider_usage(
    usage: dict[str, Any], provider_recovery: dict[str, Any] | None = None
) -> bool:
    if not isinstance(provider_recovery, dict) or not provider_recovery.get("source"):
        return False
    return (
        usage.get("estimated_cost_complete") is False
        or usage.get("estimated_cost_usd") is None
    )


def _usage_cost_nanos(usage: dict[str, Any]) -> int | None:
    from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

    value = usage.get("estimated_cost_usd")
    if value is None:
        value = usage.get("known_cost_lower_bound_usd")
    if value is None or isinstance(value, bool):
        return None
    try:
        cost = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not cost.is_finite() or cost < 0:
        return None
    return int((cost * Decimal("1000000000")).to_integral_value(rounding=ROUND_HALF_UP))


def _nonnegative_int(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return 0
    return value

