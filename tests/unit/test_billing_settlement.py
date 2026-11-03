import asyncio
from datetime import datetime, timezone

import pytest

from common.billing import (
    customer_billing_period_document_id,
    customer_wallet_document_id,
)
from common.schemas import TurnCompletedEvent
from services.agent_persistence_worker_v3.app.core.config import get_settings
from services.agent_persistence_worker_v3.app.services.billing_settlement import (
    BillingSettlementService,
)
from services.agent_gateway_v3.app.services.wallet_reservations import (
    WalletBalanceGate,
)

from tests.unit.test_wallet_reservations import FakeClient


@pytest.fixture(autouse=True)
def _reset_worker_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _run_transaction(client, operation):
    return operation(client.transaction())


def _enable_worker_billing(monkeypatch):
    monkeypatch.setenv("MONTHLY_SERVICE_FEE_NANOS", "5000000000")
    get_settings.cache_clear()


def _event():
    return TurnCompletedEvent(
        event_id="evt-1",
        turn_id="turn-1",
        agent_id="maxima",
        user_id="user-1",
        thread_id="thread-1",
        session_id="session-1",
        user_message="hello",
        assistant_message="hi",
        created_at=datetime(2026, 8, 11, 10, 0, tzinfo=timezone.utc),
        metadata={
            "request_id": "req-1",
            "billing": {
                "reservation_id": "turn-1",
                "billing_subject_id": "user-1",
                "reserved_amount_nanos": 500_000_000,
                "currency": "USD",
            },
        },
    )


def _seed_reservation_and_ledger(client):
    settings = get_settings()
    user_id = "user-1"
    wallet_id = customer_wallet_document_id(user_id)
    client.documents[(settings.wallets_collection, wallet_id)] = {
        "schema_version": 1,
        "billing_subject_id": user_id,
        "owner_uid": user_id,
        "currency": "USD",
        "status": "active",
        "available_credit_nanos": 500_000_000,
        "reserved_credit_nanos": 500_000_000,
        "settled_usage_nanos": 0,
    }
    client.documents[(settings.billing_reservations_collection, "turn-1")] = {
        "reservation_id": "turn-1",
        "turn_id": "turn-1",
        "request_id": "req-1",
        "billing_subject_id": user_id,
        "owner_uid": user_id,
        "agent_id": "maxima",
        "currency": "USD",
        "reserved_amount_nanos": 500_000_000,
        "status": "reserved",
    }
    client.documents[(settings.billing_ledger_collection, "turn-1")] = {
        "turn_id": "turn-1",
        "uid": user_id,
        "billing_subject_id": user_id,
        "billing_reservation_id": "turn-1",
        "estimated_cost_usd_nanos": 100_000_000,
        "pricing_model": "gpt-6-luna",
        "pricing_version": "test-price-v1",
    }


def test_settlement_debits_actual_usage_releases_the_rest_and_is_idempotent(monkeypatch):
    _enable_worker_billing(monkeypatch)
    settings = get_settings()
    client = FakeClient()
    _seed_reservation_and_ledger(client)
    service = BillingSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=_run_transaction,
    )

    result = asyncio.run(service.settle_if_required(_event()))
    duplicate_result = asyncio.run(service.settle_if_required(_event()))

    assert result is not None
    assert result.status == "settled"
    assert result.settled_amount_nanos == 100_000_000
    assert result.released_amount_nanos == 400_000_000
    assert duplicate_result == result
    wallet = client.documents[(settings.wallets_collection, customer_wallet_document_id("user-1"))]
    assert wallet["available_credit_nanos"] == 900_000_000
    assert wallet["reserved_credit_nanos"] == 0
    assert wallet["settled_usage_nanos"] == 100_000_000
    wallet_transaction = client.documents[(settings.wallet_transactions_collection, "usage_turn-1")]
    assert wallet_transaction["amount_nanos"] == 100_000_000
    period_id = customer_billing_period_document_id("user-1", "2026-08")
    period = client.documents[(settings.customer_billing_periods_collection, period_id)]
    assert period["usage_estimated_nanos"] == 100_000_000
    assert period["monthly_service_fee_nanos"] == 5_000_000_000
    assert period["monthly_service_fee_status"] == "pending_collection"


def test_zero_hold_fallback_records_unfunded_usage_as_shared_wallet_debt(monkeypatch):
    _enable_worker_billing(monkeypatch)
    settings = get_settings()
    client = FakeClient()
    _seed_reservation_and_ledger(client)
    wallet_id = customer_wallet_document_id("user-1")
    wallet = client.documents[(settings.wallets_collection, wallet_id)]
    wallet.update(
        {"available_credit_nanos": 60_000_000, "reserved_credit_nanos": 0}
    )
    reservation = client.documents[(settings.billing_reservations_collection, "turn-1")]
    reservation.update({"status": "completed_pending", "reserved_amount_nanos": 0})
    event = _event()
    event = event.model_copy(
        update={
            "metadata": {
                **event.metadata,
                "billing": {
                    **event.metadata["billing"],
                    "reserved_amount_nanos": 0,
                    "minimum_balance_nanos": 50_000_000,
                },
            }
        }
    )
    service = BillingSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=_run_transaction,
    )

    result = service.settle_if_required_sync(event)

    assert result is not None and result.status == "settled_shortfall"
    assert result.settled_amount_nanos == 60_000_000
    assert result.shortfall_nanos == 40_000_000
    assert wallet["available_credit_nanos"] == 0
    assert wallet["reserved_credit_nanos"] == 0
    assert wallet["settled_usage_nanos"] == 60_000_000
    assert wallet["usage_debt_nanos"] == 40_000_000
    assert wallet["lifetime_usage_debt_added_nanos"] == 40_000_000
    usage_tx = client.documents[(settings.wallet_transactions_collection, "usage_turn-1")]
    assert usage_tx["usage_debt_added_nanos"] == 40_000_000
    assert usage_tx["remaining_usage_debt_nanos"] == 40_000_000
    assert reservation["accounted_cost_nanos"] == 100_000_000
    assert reservation["period_key"] == "2026-08"


def test_zero_value_missing_usage_marker_does_not_hold_or_block_follow_up_turn(monkeypatch):
    _enable_worker_billing(monkeypatch)
    monkeypatch.setenv("BILLING_ENFORCEMENT_ENABLED", "true")
    settings = get_settings()
    client = FakeClient()
    _seed_reservation_and_ledger(client)
    ledger = client.documents[(settings.billing_ledger_collection, "turn-1")]
    ledger["estimated_cost_usd_nanos"] = None
    ledger["provider_usage_recovery"] = {
        "source": "cloud_run_adk_session_events",
        "base_url": "https://agent.example.run.app",
        "app_name": "app",
        "invocation_ids": ["invocation-1"],
    }
    client.documents[(settings.wallets_collection, customer_wallet_document_id("user-1"))].update(
        {"available_credit_nanos": 500_000_000, "reserved_credit_nanos": 0}
    )
    client.documents[(settings.billing_reservations_collection, "turn-1")].update(
        {"status": "late_usage_pending", "reserved_amount_nanos": 0}
    )
    event = _event()
    event = event.model_copy(
        update={
            "metadata": {
                **event.metadata,
                "billing": {
                    **event.metadata["billing"],
                    "reserved_amount_nanos": 0,
                },
            }
        }
    )
    worker = BillingSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=_run_transaction,
    )

    result = worker.settle_if_required_sync(event)

    assert result is not None and result.status == "unpriced_released"
    wallet_id = customer_wallet_document_id("user-1")
    wallet = client.documents[(settings.wallets_collection, wallet_id)]
    assert wallet["available_credit_nanos"] == 500_000_000
    assert wallet["reserved_credit_nanos"] == 0
    reservation = client.documents[(settings.billing_reservations_collection, "turn-1")]
    assert reservation["status"] == "late_usage_pending"

    gate = WalletBalanceGate(firestore_client_factory=lambda: client)
    next_turn = asyncio.run(
        gate.authorize(
        user_id="user-1",
            agent_id="maxima",
            request_id="req-2",
            turn_id="turn-2",
        )
    )
    assert next_turn is not None
    assert next_turn.minimum_balance_nanos == 50_000_000
    assert client.documents[(settings.wallets_collection, wallet_id)][
        "available_credit_nanos"
    ] == 500_000_000


def test_recovered_usage_is_debited_from_current_available_balance(monkeypatch):
    _enable_worker_billing(monkeypatch)
    settings = get_settings()
    client = FakeClient()
    _seed_reservation_and_ledger(client)
    ledger = client.documents[(settings.billing_ledger_collection, "turn-1")]
    ledger["estimated_cost_usd_nanos"] = None
    ledger["provider_usage_recovery"] = {
        "source": "cloud_run_adk_session_events",
        "base_url": "https://agent.example.run.app",
        "app_name": "app",
        "invocation_ids": ["invocation-1"],
    }
    service = BillingSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=_run_transaction,
    )
    service.settle_if_required_sync(_event())
    recovered_event = _event().model_copy(
        update={"usage": {"estimated_cost_usd": 0.1, "pricing_model": "gpt-6-luna"}}
    )

    result = service.settle_late_usage_sync(recovered_event)
    duplicate = service.settle_late_usage_sync(recovered_event)

    assert result is not None and result.status == "late_settled"
    assert result.settled_amount_nanos == 100_000_000
    assert duplicate == result
    wallet = client.documents[(settings.wallets_collection, customer_wallet_document_id("user-1"))]
    assert wallet["available_credit_nanos"] == 900_000_000
    assert wallet["reserved_credit_nanos"] == 0
    late_tx = client.documents[(settings.wallet_transactions_collection, "late_usage_turn-1")]
    assert late_tx["amount_nanos"] == 100_000_000
    period_id = customer_billing_period_document_id("user-1", "2026-08")
    period = client.documents[(settings.customer_billing_periods_collection, period_id)]
    assert period["usage_estimated_nanos"] == 100_000_000
    assert period["unpriced_turn_count"] == 0


def test_late_usage_shortfall_is_written_to_debt_not_collected_again(monkeypatch):
    _enable_worker_billing(monkeypatch)
    settings = get_settings()
    client = FakeClient()
    _seed_reservation_and_ledger(client)
    ledger = client.documents[(settings.billing_ledger_collection, "turn-1")]
    ledger["estimated_cost_usd_nanos"] = None
    ledger["provider_usage_recovery"] = {
        "source": "cloud_run_adk_session_events",
        "base_url": "https://agent.example.run.app",
        "app_name": "app",
        "invocation_ids": ["invocation-1"],
    }
    service = BillingSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=_run_transaction,
    )
    service.settle_if_required_sync(_event())
    event = _event().model_copy(update={"usage": {"estimated_cost_usd": 1.5}})

    result = service.settle_late_usage_sync(event)
    assert result is not None and result.shortfall_nanos == 500_000_000
    wallet_id = customer_wallet_document_id("user-1")
    wallet = client.documents[(settings.wallets_collection, wallet_id)]
    assert wallet["available_credit_nanos"] == 0
    assert wallet["usage_debt_nanos"] == 500_000_000
    assert wallet["lifetime_usage_debt_added_nanos"] == 500_000_000
    assert wallet["settled_usage_nanos"] == 1_000_000_000
    late_tx = client.documents[(settings.wallet_transactions_collection, "late_usage_turn-1")]
    assert late_tx["usage_debt_added_nanos"] == 500_000_000
    assert late_tx["remaining_usage_debt_nanos"] == 500_000_000
    period_id = customer_billing_period_document_id("user-1", "2026-08")
    period = client.documents[(settings.customer_billing_periods_collection, period_id)]
    assert period["collected_usage_nanos"] == 1_000_000_000
    assert period["uncollected_usage_nanos"] == 500_000_000

