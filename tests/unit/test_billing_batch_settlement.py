from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from common.billing import customer_billing_period_document_id, customer_wallet_document_id
from services.agent_persistence_worker_v3.app.core.config import get_settings
from services.agent_persistence_worker_v3.app.services.billing_batch_settlement import (
    BillingBatchSettlementService,
)
from tests.unit.test_billing_settlement import _event
from tests.unit.test_wallet_reservations import FakeClient


@pytest.fixture(autouse=True)
def _reset_worker_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _run_transaction(client, operation):
    return operation(client.transaction())


def _seed_correctable_turn(client, *, status="settled_shortfall", available=20, debt=0):
    settings = get_settings()
    wallet_id = customer_wallet_document_id("user-1")
    period_id = customer_billing_period_document_id("user-1", "2026-08")
    client.documents[(settings.wallets_collection, wallet_id)] = {
        "billing_subject_id": "user-1",
        "owner_uid": "user-1",
        "status": "active",
        "available_credit_nanos": available,
        "reserved_credit_nanos": 0,
        "settled_usage_nanos": 20,
        "usage_debt_nanos": debt,
        "lifetime_usage_debt_added_nanos": 80,
    }
    client.documents[(settings.billing_reservations_collection, "turn-1")] = {
        "reservation_id": "turn-1",
        "turn_id": "turn-1",
        "owner_uid": "user-1",
        "billing_subject_id": "user-1",
        "agent_id": "maxima",
        "status": status,
        "estimated_cost_nanos": 100,
        "accounted_cost_nanos": 100,
        "settled_amount_nanos": 20,
        "shortfall_nanos": 80 if status.endswith("shortfall") else 0,
        "period_key": "2026-08",
        "late_usage": False,
        "billing_revision": 0,
    }
    client.documents[(settings.billing_ledger_collection, "turn-1")] = {
        "turn_id": "turn-1",
        "uid": "user-1",
        "billing_subject_id": "user-1",
    }
    client.documents[(settings.customer_billing_periods_collection, period_id)] = {
        "billing_subject_id": "user-1",
        "owner_uid": "user-1",
        "period_key": "2026-08",
        "usage_estimated_nanos": 100,
        "collected_usage_nanos": 20,
        "uncollected_usage_nanos": 80 if status.endswith("shortfall") else 0,
    }
    return wallet_id, period_id


def _corrected_event(cost_nanos):
    return _event().model_copy(
        update={"usage": {"estimated_cost_usd": cost_nanos / 1_000_000_000}}
    )


def test_settled_shortfall_provider_increase_charges_only_the_difference_and_records_debt():
    settings = get_settings()
    client = FakeClient()
    wallet_id, period_id = _seed_correctable_turn(client, status="settled", available=5)
    client.documents[(settings.billing_reservations_collection, "turn-1")]["shortfall_nanos"] = 0
    client.documents[(settings.customer_billing_periods_collection, period_id)]["uncollected_usage_nanos"] = 0
    service = BillingBatchSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=_run_transaction,
    )

    result = service.enqueue_usage_correction_sync(_corrected_event(130))
    duplicate_result = service.enqueue_usage_correction_sync(_corrected_event(130))

    wallet = client.documents[(settings.wallets_collection, wallet_id)]
    reservation = client.documents[(settings.billing_reservations_collection, "turn-1")]
    period = client.documents[(settings.customer_billing_periods_collection, period_id)]
    tx = client.documents[(settings.wallet_transactions_collection, "usage_correction_turn-1_1")]
    assert result is True
    assert duplicate_result is False
    assert wallet["available_credit_nanos"] == 0
    assert wallet["usage_debt_nanos"] == 25
    assert tx["delta_nanos"] == 30
    assert tx["wallet_amount_nanos"] == 5
    assert tx["usage_debt_added_nanos"] == 25
    assert reservation["accounted_cost_nanos"] == 130
    assert reservation["shortfall_nanos"] == 25
    assert reservation["status"] == "settled_shortfall"
    assert period["usage_estimated_nanos"] == 130
    assert period["collected_usage_nanos"] == 25
    assert period["uncollected_usage_nanos"] == 25


def test_lower_correction_credits_difference_after_topup_already_repaid_wallet_debt():
    settings = get_settings()
    client = FakeClient()
    # The historical $0.80 debt was repaid by a later top-up; only $0.20 remains
    # available from that top-up after the initial $0.20 debit.
    wallet_id, period_id = _seed_correctable_turn(client, available=20, debt=0)
    service = BillingBatchSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=_run_transaction,
    )

    service.enqueue_usage_correction_sync(_corrected_event(10))

    wallet = client.documents[(settings.wallets_collection, wallet_id)]
    reservation = client.documents[(settings.billing_reservations_collection, "turn-1")]
    period = client.documents[(settings.customer_billing_periods_collection, period_id)]
    tx = client.documents[(settings.wallet_transactions_collection, "usage_correction_turn-1_1")]
    assert wallet["available_credit_nanos"] == 110
    assert wallet["usage_debt_nanos"] == 0
    assert tx["delta_nanos"] == -90
    assert tx["wallet_amount_nanos"] == 90
    assert tx["usage_debt_reduced_nanos"] == 0
    assert reservation["status"] == "settled"
    assert reservation["shortfall_nanos"] == 0
    assert reservation["settled_amount_nanos"] == 0
    assert period["usage_estimated_nanos"] == 10
    assert period["collected_usage_nanos"] == 0
    assert period["uncollected_usage_nanos"] == 0


def test_pending_batch_provider_correction_updates_only_the_pending_delta():
    settings = get_settings()
    client = FakeClient()
    wallet_id, _ = _seed_correctable_turn(client, status="batch_pending")
    reservation = client.documents[(settings.billing_reservations_collection, "turn-1")]
    reservation["estimated_cost_nanos"] = 100
    client.documents[(settings.wallets_collection, wallet_id)]["pending_batch_usage_nanos"] = 100
    service = BillingBatchSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=_run_transaction,
    )

    result = service.enqueue_usage_correction_sync(_corrected_event(130))

    wallet = client.documents[(settings.wallets_collection, wallet_id)]
    reservation = client.documents[(settings.billing_reservations_collection, "turn-1")]
    assert result is True
    assert wallet["pending_batch_usage_nanos"] == 130
    assert wallet["available_credit_nanos"] == 20
    assert wallet["usage_debt_nanos"] == 0
    assert reservation["estimated_cost_nanos"] == 130
    assert reservation["estimated_cost_complete"] is True
    assert not any(key[0] == settings.wallet_transactions_collection for key in client.documents)


def test_matching_pending_provider_cost_does_not_rewrite_wallet():
    settings = get_settings()
    client = FakeClient()
    wallet_id, _ = _seed_correctable_turn(client, status="batch_pending")
    reservation = client.documents[(settings.billing_reservations_collection, "turn-1")]
    reservation["estimated_cost_nanos"] = 100
    client.documents[(settings.wallets_collection, wallet_id)]["pending_batch_usage_nanos"] = 100
    wallet_before = dict(client.documents[(settings.wallets_collection, wallet_id)])
    service = BillingBatchSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=_run_transaction,
    )

    result = service.enqueue_usage_correction_sync(_corrected_event(100))

    wallet_after = client.documents[(settings.wallets_collection, wallet_id)]
    reservation_after = client.documents[(settings.billing_reservations_collection, "turn-1")]
    assert result is False
    assert wallet_after == wallet_before
    assert reservation_after["estimated_cost_complete"] is True
    assert reservation_after["usage_reconciliation_status"] == "complete"


def test_batch_close_debits_available_credit_and_records_unpaid_remainder_as_debt():
    settings = get_settings()
    client = FakeClient()
    wallet_id = customer_wallet_document_id("user-1")
    turn_id = "turn-batch"
    period_key = "2026-08"
    queued_at = datetime(2026, 8, 11, 10, 0, tzinfo=timezone.utc)
    reservation_ref = client.collection(settings.billing_reservations_collection).document(turn_id)
    reservation = {
        "reservation_id": turn_id,
        "turn_id": turn_id,
        "owner_uid": "user-1",
        "billing_subject_id": "user-1",
        "agent_id": "maxima",
        "status": "batch_pending",
        "estimated_cost_nanos": 100,
        "initial_estimated_cost_nanos": 100,
        "reserved_amount_nanos": 0,
        "period_key": period_key,
        "turn_created_at": queued_at,
        "billing_batch_created_at": queued_at,
        "late_usage": False,
    }
    client.documents[reservation_ref.key] = dict(reservation)
    client.documents[(settings.billing_ledger_collection, turn_id)] = {
        "turn_id": turn_id,
        "uid": "user-1",
        "billing_subject_id": "user-1",
        "estimated_cost_usd_nanos": 100,
    }
    client.documents[(settings.wallets_collection, wallet_id)] = {
        "billing_subject_id": "user-1",
        "owner_uid": "user-1",
        "status": "active",
        "available_credit_nanos": 60,
        "reserved_credit_nanos": 0,
        "pending_batch_usage_nanos": 100,
        "settled_usage_nanos": 0,
        "usage_debt_nanos": 0,
        "lifetime_usage_debt_added_nanos": 0,
    }
    service = BillingBatchSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=_run_transaction,
    )
    snapshot = SimpleNamespace(
        reference=reservation_ref,
        to_dict=lambda: dict(reservation),
    )

    result = service._settle_batch(client, [(snapshot, reservation)])

    wallet = client.documents[(settings.wallets_collection, wallet_id)]
    settled_reservation = client.documents[reservation_ref.key]
    batch_transaction = client.documents[(settings.wallet_transactions_collection, result.batch_id)]
    assert result.status == "settled_shortfall"
    assert result.estimated_cost_nanos == 100
    assert result.charged_amount_nanos == 60
    assert result.shortfall_nanos == 40
    assert wallet["available_credit_nanos"] == 0
    assert wallet["pending_batch_usage_nanos"] == 0
    assert wallet["settled_usage_nanos"] == 60
    assert wallet["usage_debt_nanos"] == 40
    assert settled_reservation["status"] == "settled_shortfall"
    assert settled_reservation["accounted_cost_nanos"] == 100
    assert batch_transaction["amount_nanos"] == 60
    assert batch_transaction["shortfall_nanos"] == 40
    assert client.documents[(settings.customer_billing_periods_collection,
                             customer_billing_period_document_id("user-1", period_key))][
        "uncollected_usage_nanos"
    ] == 40


def test_replayed_turn_enqueue_is_idempotent_after_batch_settlement():
    settings = get_settings()
    client = FakeClient()
    client.documents[(settings.billing_reservations_collection, "turn-1")] = {
        "reservation_id": "turn-1",
        "turn_id": "turn-1",
        "status": "settled",
    }
    service = BillingBatchSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=_run_transaction,
    )
    event = _event().model_copy(update={"usage": {"estimated_cost_usd": 0.1}})

    assert service.enqueue_turn_sync(event) is False
    assert client.writes == []

