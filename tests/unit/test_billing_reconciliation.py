from datetime import datetime, timezone

import pytest

from common.billing import customer_wallet_document_id
from services.agent_persistence_worker_v3.app.core.config import get_settings
from services.agent_persistence_worker_v3.app.services import billing_reconciliation
from services.agent_persistence_worker_v3.app.services.billing_reconciliation import (
    BillingReconciliationService,
)
from tests.unit.test_wallet_reservations import FakeClient


@pytest.fixture(autouse=True)
def _reset_worker_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _run_transaction(client, operation):
    return operation(client.transaction())


def test_expired_reservation_releases_held_credit_when_no_ledger_exists(monkeypatch):
    settings = get_settings()
    client = FakeClient()
    user_id = "user-1"
    wallet_id = customer_wallet_document_id(user_id)
    client.documents[(settings.wallets_collection, wallet_id)] = {
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
    monkeypatch.setattr(
        billing_reconciliation,
        "_run_firestore_transaction",
        _run_transaction,
    )
    service = BillingReconciliationService(
        firestore_client_factory=lambda: client,
    )

    released = service._release_expired_reservation_sync(
        client,
        client.documents[(settings.billing_reservations_collection, "turn-1")],
        datetime(2026, 8, 11, 11, 0, tzinfo=timezone.utc),
    )

    assert released is True
    wallet = client.documents[(settings.wallets_collection, wallet_id)]
    assert wallet["available_credit_nanos"] == 1_000_000_000
    assert wallet["reserved_credit_nanos"] == 0
    reservation = client.documents[(settings.billing_reservations_collection, "turn-1")]
    assert reservation["status"] == "expired_released"
    transaction = client.documents[(settings.wallet_transactions_collection, "reservation_expired_turn-1")]
    assert transaction["transaction_type"] == "reservation_expiry_release"
    assert transaction["released_amount_nanos"] == 500_000_000


def test_expired_reservation_query_filters_status_before_applying_batch_limit(
    monkeypatch,
):
    now = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setenv("BILLING_RECONCILIATION_BATCH_SIZE", "2")
    get_settings.cache_clear()

    class Snapshot:
        def __init__(self, data):
            self.data = data

        def to_dict(self):
            return dict(self.data)

    class Collection:
        def __init__(self, documents):
            self.documents = documents

        def stream(self):
            return list(self.documents)

    class Client:
        def __init__(self, documents):
            self.collection_ref = Collection(documents)

        def collection(self, name):
            return self.collection_ref

    expired = now.replace(hour=10)
    documents = [
        Snapshot({"status": "settled", "expires_at": expired})
        for _ in range(5)
    ]
    active = Snapshot({"status": "reserved", "expires_at": expired})
    documents.append(active)
    client = Client(documents)
    service = BillingReconciliationService(firestore_client_factory=lambda: client)

    results = list(service._expired_reservations(client, now))

    assert results == [active]


def test_usage_recovery_retry_uses_capped_backoff_and_retention_deadline(monkeypatch):
    monkeypatch.setenv("BILLING_USAGE_RECONCILIATION_INITIAL_BACKOFF_SECONDS", "300")
    monkeypatch.setenv("BILLING_USAGE_RECONCILIATION_MAX_BACKOFF_SECONDS", "21600")
    monkeypatch.setenv("BILLING_USAGE_RECONCILIATION_RETENTION_SECONDS", "604800")
    get_settings.cache_clear()
    now = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)

    class Document:
        def __init__(self):
            self.data = {}

        def update(self, data):
            self.data.update(data)

    class Collection:
        def __init__(self):
            self.document_ref = Document()

        def document(self, document_id):
            return self.document_ref

    class Client:
        def __init__(self):
            self.collection_ref = Collection()

        def collection(self, name):
            return self.collection_ref

    client = Client()
    service = BillingReconciliationService(firestore_client_factory=lambda: client)
    deadline = now.replace(hour=15)
    reservation = {
        "turn_id": "turn-1",
        "status": "late_usage_pending",
        "usage_reconciliation_attempt_count": 9,
        "usage_reconciliation_started_at": now,
        "usage_reconciliation_deadline_at": deadline,
    }

    service._schedule_usage_retry(client, reservation, now)

    assert client.collection_ref.document_ref.data["usage_reconciliation_attempt_count"] == 10
    assert client.collection_ref.document_ref.data["usage_reconciliation_next_attempt_at"] == deadline

    reservation["usage_reconciliation_deadline_at"] = now.replace(day=18)
    service._schedule_usage_retry(client, reservation, now)
    assert client.collection_ref.document_ref.data["usage_reconciliation_next_attempt_at"] == (
        now.replace(day=11, hour=18)
    )

