from datetime import datetime, timezone

import asyncio
import pytest

from common.billing import customer_wallet_document_id
from services.agent_gateway_v3.app.core.config import get_settings
from services.agent_gateway_v3.app.core.errors import ApiError
from services.agent_gateway_v3.app.services.wallet_reservations import WalletBalanceGate


@pytest.fixture(autouse=True)
def _reset_gateway_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class FakeSnapshot:
    def __init__(self, data):
        self._data = data
        self.exists = data is not None

    def to_dict(self):
        return dict(self._data) if self._data is not None else None


class FakeDocumentReference:
    def __init__(self, client, collection, document_id):
        self.client = client
        self.collection = collection
        self.document_id = document_id

    @property
    def id(self):
        return self.document_id

    @property
    def key(self):
        return self.collection, self.document_id

    def get(self):
        self.client.reads.append(self.key)
        return FakeSnapshot(self.client.documents.get(self.key))


class FakeCollectionReference:
    def __init__(self, client, collection):
        self.client = client
        self.collection_name = collection

    def document(self, document_id):
        return FakeDocumentReference(self.client, self.collection_name, document_id)


class FakeClient:
    """Implements Firestore test doubles and records every financial write."""

    def __init__(self):
        self.documents = {}
        self.reads = []
        self.writes = []

    def collection(self, collection):
        return FakeCollectionReference(self, collection)

    def transaction(self):
        return FakeTransaction(self)


class FakeTransaction:
    def __init__(self, client):
        self.client = client

    def get(self, document_ref):
        self.client.reads.append(document_ref.key)
        return FakeSnapshot(self.client.documents.get(document_ref.key))

    def create(self, document_ref, data):
        if document_ref.key in self.client.documents:
            raise RuntimeError("duplicate create")
        self.client.documents[document_ref.key] = dict(data)
        self.client.writes.append(("create", document_ref.key))

    def update(self, document_ref, updates):
        self.client.documents[document_ref.key].update(updates)
        self.client.writes.append(("update", document_ref.key))


def _enable_billing(monkeypatch, *, minimum="50000000"):
    monkeypatch.setenv("BILLING_ENFORCEMENT_ENABLED", "true")
    monkeypatch.setenv("BILLING_MINIMUM_BALANCE_NANOS", minimum)
    get_settings.cache_clear()


def _wallet(user_id, available_credit_nanos=50_000_000):
    return {
        "schema_version": 1,
        "billing_subject_id": user_id,
        "owner_uid": user_id,
        "currency": "USD",
        "status": "active",
        "available_credit_nanos": available_credit_nanos,
        "reserved_credit_nanos": 0,
        "settled_usage_nanos": 0,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }


def test_minimum_balance_gate_accepts_exact_threshold_without_writing(monkeypatch):
    _enable_billing(monkeypatch)
    settings = get_settings()
    client = FakeClient()
    user_id = "user-1"
    wallet_id = customer_wallet_document_id(user_id)
    wallet_key = (settings.wallets_collection, wallet_id)
    client.documents[wallet_key] = _wallet(user_id)
    before = dict(client.documents[wallet_key])
    gate = WalletBalanceGate(firestore_client_factory=lambda: client)

    context = asyncio.run(
        gate.authorize(
            user_id=user_id,
            agent_id="maxima",
            request_id="req-1",
            turn_id="turn-1",
        )
    )

    assert context is not None
    assert context.minimum_balance_nanos == 50_000_000
    assert context.event_metadata()["reserved_amount_nanos"] == 0
    assert client.documents[wallet_key] == before
    assert client.reads == [wallet_key]
    assert client.writes == []
    assert list(client.documents) == [wallet_key]


def test_minimum_balance_gate_rejects_below_threshold_without_writing(monkeypatch):
    _enable_billing(monkeypatch)
    settings = get_settings()
    client = FakeClient()
    user_id = "user-1"
    wallet_key = (settings.wallets_collection, customer_wallet_document_id(user_id))
    client.documents[wallet_key] = _wallet(user_id, available_credit_nanos=49_999_999)
    before = dict(client.documents[wallet_key])
    gate = WalletBalanceGate(firestore_client_factory=lambda: client)

    with pytest.raises(ApiError) as exc_info:
        asyncio.run(
            gate.authorize(
                user_id=user_id,
                agent_id="maxima",
                request_id="req-1",
                turn_id="turn-1",
            )
        )

    assert exc_info.value.status_code == 402
    assert exc_info.value.code == "insufficient_credit"
    assert exc_info.value.details["required_minimum_balance_nanos"] == 50_000_000
    assert client.documents[wallet_key] == before
    assert client.reads == [wallet_key]
    assert client.writes == []


def test_gate_can_be_disabled_without_reading_firestore(monkeypatch):
    monkeypatch.setenv("BILLING_ENFORCEMENT_ENABLED", "false")
    get_settings.cache_clear()

    class NoReadClient:
        def collection(self, _collection):
            raise AssertionError("disabled billing gate must not read Firestore")

    gate = WalletBalanceGate(firestore_client_factory=NoReadClient)
    result = asyncio.run(
        gate.authorize(
            user_id="user-1",
            agent_id="maxima",
            request_id="req-1",
            turn_id="turn-1",
        )
    )
    assert result is None
