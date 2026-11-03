from services.agent_persistence_worker_v3.app.services.cloud_run_adk_usage_hydrator import (
    CloudRunAdkSessionUsageHydrator,
)


class FakeSessionsClient:
    async def get_session_events(self, **kwargs):
        assert kwargs["session_id"] == "session-1"
        return [
            {
                "id": "old-event",
                "invocation_id": "another-turn",
                "usageMetadata": {
                    "promptTokenCount": 100,
                    "candidatesTokenCount": 10,
                    "cachedContentTokenCount": 0,
                },
                "customMetadata": {"openai_usage": {"cache_write_tokens": 0}},
            },
            {
                "id": "late-usage-event",
                "invocation_id": "invocation-1",
                "usageMetadata": {
                    "promptTokenCount": 10,
                    "candidatesTokenCount": 5,
                    "cachedContentTokenCount": 0,
                },
                "customMetadata": {"openai_usage": {"cache_write_tokens": 0}},
            },
        ]


async def _client_factory():
    return FakeSessionsClient()


def test_hydrator_uses_only_exact_invocation_usage():
    hydrator = CloudRunAdkSessionUsageHydrator(
        sessions_client_factory=_client_factory,
    )

    usage = hydrator.recover(
        ledger={
            "uid": "user-1",
            "session_id": "session-1",
            "provider_usage_recovery": {
                "source": "cloud_run_adk_session_events",
                "base_url": "https://agent.example.run.app",
                "app_name": "app",
                "model_name": "gpt-6-luna",
                "invocation_ids": ["invocation-1"],
            },
        },
        reservation={},
    )

    assert usage is not None
    assert usage["token_counts"]["prompt_token_count"] == 10
    assert usage["estimated_cost_usd"] is not None
    assert usage["provider_usage_recovered"] is True
