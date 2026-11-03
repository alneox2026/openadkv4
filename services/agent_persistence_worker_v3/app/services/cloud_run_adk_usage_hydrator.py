"""Recover missing provider usage from Cloud Run ADK session history."""

from __future__ import annotations

import asyncio
from typing import Any

from services.agent_gateway_v3.app.services.turn_assembler import TurnAssembler
from services.agent_gateway_v3.app.services.usage_metadata import price_usage_reports
from services.agent_persistence_worker_v3.app.services.agent_runtime_sessions import (
    get_agent_runtime_sessions_client,
)
from services.agent_persistence_worker_v3.app.services.cloud_run_adk_sessions import (
    get_cloud_run_adk_sessions_client,
)
from services.agent_persistence_worker_v3.app.core.errors import RetryableWorkerError


class CloudRunAdkSessionUsageHydrator:
    """Hydrate usage only from events belonging to the recorded invocation IDs."""

    def __init__(
        self,
        *,
        sessions_client_factory=None,
        runtime_sessions_client_factory=None,
    ) -> None:
        self._sessions_client_factory = (
            sessions_client_factory or get_cloud_run_adk_sessions_client
        )
        self._runtime_sessions_client_factory = (
            runtime_sessions_client_factory or get_agent_runtime_sessions_client
        )

    def recover(
        self,
        *,
        ledger: dict[str, Any],
        reservation: dict[str, Any],
    ) -> dict[str, Any] | None:
        recovery = ledger.get("provider_usage_recovery")
        if not isinstance(recovery, dict):
            return None
        source = recovery.get("source")
        if source not in {
            "cloud_run_adk_session_events",
            "agent_runtime_session_events",
        }:
            return None
        user_id = _string(ledger.get("uid"))
        session_id = _string(ledger.get("session_id"))
        invocation_ids = {
            str(value).strip()
            for value in recovery.get("invocation_ids", [])
            if isinstance(value, str) and value.strip()
        }
        if not all((user_id, session_id, invocation_ids)):
            return None
        if source == "cloud_run_adk_session_events":
            if not _string(recovery.get("base_url")) or not _string(
                recovery.get("app_name")
            ):
                return None
        elif not all(
            _string(recovery.get(field))
            for field in ("resource_name", "region", "started_at", "completed_at")
        ):
            return None
        try:
            events = asyncio.run(self._get_events(recovery, user_id, session_id))
        except Exception as exc:
            raise RetryableWorkerError(
                "Provider session usage lookup could not be completed: "
                f"{type(exc).__name__}."
            ) from exc
        assembler = TurnAssembler(
            model_name=_string(recovery.get("model_name")) or None
        )
        for event in events:
            invocation_id = event.get("invocation_id") or event.get("invocationId")
            if invocation_id is not None and str(invocation_id) in invocation_ids:
                assembler.add_event(event)
        usage = price_usage_reports(assembler.raw_usage_reports)
        if not usage or usage.get("estimated_cost_usd") is None:
            return None
        usage["provider_usage_recovered"] = True
        return usage

    async def _get_events(
        self,
        recovery: dict[str, Any],
        user_id: str,
        session_id: str,
    ) -> list[dict[str, Any]]:
        if recovery.get("source") == "cloud_run_adk_session_events":
            client = await self._sessions_client_factory()
            return await client.get_session_events(
                base_url=_string(recovery.get("base_url")),
                app_name=_string(recovery.get("app_name")),
                user_id=user_id,
                session_id=session_id,
                audience=_string(recovery.get("audience")) or None,
            )
        client = await self._runtime_sessions_client_factory()
        return await client.get_session_events(
            resource_name=_string(recovery.get("resource_name")),
            region=_string(recovery.get("region")),
            session_id=session_id,
            started_at=_string(recovery.get("started_at")),
            completed_at=_string(recovery.get("completed_at")),
        )


def _string(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""
