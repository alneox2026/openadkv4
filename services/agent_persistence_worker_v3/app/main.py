"""FastAPI entrypoint for the persistence worker."""

from __future__ import annotations

from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI

from services.agent_persistence_worker_v3.app.api.routes_events import router as events_router
from services.agent_persistence_worker_v3.app.api.routes_health import router as health_router
from services.agent_persistence_worker_v3.app.core.config import get_settings
from services.agent_persistence_worker_v3.app.core.logging import configure_logging
from services.agent_persistence_worker_v3.app.services.agent_runtime_sessions import (
    close_agent_runtime_sessions_client,
)
from services.agent_persistence_worker_v3.app.services.cloud_run_adk_sessions import (
    close_cloud_run_adk_sessions_client,
)
from services.agent_gateway_v3.app.services.usage_metadata import (
    validate_model_pricing_catalog,
)


LOGGER = logging.getLogger(__name__)
SETTINGS = get_settings()


@asynccontextmanager
async def lifespan(_: FastAPI):
    configure_logging(SETTINGS.log_level)
    validate_model_pricing_catalog([])
    LOGGER.info(
        "agent_persistence_worker_startup",
        extra={
            "payload": {
                "event": "agent_persistence_worker_startup",
                "project_id": SETTINGS.project_id,
                "threads_collection": SETTINGS.threads_collection,
                "messages_subcollection": SETTINGS.messages_subcollection,
            }
        },
    )
    try:
        yield
    finally:
        await close_agent_runtime_sessions_client()
        await close_cloud_run_adk_sessions_client()
        LOGGER.info(
            "agent_persistence_worker_shutdown",
            extra={"payload": {"event": "agent_persistence_worker_shutdown"}},
        )


app = FastAPI(
    title="CEOsystem Agent Persistence Worker",
    version="0.1.0",
    lifespan=lifespan,
)
app.include_router(health_router)
app.include_router(events_router)
