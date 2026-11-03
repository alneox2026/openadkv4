"""Backend resolver for gateway chat execution."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol, cast

from common.schemas import AgentConfig, ChatRequest
from services.agent_gateway_v3.app.core.errors import ApiError
from services.agent_gateway_v3.app.services.agent_runtime_client import (
    BufferedAgentResponse,
    SessionResult,
    UpstreamStreamEvent,
    get_agent_runtime_client,
)
from services.agent_gateway_v3.app.services.cloud_run_adk_client import (
    get_cloud_run_adk_client,
)


class ChatBackendClient(Protocol):
    async def ensure_session(
        self,
        *,
        agent_config: AgentConfig,
        user_id: str,
        request: ChatRequest,
    ) -> SessionResult:
        ...

    async def chat_buffered_query(
        self,
        *,
        agent_config: AgentConfig,
        user_id: str,
        session_id: str,
        message: str,
    ) -> BufferedAgentResponse:
        ...


class StreamingChatBackendClient(ChatBackendClient, Protocol):
    def stream_chat_events(
        self,
        *,
        agent_config: AgentConfig,
        user_id: str,
        session_id: str,
        message: str,
    ) -> AsyncIterator[UpstreamStreamEvent]:
        ...

    def extract_text_fragments(self, event_payload: dict[str, object]) -> list[str]:
        ...


async def get_chat_backend_client(agent_config: AgentConfig) -> ChatBackendClient:
    if agent_config.backend == "agent_runtime":
        return await get_agent_runtime_client()
    if agent_config.backend == "cloud_run_adk":
        return await get_cloud_run_adk_client()
    raise ApiError(
        500,
        "unsupported_agent_backend",
        "The requested agent backend is not supported by this gateway.",
        {"agent_id": agent_config.agent_id, "backend": agent_config.backend},
    )


async def get_streaming_chat_backend_client(
    agent_config: AgentConfig,
) -> StreamingChatBackendClient:
    backend_client = await get_chat_backend_client(agent_config)
    return cast(StreamingChatBackendClient, backend_client)
