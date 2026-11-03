import pytest

from services.agent_gateway_v3.app.core.config import get_settings
from services.agent_gateway_v3.app.core.errors import ApiError
from services.agent_gateway_v3.app.services.agent_registry import get_agent_config, load_registry


@pytest.fixture(autouse=True)
def clear_registry_cache():
    get_settings.cache_clear()
    load_registry.cache_clear()
    yield
    get_settings.cache_clear()
    load_registry.cache_clear()


def test_get_agent_config_returns_maximus2026() -> None:
    agent_config = get_agent_config("maximus2026")

    assert agent_config.agent_id == "maximus2026"
    assert agent_config.backend == "cloud_run_adk"
    assert agent_config.model == "gpt-6-luna"
    assert agent_config.app_name == "app"
    assert "maximus-2026" in agent_config.base_url
    assert agent_config.streaming_enabled is True
    assert agent_config.runtime_session_cleanup == "cloud_run_adk"


def test_get_agent_config_returns_maximus2026_dev() -> None:
    agent_config = get_agent_config("maximus2026_dev")

    assert agent_config.agent_id == "maximus2026_dev"
    assert agent_config.backend == "cloud_run_adk"
    assert agent_config.model == "gpt-6-luna"
    assert agent_config.app_name == "app"
    assert agent_config.streaming_enabled is False
    assert agent_config.runtime_session_cleanup == "cloud_run_adk"


def test_get_agent_config_returns_gpt6_agent_dev() -> None:
    agent_config = get_agent_config("gpt6_agent_dev")

    assert agent_config.agent_id == "gpt6_agent_dev"
    assert agent_config.backend == "cloud_run_adk"
    assert agent_config.model == "gpt-6-luna"
    assert agent_config.app_name == "app"
    assert agent_config.streaming_enabled is False
    assert agent_config.runtime_session_cleanup == "cloud_run_adk"


def test_get_agent_config_returns_gpt6_agent_stream_dev() -> None:
    agent_config = get_agent_config("gpt6_agent_stream_dev")

    assert agent_config.agent_id == "gpt6_agent_stream_dev"
    assert agent_config.backend == "cloud_run_adk"
    assert agent_config.model == "gpt-6-luna"
    assert agent_config.app_name == "app"
    assert agent_config.streaming_enabled is True
    assert agent_config.runtime_session_cleanup == "cloud_run_adk"


def test_get_agent_config_rejects_unknown_agent() -> None:
    with pytest.raises(ApiError) as exc_info:
        get_agent_config("unknown-agent")
    assert exc_info.value.code == "unknown_agent"


def test_get_agent_config_applies_runtime_env_override(tmp_path, monkeypatch) -> None:
    registry_path = tmp_path / "agents.yaml"
    registry_path.write_text(
        """
agents:
  custom_agent:
    agent_id: custom_agent
    resource_name: projects/test/locations/us-east1/reasoningEngines/old
    region: us-east1
    streaming_enabled: false
    persistence_enabled: true
    auth_policy: firebase
""",
        encoding="utf-8",
    )

    monkeypatch.setenv("AGENT_REGISTRY_PATH", str(registry_path))
    monkeypatch.setenv(
        "AGENT_CUSTOM_AGENT_RESOURCE_NAME",
        "projects/test/locations/us-central1/reasoningEngines/new",
    )
    monkeypatch.setenv("AGENT_CUSTOM_AGENT_REGION", "us-central1")

    agent_config = get_agent_config("custom_agent")

    assert agent_config.resource_name == (
        "projects/test/locations/us-central1/reasoningEngines/new"
    )
    assert agent_config.region == "us-central1"


def test_get_agent_config_applies_cloud_run_env_overrides(tmp_path, monkeypatch) -> None:
    registry_path = tmp_path / "agents.yaml"
    registry_path.write_text(
        """
agents:
  openai_cloudrun_agent:
    agent_id: openai_cloudrun_agent
    backend: cloud_run_adk
    base_url: https://old.example.run.app
    app_name: old_app
    region: us-east1
    streaming_enabled: false
    persistence_enabled: true
    auth_policy: firebase
    runtime_session_cleanup: none
""",
        encoding="utf-8",
    )

    monkeypatch.setenv("AGENT_REGISTRY_PATH", str(registry_path))
    monkeypatch.setenv(
        "AGENT_OPENAI_CLOUDRUN_AGENT_BASE_URL",
        "https://openai-cloudrun-canary.example.run.app",
    )
    monkeypatch.setenv("AGENT_OPENAI_CLOUDRUN_AGENT_APP_NAME", "openai_cloudrun")
    monkeypatch.setenv("AGENT_OPENAI_CLOUDRUN_AGENT_AUDIENCE", "https://audience.example")
    monkeypatch.setenv("AGENT_OPENAI_CLOUDRUN_AGENT_REGION", "us-central1")

    agent_config = get_agent_config("openai_cloudrun_agent")

    assert agent_config.base_url == "https://openai-cloudrun-canary.example.run.app"
    assert agent_config.app_name == "openai_cloudrun"
    assert agent_config.audience == "https://audience.example"
    assert agent_config.region == "us-central1"
