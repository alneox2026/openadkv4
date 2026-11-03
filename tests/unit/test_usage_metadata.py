import pytest

from services.agent_gateway_v3.app.services.usage_metadata import (
    _load_models_from_yaml_catalog,
    extract_usage_metadata,
    price_usage_reports,
    normalize_usage_metadata,
    resolve_model_pricing,
    validate_model_pricing_catalog,
)


def test_normalize_adk_usage_does_not_double_count_reasoning_tokens(monkeypatch) -> None:
    monkeypatch.setenv("DEFAULT_MODEL_NAME", "gpt-6-luna")
    usage = normalize_usage_metadata(
        {
            "prompt_token_count": 1000,
            "candidates_token_count": 200,
            "thoughts_token_count": 50,
            "total_token_count": 1200,
        }
    )

    assert usage["pricing_model"] == "gpt-6-luna"
    assert usage["token_counts"]["thoughts_token_count"] == 50
    assert usage["billable_tokens"] == {
        "input_text_image_video": 1000,
        "input_uncached": 1000,
        "input_cached": 0,
        "input_cache_write": 0,
        "output_including_thinking": 200,
    }
    assert usage["estimated_cost_usd"] == 0.0002
    assert usage["estimated_cost_complete"] is True
    assert usage["known_cost_lower_bound_usd"] == 0.0002
    assert usage["usage_event_count"] == 1


def test_price_usage_reports_aggregates_raw_usage_on_worker_path() -> None:
    usage = price_usage_reports(
        [
            {
                "model_name": "gpt-6-luna",
                "usage": {
                    "prompt_token_count": 10,
                    "candidates_token_count": 8,
                    "thoughts_token_count": 3,
                    "total_token_count": 18,
                    "cached_content_token_count": 0,
                    "cache_write_tokens": 0,
                },
            }
        ]
    )
    assert usage["estimated_cost_usd"] == 0.000005
    assert usage["token_counts"]["total_token_count"] == 18


def test_normalize_openai_usage_prices_cached_and_cache_write_tokens() -> None:
    usage = normalize_usage_metadata(
        {
            "input_tokens": 1500,
            "output_tokens": 1000,
            "total_tokens": 2500,
            "input_tokens_details": {
                "cached_tokens": 600,
                "cache_write_tokens": 300,
            },
            "output_tokens_details": {"reasoning_tokens": 250},
        },
        model_name="openai/gpt-6-luna",
    )

    assert usage["pricing_model"] == "gpt-6-luna"
    assert usage["token_counts"]["prompt_token_count"] == 1500
    assert usage["token_counts"]["candidates_token_count"] == 1000
    assert usage["token_counts"]["thoughts_token_count"] == 250
    assert usage["token_counts"]["cache_write_token_count"] == 300
    assert usage["billable_tokens"] == {
        "input_text_image_video": 1500,
        "input_uncached": 600,
        "input_cached": 600,
        "input_cache_write": 300,
        "output_including_thinking": 1000,
    }
    assert usage["pricing"]["input_cache_write"] == 0.125
    assert usage["estimated_cost_usd"] == 0.0006035
    assert usage["estimated_cost_complete"] is True
    assert usage["estimated_cost_breakdown_usd"] == {
        "input_uncached": 0.00006,
        "input_cached": 0.000006,
        "input_cache_write": 0.0000375,
        "output_including_thinking": 0.0005,
        "input_text_image_video": 0.0001035,
    }


def test_inconsistent_cached_and_cache_write_counts_are_not_priced() -> None:
    usage = normalize_usage_metadata(
        {
            "input_tokens": 100,
            "output_tokens": 10,
            "input_tokens_details": {
                "cached_tokens": 80,
                "cache_write_tokens": 30,
            },
        },
        model_name="gpt-6-luna",
    )

    assert usage["estimated_cost_usd"] is None
    assert usage["estimated_cost_complete"] is False


def test_large_prompt_requires_explicit_cached_and_cache_write_counts() -> None:
    usage = normalize_usage_metadata(
        {"input_tokens": 1200, "output_tokens": 10},
        model_name="gpt-6-luna",
    )

    assert usage["estimated_cost_usd"] is None
    assert usage["cached_input_reporting_complete"] is False
    assert usage["cache_write_reporting_complete"] is False
    assert usage["pricing_warnings"] == [
        "upstream_did_not_report_cached_input_tokens",
        "upstream_did_not_report_cache_write_tokens",
    ]


def test_normalize_openai_long_context_applies_long_rates_to_each_call() -> None:
    usage = normalize_usage_metadata(
        {
            "input_tokens": 300000,
            "output_tokens": 1000,
            "total_tokens": 301000,
            "input_tokens_details": {
                "cached_tokens": 50000,
                "cache_write_tokens": 100000,
            },
            "output_tokens_details": {"reasoning_tokens": 100},
        },
        model_name="gpt-6-luna",
    )

    assert usage["context_tier"] == "long"
    assert usage["pricing_version"] == "gpt-6-luna-standard-global-long-2026-10"
    assert usage["pricing"]["input_cache_write"] == 0.25
    assert usage["estimated_cost_usd"] == 0.05675


def test_normalize_adk_litellm_shape_marks_missing_cache_writes_incomplete() -> None:
    # ADK currently maps cached hits/reasoning but drops OpenAI cache_write_tokens.
    usage = normalize_usage_metadata(
        {
            "prompt_token_count": 1000,
            "candidates_token_count": 500,
            "thoughts_token_count": 100,
            "cached_content_token_count": 400,
            "total_token_count": 1500,
        },
        model_name="gpt-6-luna",
    )

    assert usage["billable_tokens"]["output_including_thinking"] == 500
    assert usage["estimated_cost_usd"] is None
    assert usage["known_cost_lower_bound_usd"] == 0.000314
    assert usage["cache_write_reporting_complete"] is False
    assert usage["pricing_warnings"] == [
        "upstream_did_not_report_cache_write_tokens"
    ]


def test_openai_model_resolution_is_exact_and_fails_closed(monkeypatch) -> None:
    monkeypatch.setenv("DEFAULT_MODEL_NAME", "gpt-6-luna")
    model, rates = resolve_model_pricing("openai/gpt-6-luna")
    assert model == "gpt-6-luna"
    assert rates["input_cache_write"] == 0.125

    for unsupported in (
        "gpt-6-luna-preview",
        "gpt-6-luna-2026-09-22",
        "gpt-6-luna-mini",
        "gpt-4o",
        "unknown-model",
    ):
        with pytest.raises(ValueError, match="No explicit billing catalog entry"):
            resolve_model_pricing(unsupported)


def test_unknown_default_model_fails_instead_of_falling_back(monkeypatch) -> None:
    monkeypatch.setenv("DEFAULT_MODEL_NAME", "gpt-4o")
    with pytest.raises(ValueError, match="No explicit billing catalog entry"):
        resolve_model_pricing(None)
    with pytest.raises(RuntimeError, match="DEFAULT_MODEL_NAME"):
        validate_model_pricing_catalog(["gpt-6-luna"])


def test_normalize_usage_metadata_does_not_price_total_only_usage() -> None:
    usage = normalize_usage_metadata({"total_token_count": 21})

    assert usage["total_token_count"] == 21
    assert usage["token_counts"] == {"total_token_count": 21}
    assert "estimated_cost_usd" not in usage
    assert "billable_tokens" not in usage


def test_extract_usage_metadata_finds_nested_adk_shape() -> None:
    event = {
        "payload": {
            "event": {
                "usageMetadata": {
                    "promptTokenCount": 5,
                    "candidatesTokenCount": 3,
                }
            }
        }
    }

    assert extract_usage_metadata(event) == {
        "promptTokenCount": 5,
        "candidatesTokenCount": 3,
    }


def test_extract_usage_metadata_merges_adk_openai_custom_metadata() -> None:
    event = {
        "usage_metadata": {
            "prompt_token_count": 1200,
            "candidates_token_count": 100,
            "cached_content_token_count": 300,
        },
        "custom_metadata": {
            "openai_usage": {"cache_write_tokens": 450},
        },
    }

    usage = extract_usage_metadata(event)

    assert usage is not None
    assert usage["cache_write_tokens"] == 450
    normalized = normalize_usage_metadata(usage, model_name="gpt-6-luna")
    assert normalized["cache_write_reporting_complete"] is True
    assert normalized["token_counts"]["cache_write_token_count"] == 450


def test_extract_usage_metadata_merges_zero_cache_writes_and_preserves_direct_value(
) -> None:
    event = {
        "usageMetadata": {
            "promptTokenCount": 1200,
            "candidatesTokenCount": 100,
        },
        "customMetadata": {"openaiUsage": {"cacheWriteTokens": 0}},
    }
    assert extract_usage_metadata(event)["cache_write_tokens"] == 0

    event["usageMetadata"]["cache_write_tokens"] = 7
    assert extract_usage_metadata(event)["cache_write_tokens"] == 7


def test_extract_usage_metadata_finds_openai_responses_usage() -> None:
    usage = {
        "input_tokens": 120,
        "output_tokens": 80,
        "total_tokens": 200,
        "input_tokens_details": {"cached_tokens": 25, "cache_write_tokens": 40},
    }
    assert extract_usage_metadata({"response": {"usage": usage}}) == usage


def test_resolve_model_pricing_uses_explicit_catalog_and_requires_cache_write_rate(
    tmp_path, monkeypatch
) -> None:
    catalog_path = tmp_path / "billing.yaml"
    catalog_path.write_text(
        """
schema_version: 1
environment: test
currency: USD
models:
  gpt-6-luna:
    pricing_version: test-v1
    context_threshold_tokens: 272000
    prompt_cache_min_tokens: 1024
    input_usd_per_million: 1.00
    input_cached_usd_per_million: 0.10
    input_cache_write_usd_per_million: 1.25
    output_usd_per_million: 4.00
    long_context:
      pricing_version: test-v1-long
      input_usd_per_million: 2.00
      input_cached_usd_per_million: 0.20
      input_cache_write_usd_per_million: 2.50
      output_usd_per_million: 6.00
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("BILLING_CATALOG_PATH", str(catalog_path))
    monkeypatch.delenv("DEPLOYMENT_ENVIRONMENT", raising=False)
    monkeypatch.setenv("DEFAULT_MODEL_NAME", "gpt-6-luna")
    _load_models_from_yaml_catalog.cache_clear()
    try:
        model, pricing = resolve_model_pricing(None)
        assert model == "gpt-6-luna"
        assert pricing["input_text_image_video"] == 1.0
        assert pricing["input_cache_write"] == 1.25
        assert pricing["pricing_version"] == "test-v1"
    finally:
        _load_models_from_yaml_catalog.cache_clear()


def test_bad_catalog_fails_closed(tmp_path, monkeypatch) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text(
        """
schema_version: 1
environment: test
currency: USD
models:
  gpt-6-luna:
    pricing_version: bad-v1
    context_threshold_tokens: 272000
    prompt_cache_min_tokens: 1024
    input_usd_per_million: 0.10
    input_cached_usd_per_million: 0.01
    output_usd_per_million: 0.50
    long_context:
      pricing_version: bad-v1-long
      input_usd_per_million: 0.20
      input_cached_usd_per_million: 0.02
      input_cache_write_usd_per_million: 0.25
      output_usd_per_million: 0.75
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("BILLING_CATALOG_PATH", str(path))
    _load_models_from_yaml_catalog.cache_clear()
    try:
        with pytest.raises(RuntimeError, match="input_cache_write_usd_per_million"):
            _load_models_from_yaml_catalog()
    finally:
        _load_models_from_yaml_catalog.cache_clear()
