"""Normalize ADK/LiteLLM usage and estimate OpenAI token costs.

Only explicitly catalogued models and standard OpenAI API rates are accepted.
Unknown models and malformed pricing catalogs fail closed; there is no guessed
or generic fallback price.
"""

from __future__ import annotations

import math
import os
from collections.abc import Iterable
from copy import deepcopy
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

DEFAULT_MODEL_NAME = "gpt-6-luna"
_USAGE_KEYS = ("usage_metadata", "usageMetadata", "usage", "openai_usage")
_RATE_FIELDS = {
    "input_text_image_video": "input_usd_per_million",
    "input_cached": "input_cached_usd_per_million",
    "input_cache_write": "input_cache_write_usd_per_million",
    "output_including_thinking": "output_usd_per_million",
}


def _catalog_path() -> Path:
    explicit_path = os.getenv("BILLING_CATALOG_PATH")
    if explicit_path:
        path = Path(explicit_path).expanduser().resolve()
        if not path.is_file():
            raise RuntimeError(f"BILLING_CATALOG_PATH does not identify a file: {path}")
        return path

    environment = os.getenv("DEPLOYMENT_ENVIRONMENT", "development").strip().lower()
    filename = "billing.prod.yaml" if environment == "production" else "billing.test.yaml"
    candidates = (
        Path("/app/config") / filename,
        Path(__file__).resolve().parents[4] / "config" / filename,
        Path("config") / filename,
    )
    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()
    raise RuntimeError(f"Could not find the required pricing catalog {filename}.")


def _rate(value: Any, *, path: Path, field: str) -> float:
    if isinstance(value, bool):
        raise RuntimeError(f"Pricing catalog {path} has invalid {field}.")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"Pricing catalog {path} has invalid {field}.") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise RuntimeError(f"Pricing catalog {path} has invalid {field}.")
    return parsed


def _parse_rate_tier(
    rates: dict[str, Any], *, path: Path, model_id: str, tier_name: str
) -> dict[str, Any]:
    pricing_version = rates.get("pricing_version")
    if not isinstance(pricing_version, str) or not pricing_version.strip():
        raise RuntimeError(
            f"Pricing catalog {path} needs a non-empty pricing_version for {model_id} ({tier_name})."
        )
    tier: dict[str, Any] = {
        "pricing_version": pricing_version.strip(),
    }
    for normalized_name, yaml_name in _RATE_FIELDS.items():
        if yaml_name not in rates:
            raise RuntimeError(
                f"Pricing catalog {path} is missing {yaml_name} for {model_id} ({tier_name})."
            )
        tier[normalized_name] = _rate(rates[yaml_name], path=path, field=yaml_name)
    return tier


@lru_cache(maxsize=1)
def _load_models_from_yaml_catalog() -> dict[str, dict[str, Any]]:
    """Load a complete, versioned model catalog; never silently use defaults."""
    path = _catalog_path()
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise RuntimeError(f"Could not read pricing catalog {path}.") from exc
    if not isinstance(raw, dict) or not isinstance(raw.get("models"), dict) or not raw["models"]:
        raise RuntimeError(f"Pricing catalog {path} must contain a non-empty models mapping.")
    if (
        isinstance(raw.get("schema_version"), bool)
        or raw.get("schema_version") != 1
    ):
        raise RuntimeError(f"Pricing catalog {path} must use schema_version: 1.")
    if str(raw.get("currency", "")).strip().upper() != "USD":
        raise RuntimeError(f"Pricing catalog {path} must specify currency: USD.")

    configured_environment = os.getenv("DEPLOYMENT_ENVIRONMENT", "").strip().lower()
    catalog_environment = str(raw.get("environment", "")).strip().lower()
    if configured_environment and catalog_environment:
        expected = "production" if configured_environment == "production" else "test"
        if catalog_environment not in {expected, configured_environment}:
            raise RuntimeError(
                f"Pricing catalog {path} is for '{catalog_environment}', "
                f"not deployment environment '{configured_environment}'."
            )

    catalog: dict[str, dict[str, Any]] = {}
    for raw_model_id, model_rates in raw["models"].items():
        model_id = str(raw_model_id).strip().lower()
        if not model_id or not isinstance(model_rates, dict):
            raise RuntimeError(f"Pricing catalog {path} contains an invalid model entry.")
        if model_id in catalog:
            raise RuntimeError(f"Pricing catalog {path} contains duplicate model ID '{model_id}'.")
        short = _parse_rate_tier(
            model_rates, path=path, model_id=model_id, tier_name="short"
        )
        try:
            raw_threshold = model_rates["context_threshold_tokens"]
            if isinstance(raw_threshold, bool):
                raise ValueError
            threshold = int(raw_threshold)
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Pricing catalog {path} needs context_threshold_tokens for {model_id}."
            ) from exc
        if threshold <= 0:
            raise RuntimeError(f"Pricing catalog {path} has invalid context threshold for {model_id}.")
        try:
            raw_prompt_cache_min_tokens = model_rates["prompt_cache_min_tokens"]
            if isinstance(raw_prompt_cache_min_tokens, bool):
                raise ValueError
            prompt_cache_min_tokens = int(raw_prompt_cache_min_tokens)
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Pricing catalog {path} needs prompt_cache_min_tokens for {model_id}."
            ) from exc
        if prompt_cache_min_tokens <= 0:
            raise RuntimeError(
                f"Pricing catalog {path} has invalid prompt cache threshold for {model_id}."
            )
        short["context_threshold_tokens"] = threshold
        short["prompt_cache_min_tokens"] = prompt_cache_min_tokens

        long_rates = model_rates.get("long_context")
        if not isinstance(long_rates, dict):
            raise RuntimeError(f"Pricing catalog {path} needs long_context rates for {model_id}.")
        long = _parse_rate_tier(
            long_rates, path=path, model_id=model_id, tier_name="long"
        )
        long["context_threshold_tokens"] = threshold
        long["prompt_cache_min_tokens"] = prompt_cache_min_tokens
        short["long_context"] = long
        catalog[model_id] = short
    return catalog


def _canonical_model_id(raw_model: str) -> str:
    """Remove only LiteLLM's exact OpenAI provider prefix, never model suffixes."""
    cleaned = str(raw_model).strip().lower()
    if cleaned.startswith("openai/"):
        cleaned = cleaned[len("openai/") :]
    return cleaned


def resolve_model_pricing(
    raw_model: str | None = None,
    prompt_token_count: int | None = None,
) -> tuple[str, dict[str, Any]]:
    """Resolve exact catalog entry and its per-request context tier."""
    catalog = _load_models_from_yaml_catalog()
    selected = _canonical_model_id(
        raw_model or os.getenv("DEFAULT_MODEL_NAME", DEFAULT_MODEL_NAME)
    )
    if not selected or selected not in catalog:
        raise ValueError(f"No explicit billing catalog entry for OpenAI model '{selected}'.")

    short_rates = catalog[selected]
    threshold = int(short_rates["context_threshold_tokens"])
    if prompt_token_count is not None and prompt_token_count > threshold:
        long_rates = dict(short_rates["long_context"])
        long_rates["context_tier"] = "long"
        return selected, long_rates
    result = dict(short_rates)
    result["context_tier"] = "short"
    return selected, result


def validate_model_pricing_catalog(model_names: Iterable[str | None]) -> None:
    """Fail startup when any registered agent has no explicit rate card."""
    catalog = _load_models_from_yaml_catalog()
    default_model = _canonical_model_id(
        os.getenv("DEFAULT_MODEL_NAME", DEFAULT_MODEL_NAME)
    )
    if default_model not in catalog:
        raise RuntimeError(
            f"DEFAULT_MODEL_NAME '{default_model}' is not in the active pricing catalog."
        )
    for model_name in model_names:
        if model_name:
            try:
                resolve_model_pricing(model_name)
            except ValueError as exc:
                raise RuntimeError(str(exc)) from exc


def extract_usage_metadata(event: dict[str, Any]) -> dict[str, Any] | None:
    """Find ADK/LiteLLM or raw OpenAI usage in common event envelopes.

    ADK's typed usage schema has no cache-write field. The upstream OpenAI ADK
    adapter therefore places that per-call count in ``custom_metadata``; merge
    only that validated integer into the event's normal usage record.
    """

    def merge_openai_custom_usage(
        usage: dict[str, Any], container: dict[str, Any]
    ) -> dict[str, Any]:
        if _first_int(
            usage,
            ("cache_write_tokens", "cache_write_token_count", "cacheWriteTokens"),
        ) is not None:
            return usage

        custom_metadata = container.get("custom_metadata")
        if not isinstance(custom_metadata, dict):
            custom_metadata = container.get("customMetadata")
        if not isinstance(custom_metadata, dict):
            return usage

        openai_usage = custom_metadata.get("openai_usage")
        if not isinstance(openai_usage, dict):
            openai_usage = custom_metadata.get("openaiUsage")
        if not isinstance(openai_usage, dict):
            return usage

        cache_write_tokens = _first_int(
            openai_usage,
            ("cache_write_tokens", "cache_write_token_count", "cacheWriteTokens"),
        )
        if cache_write_tokens is None:
            return usage
        merged_usage = dict(usage)
        merged_usage["cache_write_tokens"] = cache_write_tokens
        return merged_usage

    def is_usage(value: dict[str, Any]) -> bool:
        return (
            ("prompt_token_count" in value and "candidates_token_count" in value)
            or ("promptTokenCount" in value and "candidatesTokenCount" in value)
            or ("prompt_tokens" in value and "completion_tokens" in value)
            or ("input_tokens" in value and "output_tokens" in value)
        )

    def walk(value: Any, depth: int = 0) -> dict[str, Any] | None:
        if depth > 8:
            return None
        if isinstance(value, dict):
            for key in _USAGE_KEYS:
                usage = value.get(key)
                if isinstance(usage, dict) and is_usage(usage):
                    return merge_openai_custom_usage(usage, value)
            if is_usage(value):
                return merge_openai_custom_usage(value, value)
            for nested_value in value.values():
                usage = walk(nested_value, depth + 1)
                if usage is not None:
                    return usage
        elif isinstance(value, list):
            for item in value:
                usage = walk(item, depth + 1)
                if usage is not None:
                    return usage
        return None

    return walk(event)


def normalize_usage_metadata(
    usage_metadata: dict[str, Any],
    model_name: str | None = None,
) -> dict[str, Any]:
    """Preserve upstream usage and add auditable, per-model-call cost fields."""
    usage = deepcopy(usage_metadata)
    counts = _token_counts(usage_metadata)
    if counts:
        usage["token_counts"] = {
            key: value
            for key, value in counts.items()
            if key not in {
                "cached_input_reporting_complete",
                "cache_write_reporting_complete",
            }
        }

    prompt_tokens = counts.get("prompt_token_count")
    output_tokens = _output_token_count(
        prompt_token_count=prompt_tokens,
        candidates_token_count=counts.get("candidates_token_count"),
        thoughts_token_count=counts.get("thoughts_token_count"),
        total_token_count=counts.get("total_token_count"),
    )
    if prompt_tokens is None and output_tokens is None:
        return usage

    # Provider-reported model identity takes precedence over registry defaults.
    # This prevents a LiteLLM fallback/model switch from being charged at the
    # configured model's price by accident.
    model_candidate = (
        usage_metadata.get("pricing_model")
        or usage_metadata.get("model_version")
        or usage_metadata.get("model_name")
        or usage_metadata.get("model")
        or model_name
    )
    resolved_model, pricing = resolve_model_pricing(
        str(model_candidate) if model_candidate else None,
        prompt_token_count=prompt_tokens,
    )

    total_input = prompt_tokens or 0
    reported_cached_input = counts.get("cached_content_token_count", 0)
    reported_cache_write = counts.get("cache_write_token_count", 0)
    token_breakdown_valid = reported_cached_input + reported_cache_write <= total_input
    cached_input = min(reported_cached_input, total_input)
    cache_write_input = min(
        reported_cache_write, total_input - cached_input
    )
    uncached_input = total_input - cached_input - cache_write_input
    total_output = output_tokens or 0
    rates = {
        "input_text_image_video": pricing["input_text_image_video"],
        "input_cached": pricing["input_cached"],
        "input_cache_write": pricing["input_cache_write"],
        "output_including_thinking": pricing["output_including_thinking"],
    }
    billable_tokens = {
        "input_text_image_video": total_input,
        "input_uncached": uncached_input,
        "input_cached": cached_input,
        "input_cache_write": cache_write_input,
        "output_including_thinking": total_output,
    }
    costs = {
        "input_uncached": uncached_input * rates["input_text_image_video"] / 1_000_000,
        "input_cached": cached_input * rates["input_cached"] / 1_000_000,
        "input_cache_write": cache_write_input * rates["input_cache_write"] / 1_000_000,
        "output_including_thinking": total_output * rates["output_including_thinking"] / 1_000_000,
    }
    cached_input_reported = counts.get("cached_input_reporting_complete", False)
    cache_write_reported = counts.get("cache_write_reporting_complete", False)
    cache_cannot_exist = (
        prompt_tokens is not None
        and prompt_tokens < int(pricing["prompt_cache_min_tokens"])
        and cached_input == 0
    )
    cached_input_complete = cached_input_reported or cache_cannot_exist
    cache_write_complete = cache_write_reported or cache_cannot_exist
    cost_complete = (
        token_breakdown_valid
        and prompt_tokens is not None
        and cached_input_complete
        and cache_write_complete
    )
    breakdown = {
        key: _round_usd(value)
        for key, value in costs.items()
    }
    breakdown["input_text_image_video"] = _round_usd(
        costs["input_uncached"] + costs["input_cached"] + costs["input_cache_write"]
    )

    usage.update(
        {
            "pricing_model": resolved_model,
            "pricing_version": pricing["pricing_version"],
            "context_tier": pricing["context_tier"],
            "pricing_unit": "usd_per_1m_tokens",
            "pricing": {**rates, "context_tier": pricing["context_tier"]},
            "billable_tokens": billable_tokens,
            "cached_input_tokens": cached_input,
            "uncached_input_tokens": uncached_input,
            "cache_write_input_tokens": cache_write_input,
            "cached_input_reporting_complete": cached_input_complete,
            "cache_write_reporting_complete": cache_write_complete,
            "estimated_cost_complete": cost_complete,
            "usage_event_count": 1,
        }
    )
    known_cost_lower_bound = _round_usd(sum(costs.values()))
    usage["known_cost_lower_bound_usd"] = known_cost_lower_bound
    if cost_complete:
        usage["estimated_cost_usd"] = known_cost_lower_bound
        usage["estimated_cost_breakdown_usd"] = breakdown
    else:
        usage["estimated_cost_usd"] = None
        usage["known_cost_lower_bound_breakdown_usd"] = breakdown
        warnings: list[str] = []
        if not token_breakdown_valid:
            warnings.append("cached_and_cache_write_tokens_exceed_total_input_tokens")
        if not cached_input_complete:
            warnings.append("upstream_did_not_report_cached_input_tokens")
        if not cache_write_complete:
            warnings.append("upstream_did_not_report_cache_write_tokens")
        usage["pricing_warnings"] = warnings
    return usage


def normalize_usage_token_metadata(
    usage_metadata: dict[str, Any],
) -> dict[str, Any]:
    """Normalize token counts for the response path without loading prices."""
    usage = deepcopy(usage_metadata)
    counts = _token_counts(usage_metadata)
    usage["token_counts"] = {
        key: value
        for key, value in counts.items()
        if key not in {
            "cached_input_reporting_complete",
            "cache_write_reporting_complete",
        }
    }
    return usage


def aggregate_usage_token_metadata(
    reports: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Aggregate response-visible token counts without doing cost calculations."""
    items = list(reports)
    if not items:
        return {}
    result = deepcopy(items[-1])
    totals: dict[str, int] = {}
    for item in items:
        for key, value in (item.get("token_counts") or {}).items():
            if isinstance(value, int) and not isinstance(value, bool):
                totals[key] = totals.get(key, 0) + value
    result["token_counts"] = totals
    for key in (
        "prompt_tokens", "input_tokens", "completion_tokens", "output_tokens",
        "total_tokens", "promptTokenCount", "candidatesTokenCount",
        "totalTokenCount", "cached_tokens", "reasoning_tokens",
        "cache_write_tokens", "input_tokens_details", "output_tokens_details",
        "prompt_tokens_details", "completion_tokens_details",
        "inputTokensDetails", "outputTokensDetails",
    ):
        result.pop(key, None)
    aliases = {
        "prompt_token_count": "prompt_token_count",
        "candidates_token_count": "candidates_token_count",
        "thoughts_token_count": "thoughts_token_count",
        "total_token_count": "total_token_count",
        "cached_content_token_count": "cached_content_token_count",
        "cache_write_token_count": "cache_write_token_count",
    }
    for token_key, output_key in aliases.items():
        if token_key in totals:
            result[output_key] = totals[token_key]
    result["usage_event_count"] = len(items)
    return result


def price_usage_reports(reports: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Price raw provider usage reports on the worker, never on the gateway."""
    priced: list[dict[str, Any]] = []
    for report in reports:
        if not isinstance(report, dict):
            continue
        usage = report.get("usage")
        if not isinstance(usage, dict):
            continue
        model_name = report.get("model_name")
        priced.append(
            normalize_usage_metadata(
                usage,
                model_name=str(model_name) if model_name else None,
            )
        )
    return aggregate_usage_metadata(priced)


def aggregate_usage_metadata(reports: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate independent ADK model-call reports without losing tier costs."""
    items = list(reports)
    if not items:
        return {}
    if len(items) == 1:
        return deepcopy(items[0])

    result = deepcopy(items[-1])
    token_counts: dict[str, int] = {}
    billable_tokens: dict[str, int] = {}
    cost_breakdown: dict[str, Decimal] = {}
    cost_total = Decimal("0")
    lower_bound_total = Decimal("0")
    model_ids: set[str] = set()
    pricing_versions: set[str] = set()
    tiers: set[str] = set()
    pricing_warnings: set[str] = set()
    all_cached_input_reported = True
    all_cache_writes_reported = True
    all_costs_complete = True
    for item in items:
        for key, value in (item.get("token_counts") or {}).items():
            if isinstance(value, int) and not isinstance(value, bool):
                token_counts[key] = token_counts.get(key, 0) + value
        for key, value in (item.get("billable_tokens") or {}).items():
            if isinstance(value, int) and not isinstance(value, bool):
                billable_tokens[key] = billable_tokens.get(key, 0) + value
        item_breakdown = (
            item.get("estimated_cost_breakdown_usd")
            or item.get("known_cost_lower_bound_breakdown_usd")
            or {}
        )
        for key, value in item_breakdown.items():
            try:
                cost_breakdown[key] = cost_breakdown.get(key, Decimal("0")) + Decimal(str(value))
            except Exception:
                continue
        estimated = item.get("estimated_cost_usd")
        if estimated is not None:
            try:
                cost_total += Decimal(str(estimated))
            except (ArithmeticError, ValueError):
                all_costs_complete = False
        lower_bound = item.get("known_cost_lower_bound_usd", estimated)
        if lower_bound is not None:
            try:
                lower_bound_total += Decimal(str(lower_bound))
            except (ArithmeticError, ValueError):
                pass
        if item.get("pricing_model"):
            model_ids.add(str(item["pricing_model"]))
        if item.get("pricing_version"):
            pricing_versions.add(str(item["pricing_version"]))
        if item.get("context_tier"):
            tiers.add(str(item["context_tier"]))
        all_cached_input_reported &= bool(item.get("cached_input_reporting_complete"))
        all_cache_writes_reported &= bool(item.get("cache_write_reporting_complete"))
        all_costs_complete &= bool(item.get("estimated_cost_complete"))
        pricing_warnings.update(item.get("pricing_warnings") or [])

    result["token_counts"] = token_counts
    # Replace per-call raw counts on the final report with turn totals so the
    # response's familiar ADK field names do not misleadingly describe only the
    # last model invocation in a tool-using agent run.
    for key in (
        "prompt_tokens",
        "input_tokens",
        "completion_tokens",
        "output_tokens",
        "total_tokens",
        "promptTokenCount",
        "candidatesTokenCount",
        "totalTokenCount",
        "cached_tokens",
        "reasoning_tokens",
        "cache_write_tokens",
        "input_tokens_details",
        "output_tokens_details",
        "prompt_tokens_details",
        "completion_tokens_details",
        "inputTokensDetails",
        "outputTokensDetails",
    ):
        result.pop(key, None)
    for key in (
        "prompt_token_count",
        "candidates_token_count",
        "thoughts_token_count",
        "total_token_count",
        "cached_content_token_count",
        "cache_write_token_count",
    ):
        if key in token_counts:
            result[key] = token_counts[key]
    result["billable_tokens"] = billable_tokens
    result["estimated_cost_breakdown_usd"] = {
        key: _round_usd(float(value)) for key, value in cost_breakdown.items()
    }
    result["known_cost_lower_bound_usd"] = _round_usd(float(lower_bound_total))
    result["estimated_cost_usd"] = (
        _round_usd(float(cost_total)) if all_costs_complete else None
    )
    result["pricing_model"] = next(iter(model_ids)) if len(model_ids) == 1 else "mixed"
    result["pricing_version"] = (
        next(iter(pricing_versions)) if len(pricing_versions) == 1 else "mixed"
    )
    result["pricing_versions"] = sorted(pricing_versions)
    result["context_tier"] = next(iter(tiers)) if len(tiers) == 1 else "mixed"
    result["usage_event_count"] = len(items)
    result["cached_input_reporting_complete"] = all_cached_input_reported
    result["cache_write_reporting_complete"] = all_cache_writes_reported
    result["estimated_cost_complete"] = all_costs_complete
    result["cached_input_tokens"] = billable_tokens.get("input_cached", 0)
    result["uncached_input_tokens"] = billable_tokens.get("input_uncached", 0)
    result["cache_write_input_tokens"] = billable_tokens.get("input_cache_write", 0)
    if not all_costs_complete:
        result["known_cost_lower_bound_breakdown_usd"] = result.pop(
            "estimated_cost_breakdown_usd", {}
        )
        result["pricing_warnings"] = sorted(pricing_warnings) or [
            "upstream_did_not_report_complete_input_or_cache_write_usage"
        ]
    else:
        result.pop("pricing_warnings", None)
    # A single pricing object would misrepresent turns with different context
    # tiers or models; per-tier rates remain available on each event and in the
    # version/tier summary above.
    result.pop("pricing", None)
    return result


def _token_counts(usage: dict[str, Any]) -> dict[str, Any]:
    counts: dict[str, Any] = {}
    fields = {
        "prompt_token_count": ("prompt_token_count", "promptTokenCount", "prompt_tokens", "input_tokens"),
        "candidates_token_count": (
            "candidates_token_count", "candidatesTokenCount", "completion_tokens", "output_tokens"
        ),
        "thoughts_token_count": ("thoughts_token_count", "thoughtsTokenCount", "reasoning_tokens"),
        "total_token_count": ("total_token_count", "totalTokenCount", "total_tokens"),
        "cached_content_token_count": (
            "cached_content_token_count", "cachedContentTokenCount", "cached_tokens"
        ),
        "tool_use_prompt_token_count": ("tool_use_prompt_token_count", "toolUsePromptTokenCount"),
        "cache_write_token_count": ("cache_write_tokens", "cache_write_token_count"),
    }
    cached_input_reported = False
    cache_write_reported = False
    for normalized_key, aliases in fields.items():
        value = _first_int(usage, aliases)
        if value is not None:
            counts[normalized_key] = value
            if normalized_key == "cached_content_token_count":
                cached_input_reported = True
            if normalized_key == "cache_write_token_count":
                cache_write_reported = True

    input_details = _nested_detail_objects(
        usage, ("input_tokens_details", "inputTokensDetails", "prompt_tokens_details", "promptTokensDetails")
    )
    output_details = _nested_detail_objects(
        usage, ("output_tokens_details", "outputTokensDetails", "completion_tokens_details", "completionTokensDetails")
    )
    for details in input_details:
        cached = _first_int(details, ("cached_tokens", "cachedTokens"))
        if cached is not None and "cached_content_token_count" not in counts:
            counts["cached_content_token_count"] = cached
            cached_input_reported = True
        elif cached is not None:
            cached_input_reported = True
        cache_write = _first_int(details, ("cache_write_tokens", "cacheWriteTokens"))
        if cache_write is not None:
            counts["cache_write_token_count"] = cache_write
            cache_write_reported = True
    # ADK's current Python LiteLLM bridge reports cache hits but drops OpenAI's
    # cache_write_tokens field. Presence of the details object alone is not
    # evidence that the count was reported.
    counts["cached_input_reporting_complete"] = cached_input_reported
    counts["cache_write_reporting_complete"] = cache_write_reported
    for details in output_details:
        reasoning = _first_int(details, ("reasoning_tokens", "reasoningTokens"))
        if reasoning is not None and "thoughts_token_count" not in counts:
            counts["thoughts_token_count"] = reasoning
    return counts


def _nested_detail_objects(source: dict[str, Any], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for key in keys:
        value = source.get(key)
        if isinstance(value, dict):
            details.append(value)
        elif isinstance(value, list):
            details.extend(item for item in value if isinstance(item, dict))
    return details


def _output_token_count(
    *,
    prompt_token_count: int | None,
    candidates_token_count: int | None,
    thoughts_token_count: int | None,
    total_token_count: int | None,
) -> int | None:
    # OpenAI completion/output tokens already include the reasoning subset.
    # ADK LiteLLM maps that inclusive provider count to candidates_token_count.
    if candidates_token_count is not None:
        return candidates_token_count
    if total_token_count is not None and prompt_token_count is not None:
        return max(total_token_count - prompt_token_count, 0)
    return thoughts_token_count or None


def _first_int(source: dict[str, Any], keys: tuple[str, ...]) -> int | None:
    for key in keys:
        value = source.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            return max(value, 0)
        if isinstance(value, float) and value.is_integer():
            return max(int(value), 0)
        if isinstance(value, str):
            try:
                return max(int(value), 0)
            except ValueError:
                continue
    return None


def _round_usd(value: float) -> float:
    return round(value, 9)
