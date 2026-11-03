"""Helpers to assemble streamed tokens into a final turn result."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

from services.agent_gateway_v3.app.services.usage_metadata import (
    aggregate_usage_token_metadata,
    extract_usage_metadata,
    normalize_usage_token_metadata,
)


@dataclass
class TurnAssembler:
    text_fragments: list[str] = field(default_factory=list)
    usage: dict[str, Any] = field(default_factory=dict)
    raw_events: list[dict[str, Any]] = field(default_factory=list)
    model_name: str | None = None
    _accumulated_text: str = field(default="", init=False, repr=False)
    _usage_reports: list[dict[str, Any]] = field(default_factory=list, init=False, repr=False)
    _usage_event_ids: set[str] = field(default_factory=set, init=False, repr=False)

    def add_text(self, value: str) -> str:
        if not value:
            return ""

        current_text = self._accumulated_text
        if not current_text and self.text_fragments:
            current_text = "".join(self.text_fragments)
            self._accumulated_text = current_text

        if value.startswith(current_text):
            delta = value[len(current_text) :]
            if delta:
                self.text_fragments.append(delta)
                self._accumulated_text += delta
            return delta

        overlap = self._suffix_prefix_overlap(current_text, value)
        delta = value[overlap:]
        if delta:
            self.text_fragments.append(delta)
            self._accumulated_text += delta
        return delta

    def add_event(self, event: dict[str, Any]) -> None:
        self.raw_events.append(event)
        model = (
            event.get("model_version")
            or event.get("model_name")
            or event.get("model")
        )
        usage = extract_usage_metadata(event)
        if usage is not None:
            event_id = event.get("id")
            if event_id is not None:
                normalized_event_id = str(event_id)
                if normalized_event_id in self._usage_event_ids:
                    return
                self._usage_event_ids.add(normalized_event_id)
            usage_model = str(model).strip() if model else self.model_name
            self._usage_reports.append(
                {
                    "usage": deepcopy(usage),
                    "model_name": usage_model,
                }
            )
            self.usage = aggregate_usage_token_metadata(
                [normalize_usage_token_metadata(item["usage"]) for item in self._usage_reports]
            )

    def set_usage(self, usage: dict[str, Any]) -> None:
        self._usage_reports = [
            {"usage": deepcopy(usage), "model_name": self.model_name}
        ]
        self._usage_event_ids.clear()
        self.usage = aggregate_usage_token_metadata(
            [normalize_usage_token_metadata(usage)]
        )

    @property
    def raw_usage_reports(self) -> list[dict[str, Any]]:
        """Raw usage payloads for asynchronous worker-side pricing."""
        return deepcopy(self._usage_reports)

    def reply_text(self) -> str:
        if self._accumulated_text:
            return self._accumulated_text.strip()
        return "".join(self.text_fragments).strip()

    def _suffix_prefix_overlap(self, current_text: str, next_value: str) -> int:
        max_overlap = min(len(current_text), len(next_value))
        for overlap in range(max_overlap, 0, -1):
            if current_text[-overlap:] == next_value[:overlap]:
                return overlap
        return 0
