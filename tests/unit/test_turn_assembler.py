from services.agent_gateway_v3.app.services.turn_assembler import TurnAssembler


def test_turn_assembler_collects_text_and_usage() -> None:
    assembler = TurnAssembler(model_name="gpt-6-luna")
    event = {
        "usage_metadata": {
            "prompt_token_count": 10,
            "candidates_token_count": 8,
            "thoughts_token_count": 3,
            "total_token_count": 18,
        }
    }
    assembler.add_event(event)
    assembler.add_text("hello")
    assembler.add_text(" world")

    assert assembler.reply_text() == "hello world"
    assert assembler.usage["token_counts"]["total_token_count"] == 18
    assert "estimated_cost_usd" not in assembler.usage
    assert assembler.raw_usage_reports == [
        {
            "usage": event["usage_metadata"],
            "model_name": "gpt-6-luna",
        }
    ]
    assert assembler.usage["usage_event_count"] == 1
    assert assembler.raw_events == [event]


def test_turn_assembler_sums_each_adk_model_call_and_deduplicates_event_ids() -> None:
    assembler = TurnAssembler(model_name="gpt-6-luna")
    event = {
        "id": "llm-response-1",
        "model_version": "openai/gpt-6-luna",
        "usage_metadata": {
            "prompt_token_count": 100,
            "candidates_token_count": 10,
            "total_token_count": 110,
            "cached_content_token_count": 20,
            "cache_write_tokens": 30,
        },
    }
    assembler.add_event(event)
    assembler.add_event(event)  # same ADK event must not be charged twice
    assembler.add_event(
        {
            "id": "llm-response-2",
            "model_version": "gpt-6-luna",
            "usage_metadata": {
                "prompt_token_count": 100,
                "candidates_token_count": 10,
                "total_token_count": 110,
                "cached_content_token_count": 20,
                "cache_write_tokens": 30,
            },
        }
    )

    assert assembler.usage["usage_event_count"] == 2
    assert assembler.usage["token_counts"]["prompt_token_count"] == 200
    assert assembler.usage["token_counts"]["total_token_count"] == 220
    assert "estimated_cost_usd" not in assembler.usage
    assert len(assembler.raw_usage_reports) == 2
    assert assembler.raw_usage_reports[0]["model_name"] == "openai/gpt-6-luna"
