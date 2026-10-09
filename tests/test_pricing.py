"""Tests for cost estimates from provider-reported usage."""

from decimal import Decimal

from sheetbench_runner.entities import SolveUsage
from sheetbench_runner.pricing import (
    ModelRates,
    PricingSnapshot,
    estimate_cost_usd,
    estimate_solve_cost_usd,
    pricing_for,
)
from sheetbench_runner.solve_profile import ProfileModel

OPUS_5_5 = ProfileModel.model_validate(
    {"transport": "anthropic", "apiKeyEnv": "KEY", "request": {"model": "claude-opus-5-5"}}
)


def usage(**parts: int) -> SolveUsage:
    return SolveUsage.model_validate(
        {"turns": 1, "tool_calls": 0, "input_tokens": 12_500_000, "output_tokens": 100_000, **parts}
    )


def opus_rates() -> ModelRates:
    pricing = pricing_for(OPUS_5_5)
    assert pricing is not None
    return pricing.rates


def test_opus_5_5_prices_each_part_of_the_input_at_its_rate() -> None:
    # Arrange: $4 uncached + $2 read + $5 5m write + $4 1h write + $2 output.
    solve_usage = usage(
        uncached_input_tokens=1_000_000,
        cache_read_input_tokens=10_000_000,
        cache_write_input_tokens=1_500_000,
        cache_write_5m_input_tokens=1_000_000,
        cache_write_1h_input_tokens=500_000,
    )

    # Act
    cost = estimate_cost_usd(solve_usage, opus_rates())

    # Assert
    assert cost == Decimal("17")


def test_the_catalog_records_the_opus_5_5_rates_and_their_source() -> None:
    # Act
    pricing = pricing_for(OPUS_5_5)

    # Assert
    assert pricing is not None
    assert pricing.rates == ModelRates(
        input=Decimal("4"),
        output=Decimal("20"),
        cache_read=Decimal("0.20"),
        cache_write_5m=Decimal("5"),
        cache_write_1h=Decimal("8"),
    )
    assert pricing.source
    assert "solve" in pricing.scope and "probe" in pricing.scope


def test_a_model_outside_the_catalog_has_no_pricing() -> None:
    # Arrange
    unknown = [
        OPUS_5_5.model_copy(update={"request": {"model": "claude-opus-5-5-private"}}),
        OPUS_5_5.model_copy(update={"transport": "openai-compatible"}),
        ProfileModel.model_validate(
            {
                "transport": "openai-compatible",
                "apiKeyEnv": "KEY",
                "baseUrl": "http://localhost:8000/v1",
                "request": {"model": "claude-opus-5-5"},
            }
        ),
    ]

    # Act
    found = [pricing_for(model) for model in unknown]

    # Assert
    assert found == [None, None, None]


def test_usage_without_parts_from_an_older_server_has_no_estimate() -> None:
    # Act
    cost = estimate_cost_usd(usage(), opus_rates())

    # Assert
    assert cost is None


def test_parts_that_do_not_add_up_to_the_input_total_have_no_estimate() -> None:
    # Arrange: one call's input was reported without parts, so the parts fall short.
    solve_usage = usage(
        uncached_input_tokens=1_000_000,
        cache_read_input_tokens=10_000_000,
        cache_write_input_tokens=1_000_000,
        cache_write_5m_input_tokens=1_000_000,
    )

    # Act
    cost = estimate_cost_usd(solve_usage, opus_rates())

    # Assert
    assert cost is None


def test_cache_writes_without_a_ttl_have_no_estimate_at_ttl_rates() -> None:
    # Arrange
    solve_usage = usage(
        uncached_input_tokens=1_000_000,
        cache_read_input_tokens=10_000_000,
        cache_write_input_tokens=1_500_000,
    )

    # Act
    cost = estimate_cost_usd(solve_usage, opus_rates())

    # Assert
    assert cost is None


def test_cache_writes_without_a_ttl_are_priced_at_a_flat_write_rate() -> None:
    # Arrange: $1 uncached + $1 read + $1.5 write + $2 output.
    rates = ModelRates(
        input=Decimal("1"),
        output=Decimal("20"),
        cache_read=Decimal("0.1"),
        cache_write=Decimal("1"),
    )
    solve_usage = usage(
        uncached_input_tokens=1_000_000,
        cache_read_input_tokens=10_000_000,
        cache_write_input_tokens=1_500_000,
    )

    # Act
    cost = estimate_cost_usd(solve_usage, rates)

    # Assert
    assert cost == Decimal("5.5")


def test_haiku_5_5_prices_each_part_of_the_input_at_its_short_prompt_rate() -> None:
    # Arrange: $0.10 uncached + $0.10 read + $0.125 5m write + $0.10 1h write + $0.05 output.
    haiku = OPUS_5_5.model_copy(update={"request": {"model": "claude-haiku-5-5"}})
    pricing = pricing_for(haiku)
    assert pricing is not None
    solve_usage = usage(
        uncached_input_tokens=1_000_000,
        cache_read_input_tokens=10_000_000,
        cache_write_input_tokens=1_500_000,
        cache_write_5m_input_tokens=1_000_000,
        cache_write_1h_input_tokens=500_000,
    )

    # Act
    cost = estimate_cost_usd(solve_usage, pricing.rates)

    # Assert
    assert cost == Decimal("0.475")


def snapshot(model: str) -> PricingSnapshot:
    pricing = pricing_for(OPUS_5_5.model_copy(update={"request": {"model": model}}))
    assert pricing is not None
    return pricing


def review(model: str, **tokens: int) -> dict[str, object]:
    return {"model": model, "turns": 1, "toolCalls": 1, "outputTokens": 0, **tokens}


def test_a_review_on_another_model_is_priced_at_that_models_rates() -> None:
    # Arrange: the whole run read 1M uncached tokens; the review read 400K of them on Opus.
    whole = SolveUsage.model_validate(
        {
            "turns": 3,
            "tool_calls": 2,
            "input_tokens": 1_000_000,
            "output_tokens": 0,
            "uncached_input_tokens": 1_000_000,
            "cache_read_input_tokens": 0,
            "cache_write_input_tokens": 0,
        }
    )
    reviewed = review(
        "claude-opus-5-5",
        inputTokens=400_000,
        uncachedInputTokens=400_000,
        cacheReadInputTokens=0,
        cacheWriteInputTokens=0,
    )
    transcript = {"reviews": [reviewed]}

    # Act
    cost = estimate_solve_cost_usd(
        whole, transcript, snapshot("claude-haiku-5-5"), snapshot("claude-opus-5-5")
    )

    # Assert: 600K at Haiku's $0.10 plus 400K at Opus's $4.
    assert cost == Decimal("0.06") + Decimal("1.6")


def test_a_review_on_the_solvers_model_is_priced_with_the_rest() -> None:
    # Arrange
    whole = usage(
        uncached_input_tokens=12_500_000, cache_read_input_tokens=0, cache_write_input_tokens=0
    )
    transcript = {"reviews": [review("claude-opus-5-5", inputTokens=1)]}

    # Act
    cost = estimate_solve_cost_usd(whole, transcript, snapshot("claude-opus-5-5"))

    # Assert
    assert cost == estimate_cost_usd(whole, opus_rates())


def test_a_review_on_an_unpriced_model_leaves_the_cost_unknown() -> None:
    # Arrange
    whole = usage(
        uncached_input_tokens=12_500_000, cache_read_input_tokens=0, cache_write_input_tokens=0
    )
    transcript = {"reviews": [review("claude-sonnet-5-5", inputTokens=1)]}

    # Act
    cost = estimate_solve_cost_usd(whole, transcript, snapshot("claude-haiku-5-5"))

    # Assert
    assert cost is None
