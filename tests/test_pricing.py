"""Tests for cost estimates from provider-reported usage."""

from decimal import Decimal

from sheetbench_runner.entities import SolveUsage
from sheetbench_runner.pricing import ModelRates, estimate_cost_usd, pricing_for
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
