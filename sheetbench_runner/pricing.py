"""USD cost estimates from the token usage the providers reported."""

from decimal import Decimal
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

from .solve_profile import ProfileModel

if TYPE_CHECKING:
    from .entities import SolveUsage

SCOPE = (
    "The /solve calls only: solver, reviewer and summariser. "
    "Excludes the solve-context probe and every call outside /solve."
)


class ModelRates(BaseModel):
    """USD per million tokens. A cache rate is needed only when the usage has tokens of its kind."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    input: Decimal
    output: Decimal
    cache_read: Decimal | None = None
    cache_write_5m: Decimal | None = None
    cache_write_1h: Decimal | None = None
    # Writes the provider reports without a TTL.
    cache_write: Decimal | None = None


class PricingSnapshot(BaseModel):
    """The rates of one model, recorded in run.json so a run's estimates stay reproducible."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    transport: str
    model: str
    base_url: str | None = None
    source: str
    rates: ModelRates
    scope: str = SCOPE


CATALOG = (
    PricingSnapshot(
        transport="anthropic",
        model="claude-opus-5-5",
        source="Anthropic list prices for claude-opus-5-5, entered 2026-09-24",
        rates=ModelRates(
            input=Decimal("4"),
            output=Decimal("20"),
            cache_read=Decimal("0.20"),
            cache_write_5m=Decimal("5"),
            cache_write_1h=Decimal("8"),
        ),
    ),
)


def pricing_for(model: ProfileModel) -> PricingSnapshot | None:
    """The catalog entry for exactly this transport, endpoint and model name."""
    key = (model.transport, model.baseUrl, model.model)
    return next((p for p in CATALOG if (p.transport, p.base_url, p.model) == key), None)


def estimate_cost_usd(usage: "SolveUsage", rates: ModelRates) -> Decimal | None:
    """None when the parts do not cover the input total, or a part with tokens has no rate."""
    uncached = usage.uncached_input_tokens
    cache_read = usage.cache_read_input_tokens
    cache_write = usage.cache_write_input_tokens
    if uncached is None or cache_read is None or cache_write is None:
        return None
    if uncached + cache_read + cache_write != usage.input_tokens:
        return None
    write_5m = usage.cache_write_5m_input_tokens or 0
    write_1h = usage.cache_write_1h_input_tokens or 0
    priced = [
        (uncached, rates.input),
        (cache_read, rates.cache_read),
        (write_5m, rates.cache_write_5m),
        (write_1h, rates.cache_write_1h),
        (cache_write - write_5m - write_1h, rates.cache_write),
        (usage.output_tokens, rates.output),
    ]
    if any(tokens < 0 or (tokens > 0 and rate is None) for tokens, rate in priced):
        return None
    return (
        sum((tokens * rate for tokens, rate in priced if rate is not None), Decimal(0)) / 1_000_000
    )
