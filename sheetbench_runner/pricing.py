"""USD cost estimates from the token usage the providers reported."""

from collections.abc import Sequence
from decimal import Decimal
from typing import TYPE_CHECKING, Any

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
    PricingSnapshot(
        transport="anthropic",
        model="claude-haiku-5-5",
        source="Anthropic list prices for claude-haiku-5-5 up to 100K tokens, entered 2026-10-09",
        rates=ModelRates(
            input=Decimal("0.10"),
            output=Decimal("0.50"),
            cache_read=Decimal("0.01"),
            cache_write_5m=Decimal("0.125"),
            cache_write_1h=Decimal("0.20"),
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


# Each SolveUsage field and the key a transcript's review records it under.
REVIEW_USAGE_KEYS = {
    "turns": "turns",
    "tool_calls": "toolCalls",
    "input_tokens": "inputTokens",
    "output_tokens": "outputTokens",
    "uncached_input_tokens": "uncachedInputTokens",
    "cache_read_input_tokens": "cacheReadInputTokens",
    "cache_write_input_tokens": "cacheWriteInputTokens",
    "cache_write_5m_input_tokens": "cacheWrite5mInputTokens",
    "cache_write_1h_input_tokens": "cacheWrite1hInputTokens",
}


def _summed_review_usage(reviews: list[dict[str, Any]]) -> "SolveUsage | None":
    """The reviews' usage added up; a part is None when any review lacks it."""
    from .entities import SolveUsage

    parts: dict[str, int | None] = {}
    for field, key in REVIEW_USAGE_KEYS.items():
        values = [review.get(key) for review in reviews]
        counts = [value for value in values if isinstance(value, int)]
        parts[field] = sum(counts) if len(counts) == len(values) else None
    required = ("turns", "tool_calls", "input_tokens", "output_tokens")
    if any(parts[field] is None for field in required):
        return None
    return SolveUsage.model_validate(parts)


def _without(usage: "SolveUsage", part: "SolveUsage") -> "SolveUsage | None":
    """`usage` less `part`, field by field; None when a field would go negative."""
    from .entities import SolveUsage

    rest: dict[str, int | None] = {}
    for field in REVIEW_USAGE_KEYS:
        whole, taken = getattr(usage, field), getattr(part, field)
        if whole is None or taken is None:
            rest[field] = None
        elif whole < taken:
            return None
        else:
            rest[field] = whole - taken
    return SolveUsage.model_validate(rest)


def estimate_solve_cost_usd(
    usage: "SolveUsage",
    transcript: dict[str, Any],
    pricing: PricingSnapshot,
    model_pricing: Sequence[PricingSnapshot] = (),
) -> Decimal | None:
    """
    A solve's cost, priced by the model each part ran on.

    The transcript's reviews that ran on another model than `pricing`'s are priced at that
    model's entry in `model_pricing`, and the rest of `usage` at `pricing`. None when such a
    review ran on a model `model_pricing` does not price, or its usage cannot be taken out of
    the whole.
    """
    by_model = {snapshot.model: snapshot for snapshot in model_pricing}
    reviews_by_model: dict[str, list[dict[str, Any]]] = {}
    for review in transcript.get("reviews") or []:
        if isinstance(review, dict) and review.get("model") != pricing.model:
            reviews_by_model.setdefault(str(review.get("model")), []).append(review)
    total = Decimal(0)
    rest = usage
    for model, reviews in reviews_by_model.items():
        snapshot = by_model.get(model)
        review_usage = _summed_review_usage(reviews)
        if snapshot is None or review_usage is None:
            return None
        remaining = _without(rest, review_usage)
        review_cost = estimate_cost_usd(review_usage, snapshot.rates)
        if remaining is None or review_cost is None:
            return None
        rest = remaining
        total += review_cost
    solver_cost = estimate_cost_usd(rest, pricing.rates)
    return total + solver_cost if solver_cost is not None else None
