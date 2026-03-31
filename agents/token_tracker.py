"""
Token usage tracking and cost estimation per pipeline stage.
Provides a circuit breaker to stop runaway agentic loops.
"""
import logging

log = logging.getLogger("token_tracker")

# Approximate cost per million tokens (USD) — (input_rate, output_rate)
_COST_PER_MTOK: dict[str, tuple[float, float]] = {
    "claude-opus-4-6":   (15.00, 75.00),
    "claude-sonnet-4-6": (3.00,  15.00),
    "gpt-4o":            (5.00,  15.00),
    "gpt-4o-mini":       (0.15,  0.60),
}


class TokenTracker:
    def __init__(self, max_cost_usd: float = 5.0, model: str = ""):
        self.max_cost_usd = max_cost_usd
        self.model = model
        self.input_tokens = 0
        self.output_tokens = 0
        self.calls = 0

    def record(self, response) -> None:
        """Record token usage from a LangChain response object."""
        usage = getattr(response, "usage_metadata", None)
        if usage:
            self.input_tokens += usage.get("input_tokens", 0)
            self.output_tokens += usage.get("output_tokens", 0)
        self.calls += 1

        cost = self.estimated_cost_usd()
        if cost > self.max_cost_usd:
            raise RuntimeError(
                f"Token circuit breaker: estimated cost ${cost:.2f} exceeds "
                f"limit ${self.max_cost_usd:.2f} after {self.calls} LLM calls "
                f"({self.input_tokens:,} input + {self.output_tokens:,} output tokens). "
                f"Increase max_cost_per_stage in config.yaml to continue."
            )

    def estimated_cost_usd(self) -> float:
        input_rate, output_rate = _COST_PER_MTOK.get(self.model, (5.0, 15.0))
        return (self.input_tokens * input_rate + self.output_tokens * output_rate) / 1_000_000

    def summary(self) -> str:
        cost = self.estimated_cost_usd()
        return (
            f"{self.calls} calls, {self.input_tokens:,} in + {self.output_tokens:,} out tokens, "
            f"~${cost:.3f}"
        )
