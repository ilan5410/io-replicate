"""
LLM provider abstraction — supports Anthropic (Claude) and OpenAI (GPT).
Usage:
    llm = get_llm("reviewer", config)
    llm_with_tools = llm.bind_tools(tools)
"""
import os
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from langchain_core.language_models.chat_models import BaseChatModel


class LLMProvider(Enum):
    ANTHROPIC_OPUS = "claude-opus-4-6"
    ANTHROPIC_SONNET = "claude-sonnet-4-6"
    OPENAI_GPT4O = "gpt-4o"
    OPENAI_GPT4O_MINI = "gpt-4o-mini"


# Default routing — mirrors config.yaml but used as fallback when config is absent.
# Note: openai/gpt-4o-mini works on Python 3.10-3.12 but langchain-openai has
# pydantic v1 serialization issues on Python 3.14+ that cause OpenAI 400 errors.
# All stages default to Anthropic; switch data_acquirer/output_producer to
# openai/gpt-4o-mini in config.yaml if using Python ≤3.12 for lower cost.
DEFAULT_ROUTING: dict[str, str] = {
    "paper_analyst":   "anthropic/claude-opus-4-6",
    "data_acquirer":   "anthropic/claude-sonnet-4-6",
    "data_preparer":   "anthropic/claude-sonnet-4-6",
    "output_producer": "anthropic/claude-sonnet-4-6",
    "reviewer":        "anthropic/claude-sonnet-4-6",
}

DEFAULT_FALLBACK = "anthropic/claude-sonnet-4-6"

# Module-level cache — keyed by (agent_name, config identity) so the same config
# object always returns the same LLM instance. Using id(config) is safe because
# config dicts are loaded once at pipeline startup and never mutated.
_llm_cache: dict[str, object] = {}


def get_llm(agent_name: str, config: dict):
    """
    Instantiate the correct LangChain chat model for the given agent,
    reading routing from config["llm"]["routing"]. Falls back gracefully
    if the preferred provider's API key is missing.
    """
    cache_key = f"{agent_name}:{id(config)}"
    if cache_key in _llm_cache:
        return _llm_cache[cache_key]

    routing = config.get("llm", {}).get("routing", DEFAULT_ROUTING)
    fallback_str = config.get("llm", {}).get("fallback", DEFAULT_FALLBACK)

    model_str = routing.get(agent_name, fallback_str)

    providers_cfg = config.get("llm", {}).get("providers", {})
    anthropic_key_env = providers_cfg.get("anthropic", {}).get("api_key_env", "ANTHROPIC_API_KEY")
    openai_key_env = providers_cfg.get("openai", {}).get("api_key_env", "OPENAI_API_KEY")
    temperature = config.get("llm", {}).get("temperatures", {}).get(agent_name)

    provider, model_id = _parse_model_str(model_str)

    import logging as _logging
    _log = _logging.getLogger("llm")

    llm = None

    if provider == "anthropic":
        api_key = os.environ.get(anthropic_key_env)
        if not api_key:
            fallback_provider, fallback_model = _parse_model_str(fallback_str)
            if fallback_provider == "openai":
                openai_key = os.environ.get(openai_key_env)
                if openai_key:
                    _log.warning(
                        f"Falling back from anthropic/{model_id} to openai/{fallback_model} "
                        f"(ANTHROPIC_API_KEY not set)"
                    )
                    llm = _make_openai(fallback_model, openai_key, temperature)
            if llm is None:
                raise EnvironmentError(
                    f"ANTHROPIC_API_KEY (env var: {anthropic_key_env}) not set. "
                    f"Set it or add OPENAI_API_KEY as fallback."
                )
        else:
            llm = _make_anthropic(model_id, api_key, temperature)

    elif provider == "openai":
        api_key = os.environ.get(openai_key_env)
        if not api_key:
            anthropic_key = os.environ.get(anthropic_key_env)
            if anthropic_key:
                fallback_provider, fallback_model = _parse_model_str(fallback_str)
                if fallback_provider == "anthropic":
                    _log.warning(
                        f"Falling back from openai/{model_id} to anthropic/{fallback_model} "
                        f"(OPENAI_API_KEY not set)"
                    )
                    llm = _make_anthropic(fallback_model, anthropic_key, temperature)
            if llm is None:
                raise EnvironmentError(f"OPENAI_API_KEY (env var: {openai_key_env}) not set.")
        else:
            llm = _make_openai(model_id, api_key, temperature)

    else:
        raise ValueError(f"Unknown provider '{provider}' in model string '{model_str}'")

    _llm_cache[cache_key] = llm
    return llm


def _parse_model_str(model_str: str) -> tuple[str, str]:
    """Parse 'anthropic/claude-opus-4-20250514' into ('anthropic', 'claude-opus-4-20250514')."""
    if "/" in model_str:
        provider, model_id = model_str.split("/", 1)
        return provider, model_id
    # Guess from model name
    if model_str.startswith("claude"):
        return "anthropic", model_str
    return "openai", model_str


def _make_anthropic(model_id: str, api_key: str, temperature: float | None = None) -> "BaseChatModel":
    from langchain_anthropic import ChatAnthropic
    kwargs: dict = {"model": model_id, "api_key": api_key, "max_tokens": 8192}
    if temperature is not None:
        kwargs["temperature"] = temperature
    return ChatAnthropic(**kwargs)


def _make_openai(model_id: str, api_key: str, temperature: float | None = None) -> "BaseChatModel":
    from langchain_openai import ChatOpenAI
    kwargs: dict = {"model": model_id, "api_key": api_key}
    if temperature is not None:
        kwargs["temperature"] = temperature
    return ChatOpenAI(**kwargs)
