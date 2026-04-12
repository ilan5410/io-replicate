"""
Stage 0: Paper Analyst
ONE single Anthropic API call: read the PDF, produce replication_spec.yaml.
No tool-calling loop — the full paper text is sent in one prompt.
"""
import logging
import os
import re
from pathlib import Path

import yaml

from agents.state import PipelineState
from agents.validators import validate_spec

log = logging.getLogger("paper_analyst")

_SCHEMA_PATH = Path(__file__).parents[1] / "schemas" / "replication_spec_schema.yaml"
_EXAMPLE_SPEC_PATH = Path(__file__).parents[1] / "specs" / "figaro_2019" / "replication_spec.yaml"


def paper_analyst_node(state: PipelineState) -> dict:
    """
    Read the paper PDF and produce replication_spec.yaml in one Anthropic API call.
    Skips if a spec is already loaded in state.
    """
    # Skip if spec already provided (--spec flag)
    if state.get("replication_spec") and state.get("replication_spec_path"):
        log.info("Spec already provided — skipping Paper Analyst")
        return {"spec_approved": state.get("spec_approved", False), "current_stage": 0}

    run_dir = Path(state["run_dir"])
    paper_path = state.get("paper_pdf_path", "")
    config = state.get("config", {})

    if not paper_path or not Path(paper_path).exists():
        raise FileNotFoundError(f"Paper PDF not found: {paper_path}")

    # 1. Extract text from PDF
    log.info(f"Reading PDF: {paper_path}")
    paper_text = _read_pdf(paper_path)
    log.info(f"PDF extracted: {len(paper_text)} characters")

    # 2. Load schema only (no example spec — that's thousands of wasted tokens)
    schema_text = _SCHEMA_PATH.read_text()

    # 3. Build prompts (split for caching)
    user_hints = state.get("user_hints") or ""
    system_text = _build_system_prompt(schema_text)
    task_text = _build_task_text(user_hints)

    # 4. ONE Anthropic API call with prompt caching
    log.info("Sending single prompt to LLM (with prompt caching)...")
    spec_yaml = _call_llm(system_text, paper_text, task_text, config)

    # 5. Parse and validate
    try:
        spec = yaml.safe_load(spec_yaml)
    except yaml.YAMLError as e:
        log.error(f"LLM produced invalid YAML. First 500 chars:\n{spec_yaml[:500]}")
        raw_path = run_dir / "replication_spec_raw.txt"
        raw_path.write_text(spec_yaml)
        raise ValueError(
            f"Paper Analyst produced invalid YAML. Raw output saved to {raw_path}. "
            f"YAML error: {e}"
        ) from e
    is_valid, errors = validate_spec(spec)
    if not is_valid:
        log.warning(f"Spec has validation issues (will still proceed): {errors}")

    # 6. Save
    spec_path = run_dir / "replication_spec.yaml"
    spec_path.write_text(spec_yaml)
    log.info(f"Spec written to {spec_path}")

    return {
        "replication_spec": spec,
        "replication_spec_path": str(spec_path),
        "spec_approved": False,
        "current_stage": 0,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_pdf(path: str) -> str:
    """Extract all text from a PDF using pypdf."""
    try:
        from pypdf import PdfReader
        reader = PdfReader(path)
        pages = [page.extract_text() or "" for page in reader.pages]
        return "\n\n".join(pages)
    except ImportError:
        raise ImportError("pypdf is required to read PDFs. Run: pip install pypdf")


def _build_system_prompt(schema_text: str) -> str:
    """Stable system prompt — cached on every run after the first."""
    return f"""You are analyzing an Input-Output economics paper to produce a structured replication spec.

## Your task

Read the paper in the user message and produce a complete `replication_spec.yaml`. Output ONLY the raw YAML — no prose, no markdown fences.

## Schema

```yaml
{schema_text}
```

## Instructions

1. Extract all required fields: paper metadata, geography, classification, data_sources, methodology, decompositions, outputs, benchmarks, limitations.
2. `industry_list`: list ALL industries from the paper's annexes with 1-based index, code, and label.
3. `benchmarks.values`: extract EVERY numerical result in the paper (employment totals, shares, industry totals).
4. `outputs`: list EVERY table and figure including annexes.
5. Flag ambiguities with YAML comments: `# AMBIGUITY: ...`
6. `tolerances`: warning_pct: 10, error_pct: 25"""


def _build_task_text(user_hints: str) -> str:
    """Short task suffix appended after the cached paper text (not cached)."""
    hints_section = f"\n\nUser hints:\n{user_hints}" if user_hints else ""
    return f"Produce the replication_spec.yaml for the paper above.{hints_section}\n\n## Output (raw YAML only):"


_anthropic_client = None


def _call_llm(system_text: str, paper_text: str, task_text: str, config: dict) -> str:
    """Single Anthropic API call with prompt caching on both system and paper content."""
    global _anthropic_client
    import anthropic

    providers_cfg = config.get("llm", {}).get("providers", {})
    key_env = providers_cfg.get("anthropic", {}).get("api_key_env", "ANTHROPIC_API_KEY")
    api_key = os.environ.get(key_env)
    if not api_key:
        raise EnvironmentError(f"ANTHROPIC_API_KEY not set (env var: {key_env})")

    model = config.get("llm", {}).get("routing", {}).get("paper_analyst", "claude-sonnet-4-6")
    if "/" in model:
        model = model.split("/", 1)[1]

    max_tokens = config.get("llm", {}).get("paper_analyst_max_tokens", 16000)

    if _anthropic_client is None:
        _anthropic_client = anthropic.Anthropic(api_key=api_key)

    # cache_control on system prompt (stable across all runs) and paper text (stable per paper).
    # First run writes the cache; subsequent runs on the same paper pay ~$0.03/MTok instead of $3.
    params = dict(
        model=model,
        max_tokens=max_tokens,
        system=[{
            "type": "text",
            "text": system_text,
            "cache_control": {"type": "ephemeral"},
        }],
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": paper_text,
                    "cache_control": {"type": "ephemeral"},
                },
                {
                    "type": "text",
                    "text": task_text,
                },
            ],
        }],
    )

    # Use streaming — required by the Anthropic SDK when max_tokens is large
    with _anthropic_client.messages.stream(**params) as stream:
        response = stream.get_final_message()

    usage = response.usage
    cache_read = getattr(usage, "cache_read_input_tokens", 0) or 0
    cache_write = getattr(usage, "cache_creation_input_tokens", 0) or 0
    billed_input = usage.input_tokens - cache_read  # cache reads are cheaper
    cost_usd = (billed_input * 3 + cache_read * 0.30 + usage.output_tokens * 15) / 1_000_000
    log.info(
        f"paper_analyst LLM: {usage.input_tokens:,} in "
        f"({cache_read:,} cache_read, {cache_write:,} cache_write) "
        f"+ {usage.output_tokens:,} out tokens (~${cost_usd:.4f})"
    )
    if cache_read:
        log.info(f"Prompt cache HIT — {cache_read:,} tokens read from cache")
    elif cache_write:
        log.info(f"Prompt cache WRITE — {cache_write:,} tokens written to cache")

    if response.stop_reason == "max_tokens":
        raise ValueError(
            f"Paper Analyst output was truncated at {max_tokens} tokens — the spec is too long. "
            f"Increase llm.paper_analyst_max_tokens in config.yaml (current: {max_tokens}), "
            f"or reduce the scope of benchmarks/outputs requested in the prompt."
        )

    raw = response.content[0].text.strip()

    # Strip markdown fences if the model added them anyway
    raw = re.sub(r"^```ya?ml\s*\n", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\n```\s*$", "", raw)

    return raw.strip()
