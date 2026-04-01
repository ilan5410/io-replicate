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

    # 3. Build the single prompt
    user_hints = state.get("user_hints") or ""
    prompt = _build_prompt(paper_text, schema_text, user_hints)

    # 4. ONE Anthropic API call
    log.info("Sending single prompt to Opus...")
    spec_yaml = _call_opus(prompt, config)

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


def _build_prompt(paper_text: str, schema_text: str, user_hints: str) -> str:
    hints_section = f"\n\nUser hints:\n{user_hints}" if user_hints else ""

    return f"""You are analyzing an Input-Output economics paper to produce a structured replication spec.

## Your task

Read the paper below and produce a complete `replication_spec.yaml`. Output ONLY the raw YAML — no prose, no markdown fences.

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
6. `tolerances`: warning_pct: 10, error_pct: 25{hints_section}

## Paper

{paper_text}

## Output (raw YAML only):"""


_anthropic_client = None


def _call_opus(prompt: str, config: dict) -> str:
    """Make a single Anthropic API call and return the YAML string."""
    global _anthropic_client
    import anthropic

    providers_cfg = config.get("llm", {}).get("providers", {})
    key_env = providers_cfg.get("anthropic", {}).get("api_key_env", "ANTHROPIC_API_KEY")
    api_key = os.environ.get(key_env)
    if not api_key:
        raise EnvironmentError(f"ANTHROPIC_API_KEY not set (env var: {key_env})")

    model = config.get("llm", {}).get("routing", {}).get("paper_analyst", "claude-opus-4-6")
    # Strip provider prefix if present
    if "/" in model:
        model = model.split("/", 1)[1]

    max_tokens = config.get("llm", {}).get("paper_analyst_max_tokens", 16000)

    if _anthropic_client is None:
        _anthropic_client = anthropic.Anthropic(api_key=api_key)
    response = _anthropic_client.messages.create(
        model=model,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )

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
