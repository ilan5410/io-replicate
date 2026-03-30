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

    # 2. Load schema and example spec for context
    schema_text = _SCHEMA_PATH.read_text()
    example_spec_text = _EXAMPLE_SPEC_PATH.read_text()

    # 3. Build the single prompt
    user_hints = state.get("user_hints") or ""
    prompt = _build_prompt(paper_text, schema_text, example_spec_text, user_hints)

    # 4. ONE Anthropic API call
    log.info("Sending single prompt to Opus...")
    spec_yaml = _call_opus(prompt, config)

    # 5. Parse and validate
    spec = yaml.safe_load(spec_yaml)
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


def _build_prompt(paper_text: str, schema_text: str, example_spec_text: str, user_hints: str) -> str:
    hints_section = f"\n\nUser hints:\n{user_hints}" if user_hints else ""

    return f"""You are analyzing an Input-Output economics paper to produce a structured replication spec.

## Your task

Read the paper below and produce a complete `replication_spec.yaml` that captures everything needed to replicate the paper's results. Output ONLY the YAML — no prose, no markdown fences, just the raw YAML content.

## Schema to follow

```yaml
{schema_text}
```

## Example of a well-formed spec (for Rémond-Tiedrez et al. 2019)

```yaml
{example_spec_text}
```

## Instructions

1. Extract all required fields from the paper (geography, classification, data sources, methodology, decompositions, outputs, benchmarks, limitations).
2. For `industry_list`: list ALL industries found in the paper's annexes or methodology section with 1-based index, code, and label.
3. For `benchmarks.values`: extract EVERY numerical result the paper reports that could be used for validation.
4. For `outputs`: list EVERY table and figure in the paper, including annexes.
5. If something is ambiguous, add a YAML comment `# AMBIGUITY: ...` on that line.
6. Do NOT use the example spec's values — read them from the paper itself.{hints_section}

## Paper text

{paper_text}

## Output

Produce the complete replication_spec.yaml now (raw YAML only, no markdown fences):"""


def _call_opus(prompt: str, config: dict) -> str:
    """Make a single Anthropic API call and return the YAML string."""
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

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model=model,
        max_tokens=8000,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = response.content[0].text.strip()

    # Strip markdown fences if the model added them anyway
    raw = re.sub(r"^```ya?ml\s*\n", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\n```\s*$", "", raw)

    return raw.strip()
