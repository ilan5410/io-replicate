"""
Stage 2: Data Preparer — single-shot code generation + deterministic validator.

Instead of an agentic tool-calling loop (expensive: 20 LLM calls × growing context),
uses a single LLM call to generate the complete parsing script, executes it, and
retries once with error context if it fails. Cost: ~$0.10 vs ~$3 for the agentic loop.
"""
import logging
import subprocess
import time
from pathlib import Path

import yaml as _yaml
from rich.console import Console
from rich.panel import Panel

from agents.llm import get_llm
from agents.state import PipelineState
from agents.tools import make_execute_python_tool
from agents.validators import validate_prepared_data

log = logging.getLogger("data_preparer")
_console = Console()
MAX_FIX_ATTEMPTS = 3   # LLM calls to fix a failing script


def data_preparer_node(state: PipelineState) -> dict:
    """LangGraph node: single-shot code generation + deterministic validation gate."""
    run_dir = Path(state["run_dir"])
    config = state["config"]
    spec = state["replication_spec"]
    retry_count = state.get("retry_count", 0)

    _console.print(Panel(
        f"[bold]Stage 2 — Data Preparer[/bold]  (attempt {retry_count + 1})\n"
        f"Single-shot codegen: Haiku writes a parse script, then it runs deterministically\n"
        f"[dim]Input: data/raw/   →   Output: Z, e, x, Em matrices in data/prepared/[/dim]",
        style="blue"
    ))

    prepared_dir = run_dir / "data" / "prepared"
    prepared_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = run_dir / "data" / "raw"

    execute_python = make_execute_python_tool(str(run_dir))

    # Build the minimal spec needed for matrix construction (drops benchmarks, outputs, etc.)
    spec_str = _yaml.dump(_minimal_spec(spec), default_flow_style=False)
    data_preview = _build_data_preview(raw_dir)

    prior_errors = state.get("preparation_errors", [])
    prior_script = _load_latest_script(run_dir)

    prompt = _build_prompt(
        raw_dir=str(raw_dir),
        prepared_dir=str(prepared_dir),
        spec_str=spec_str,
        data_preview=data_preview,
        prior_errors=prior_errors,
        prior_script=prior_script,
    )

    # Single LLM call → script → execute → optionally fix
    llm = get_llm("data_preparer", config)
    _console.print("  Generating parse script with Haiku...")
    script_code = _generate_script(llm, prompt)

    script_name = f"prepare_data_attempt{retry_count + 1}"
    _console.print(f"  Executing [bold]{script_name}.py[/bold] ...")
    result = execute_python.invoke({"script_content": script_code, "script_name": script_name})

    if not result.get("success", False):
        stderr = result.get("stderr", "")[:3000]
        stdout = result.get("stdout", "")[:1000]
        _console.print(f"  [red]✗[/red] Script failed (rc={result.get('returncode')}) — generating fix...")
        log.warning(f"Script failed (returncode {result.get('returncode')}). Attempting fix...")

        # One fix call — starts fresh, only needs the error context
        fix_prompt = _build_fix_prompt(
            script_code=script_code,
            stderr=stderr,
            stdout=stdout,
            raw_dir=str(raw_dir),
            prepared_dir=str(prepared_dir),
            spec_str=spec_str,
        )
        fixed_script = _generate_script(llm, fix_prompt)
        _console.print(f"  Executing [bold]{script_name}_fixed.py[/bold] ...")
        result = execute_python.invoke({
            "script_content": fixed_script,
            "script_name": f"{script_name}_fixed",
        })
        if not result.get("success", False):
            _console.print(f"  [red]✗[/red] Fixed script also failed")
            log.warning(f"Fixed script also failed: {result.get('stderr', '')[:500]}")
        else:
            _console.print(f"  [green]✓[/green] Fixed script succeeded")

    # Build prepared_data_paths
    prepared_data_paths = {
        "metadata": str(prepared_dir / "metadata.json"),
        "Z_EU": str(prepared_dir / "Z_EU.csv"),
        "e_nonEU": str(prepared_dir / "e_nonEU.csv"),
        "x_EU": str(prepared_dir / "x_EU.csv"),
        "Em_EU": str(prepared_dir / "Em_EU.csv"),
    }

    # Deterministic validation
    _console.print("  Running deterministic validation...")
    is_valid, errors = validate_prepared_data(prepared_data_paths, spec)

    if is_valid:
        _console.print("[green]✓[/green] Stage 2 complete — matrices validated")
    else:
        _console.print(f"[red]✗[/red] Validation FAILED (attempt {retry_count + 1}): {errors[:3]}")
        log.warning(f"Preparation validation FAILED (attempt {retry_count + 1}): {errors}")

    return {
        "prepared_data_paths": prepared_data_paths,
        "preparation_valid": is_valid,
        "preparation_errors": errors,
        "retry_count": retry_count + 1,
        "current_stage": 2,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _minimal_spec(spec: dict) -> dict:
    """Extract only what's needed for matrix construction. Drops benchmarks, outputs, etc."""
    return {
        "reference_year": spec["paper"]["reference_year"],
        "export_definition": spec.get("methodology", {}).get("export_definition"),
        "model_variant": spec.get("methodology", {}).get("model_variant"),
        "analysis_entities": [
            {"code": e["code"], "name": e["name"]}
            for e in spec["geography"]["analysis_entities"]
        ],
        "external_entities": [
            {"code": e["code"], "name": e["name"]}
            for e in spec["geography"].get("external_entities", [])
        ],
        "n_industries": spec["classification"]["n_industries"],
        "industry_list": [
            {"code": i["code"], "label": i["label"]}
            for i in spec["classification"]["industry_list"]
        ],
    }


def _build_data_preview(raw_dir: Path) -> str:
    try:
        import pandas as pd
        lines = ["## Data Preview (actual column values)\n"]

        iot_files = sorted((raw_dir / "ic_iot").glob("*.csv"))
        if iot_files:
            df = pd.read_csv(iot_files[0], nrows=3)
            lines.append(f"IC-IOT ({iot_files[0].name}) first 3 rows:")
            lines.append(df.to_string(index=False))
            lines.append(f"  prd_ava sample: {df['prd_ava'].unique()[:2].tolist()}")
            lines.append(f"  c_dest sample: {df['c_dest'].unique()[:4].tolist()}")

        emp_files = sorted((raw_dir / "employment").glob("*.csv"))
        if emp_files:
            df = pd.read_csv(emp_files[0], nrows=3)
            lines.append(f"\nEmployment ({emp_files[0].name}) first 3 rows:")
            lines.append(df.to_string(index=False))
            lines.append(f"  nace_r2 sample: {df['nace_r2'].unique()[:3].tolist()}")

        return "\n".join(lines)
    except Exception as e:
        return f"(Data preview unavailable: {e})"


def _load_latest_script(run_dir: Path) -> str:
    scripts_dir = run_dir / "generated_scripts"
    if not scripts_dir.exists():
        return ""
    scripts = sorted(
        [p for p in scripts_dir.glob("prepare_data*.py")],
        key=lambda p: p.stat().st_mtime,
    )
    if not scripts:
        return ""
    return scripts[-1].read_text()[:4000]


def _generate_script(llm, prompt: str) -> str:
    """Single LLM call — returns only the Python script text."""
    from langchain_core.messages import HumanMessage
    import re
    response = llm.invoke([HumanMessage(content=prompt)])
    raw = response.content.strip()
    # Strip markdown fences if present
    raw = re.sub(r"^```python\s*\n", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\n```\s*$", "", raw)
    return raw.strip()


def _build_prompt(
    raw_dir: str,
    prepared_dir: str,
    spec_str: str,
    data_preview: str,
    prior_errors: list[str],
    prior_script: str,
) -> str:
    prior_section = ""
    if prior_errors:
        prior_section = (
            f"\n\n## Previous attempt failed\nErrors:\n"
            + "\n".join(f"- {e}" for e in prior_errors)
        )
        if prior_script:
            prior_section += f"\n\nPrevious script (fix the specific errors, not a full rewrite):\n```python\n{prior_script}\n```"

    return f"""You are writing a Python data preparation script for an IO economics replication pipeline.

## Task
Write a complete Python script that parses raw Eurostat data into analysis-ready matrices.

## Paths (use these exact absolute paths)
- raw_dir = "{raw_dir}"
- prepared_dir = "{prepared_dir}"

## Required output files (all in prepared_dir)
1. `Z_EU.csv` — (N×N) intra-EU intermediate flow matrix, row/col = country×industry flat index
2. `e_nonEU.csv` — (N×1) export vector: EU→non-EU flows (intermediate + final demand) + intra-EU final demand
3. `x_EU.csv` — (N×1) total output vector
4. `Em_EU.csv` — (N×1) employment vector in thousand persons
5. `metadata.json` — dimension info (see format below)

## Spec (countries, industries, methodology)
```yaml
{spec_str}
```

{data_preview}

## Data format — CRITICAL
IC-IOT CSVs (one per origin country in raw_dir/ic_iot/): columns are `freq, prd_use, prd_ava, c_dest, unit, c_orig, time, value`
- `prd_ava` and `prd_use` contain FULL TEXT LABELS matching spec `industry_list[i]['label']` (NOT CPA codes)
- `c_orig` and `c_dest` contain FULL COUNTRY NAMES matching spec `analysis_entities[j]['name']` (NOT ISO codes)
- `value` is float MIO_EUR (may be NaN — treat as 0)
- Final demand labels in prd_use: "Final consumption expenditure by government", "Final consumption expenditure by households", "Final consumption expenditure by non-profit organisations", "Gross capital formation", "Exports of goods and services"
- Value-added rows in prd_ava (exclude from Z matrix): rows NOT in industry_list labels

Employment CSVs (one per country in raw_dir/employment/): columns `freq, unit, nace_r2, na_item, geo, time, value`
- `nace_r2` is NACE activity LABEL (not code), `geo` is country NAME
- `value` is thousand persons (may be NaN)
- Use only leaf-level entries (individual activities, not "Total - all NACE activities" etc.)
- Map NACE labels to CPA labels using best-effort label matching against industry_list

## Matrix construction
Build label→index mappings from spec:
```python
label_to_idx = {{item['label']: i for i, item in enumerate(spec['industry_list'])}}
name_to_idx  = {{e['name']: i for i, e in enumerate(spec['analysis_entities'])}}
eu_names     = [e['name'] for e in spec['analysis_entities']]
ext_names    = [e['name'] for e in spec['external_entities']]
N = n_countries * n_industries
```

For Z_EU: filter rows where c_orig IN eu_names AND c_dest IN eu_names AND prd_ava IN label_to_idx AND prd_use IN label_to_idx.
For e_nonEU: EU→non-EU intermediate flows (c_orig IN eu_names, c_dest IN ext_names, prd_ava IN eu industries) + intra-EU final demand flows.
For x_EU: sum all outflows from each EU country-industry (all c_dest, all uses including value-added rows).

## metadata.json format
```json
{{"eu_countries": ["AT", ...], "cpa_codes": ["A01", ...], "n_countries": 28, "n_industries": 64, "n_total": 1792, "reference_year": 2010, "unit_Z": "MIO_EUR", "unit_x": "MIO_EUR", "unit_e": "MIO_EUR", "unit_Em": "THS_PER"}}
```

## Script requirements
- All imports at top of script
- Use absolute paths (raw_dir, prepared_dir) as given — do NOT use __file__ or relative navigation
- `os.makedirs(prepared_dir, exist_ok=True)` at the start
- Fill NaN values with 0
- Print progress so failures are diagnosable
- Save all 5 files before exiting{prior_section}

Output ONLY the raw Python script — no prose, no markdown fences."""


def _build_fix_prompt(
    script_code: str,
    stderr: str,
    stdout: str,
    raw_dir: str,
    prepared_dir: str,
    spec_str: str,
) -> str:
    return f"""Fix this Python script. It failed with the error shown below.
Output ONLY the corrected Python script — no prose, no markdown fences.

## Error
stderr:
{stderr}

stdout (last 1000 chars):
{stdout}

## Paths
raw_dir = "{raw_dir}"
prepared_dir = "{prepared_dir}"

## Spec
```yaml
{spec_str}
```

## Original script
```python
{script_code[:5000]}
```"""
