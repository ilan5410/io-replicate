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
    """Sample the first few rows of the bulk TSV files so the LLM sees actual column values."""
    try:
        import pandas as pd
        lines = ["## Data Preview (actual file contents)\n"]

        iot_path = raw_dir / "naio_10_fcp_ip1.tsv.gz"
        if iot_path.exists():
            df = pd.read_csv(iot_path, sep='\t', compression='gzip', dtype=str, nrows=3)
            lines.append(f"IC-IOT bulk TSV — first 3 rows:")
            lines.append(df.to_string(index=False))
            lines.append(f"  Key column name: {repr(df.columns[0])}")
            lines.append(f"  Year columns sample: {[c for c in df.columns[1:5]]}")

        emp_path = raw_dir / "nama_10_a64_e.tsv.gz"
        if emp_path.exists():
            df = pd.read_csv(emp_path, sep='\t', compression='gzip', dtype=str, nrows=3)
            lines.append(f"\nEmployment bulk TSV — first 3 rows:")
            lines.append(df.to_string(index=False))
            lines.append(f"  Key column name: {repr(df.columns[0])}")

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
Write a complete Python script that parses raw Eurostat bulk-TSV data into analysis-ready matrices.

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

## Input files
- `raw_dir/naio_10_fcp_ip1.tsv.gz` — full IC-IOT table, all countries, gzip-compressed TSV
- `raw_dir/nama_10_a64_e.tsv.gz` — full employment table, all countries, gzip-compressed TSV

## IC-IOT TSV format — CRITICAL

The file is a **wide-format** TSV where the first column packs multiple dimensions as a comma-separated string, and the remaining columns are year values.

**Header row example:**
```
freq,prd_use,prd_ava,c_dest,unit,c_orig\\TIME_PERIOD\t2010 \t2011 \t2012 \t2013
```

**Data row example:**
```
A,CPA_A01,CPA_A01,AT,MIO_EUR,AT\t123.45 \t456.78
```

**Parsing pattern:**
```python
import pandas as pd, numpy as np, json, os, gzip

df = pd.read_csv(f"{{raw_dir}}/naio_10_fcp_ip1.tsv.gz", sep='\\t', compression='gzip', dtype=str)
key_col = df.columns[0]   # 'freq,prd_use,prd_ava,c_dest,unit,c_orig\\TIME_PERIOD'
split = df[key_col].str.split(',', expand=True)
split.columns = ['freq', 'prd_use', 'prd_ava', 'c_dest', 'unit', 'c_orig']
df = pd.concat([split, df.drop(columns=[key_col])], axis=1)

# Year columns have trailing spaces — find the reference year column
year_col = next(c for c in df.columns if c.strip() == str(reference_year))

# Extract value for target year, strip Eurostat flags ('b','e','p',':') and convert to float
df['value'] = pd.to_numeric(
    df[year_col].str.strip().str.replace(r'[^0-9.\\-]', '', regex=True),
    errors='coerce'
).fillna(0.0)
```

**Dimension values are CODES (not labels):**
- `c_orig`, `c_dest`: ISO country codes — match spec `analysis_entities[i]['code']` (e.g. "AT", "BE")
- `prd_ava`, `prd_use`: CPA codes — match spec `industry_list[i]['code']` (e.g. "CPA_A01", "CPA_B")
- Value-added rows in `prd_ava` (exclude from Z): `B2A3G`, `D1`, `D21X31`, `D29X39`, `OP_NRES`, `OP_RES`
- Final demand codes in `prd_use`: `P3_S13`, `P3_S14`, `P3_S15`, `P51G`, `P5M`
- Filter: `unit == 'MIO_EUR'` only

**Build code→index mappings from spec:**
```python
code_to_idx  = {{item['code']: i for i, item in enumerate(spec['industry_list'])}}
ctry_to_idx  = {{e['code']: i for i, e in enumerate(spec['analysis_entities'])}}
eu_codes     = [e['code'] for e in spec['analysis_entities']]
ext_codes    = [e['code'] for e in spec['external_entities']]
VA_ROWS      = {{'B2A3G', 'D1', 'D21X31', 'D29X39', 'OP_NRES', 'OP_RES'}}
FD_COLS      = {{'P3_S13', 'P3_S14', 'P3_S15', 'P51G', 'P5M'}}
```

For Z_EU: rows where `c_orig IN eu_codes AND c_dest IN eu_codes AND prd_ava IN code_to_idx AND prd_use IN code_to_idx`.
For e_nonEU: EU→non-EU intermediate (c_orig IN eu_codes, c_dest IN ext_codes, prd_ava in CPA industries) + intra-EU final demand (c_orig IN eu_codes, c_dest IN eu_codes, prd_use IN FD_COLS, prd_ava IN CPA industries).
For x_EU: all rows where `c_orig IN eu_codes AND prd_ava IN code_to_idx`, summing over all `c_dest` and all `prd_use`.

## Employment TSV format

**Header:** `freq,unit,nace_r2,na_item,geo\\TIME_PERIOD\t1975 \t...\t2010 \t...`

**Parsing pattern:**
```python
emp = pd.read_csv(f"{{raw_dir}}/nama_10_a64_e.tsv.gz", sep='\\t', compression='gzip', dtype=str)
key_col = emp.columns[0]
split = emp[key_col].str.split(',', expand=True)
split.columns = ['freq', 'unit', 'nace_r2', 'na_item', 'geo']
emp = pd.concat([split, emp.drop(columns=[key_col])], axis=1)

year_col = next(c for c in emp.columns if c.strip() == str(reference_year))
emp['value'] = pd.to_numeric(
    emp[year_col].str.strip().str.replace(r'[^0-9.\\-]', '', regex=True),
    errors='coerce'
).fillna(0.0)

# Filter to target unit and item
emp = emp[(emp['unit'] == 'THS_PER') & (emp['na_item'] == 'EMP_DC')]
emp = emp[emp['geo'].isin(eu_codes)]
```

**NACE codes** (`nace_r2`) are short codes like `A01`, `A02`, `B`, `C10-C12`. The spec provides a mapping in `classification.nace_to_cpa` (dict of nace_code → cpa_code) or use `industry_list` to build it. Sum multiple NACE codes that map to the same CPA code. Use leaf codes only (not aggregates like `A`, `B-E`, `C`).

## metadata.json format
```json
{{"eu_countries": ["AT", ...], "cpa_codes": ["CPA_A01", ...], "n_countries": 28, "n_industries": 64, "n_total": 1792, "reference_year": 2010, "unit_Z": "MIO_EUR", "unit_x": "MIO_EUR", "unit_e": "MIO_EUR", "unit_Em": "THS_PER"}}
```

## Script requirements
- All imports at top of script
- Use absolute paths as given — do NOT use __file__ or relative navigation
- `os.makedirs(prepared_dir, exist_ok=True)` at the start
- Print progress (rows loaded, matrix shapes) so failures are diagnosable
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
