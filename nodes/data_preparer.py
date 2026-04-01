"""
Stage 2: Data Preparer (AGENTIC + deterministic validator)
Writes and executes parsing scripts; validator runs after each attempt.
"""
import logging
from pathlib import Path

from agents.agent_runner import run_agent_loop
from agents.llm import get_llm
from agents.prompts import DATA_PREPARER_SYSTEM_PROMPT
from agents.state import PipelineState
from agents.tools import make_execute_python_tool, read_file, write_file, list_files
from agents.validators import validate_prepared_data

log = logging.getLogger("data_preparer")
MAX_AGENT_ITERATIONS = 20  # write parse script + execute + fix if needed


def data_preparer_node(state: PipelineState) -> dict:
    """LangGraph node: agentic data preparation with deterministic validation gate."""
    run_dir = Path(state["run_dir"])
    config = state["config"]
    spec = state["replication_spec"]
    retry_count = state.get("retry_count", 0)

    prepared_dir = run_dir / "data" / "prepared"
    prepared_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = run_dir / "data" / "raw"

    execute_python = make_execute_python_tool(str(run_dir))
    tools = [execute_python, read_file, write_file, list_files]
    llm = get_llm("data_preparer", config).bind_tools(tools)
    system_prompt = DATA_PREPARER_SYSTEM_PROMPT.replace("{run_dir}", str(run_dir))

    import yaml as _yaml
    spec_str = _yaml.dump({
        "geography": spec["geography"],
        "classification": spec["classification"],
        "methodology": spec["methodology"],
        "paper": {"reference_year": spec["paper"]["reference_year"]},
    }, default_flow_style=False)

    prior_errors = state.get("preparation_errors", [])
    error_context = ""
    if prior_errors:
        # Include the most recent generated script so the agent can see what it tried
        scripts_dir = run_dir / "generated_scripts"
        prior_script_text = ""
        if scripts_dir.exists():
            scripts = sorted(scripts_dir.glob("*.py"), key=lambda p: p.stat().st_mtime)
            if scripts:
                prior_script_text = scripts[-1].read_text()[:3000]
        script_section = (
            f"\n\nPrevious script you wrote:\n```python\n{prior_script_text}\n```\n"
            if prior_script_text else ""
        )
        error_context = (
            f"\n\nPrevious attempt FAILED validation with these errors:\n"
            + "\n".join(f"- {e}" for e in prior_errors)
            + script_section
            + "\nFix these errors. Do NOT rewrite from scratch — modify the approach."
        )

    # Sample a few rows from raw data so the agent knows the exact format upfront
    data_preview = _build_data_preview(raw_dir)

    initial_message = (
        f"Parse the raw data into analysis-ready matrices.\n\n"
        f"Raw data directory: {raw_dir}\n"
        f"Output directory: {prepared_dir}\n"
        f"Data manifest: {raw_dir}/data_manifest.yaml\n\n"
        f"Spec:\n```yaml\n{spec_str}\n```"
        f"\n\n{data_preview}"
        f"{error_context}\n"
    )

    max_cost = config.get("pipeline", {}).get("max_cost_per_stage", 5.0)  # data prep is token-heavy
    run_agent_loop(
        llm=llm, tools=tools, system_prompt=system_prompt,
        initial_message=initial_message, max_iterations=MAX_AGENT_ITERATIONS,
        stage_name="data_preparer", max_cost_usd=max_cost,
    )

    # Build prepared_data_paths
    prepared_data_paths = {
        "metadata": str(prepared_dir / "metadata.json"),
        "Z_EU": str(prepared_dir / "Z_EU.csv"),
        "e_nonEU": str(prepared_dir / "e_nonEU.csv"),
        "x_EU": str(prepared_dir / "x_EU.csv"),
        "Em_EU": str(prepared_dir / "Em_EU.csv"),
    }

    # Deterministic validation
    is_valid, errors = validate_prepared_data(prepared_data_paths, spec)

    if not is_valid:
        log.warning(f"Preparation validation FAILED (attempt {retry_count+1}): {errors}")

    return {
        "prepared_data_paths": prepared_data_paths,
        "preparation_valid": is_valid,
        "preparation_errors": errors,
        "retry_count": retry_count + 1,
        "current_stage": 2,
    }


def _build_data_preview(raw_dir: Path) -> str:
    """
    Read a few rows from the first IC-IOT and employment file to show
    the agent the exact column names and value formats upfront.
    """
    try:
        import pandas as pd
        lines = ["## Data Preview (actual column values — use these for filtering/mapping)\n"]

        iot_files = sorted((raw_dir / "ic_iot").glob("*.csv"))
        if iot_files:
            df = pd.read_csv(iot_files[0], nrows=4)
            lines.append(f"IC-IOT ({iot_files[0].name}) — first 4 rows:")
            lines.append(df.to_string(index=False))
            lines.append(f"\n  prd_ava unique sample: {df['prd_ava'].unique()[:2].tolist()}")
            lines.append(f"  prd_use unique sample: {df['prd_use'].unique()[:2].tolist()}")
            lines.append(f"  c_orig values: {df['c_orig'].unique().tolist()}")
            lines.append(f"  c_dest sample: {df['c_dest'].unique()[:5].tolist()}")

        emp_files = sorted((raw_dir / "employment").glob("*.csv"))
        if emp_files:
            df = pd.read_csv(emp_files[0], nrows=4)
            lines.append(f"\nEmployment ({emp_files[0].name}) — first 4 rows:")
            lines.append(df.to_string(index=False))
            lines.append(f"  nace_r2 sample: {df['nace_r2'].unique()[:3].tolist()}")
            lines.append(f"  geo values: {df['geo'].unique().tolist()}")

        return "\n".join(lines)
    except Exception as e:
        return f"(Data preview unavailable: {e})"
