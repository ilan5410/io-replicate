"""
Stage 1: Data Acquirer (AGENTIC)
Reads data_sources from spec, writes + executes download scripts.
"""
import logging
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

from agents.agent_runner import run_agent_loop
from agents.llm import get_llm
from agents.prompts import DATA_ACQUIRER_SYSTEM_PROMPT
from agents.state import PipelineState
from agents.tools import make_execute_python_tool, read_file, write_file, list_files

log = logging.getLogger("data_acquirer")
_console = Console()
MAX_ITERATIONS = 20  # write download script + execute + fix if needed


def data_acquirer_node(state: PipelineState) -> dict:
    """LangGraph node: agentic data download."""
    run_dir = Path(state["run_dir"])
    config = state["config"]
    spec = state["replication_spec"]

    _console.print(Panel(
        "[bold]Stage 1 — Data Acquirer[/bold]\n"
        "Bulk downloading IC-IOT + employment from Eurostat (2 requests, all countries)\n"
        "[dim]Expected: ~2-3 min total[/dim]",
        style="blue"
    ))

    raw_dir = run_dir / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    execute_python = make_execute_python_tool(str(run_dir))
    tools = [execute_python, read_file, write_file, list_files]
    llm = get_llm("data_acquirer", config).bind_tools(tools)
    system_prompt = DATA_ACQUIRER_SYSTEM_PROMPT.replace("{run_dir}", str(run_dir))

    import yaml as _yaml
    spec_str = _yaml.dump({"data_sources": spec["data_sources"], "geography": spec["geography"], "classification": spec["classification"]}, default_flow_style=False)

    initial_message = (
        f"Download all required data for this replication.\n\n"
        f"Raw data output directory: {raw_dir}\n"
        f"Manifest output path: {raw_dir}/data_manifest.yaml\n\n"
        f"Relevant spec sections:\n```yaml\n{spec_str}\n```\n\n"
        f"Reference year: {spec['paper']['reference_year']}\n"
    )

    max_cost = state.get("config", {}).get("pipeline", {}).get("max_cost_per_stage", 2.0)
    run_agent_loop(
        llm=llm, tools=tools, system_prompt=system_prompt,
        initial_message=initial_message, max_iterations=MAX_ITERATIONS,
        stage_name="data_acquirer", max_cost_usd=max_cost,
    )

    # Load manifest — fail hard if missing (Stage 2 cannot run without data)
    manifest_path = raw_dir / "data_manifest.yaml"
    if not manifest_path.exists():
        # List any generated scripts to help diagnose
        scripts = sorted((run_dir / "generated_scripts").glob("*.py"))
        script_names = ", ".join(s.name for s in scripts) if scripts else "(none)"
        raise RuntimeError(
            f"Data Acquirer finished without producing data_manifest.yaml. "
            f"No raw data was downloaded — Stage 2 cannot proceed.\n\n"
            f"Generated scripts: {script_names}\n"
            f"Check the scripts in {run_dir}/generated_scripts/ for errors.\n"
            f"To resume from Stage 1 after fixing: "
            f"io-replicate run --spec <spec> --start-stage 1"
        )

    import yaml as _yaml2
    with open(manifest_path) as f:
        data_manifest = _yaml2.safe_load(f) or {}
    log.info(f"Data manifest loaded: {list(data_manifest.keys())}")
    n_files = sum(len(v) if isinstance(v, list) else 1 for v in data_manifest.values())
    _console.print(f"[green]✓[/green] Stage 1 complete — manifest: {list(data_manifest.keys())} ({n_files} entries)")

    return {
        "data_manifest": data_manifest,
        "acquisition_complete": True,
        "current_stage": 1,
    }
