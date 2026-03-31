"""
Stage 1: Data Acquirer (AGENTIC)
Reads data_sources from spec, writes + executes download scripts.
"""
import logging
from pathlib import Path

from agents.agent_runner import run_agent_loop
from agents.llm import get_llm
from agents.prompts import DATA_ACQUIRER_SYSTEM_PROMPT
from agents.state import PipelineState
from agents.tools import make_execute_python_tool, read_file, write_file, list_files

log = logging.getLogger("data_acquirer")
MAX_ITERATIONS = 20  # write download script + execute + fix if needed


def data_acquirer_node(state: PipelineState) -> dict:
    """LangGraph node: agentic data download."""
    run_dir = Path(state["run_dir"])
    config = state["config"]
    spec = state["replication_spec"]

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

    # Load manifest
    manifest_path = raw_dir / "data_manifest.yaml"
    data_manifest = {}
    if manifest_path.exists():
        import yaml as _yaml2
        with open(manifest_path) as f:
            data_manifest = _yaml2.safe_load(f) or {}
        log.info(f"Data manifest loaded: {list(data_manifest.keys())}")
    else:
        log.warning("data_manifest.yaml not found after Data Acquirer run")

    return {
        "data_manifest": data_manifest,
        "acquisition_complete": manifest_path.exists(),
        "current_stage": 1,
    }
