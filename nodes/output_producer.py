"""
Stage 5: Output Producer (AGENTIC)
Produces all tables and figures specified in replication_spec.outputs.
"""
import logging
from pathlib import Path

from agents.agent_runner import run_agent_loop
from agents.llm import get_llm
from agents.prompts import OUTPUT_PRODUCER_SYSTEM_PROMPT
from agents.state import PipelineState
from agents.tools import make_execute_python_tool, read_file, write_file, list_files

log = logging.getLogger("output_producer")
MAX_ITERATIONS = 15  # write script + execute + fix if needed, per output item


def output_producer_node(state: PipelineState) -> dict:
    """LangGraph node: agentic spec-driven output generation."""
    run_dir = Path(state["run_dir"])
    config = state["config"]
    spec = state["replication_spec"]

    outputs_dir = run_dir / "outputs"
    (outputs_dir / "tables").mkdir(parents=True, exist_ok=True)
    (outputs_dir / "figures").mkdir(parents=True, exist_ok=True)

    execute_python = make_execute_python_tool(str(run_dir))
    tools = [execute_python, read_file, write_file, list_files]
    llm = get_llm("output_producer", config).bind_tools(tools)
    system_prompt = OUTPUT_PRODUCER_SYSTEM_PROMPT.replace("{run_dir}", str(run_dir))

    import yaml as _yaml
    outputs_spec_str = _yaml.dump({"outputs": spec["outputs"]}, default_flow_style=False)

    initial_message = (
        f"Produce all tables and figures specified in the outputs section.\n\n"
        f"Decomposition data directory: {run_dir / 'data' / 'decomposition'}\n"
        f"Prepared data directory: {run_dir / 'data' / 'prepared'}\n"
        f"Output tables directory: {outputs_dir / 'tables'}\n"
        f"Output figures directory: {outputs_dir / 'figures'}\n\n"
        f"Outputs to produce:\n```yaml\n{outputs_spec_str}\n```\n\n"
        f"EU countries (for labeling): {[e['code'] for e in spec['geography']['analysis_entities']]}\n"
    )

    max_cost = config.get("pipeline", {}).get("max_cost_per_stage", 2.0)
    run_agent_loop(
        llm=llm, tools=tools, system_prompt=system_prompt,
        initial_message=initial_message, max_iterations=MAX_ITERATIONS,
        stage_name="output_producer", max_cost_usd=max_cost,
    )

    # Collect output paths
    output_paths = {}
    for table in spec["outputs"].get("tables", []):
        tid = table["id"]
        csv_p = outputs_dir / "tables" / f"{tid}.csv"
        xlsx_p = outputs_dir / "tables" / f"{tid}.xlsx"
        if csv_p.exists():
            output_paths[tid] = str(csv_p)
        if xlsx_p.exists():
            output_paths[f"{tid}_xlsx"] = str(xlsx_p)

    for figure in spec["outputs"].get("figures", []):
        fid = figure["id"]
        png_p = outputs_dir / "figures" / f"{fid}.png"
        pdf_p = outputs_dir / "figures" / f"{fid}.pdf"
        if png_p.exists():
            output_paths[fid] = str(png_p)
        if pdf_p.exists():
            output_paths[f"{fid}_pdf"] = str(pdf_p)

    log.info(f"Output paths collected: {list(output_paths.keys())}")
    return {"output_paths": output_paths, "current_stage": 5}
