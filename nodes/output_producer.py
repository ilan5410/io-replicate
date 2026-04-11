"""
Stage 5: Output Producer (AGENTIC)
Produces all tables and figures specified in replication_spec.outputs.
"""
import logging
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

from agents.agent_runner import run_agent_loop
from agents.llm import get_llm
from agents.prompts import OUTPUT_PRODUCER_SYSTEM_PROMPT
from agents.state import PipelineState
from agents.tools import make_execute_python_tool, read_file, write_file, list_files

log = logging.getLogger("output_producer")
MAX_ITERATIONS = 5  # 1 code-gen + 1 execute + 1 fix max
_console = Console()


def output_producer_node(state: PipelineState) -> dict:
    """LangGraph node: agentic spec-driven output generation."""
    run_dir = Path(state["run_dir"])
    config = state["config"]
    spec = state["replication_spec"]

    n_tables = len(spec["outputs"].get("tables", []))
    n_figures = len(spec["outputs"].get("figures", []))
    _console.print(Panel(
        f"[bold]Stage 5 — Output Producer[/bold]\n"
        f"Generating {n_tables} tables and {n_figures} figures",
        style="blue"
    ))

    outputs_dir = run_dir / "outputs"
    (outputs_dir / "tables").mkdir(parents=True, exist_ok=True)
    (outputs_dir / "figures").mkdir(parents=True, exist_ok=True)

    execute_python = make_execute_python_tool(str(run_dir))
    tools = [execute_python, read_file, write_file, list_files]
    llm = get_llm("output_producer", config).bind_tools(tools)
    system_prompt = OUTPUT_PRODUCER_SYSTEM_PROMPT.replace("{run_dir}", str(run_dir))

    import yaml as _yaml
    import pandas as _pd

    outputs_spec_str = _yaml.dump({"outputs": spec["outputs"]}, default_flow_style=False)

    # Pre-load file previews so the agent never needs to call read_file/list_files
    decomp_dir = run_dir / "data" / "decomposition"
    prepared_dir = run_dir / "data" / "prepared"
    file_previews = []
    for search_dir in [decomp_dir, prepared_dir]:
        if search_dir.exists():
            for csv_path in sorted(search_dir.glob("*.csv")):
                try:
                    df_head = _pd.read_csv(csv_path, index_col=None, nrows=3)
                    file_previews.append(
                        f"### {csv_path.relative_to(run_dir)}\n"
                        f"columns: {list(df_head.columns)}\n"
                        f"{df_head.to_string(index=False, max_cols=8)}\n"
                    )
                except Exception:
                    pass
    previews_str = "\n".join(file_previews)

    initial_message = (
        f"Paths:\n"
        f"- decomp: {decomp_dir}\n"
        f"- prepared: {prepared_dir}\n"
        f"- out_tables: {outputs_dir / 'tables'}\n"
        f"- out_figures: {outputs_dir / 'figures'}\n\n"
        f"Data files (all you need — do NOT call read_file or list_files):\n"
        f"{previews_str}\n"
        f"Outputs spec:\n```yaml\n{outputs_spec_str}\n```\n"
        f"Countries: {[e['code'] for e in spec['geography']['analysis_entities']]}\n\n"
        f"Write ONE script producing ALL outputs. Call execute_python now."
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
