"""
Stage 2: Data Preparer (AGENTIC + deterministic validator)
Writes and executes parsing scripts; validator runs after each attempt.
"""
import logging
from pathlib import Path

from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage

from agents.llm import get_llm
from agents.prompts import DATA_PREPARER_SYSTEM_PROMPT
from agents.state import PipelineState
from agents.tools import make_execute_python_tool, read_file, write_file, list_files
from agents.validators import validate_prepared_data

log = logging.getLogger("data_preparer")
MAX_AGENT_ITERATIONS = 25


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
    tool_map = {t.name: t for t in tools}

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
        error_context = f"\n\nPrevious attempt failed validation with these errors:\n" + "\n".join(f"- {e}" for e in prior_errors)

    initial_message = (
        f"Parse the raw data into analysis-ready matrices.\n\n"
        f"Raw data directory: {raw_dir}\n"
        f"Output directory: {prepared_dir}\n"
        f"Data manifest: {raw_dir}/data_manifest.yaml\n\n"
        f"Spec:\n```yaml\n{spec_str}\n```"
        f"{error_context}\n"
    )

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=initial_message),
    ]

    for iteration in range(MAX_AGENT_ITERATIONS):
        response = llm.invoke(messages)
        messages.append(response)

        if not response.tool_calls:
            log.info(f"Data Preparer agent finished after {iteration+1} iterations")
            break

        for tool_call in response.tool_calls:
            tool_name = tool_call["name"]
            tool_id = tool_call["id"]
            if tool_name not in tool_map:
                result = f"ERROR: Unknown tool '{tool_name}'"
            else:
                result = tool_map[tool_name].invoke(tool_call["args"])
            messages.append(ToolMessage(content=str(result), tool_call_id=tool_id))

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
