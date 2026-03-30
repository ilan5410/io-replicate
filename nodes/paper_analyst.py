"""
Stage 0: Paper Analyst (AGENTIC)
Reads a paper PDF/text and produces replication_spec.yaml.
"""
import logging
from pathlib import Path

import yaml
from langchain_core.messages import HumanMessage, SystemMessage

from agents.llm import get_llm
from agents.prompts import PAPER_ANALYST_SYSTEM_PROMPT
from agents.state import PipelineState
from agents.tools import read_file, write_file, list_files
from agents.validators import validate_spec

log = logging.getLogger("paper_analyst")

TOOLS = [read_file, write_file, list_files]
MAX_ITERATIONS = 20


def paper_analyst_node(state: PipelineState) -> dict:
    """LangGraph node: agentic paper analysis → replication_spec.yaml"""
    # Skip if spec was provided directly (--spec flag)
    if state.get("replication_spec") and state.get("replication_spec_path"):
        log.info("Spec already provided — skipping Paper Analyst")
        return {"spec_approved": state.get("spec_approved", False), "current_stage": 0}

    run_dir = Path(state["run_dir"])
    config = state["config"]
    paper_path = state.get("paper_pdf_path", "")
    user_hints = state.get("user_hints", "")

    spec_output_path = run_dir / "replication_spec.yaml"

    llm = get_llm("paper_analyst", config).bind_tools(TOOLS)

    system_prompt = PAPER_ANALYST_SYSTEM_PROMPT.replace("{run_dir}", str(run_dir))
    initial_message = (
        f"Please analyze the following paper and produce a replication_spec.yaml.\n\n"
        f"Paper path: {paper_path}\n"
        f"Output path: {spec_output_path}\n"
    )
    if user_hints:
        initial_message += f"\nUser hints: {user_hints}\n"

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=initial_message),
    ]

    tool_map = {t.name: t for t in TOOLS}

    for iteration in range(MAX_ITERATIONS):
        response = llm.invoke(messages)
        messages.append(response)

        if not response.tool_calls:
            # Agent is done
            log.info(f"Paper Analyst finished after {iteration+1} iterations")
            break

        # Execute tool calls
        for tool_call in response.tool_calls:
            tool_name = tool_call["name"]
            tool_args = tool_call["args"]
            tool_id = tool_call["id"]

            if tool_name not in tool_map:
                result = f"ERROR: Unknown tool '{tool_name}'"
            else:
                result = tool_map[tool_name].invoke(tool_args)

            from langchain_core.messages import ToolMessage
            messages.append(ToolMessage(content=str(result), tool_call_id=tool_id))

    # Load and validate the written spec
    if not spec_output_path.exists():
        raise RuntimeError(
            f"Paper Analyst failed to write spec to {spec_output_path}. "
            f"Check the agent's output for errors."
        )

    with open(spec_output_path) as f:
        spec = yaml.safe_load(f)

    is_valid, errors = validate_spec(spec)
    if not is_valid:
        log.warning(f"Spec validation errors: {errors}")
        # Still continue — human will review before pipeline proceeds

    log.info(f"Spec written to {spec_output_path}, valid={is_valid}")

    return {
        "replication_spec": spec,
        "replication_spec_path": str(spec_output_path),
        "spec_approved": False,  # Must go through human_approval
        "current_stage": 0,
    }
