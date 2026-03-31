"""
Stage 6: Reviewer (AGENTIC)
Validates pipeline results against spec benchmarks; produces review_report.md.
"""
import logging
from pathlib import Path

from agents.agent_runner import run_agent_loop
from agents.llm import get_llm
from agents.prompts import REVIEWER_SYSTEM_PROMPT
from agents.state import PipelineState
from agents.tools import read_file, write_file, list_files

log = logging.getLogger("reviewer")
MAX_ITERATIONS = 8  # reviewer only needs: read 2-3 files + write report


def reviewer_node(state: PipelineState) -> dict:
    """LangGraph node: agentic benchmark validation and review report generation."""
    run_dir = Path(state["run_dir"])
    config = state["config"]
    spec = state["replication_spec"]

    outputs_dir = run_dir / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    report_path = outputs_dir / "review_report.md"

    tools = [read_file, write_file, list_files]
    tool_map = {t.name: t for t in tools}

    llm = get_llm("reviewer", config).bind_tools(tools)
    system_prompt = REVIEWER_SYSTEM_PROMPT.replace("{run_dir}", str(run_dir))

    import yaml as _yaml
    benchmark_str = _yaml.dump({"benchmarks": spec.get("benchmarks", {})}, default_flow_style=False)
    limitations_str = "\n".join(f"- {l}" for l in spec.get("limitations", []))

    initial_message = (
        f"Review pipeline results for: {spec['paper']['title']} ({spec['paper']['year']})\n\n"
        f"Report path: {report_path}\n\n"
        f"Benchmarks:\n```yaml\n{benchmark_str}\n```\n\n"
        f"Known limitations:\n{limitations_str}\n\n"
        f"READ ONLY THESE FILES (small, safe):\n"
        f"  - {run_dir}/data/decomposition/country_decomposition.csv\n"
        f"  - {run_dir}/data/decomposition/industry_table4.csv\n"
        f"  - {run_dir}/data/decomposition/industry_figure3.csv\n\n"
        f"DO NOT read Z_EU, A_EU, L_EU, or em_exports_country_matrix — they are huge matrices.\n"
        f"All country and industry results you need are in country_decomposition.csv and industry_table4.csv.\n"
    )

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=initial_message),
    ]

    for iteration in range(MAX_ITERATIONS):
        response = llm.invoke(messages)
        messages.append(response)

        if not response.tool_calls:
            log.info(f"Reviewer finished after {iteration+1} iterations")
            break

        for tool_call in response.tool_calls:
            tool_name = tool_call["name"]
            tool_id = tool_call["id"]
            if tool_name not in tool_map:
                result = f"ERROR: Unknown tool '{tool_name}'"
            else:
                result = tool_map[tool_name].invoke(tool_call["args"])
            content = str(result)
            if len(content) > 4000:
                content = content[:4000] + f"\n[TRUNCATED — {len(str(result)):,} chars total]"
            messages.append(ToolMessage(content=content, tool_call_id=tool_id))
    else:
        log.warning(f"Reviewer hit MAX_ITERATIONS ({MAX_ITERATIONS}) without finishing — results may be incomplete")

    # Parse review results from the written report
    review_passed = False
    review_warnings = []
    review_errors = []

    if report_path.exists():
        report_text = report_path.read_text()
        import re
        # Match specific summary line: "PASS: X | WARN: Y | FAIL: Z"
        summary_match = re.search(r"PASS:\s*(\d+)\s*\|\s*WARN:\s*(\d+)\s*\|\s*FAIL:\s*(\d+)", report_text)
        if summary_match:
            n_fails = int(summary_match.group(3))
            n_warns = int(summary_match.group(2))
        else:
            # Fallback: count PASS/WARN/FAIL markers in benchmark table cells
            n_fails = len(re.findall(r"\|\s*FAIL\s*\|", report_text))
            n_warns = len(re.findall(r"\|\s*WARN\s*\|", report_text))
        review_passed = n_fails == 0
        if n_warns > 0:
            review_warnings = [f"{n_warns} warning(s) in review report"]
        if n_fails > 0:
            review_errors = [f"{n_fails} failure(s) in review report — see {report_path}"]
        log.info(f"Review: passed={review_passed}, warns={n_warns}, fails={n_fails}")
    else:
        log.warning(f"Review report not found at {report_path}")
        review_errors = ["Review report was not written"]

    return {
        "review_report_path": str(report_path),
        "review_passed": review_passed,
        "review_warnings": review_warnings,
        "review_errors": review_errors,
        "current_stage": 6,
    }
