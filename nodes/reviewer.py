"""
Stage 6: Reviewer
Two-phase approach that keeps the system generic:
  1. Deterministic benchmark checks — reads decomposition CSVs, compares against
     spec benchmarks that carry a `source` descriptor (no LLM, free, reproducible).
  2. Single LLM call for the Interpretation section — narrative context, caveats,
     and verification of any benchmarks that lack a `source` descriptor.

For a new paper: add `source` descriptors to benchmark entries in the spec to get
deterministic checking automatically. Any benchmark without a source is described to
the LLM for narrative treatment.
"""
import logging
import re
from pathlib import Path

import yaml as _yaml

from agents.llm import get_llm
from agents.state import PipelineState
from agents.validators.benchmark_validator import (
    format_benchmark_table,
    run_benchmark_checks,
    summarize,
)

log = logging.getLogger("reviewer")


def reviewer_node(state: PipelineState) -> dict:
    """LangGraph node: deterministic benchmark validation + LLM interpretation."""
    run_dir = Path(state["run_dir"])
    config = state["config"]
    spec = state["replication_spec"]
    run_id = state.get("run_id", run_dir.name)

    outputs_dir = run_dir / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    report_path = outputs_dir / "review_report.md"

    decomp_dir = run_dir / "data" / "decomposition"

    # ── Phase 1: deterministic benchmark checks ────────────────────────────────
    bm_results = run_benchmark_checks(spec, decomp_dir)
    n_pass, n_warn, n_fail, n_unverified = summarize(bm_results)
    log.info(f"Benchmark checks: PASS={n_pass} WARN={n_warn} FAIL={n_fail} UNVERIFIED={n_unverified}")

    # ── Phase 2: single LLM call for interpretation ────────────────────────────
    interpretation = _get_interpretation(bm_results, spec, config)

    # ── Phase 3: assemble and write report ────────────────────────────────────
    report_md = _build_report(spec, run_id, bm_results, interpretation)
    report_path.write_text(report_md)
    log.info(f"Review report written to {report_path}")

    review_passed = n_fail == 0
    return {
        "review_report_path": str(report_path),
        "review_passed": review_passed,
        "review_warnings": [f"{n_warn} warning(s) in review report"] if n_warn else [],
        "review_errors": [f"{n_fail} failure(s) in review report — see {report_path}"] if n_fail else [],
        "current_stage": 6,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_interpretation(bm_results: list[dict], spec: dict, config: dict) -> str:
    """Single LLM call — returns the Interpretation section as markdown prose."""
    from langchain_core.messages import HumanMessage

    # Summarise results for the prompt
    table_md = format_benchmark_table(bm_results)
    limitations = "\n".join(f"- {l}" for l in spec.get("limitations", []))
    notes = "\n".join(f"- {n}" for n in spec.get("benchmarks", {}).get("notes", []))
    unverified = [r for r in bm_results if r["status"] in ("UNVERIFIED", "ERROR")]
    unverified_section = ""
    if unverified:
        unverified_section = (
            "\n\nThe following benchmarks could not be checked deterministically "
            "(no `source` descriptor in the spec). Comment on them in your interpretation:\n"
            + "\n".join(f"- {r['name']}: expected {r['expected']} {r['unit']}" for r in unverified)
        )

    prompt = (
        f"You are reviewing the replication of:\n"
        f"  {spec['paper']['title']} ({spec['paper']['year']})\n\n"
        f"Benchmark results (already computed):\n{table_md}\n\n"
        f"Known limitations of this replication:\n{limitations}\n\n"
        f"Methodological notes:\n{notes}"
        f"{unverified_section}\n\n"
        "Write ONLY the '## Interpretation' section of the review report (2-4 paragraphs). "
        "Explain what the results mean, how known limitations account for deviations, "
        "and whether the replication is considered successful overall. "
        "Do not repeat the benchmark table — just provide analytical commentary."
    )

    try:
        llm = get_llm("reviewer", config)
        response = llm.invoke([HumanMessage(content=prompt)])
        return response.content
    except Exception as e:
        log.warning(f"LLM interpretation call failed: {e}")
        return (
            "_Interpretation unavailable — LLM call failed. "
            f"Error: {e}_\n\n"
            "Please review the benchmark table above manually."
        )


def _build_report(spec: dict, run_id: str, bm_results: list[dict], interpretation: str) -> str:
    n_pass, n_warn, n_fail, n_unverified = summarize(bm_results)
    table_md = format_benchmark_table(bm_results)
    limitations = "\n".join(f"- {l}" for l in spec.get("limitations", []))

    return f"""# IO Replication Review Report
**Paper**: {spec['paper']['title']} ({spec['paper']['year']})
**Run**: {run_id}

## Summary
PASS: {n_pass} | WARN: {n_warn} | FAIL: {n_fail} | UNVERIFIED: {n_unverified}

## Benchmark Results
{table_md}

## Known Limitations
{limitations}

## Interpretation
{interpretation}
"""
