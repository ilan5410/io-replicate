"""
run_deterministic.py — zero-LLM fallback runner.
Runs stages 3-6 (Model Builder → Decomposer → Output Producer → Reviewer)
using pre-prepared matrices and a pre-written replication_spec.yaml.

Use this when:
  - Data is already downloaded and parsed (stages 1-2 done)
  - You want to re-run the math/outputs without LLM cost
  - You're iterating on the spec's outputs section

Usage:
    python run_deterministic.py --spec specs/figaro_2019/replication_spec.yaml --run-dir runs/20250330_142301
    python run_deterministic.py --spec specs/figaro_2019/replication_spec.yaml --run-dir runs/my_run --start-stage 4
"""
import argparse
import json
import logging
import sys
import time
from pathlib import Path

import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("run_deterministic")


def main():
    parser = argparse.ArgumentParser(
        description="Deterministic runner for IO Replicator stages 3-6 (no LLM)."
    )
    parser.add_argument("--spec", required=True, type=str,
                        help="Path to replication_spec.yaml")
    parser.add_argument("--run-dir", required=True, type=str,
                        help="Path to run directory containing data/prepared/")
    parser.add_argument("--start-stage", type=int, default=3, choices=[3, 4, 5, 6],
                        help="Stage to start from (3=model_builder, default)")
    parser.add_argument("--config", type=str, default="config.yaml",
                        help="Infrastructure config (used for LLM in stages 5-6 if needed)")
    args = parser.parse_args()

    run_dir = Path(args.run_dir)
    if not run_dir.exists():
        log.error(f"Run directory not found: {run_dir}")
        sys.exit(1)

    with open(args.spec) as f:
        spec = yaml.safe_load(f)

    cfg = {}
    if Path(args.config).exists():
        with open(args.config) as f:
            cfg = yaml.safe_load(f)

    # Build minimal state from disk
    prepared_dir = run_dir / "data" / "prepared"
    model_dir = run_dir / "data" / "model"

    prepared_data_paths = {
        "metadata": str(prepared_dir / "metadata.json"),
        "Z_EU":     str(prepared_dir / "Z_EU.csv"),
        "e_nonEU":  str(prepared_dir / "e_nonEU.csv"),
        "x_EU":     str(prepared_dir / "x_EU.csv"),
        "Em_EU":    str(prepared_dir / "Em_EU.csv"),
    }

    model_paths = {
        "A_EU":                   str(model_dir / "A_EU.csv"),
        "L_EU":                   str(model_dir / "L_EU.csv"),
        "d_EU":                   str(model_dir / "d_EU.csv"),
        "em_exports_total":       str(model_dir / "em_exports_total.csv"),
        "em_exports_country_matrix": str(model_dir / "em_exports_country_matrix.csv"),
    }

    decomp_dir = run_dir / "data" / "decomposition"
    decomposition_paths = {
        "country_decomposition": str(decomp_dir / "country_decomposition.csv"),
        "annex_c_matrix":        str(decomp_dir / "annex_c_matrix.csv"),
        "industry_table4":       str(decomp_dir / "industry_table4.csv"),
        "industry_figure3":      str(decomp_dir / "industry_figure3.csv"),
    }

    state = {
        "run_id": run_dir.name,
        "run_dir": str(run_dir),
        "config": cfg,
        "replication_spec": spec,
        "replication_spec_path": args.spec,
        "spec_approved": True,
        "prepared_data_paths": prepared_data_paths,
        "preparation_valid": True,
        "preparation_errors": [],
        "model_paths": model_paths,
        "model_valid": True,
        "model_checks": {},
        "decomposition_paths": decomposition_paths,
        "decomposition_valid": True,
        "output_paths": {},
        "review_report_path": "",
        "review_passed": False,
        "review_warnings": [],
        "review_errors": [],
        "current_stage": args.start_stage,
        "retry_count": 0,
        "error_log": [],
        # Required by TypedDict but not used in deterministic stages
        "paper_pdf_path": None,
        "user_hints": None,
        "data_manifest": {},
        "acquisition_complete": True,
    }

    # Lazy imports so stages 3-4 work without langchain installed
    from nodes.model_builder import model_builder_node
    from nodes.decomposer import decomposer_node

    stage_fns = {
        3: ("model_builder", model_builder_node),
        4: ("decomposer", decomposer_node),
    }

    if args.start_stage <= 5:
        try:
            from nodes.output_producer import output_producer_node
            stage_fns[5] = ("output_producer", output_producer_node)
        except ImportError as e:
            log.warning(f"Stage 5 (output_producer) unavailable — install langchain deps: {e}")

    if args.start_stage <= 6:
        try:
            from nodes.reviewer import reviewer_node
            stage_fns[6] = ("reviewer", reviewer_node)
        except ImportError as e:
            log.warning(f"Stage 6 (reviewer) unavailable — install langchain deps: {e}")

    for stage_num in range(args.start_stage, 7):
        if stage_num not in stage_fns:
            log.warning(f"Stage {stage_num} not available — stopping here")
            break
        name, fn = stage_fns[stage_num]
        log.info(f"=== Stage {stage_num}: {name} ===")
        updates = fn(state)
        state.update(updates)

    log.info("=== Deterministic run complete ===")
    report = state.get("review_report_path", "")
    if report and Path(report).exists():
        log.info(f"Review report: {report}")
        log.info(f"review_passed: {state.get('review_passed')}")
    return 0 if state.get("review_passed", False) else 1


if __name__ == "__main__":
    sys.exit(main())
