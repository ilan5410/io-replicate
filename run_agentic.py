"""
run_agentic.py — programmatic entry point for the IO Replicator pipeline.
For the full guided CLI, use: io-replicate run --paper paper.pdf
"""
import argparse
import logging
import sys
import time
import yaml
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("run_agentic")


def main():
    parser = argparse.ArgumentParser(
        description="IO Replicator — run the full agentic pipeline."
    )
    parser.add_argument("--paper", type=str, default=None,
                        help="Path to paper PDF (runs Paper Analyst if provided).")
    parser.add_argument("--spec", type=str, default=None,
                        help="Path to existing replication_spec.yaml (skips Paper Analyst).")
    parser.add_argument("--config", type=str, default="config.yaml",
                        help="Infrastructure config file (default: config.yaml).")
    parser.add_argument("--start-stage", type=int, default=None,
                        help="Resume from stage N (requires --spec).")
    parser.add_argument("--only", type=str, default=None,
                        help="Run only one stage node by name.")
    parser.add_argument("--auto-approve", action="store_true",
                        help="Skip human approval checkpoint.")
    args = parser.parse_args()

    if not args.paper and not args.spec:
        parser.error("Provide either --paper or --spec.")

    # Load config
    cfg = {}
    if Path(args.config).exists():
        with open(args.config) as f:
            cfg = yaml.safe_load(f)
    else:
        log.warning(f"Config file not found: {args.config}. Using defaults.")

    # Set up run
    run_id = time.strftime("%Y%m%d_%H%M%S")
    runs_dir = Path(cfg.get("pipeline", {}).get("runs_dir", "runs"))
    run_dir = runs_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"Run ID: {run_id} — {run_dir}")

    from agents.orchestrator import build_graph, build_graph_from_stage
    from agents.state import PipelineState

    initial_state: PipelineState = {
        "run_id": run_id,
        "run_dir": str(run_dir),
        "config": cfg,
        "paper_pdf_path": args.paper,
        "user_hints": None,
        "replication_spec": {},
        "replication_spec_path": "",
        "spec_approved": False,
        "data_manifest": {},
        "acquisition_complete": False,
        "prepared_data_paths": {},
        "preparation_valid": False,
        "preparation_errors": [],
        "model_paths": {},
        "model_valid": False,
        "model_checks": {},
        "decomposition_paths": {},
        "decomposition_valid": False,
        "output_paths": {},
        "review_report_path": "",
        "review_passed": False,
        "review_warnings": [],
        "review_errors": [],
        "current_stage": args.start_stage or 0,
        "retry_count": 0,
        "error_log": [],
    }

    if args.spec:
        with open(args.spec) as f:
            initial_state["replication_spec"] = yaml.safe_load(f)
        initial_state["replication_spec_path"] = args.spec
        initial_state["spec_approved"] = args.auto_approve

    checkpoint_db = str(runs_dir / "checkpoints.sqlite")
    if args.only:
        app = build_graph_from_stage(args.start_stage or 0, only_stage=args.only,
                                     checkpoint_db=checkpoint_db)
    elif args.start_stage is not None:
        app = build_graph_from_stage(args.start_stage, checkpoint_db=checkpoint_db)
    else:
        app = build_graph(checkpoint_db=checkpoint_db)

    thread_id = {"configurable": {"thread_id": run_id}}
    final_state = app.invoke(initial_state, thread_id)

    log.info(f"Pipeline complete. review_passed={final_state.get('review_passed')}")
    report = final_state.get("review_report_path", "")
    if report and Path(report).exists():
        log.info(f"Review report: {report}")

    return 0 if final_state.get("review_passed", False) else 1


if __name__ == "__main__":
    sys.exit(main())
