"""
LangGraph orchestrator — defines the full IO Replicator pipeline graph.
"""
import logging
from pathlib import Path

from langgraph.graph import StateGraph, END

from agents.state import PipelineState
from nodes import (
    paper_analyst_node,
    data_acquirer_node,
    data_preparer_node,
    model_builder_node,
    decomposer_node,
    output_producer_node,
    reviewer_node,
)

log = logging.getLogger("orchestrator")


# ---------------------------------------------------------------------------
# Human checkpoint nodes
# ---------------------------------------------------------------------------

def human_approval_node(state: PipelineState) -> dict:
    """
    Blocks for human review of the replication_spec.
    In CLI mode, this is handled by the CLI before invoking the graph.
    In programmatic mode, set spec_approved=True in the initial state to skip.
    """
    if state.get("spec_approved", False):
        log.info("Spec pre-approved — skipping human approval checkpoint")
        return {}
    # This node should not normally be reached without prior approval
    # (the CLI sets spec_approved=True after the human confirms)
    raise RuntimeError(
        "replication_spec must be reviewed and approved before the pipeline can proceed. "
        "Use the CLI (io-replicate run) which handles this interactively, or set "
        "spec_approved=True in the initial state."
    )


def human_escalation_node(state: PipelineState) -> dict:
    """Terminal node for unrecoverable errors requiring human intervention."""
    errors = state.get("preparation_errors") or state.get("review_errors") or state.get("error_log", [])
    error_summary = "\n".join(f"  - {e}" for e in errors) if errors else "  (no error details available)"

    msg = (
        f"\n{'='*60}\n"
        f"PIPELINE ESCALATION — Human intervention required\n"
        f"{'='*60}\n"
        f"Stage: {state.get('current_stage', 'unknown')}\n"
        f"Errors:\n{error_summary}\n\n"
        f"Options:\n"
        f"  1. Fix the issue in the raw data and re-run from stage {state.get('current_stage', 2)}\n"
        f"  2. Edit the replication spec and re-run\n"
        f"  3. Contact the pipeline maintainer\n"
        f"{'='*60}\n"
    )
    log.error(msg)
    raise RuntimeError(msg)


# ---------------------------------------------------------------------------
# Routing functions
# ---------------------------------------------------------------------------

def route_after_approval(state: PipelineState) -> str:
    if state.get("spec_approved", False):
        return "data_acquirer"
    return "paper_analyst"


def route_after_prep_validator(state: PipelineState) -> str:
    if state.get("preparation_valid", False):
        return "model_builder"
    max_retries = state.get("config", {}).get("pipeline", {}).get("max_retries", 3)
    if state.get("retry_count", 0) < max_retries:
        return "data_preparer"
    return "human_escalation"


def route_after_reviewer(state: PipelineState) -> str:
    if state.get("review_passed", False):
        return END
    return "human_escalation"


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def build_graph(use_checkpointing: bool = True, checkpoint_db: str = None):
    """
    Build and compile the full pipeline graph.

    Args:
        use_checkpointing: Whether to attach a SQLite checkpointer for resumability.
        checkpoint_db: Path to the SQLite DB. Defaults to runs/checkpoints.sqlite.
    """
    graph = StateGraph(PipelineState)

    # Add nodes
    graph.add_node("paper_analyst", paper_analyst_node)
    graph.add_node("human_approval", human_approval_node)
    graph.add_node("data_acquirer", data_acquirer_node)
    graph.add_node("data_preparer", data_preparer_node)
    graph.add_node("model_builder", model_builder_node)
    graph.add_node("decomposer", decomposer_node)
    graph.add_node("output_producer", output_producer_node)
    graph.add_node("reviewer", reviewer_node)
    graph.add_node("human_escalation", human_escalation_node)

    # Entry point
    graph.set_entry_point("paper_analyst")

    # Edges
    graph.add_edge("paper_analyst", "human_approval")
    graph.add_conditional_edges("human_approval", route_after_approval)
    graph.add_edge("data_acquirer", "data_preparer")

    # Validation gate after data preparation
    graph.add_conditional_edges("data_preparer", route_after_prep_validator)

    graph.add_edge("model_builder", "decomposer")
    graph.add_edge("decomposer", "output_producer")
    graph.add_edge("output_producer", "reviewer")

    # Review gate
    graph.add_conditional_edges("reviewer", route_after_reviewer)

    # Compile
    if use_checkpointing:
        try:
            from langgraph.checkpoint.sqlite import SqliteSaver
            db_path = checkpoint_db or "runs/checkpoints.sqlite"
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            checkpointer = SqliteSaver.from_conn_string(db_path)
            return graph.compile(checkpointer=checkpointer)
        except ImportError:
            log.warning("langgraph.checkpoint.sqlite not available — running without checkpointing")

    return graph.compile()


def build_graph_from_stage(start_stage: int, only_stage: str = None, **kwargs):
    """
    Build a graph that starts from a specific stage.
    Useful for resuming after the data is already downloaded.
    """
    stage_nodes = {
        0: "paper_analyst",
        1: "data_acquirer",
        2: "data_preparer",
        3: "model_builder",
        4: "decomposer",
        5: "output_producer",
        6: "reviewer",
    }

    if only_stage:
        # Single-node graph
        graph = StateGraph(PipelineState)
        node_fn_map = {
            "paper_analyst": paper_analyst_node,
            "data_acquirer": data_acquirer_node,
            "data_preparer": data_preparer_node,
            "model_builder": model_builder_node,
            "decomposer": decomposer_node,
            "output_producer": output_producer_node,
            "reviewer": reviewer_node,
        }
        if only_stage not in node_fn_map:
            raise ValueError(f"Unknown stage: {only_stage}. Valid: {list(node_fn_map.keys())}")
        graph.add_node(only_stage, node_fn_map[only_stage])
        graph.set_entry_point(only_stage)
        graph.add_edge(only_stage, END)
        return graph.compile()

    # Standard graph but with entry point at start_stage
    # (LangGraph doesn't support arbitrary entry points natively;
    #  we compile the full graph and pass pre-populated state)
    return build_graph(**kwargs)
