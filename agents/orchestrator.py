"""
LangGraph orchestrator — defines the full IO Replicator pipeline graph.
"""
import logging
from pathlib import Path

from langgraph.graph import StateGraph, END

from agents.state import PipelineState
from nodes import (
    paper_analyst_node,
    classification_mapper_node,
    data_acquirer_node,
    data_guide_node,
    data_preparer_node,
    model_builder_node,
    decomposer_node,
    output_producer_node,
    spec_reconciler_node,
    reviewer_node,
)

log = logging.getLogger("orchestrator")


# ---------------------------------------------------------------------------
# Human checkpoint nodes
# ---------------------------------------------------------------------------

def human_approval_node(state: PipelineState) -> dict:
    """
    Checks spec approval before proceeding to data acquisition.
    The CLI sets spec_approved=True after the human confirms (or --auto-approve).
    If reached without approval, raises to force the user through the CLI.
    """
    if state.get("spec_approved", False):
        log.info("Spec approved — proceeding to data acquisition")
        return {}
    raise RuntimeError(
        "replication_spec must be reviewed and approved before the pipeline can proceed. "
        "Use: io-replicate run --spec <path>  (will prompt for approval)\n"
        "Or:  io-replicate run --spec <path> --auto-approve  (skip prompt)"
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
    graph.add_node("classification_mapper", classification_mapper_node)
    graph.add_node("human_approval", human_approval_node)
    graph.add_node("data_acquirer", data_acquirer_node)
    graph.add_node("data_guide", data_guide_node)
    graph.add_node("data_preparer", data_preparer_node)
    graph.add_node("model_builder", model_builder_node)
    graph.add_node("decomposer", decomposer_node)
    graph.add_node("output_producer", output_producer_node)
    graph.add_node("spec_reconciler", spec_reconciler_node)
    graph.add_node("reviewer", reviewer_node)
    graph.add_node("human_escalation", human_escalation_node)

    # Entry point
    graph.set_entry_point("paper_analyst")

    # Edges
    graph.add_edge("paper_analyst", "classification_mapper")
    graph.add_edge("classification_mapper", "human_approval")
    graph.add_conditional_edges("human_approval", route_after_approval)
    graph.add_edge("data_acquirer", "data_guide")
    graph.add_edge("data_guide", "data_preparer")

    # Validation gate after data preparation
    graph.add_conditional_edges("data_preparer", route_after_prep_validator)

    graph.add_edge("model_builder", "decomposer")
    graph.add_edge("decomposer", "spec_reconciler")   # patch spec BEFORE output_producer sees it
    graph.add_edge("spec_reconciler", "output_producer")
    graph.add_edge("output_producer", "reviewer")

    # Review gate
    graph.add_conditional_edges("reviewer", route_after_reviewer)

    # Compile
    if use_checkpointing:
        try:
            from langgraph.checkpoint.sqlite import SqliteSaver
            db_path = checkpoint_db or "runs/checkpoints.sqlite"
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            checkpointer = SqliteSaver.from_conn_string(db_path + "?check_same_thread=False")
            return graph.compile(checkpointer=checkpointer)
        except ImportError:
            log.warning("langgraph.checkpoint.sqlite not available — running without checkpointing")

    return graph.compile()


_NODE_FN_MAP = {
    "paper_analyst":        paper_analyst_node,
    "classification_mapper": classification_mapper_node,
    "data_acquirer":        data_acquirer_node,
    "data_guide":           data_guide_node,
    "data_preparer":        data_preparer_node,
    "model_builder":        model_builder_node,
    "decomposer":           decomposer_node,
    "output_producer":      output_producer_node,
    "spec_reconciler":      spec_reconciler_node,
    "reviewer":             reviewer_node,
}

_STAGE_TO_NODE = {
    0:   "paper_analyst",
    1:   "data_acquirer",
    1.5: "data_guide",
    2:   "data_preparer",
    3:   "model_builder",
    4:   "decomposer",
    4.5: "spec_reconciler",  # runs after decomposer, before output_producer
    5:   "output_producer",
    6:   "reviewer",
}

# Ordered list of nodes for building partial graphs
_NODE_ORDER = [
    "data_acquirer", "data_guide", "data_preparer", "model_builder",
    "decomposer", "spec_reconciler", "output_producer", "reviewer",
]


def build_graph_from_stage(start_stage: int, only_stage: str = None, **kwargs):
    """
    Build a graph that starts from a specific stage.
    - only_stage: run exactly one named node, then END.
    - start_stage: build a subgraph from that stage to the end.
      Stages 0-2 require the full graph (they go through paper_analyst / human_approval).
      Stages 3-6 build a trimmed graph that skips acquisition/preparation.
    """
    if only_stage:
        if only_stage not in _NODE_FN_MAP:
            raise ValueError(f"Unknown stage: '{only_stage}'. Valid: {list(_NODE_FN_MAP.keys())}")
        graph = StateGraph(PipelineState)
        graph.add_node(only_stage, _NODE_FN_MAP[only_stage])
        graph.set_entry_point(only_stage)
        graph.add_edge(only_stage, END)
        return graph.compile()

    if start_stage not in _STAGE_TO_NODE:
        raise ValueError(f"start_stage must be 0-6, got {start_stage}")

    start_node = _STAGE_TO_NODE[start_stage]

    # Stages 0 and 1 need the full graph (paper_analyst → human_approval → data_acquirer…)
    # Stage 2 (data_preparer) can be built as a trimmed graph like 3-6.
    if start_stage <= 1:
        return build_graph(**kwargs)

    # spec_reconciler must always run before output_producer and reviewer so that
    # auto-sourced benchmark patches reach the reviewer even when resuming from stage 5+.
    if start_node in ("output_producer", "reviewer") and "spec_reconciler" not in [start_node]:
        start_node = "spec_reconciler"

    # For stages 3-6, build a trimmed graph starting at the requested node
    # (data is assumed to be already prepared in the run_dir)
    nodes_to_include = _NODE_ORDER[_NODE_ORDER.index(start_node):]

    graph = StateGraph(PipelineState)
    for node_name in nodes_to_include:
        graph.add_node(node_name, _NODE_FN_MAP[node_name])

    graph.set_entry_point(nodes_to_include[0])

    needs_escalation = "data_preparer" in nodes_to_include or nodes_to_include[-1] == "reviewer"
    if needs_escalation:
        graph.add_node("human_escalation", human_escalation_node)

    for i in range(len(nodes_to_include) - 1):
        curr = nodes_to_include[i]
        nxt = nodes_to_include[i + 1]
        if curr == "data_preparer":
            graph.add_conditional_edges("data_preparer", route_after_prep_validator)
        else:
            graph.add_edge(curr, nxt)

    last = nodes_to_include[-1]
    if last == "reviewer":
        graph.add_conditional_edges("reviewer", route_after_reviewer)
    else:
        graph.add_edge(last, END)

    checkpoint_db = kwargs.get("checkpoint_db")
    if checkpoint_db:
        try:
            from langgraph.checkpoint.sqlite import SqliteSaver
            Path(checkpoint_db).parent.mkdir(parents=True, exist_ok=True)
            return graph.compile(checkpointer=SqliteSaver.from_conn_string(checkpoint_db + "?check_same_thread=False"))
        except ImportError:
            pass
    return graph.compile()
