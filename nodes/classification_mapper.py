"""
Stage 0.5: Classification Mapper (AGENTIC — between Paper Analyst and Data Acquirer)

Resolves spec.concepts_to_map into concrete code lists with explicit reasoning.
When concepts_to_map is empty (the common case), this stage is a no-op.
"""
import json
import logging
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

from agents.agent_runner import run_agent_loop
from agents.llm import get_llm
from agents.prompts.classification_mapper import CLASSIFICATION_MAPPER_SYSTEM_PROMPT
from agents.state import PipelineState
from agents.tools.classification_tools import (
    tool_search_by_description,
    tool_expand_prefix,
    tool_load_concordance,
    tool_validate_mapping,
    tool_list_classifications,
    tool_write_concept_mapping,
)

log = logging.getLogger("classification_mapper")
_console = Console()

MAX_ITERATIONS = 20  # n_concepts * ~4 tool calls each


def classification_mapper_node(state: PipelineState) -> dict:
    """LangGraph node: map paper concepts to classification codes, or no-op."""
    spec = state["replication_spec"]
    run_dir = Path(state["run_dir"])
    config = state["config"]

    concepts = spec.get("concepts_to_map", [])

    if not concepts:
        _console.print(Panel(
            "[bold]Stage 0.5 — Classification Mapper[/bold]\n"
            "[dim]No concepts to map — passing through[/dim]",
            style="dim"
        ))
        return {"concept_mappings": {}, "current_stage": 0}

    _console.print(Panel(
        f"[bold]Stage 0.5 — Classification Mapper[/bold]\n"
        f"Mapping {len(concepts)} concept(s) to classification codes",
        style="blue"
    ))

    tools = [
        tool_search_by_description,
        tool_expand_prefix,
        tool_load_concordance,
        tool_validate_mapping,
        tool_list_classifications,
        tool_write_concept_mapping,
    ]
    llm = get_llm("classification_mapper", config).bind_tools(tools)

    concepts_str = json.dumps(concepts, indent=2)
    initial_message = (
        f"run_dir: {run_dir}\n\n"
        f"concepts_to_map:\n{concepts_str}\n\n"
        f"Map every concept. Call tool_write_concept_mapping for each one."
    )

    max_cost = config.get("pipeline", {}).get("max_cost_per_stage", 2.0)
    run_agent_loop(
        llm=llm, tools=tools,
        system_prompt=CLASSIFICATION_MAPPER_SYSTEM_PROMPT,
        initial_message=initial_message,
        max_iterations=MAX_ITERATIONS,
        stage_name="classification_mapper",
        max_cost_usd=max_cost,
    )

    # Load saved mappings
    mappings_path = run_dir / "data" / "concept_mappings.json"
    concept_mappings = {}
    if mappings_path.exists():
        concept_mappings = json.loads(mappings_path.read_text())
        _console.print(
            f"[green]✓[/green] Classification Mapper complete — "
            f"{len(concept_mappings)}/{len(concepts)} concepts mapped"
        )
    else:
        log.warning("concept_mappings.json not written — agent may not have called write tool")

    # Patch spec so downstream stages can use concept codes
    spec["concept_mappings"] = concept_mappings

    return {
        "concept_mappings": concept_mappings,
        "replication_spec": spec,
        "current_stage": 0,
    }
