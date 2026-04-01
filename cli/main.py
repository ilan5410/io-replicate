"""
io-replicate CLI — guided interface for IO paper replication.

Usage:
    io-replicate run --paper paper.pdf
    io-replicate run --spec specs/figaro_2019/replication_spec.yaml --start-stage 3
    io-replicate run --spec replication_spec.yaml --only reviewer
    io-replicate validate --spec replication_spec.yaml
    io-replicate sources --list
"""
import subprocess
import sys
import time
from pathlib import Path

import click
import yaml
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

console = Console()

SUPPORTED_SOURCES = [
    {"id": "figaro_iciot", "name": "FIGARO IC-IOT", "provider": "Eurostat",
     "code": "naio_10_fcp_ip1", "years": "2010-2013"},
    {"id": "figaro_sut",   "name": "FIGARO SUT",    "provider": "Eurostat",
     "code": "naio_10_fcp_r2", "years": "2010-2013"},
    {"id": "wiod",         "name": "WIOD",           "provider": "WIOD consortium",
     "code": "WIOT",     "years": "2000-2014"},
    {"id": "oecd_icio",    "name": "OECD ICIO",      "provider": "OECD",
     "code": "ICIO",     "years": "2005-2018"},
    {"id": "exiobase",     "name": "EXIOBASE 3",     "provider": "EXIOBASE consortium",
     "code": "MR-SUT/IOT", "years": "1995-2022"},
]


@click.group()
def cli():
    """IO Replicator — generic multi-agent Input-Output paper replication."""
    pass


@cli.command()
@click.option("--paper", type=click.Path(exists=True), default=None,
              help="Path to the paper PDF. Runs Paper Analyst first.")
@click.option("--spec", type=click.Path(exists=True), default=None,
              help="Path to an existing replication_spec.yaml. Skips Paper Analyst.")
@click.option("--config", type=click.Path(exists=True), default="config.yaml",
              help="Path to infrastructure config (default: config.yaml).")
@click.option("--start-stage", type=int, default=None,
              help="Resume from this stage (0-6). Requires --spec.")
@click.option("--only", type=str, default=None,
              help="Run only this stage node (e.g. 'reviewer').")
@click.option("--auto-approve", is_flag=True, default=False,
              help="Skip the human spec approval checkpoint (non-interactive mode).")
def run(paper, spec, config, start_stage, only, auto_approve):
    """Replicate an IO paper end-to-end or resume from a stage."""
    if not paper and not spec:
        console.print("[red]ERROR:[/red] Provide either --paper or --spec.")
        sys.exit(1)
    if start_stage is not None and not spec:
        console.print("[red]ERROR:[/red] --start-stage requires --spec (so we know the paper parameters).")
        sys.exit(1)

    # Load config
    config_path = Path(config)
    if not config_path.exists():
        console.print(f"[yellow]Warning:[/yellow] config.yaml not found at {config}. Using defaults.")
        cfg = {}
    else:
        with open(config_path) as f:
            cfg = yaml.safe_load(f)

    # Set up run directory
    run_id = time.strftime("%Y%m%d_%H%M%S")
    runs_dir = Path(cfg.get("pipeline", {}).get("runs_dir", "runs"))
    run_dir = runs_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    console.print(f"[bold]Run ID:[/bold] {run_id}")
    console.print(f"[bold]Run directory:[/bold] {run_dir}")

    # Import here to avoid slow startup when just running --help
    from agents.orchestrator import build_graph, build_graph_from_stage
    from agents.state import PipelineState

    # Build initial state
    initial_state: PipelineState = {
        "run_id": run_id,
        "run_dir": str(run_dir),
        "config": cfg,
        "paper_pdf_path": str(paper) if paper else None,
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
        "current_stage": start_stage or 0,
        "retry_count": 0,
        "error_log": [],
    }

    checkpoint_db = str(runs_dir / "checkpoints.sqlite")

    # If spec provided, load it and skip Paper Analyst
    if spec:
        with open(spec) as f:
            spec_data = yaml.safe_load(f)
        initial_state["replication_spec"] = spec_data
        initial_state["replication_spec_path"] = str(spec)

        if not auto_approve:
            _show_spec_summary(spec_data)
            approved = click.confirm("\nApprove this spec and proceed with the pipeline?", default=True)
            if not approved:
                console.print("[yellow]Pipeline cancelled. Edit the spec and re-run.[/yellow]")
                sys.exit(0)

        initial_state["spec_approved"] = True

    elif paper:
        # --paper flow: run Paper Analyst first, prompt for approval, then continue
        console.print(Panel("[bold]Stage 0: Paper Analyst — reading paper PDF...[/bold]"))
        from nodes.paper_analyst import paper_analyst_node
        try:
            updates = paper_analyst_node(initial_state)
            initial_state.update(updates)
        except Exception as e:
            _print_error(e)
            sys.exit(1)

        spec_data = initial_state.get("replication_spec", {})
        spec_path = initial_state.get("replication_spec_path", "")
        if not spec_data:
            console.print("[red]Paper Analyst did not produce a spec. Check the run directory.[/red]")
            sys.exit(1)

        console.print(f"\nSpec written to: [bold]{spec_path}[/bold]")

        if not auto_approve:
            _show_spec_summary(spec_data)
            console.print(f"\nYou can edit the spec at: [bold]{spec_path}[/bold]")
            approved = click.confirm("\nApprove this spec and proceed with the pipeline?", default=True)
            if not approved:
                console.print(f"[yellow]Pipeline paused. Edit the spec then re-run:[/yellow]")
                console.print(f"  io-replicate run --spec {spec_path} --start-stage 1")
                sys.exit(0)

        initial_state["spec_approved"] = True

    # Build and run the graph from the appropriate stage
    if only:
        app = build_graph_from_stage(start_stage or 0, only_stage=only, checkpoint_db=checkpoint_db)
    elif start_stage is not None:
        app = build_graph_from_stage(start_stage, checkpoint_db=checkpoint_db)
    elif paper:
        # Paper Analyst already ran — start from data_acquirer
        app = build_graph_from_stage(1, checkpoint_db=checkpoint_db)
    else:
        app = build_graph(checkpoint_db=checkpoint_db)

    # Check API keys before starting — fail fast with a clear message
    _check_api_keys(cfg, start_stage or (1 if spec else 0))

    console.print(Panel("[bold green]Starting pipeline...[/bold green]"))

    try:
        thread_id = {"configurable": {"thread_id": run_id}}
        final_state = app.invoke(initial_state, thread_id)
        _print_results(final_state)
    except Exception as e:
        _print_error(e)
        sys.exit(1)


@cli.command()
@click.option("--spec", required=True, type=click.Path(exists=True),
              help="Path to replication_spec.yaml to validate.")
def validate(spec):
    """Validate a replication_spec.yaml against the schema."""
    from agents.validators import validate_spec_file

    console.print(f"Validating [bold]{spec}[/bold]...")
    is_valid, errors = validate_spec_file(spec)

    if is_valid:
        console.print("[bold green]✓ Spec is valid.[/bold green]")
    else:
        console.print(f"[bold red]✗ Spec has {len(errors)} error(s):[/bold red]")
        for e in errors:
            console.print(f"  [red]•[/red] {e}")
        sys.exit(1)


@cli.command()
@click.option("--list", "list_sources", is_flag=True, default=True,
              help="List all supported data source connectors.")
def sources(list_sources):
    """List available data source connectors."""
    table = Table(title="Supported Data Sources", show_header=True)
    table.add_column("ID", style="cyan")
    table.add_column("Name")
    table.add_column("Provider", style="green")
    table.add_column("Code")
    table.add_column("Years")
    for s in SUPPORTED_SOURCES:
        table.add_row(s["id"], s["name"], s["provider"], s["code"], s["years"])
    console.print(table)
    console.print("\nTo use a source, set [bold]data_sources.io_table.type[/bold] in your spec.")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _show_spec_summary(spec: dict):
    paper = spec.get("paper", {})
    geo = spec.get("geography", {})
    cls = spec.get("classification", {})
    n_tables = len(spec.get("outputs", {}).get("tables", []))
    n_figures = len(spec.get("outputs", {}).get("figures", []))
    n_benchmarks = len(spec.get("benchmarks", {}).get("values", []))

    summary = (
        f"[bold]Paper:[/bold] {paper.get('title', 'N/A')}\n"
        f"[bold]Authors:[/bold] {', '.join(paper.get('authors', []))}\n"
        f"[bold]Reference year:[/bold] {paper.get('reference_year', 'N/A')}\n"
        f"[bold]Countries:[/bold] {len(geo.get('analysis_entities', []))} analysis + "
        f"{len(geo.get('external_entities', []))} external\n"
        f"[bold]Industries:[/bold] {cls.get('n_industries', 'N/A')} ({cls.get('system', 'N/A')})\n"
        f"[bold]Outputs:[/bold] {n_tables} tables, {n_figures} figures\n"
        f"[bold]Benchmarks:[/bold] {n_benchmarks} values to validate\n"
    )
    console.print(Panel(summary, title="Replication Spec Summary", border_style="blue"))


def _print_results(state: dict):
    review_passed = state.get("review_passed", False)
    warnings = state.get("review_warnings", [])
    report_path = state.get("review_report_path", "")

    if review_passed:
        console.print(Panel("[bold green]✓ Pipeline completed successfully.[/bold green]"))
    else:
        console.print(Panel("[bold yellow]⚠ Pipeline completed with issues.[/bold yellow]"))

    if warnings:
        console.print("[yellow]Warnings:[/yellow]")
        for w in warnings:
            console.print(f"  • {w}")

    if report_path and Path(report_path).exists():
        console.print(f"\nReview report: [bold]{report_path}[/bold]")

    outputs = state.get("output_paths", {})
    if outputs:
        console.print(f"\nOutputs written: {', '.join(outputs.keys())}")


def _check_api_keys(cfg: dict, start_stage: int):
    """Fail fast if required API keys are missing before the pipeline starts."""
    import os
    providers = cfg.get("llm", {}).get("providers", {})
    anthropic_env = providers.get("anthropic", {}).get("api_key_env", "ANTHROPIC_API_KEY")
    openai_env = providers.get("openai", {}).get("api_key_env", "OPENAI_API_KEY")

    has_anthropic = bool(os.environ.get(anthropic_env))
    has_openai = bool(os.environ.get(openai_env))

    if not has_anthropic and not has_openai:
        console.print(Panel(
            f"[red]No API keys found.[/red]\n\n"
            f"Set at least one of:\n"
            f"  export {anthropic_env}=sk-ant-...\n"
            f"  export {openai_env}=sk-...\n\n"
            f"Stages 0, 2, 6 require Anthropic. Stages 1, 5 use OpenAI (fall back to Anthropic if absent).",
            title="Missing API Keys", border_style="red"
        ))
        sys.exit(1)

    # Stages that require Anthropic (no OpenAI fallback available)
    anthropic_only_stages = {0, 2, 6}
    needs_anthropic = start_stage in anthropic_only_stages or start_stage <= 2
    if needs_anthropic and not has_anthropic:
        console.print(Panel(
            f"[red]{anthropic_env} is not set.[/red]\n\n"
            f"Stages 0, 2, 6 use Claude and require an Anthropic API key.\n"
            f"Set it with:  export {anthropic_env}=sk-ant-...",
            title="Missing Anthropic API Key", border_style="red"
        ))
        sys.exit(1)


def _print_error(e: Exception):
    msg = str(e)
    if "dimension mismatch" in msg.lower():
        # Actionable error message (plan §11.3)
        console.print(Panel(
            f"[red]Data preparation produced unexpected matrix dimensions.[/red]\n\n"
            f"Details: {msg}\n\n"
            f"[bold]Options:[/bold]\n"
            f"  1. Check that all countries in the spec exist in the raw data\n"
            f"  2. Verify the reference_year in your spec matches the downloaded data\n"
            f"  3. Re-run from stage 1: [bold]io-replicate run --spec <spec> --start-stage 1[/bold]",
            title="Data Preparation Error", border_style="red"
        ))
    else:
        console.print(Panel(f"[red]{msg}[/red]", title="Pipeline Error", border_style="red"))
