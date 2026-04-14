"""
Human Data Upload Gate — pauses the pipeline when data_acquirer cannot auto-download.

Prints clear instructions, waits for user confirmation, verifies the files exist,
then updates the data manifest with the actual file paths so data_guide can profile them.
"""
import logging
from pathlib import Path

import click
import yaml
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from agents.state import PipelineState

log = logging.getLogger("human_data_upload_gate")
_console = Console()


def human_data_upload_gate_node(state: PipelineState) -> dict:
    """
    LangGraph node: pause and ask the user to manually place data files.

    Triggered when state['manual_download_required'] is True.
    On confirmation: verifies files exist, updates manifest, clears the sentinel flag.
    On refusal: raises RuntimeError (aborts pipeline).
    """
    run_dir = Path(state["run_dir"])
    raw_dir = run_dir / "data" / "raw"
    instructions: dict = state.get("manual_download_instructions", {})
    auto_approve: bool = state.get("config", {}).get("pipeline", {}).get("auto_approve", False)

    reason = instructions.get("reason", "Data source has no public API.")
    files_needed = instructions.get("files_needed", [])
    satellite_needed = instructions.get("satellite_needed", [])
    all_files = files_needed + satellite_needed

    # ── Print instructions panel ──────────────────────────────────────────────
    table = Table(show_header=True, header_style="bold yellow")
    table.add_column("File", style="cyan")
    table.add_column("Download from")
    table.add_column("Place at")

    for entry in all_files:
        table.add_row(
            entry.get("filename", "?"),
            entry.get("source_url", "—"),
            str(raw_dir / entry.get("filename", "?")),
        )
        if entry.get("notes"):
            table.add_row(f"  [dim]{entry['notes']}[/dim]", "", "")

    _console.print(Panel(
        f"[bold yellow]Manual Data Download Required[/bold yellow]\n\n"
        f"[red]{reason}[/red]\n\n"
        f"The pipeline cannot auto-download this dataset. Please:\n"
        f"  1. Download the file(s) listed below\n"
        f"  2. Place them in [bold]{raw_dir}[/bold]\n"
        f"  3. Press [bold]Y[/bold] to continue",
        title="Stage 1 — Human Upload Gate",
        style="yellow",
    ))
    _console.print(table)

    if auto_approve:
        _console.print("[yellow]--auto-approve set — skipping prompt, assuming files are present[/yellow]")
    else:
        if not click.confirm("\nFiles placed? Continue pipeline?", default=True):
            raise RuntimeError(
                "Pipeline cancelled at data upload gate.\n"
                f"Re-run with --run-dir {run_dir} --start-stage 1.5 after placing the files."
            )

    # ── Verify files exist ────────────────────────────────────────────────────
    missing = []
    found = []
    for entry in all_files:
        expected_name = entry.get("filename", "")
        if not expected_name:
            continue
        # Support glob-like stems (e.g. "WIOT2019_Nov16_ROW.xlsx" may have year in name)
        stem = expected_name.replace("<year>", "*").replace("<YEAR>", "*")
        matches = list(raw_dir.glob(stem)) if "*" in stem else [raw_dir / expected_name]
        real_matches = [m for m in matches if m.exists()]
        if real_matches:
            found.append(real_matches[0])
        else:
            missing.append(expected_name)

    if missing:
        _console.print(f"[red]✗[/red] Files not found in {raw_dir}:")
        for f in missing:
            _console.print(f"    [red]- {f}[/red]")
        raise RuntimeError(
            f"Expected file(s) not found: {missing}\n"
            f"Place them in {raw_dir} and re-run with "
            f"--run-dir {run_dir} --start-stage 1.5"
        )

    _console.print(f"[green]✓[/green] All files found ({len(found)} file(s))")
    for f in found:
        _console.print(f"    [green]  {f.name}[/green]  ({f.stat().st_size:,} bytes)")

    # ── Rebuild manifest with real file info ──────────────────────────────────
    manifest_path = raw_dir / "data_manifest.yaml"
    existing_manifest: dict = state.get("data_manifest", {}) or {}

    # Update or add entries for each found file
    for entry, fpath in zip(all_files, found):
        # Figure out the manifest key from context (io_table vs satellite_account)
        key = "satellite_account" if fpath in [raw_dir / e.get("filename", "") for e in satellite_needed] else "io_table"
        # Don't override a good existing entry
        existing_entry = existing_manifest.get(key, {})
        if existing_entry.get("status") == "manual_download_required" or key not in existing_manifest:
            existing_manifest[key] = {
                "path": str(fpath.relative_to(run_dir)),
                "size_bytes": fpath.stat().st_size,
                "status": "manually_provided",
            }

    manifest_path.write_text(yaml.dump(existing_manifest, default_flow_style=False))
    log.info(f"Manifest updated with manually provided files: {manifest_path}")

    # Remove the sentinel so --start-stage 1.5 reruns don't re-trigger this gate
    sentinel_path = raw_dir / "MANUAL_DOWNLOAD_REQUIRED.yaml"
    if sentinel_path.exists():
        sentinel_path.unlink()

    _console.print("[green]✓[/green] Upload gate passed — proceeding to data profiling")

    return {
        "data_manifest": existing_manifest,
        "acquisition_complete": True,
        "manual_download_required": False,
        "current_stage": 1,
    }
