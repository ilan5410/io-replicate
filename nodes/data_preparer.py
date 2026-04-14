"""
Stage 2: Data Preparer — deterministic IO parser dispatcher.

Reads the raw files downloaded by Stage 1, selects the correct parser skill
based on spec.data_sources.io_table.type, and saves the standardized matrices
needed by Stage 3: Z_EU.csv, e_nonEU.csv, x_EU.csv, Em_EU.csv + metadata.json
"""
import json
import logging
from pathlib import Path

import pandas as pd
from rich.console import Console
from rich.panel import Panel

from agents.state import PipelineState
from agents.validators import validate_prepared_data
from skills.io_parsers import load_parser

log = logging.getLogger("data_preparer")
_console = Console()

# Filename patterns that unambiguously identify a parser type regardless of spec
_FILE_SIGNATURES: list[tuple[str, str]] = [
    ("naio_10_fcp_ip1",   "figaro_iciot"),
    ("figaro",            "figaro_iciot"),
    ("wiot",              "wiod_mrio"),
    ("wiod",              "wiod_mrio"),
    ("icio",              "oecd_icio"),
    ("mriot",             "exiobase"),
    ("exiobase",          "exiobase"),
]


def _autodetect_parser(raw_dir: Path, spec_type: str) -> str:
    """Return the parser type inferred from raw file names, falling back to spec_type."""
    if not raw_dir.exists():
        return spec_type
    filenames = [f.name.lower() for f in raw_dir.iterdir()]
    for sig, detected in _FILE_SIGNATURES:
        if any(sig in fn for fn in filenames):
            if detected != spec_type:
                log.warning(
                    f"Auto-detected parser '{detected}' from raw files "
                    f"(spec said '{spec_type}') — using '{detected}'"
                )
            return detected
    return spec_type


def data_preparer_node(state: PipelineState) -> dict:
    """LangGraph node: dispatch to the correct IO parser skill, then save + validate."""
    run_dir = Path(state["run_dir"])
    spec = state["replication_spec"]
    retry_count = state.get("retry_count", 0)

    parser_type = spec["data_sources"]["io_table"]["type"]

    raw_dir = run_dir / "data" / "raw"

    # Auto-detect parser from raw files when spec may be wrong
    parser_type = _autodetect_parser(raw_dir, parser_type)

    _console.print(Panel(
        f"[bold]Stage 2 — Data Preparer[/bold]  (attempt {retry_count + 1})\n"
        f"Parser: {parser_type}\n"
        "[dim]Input: data/raw/   →   Output: data/prepared/[/dim]",
        style="blue"
    ))

    prepared_dir = run_dir / "data" / "prepared"
    prepared_dir.mkdir(parents=True, exist_ok=True)

    # ── Parse ─────────────────────────────────────────────────────────────────
    load = load_parser(parser_type)
    matrices = load(raw_dir, spec)

    _console.print(
        f"  Z sum={matrices.Z.sum():,.0f}  "
        f"e sum={matrices.e.sum():,.0f}  "
        f"x sum={matrices.x.sum():,.0f}  "
        f"Em sum={matrices.Em.sum():,.1f}"
    )

    # ── Save ──────────────────────────────────────────────────────────────────
    _console.print("  Saving matrices...")

    z_path = prepared_dir / "Z_EU.csv"
    pd.DataFrame(matrices.Z, index=matrices.labels, columns=matrices.labels).to_csv(z_path)

    e_path = prepared_dir / "e_nonEU.csv"
    pd.DataFrame({"e_nonEU_MIO_EUR": matrices.e}).to_csv(e_path, index=False)

    x_path = prepared_dir / "x_EU.csv"
    pd.DataFrame({"x_EU_MIO_EUR": matrices.x}).to_csv(x_path, index=False)

    em_path = prepared_dir / "Em_EU.csv"
    pd.DataFrame({"em_EU_THS_PER": matrices.Em}).to_csv(em_path, index=False)

    meta = {
        "eu_countries": matrices.eu_codes,
        "cpa_codes": matrices.cpa_codes,
        "n_countries": len(matrices.eu_codes),
        "n_industries": len(matrices.cpa_codes),
        "n_total": len(matrices.labels),
        "reference_year": matrices.year,
        "source": matrices.source,
        "unit_Z": "MIO_EUR",
        "unit_x": "MIO_EUR",
        "unit_e": "MIO_EUR",
        "unit_Em": "THS_PER",
    }
    meta_path = prepared_dir / "metadata.json"
    meta_path.write_text(json.dumps(meta, indent=2))

    prepared_data_paths = {
        "metadata": str(meta_path),
        "Z_EU":     str(z_path),
        "e_nonEU":  str(e_path),
        "x_EU":     str(x_path),
        "Em_EU":    str(em_path),
    }

    # ── Validate ──────────────────────────────────────────────────────────────
    _console.print("  Running deterministic validation...")
    is_valid, errors = validate_prepared_data(prepared_data_paths, spec)
    if is_valid:
        _console.print("[green]✓[/green] Stage 2 complete — matrices validated")
    else:
        _console.print(f"[red]✗[/red] Validation FAILED: {errors[:3]}")
        log.warning(f"Preparation validation FAILED: {errors}")

    return {
        "prepared_data_paths": prepared_data_paths,
        "preparation_valid":   is_valid,
        "preparation_errors":  errors,
        "retry_count":         retry_count + 1,
        "current_stage":       2,
    }
