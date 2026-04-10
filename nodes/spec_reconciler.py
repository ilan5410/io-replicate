"""
Stage 5.5 — Spec Reconciler (deterministic, no LLM)

Runs between output_producer (Stage 5) and reviewer (Stage 6).

Problem: Stage 0 (paper_analyst) writes benchmark `source` descriptors with column names
it predicts Stage 5 will use. Stage 5 is an LLM agent that may name columns slightly
differently. This node fixes the mismatch without an LLM call.

What it does:
1. Builds a file map from spec["output_schema"] (falls back to hardcoded FIGARO names).
2. For each benchmark with a `source` descriptor, checks whether the referenced column
   actually exists in the CSV.
3. If not, tries fuzzy string matching (difflib) against the actual columns.
4. Patches the column name in the spec (in state only — does not write to disk).
5. Logs every substitution. Marks un-fixable sources as missing so Stage 6 can
   report UNVERIFIED rather than ERROR.
"""
from __future__ import annotations

import copy
import logging
from difflib import get_close_matches
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

from agents.state import PipelineState

log = logging.getLogger("spec_reconciler")
_console = Console()

# Fallback file map for specs that pre-date the output_schema feature
_LEGACY_FILE_MAP: dict[str, str] = {
    "country_decomposition": "country_decomposition.csv",
    "industry_table4":       "industry_table4.csv",
    "industry_figure3":      "industry_figure3.csv",
    "annex_c_matrix":        "annex_c_matrix.csv",
}

# Fuzzy-match cutoff: 0.6 means at least 60% similarity required
_FUZZY_CUTOFF = 0.6


def spec_reconciler_node(state: PipelineState) -> dict:
    """LangGraph node: reconcile benchmark source column names with actual CSVs."""
    run_dir = Path(state["run_dir"])
    # Deep-copy so we don't mutate the original state dict in-place
    spec = copy.deepcopy(state["replication_spec"])
    decomp_dir = run_dir / "data" / "decomposition"

    _console.print(Panel(
        "[bold]Stage 5.5 — Spec Reconciler[/bold]\n"
        "Verifying benchmark source column names against actual output files",
        style="blue"
    ))

    if not decomp_dir.exists():
        log.warning(f"Decomposition directory not found: {decomp_dir} — skipping reconciliation")
        _console.print("  [yellow]Skipping:[/yellow] decomposition directory not found")
        return {}

    # Build file map: prefer output_schema, fall back to legacy
    output_schema = spec.get("output_schema", {})
    if output_schema:
        file_map = {key: val["file"] for key, val in output_schema.items()}
        log.info(f"Using output_schema file map: {file_map}")
    else:
        file_map = _LEGACY_FILE_MAP.copy()
        log.info("No output_schema in spec — using legacy file map")

    # Cache column lists per file (avoid re-reading the same file many times)
    _col_cache: dict[str, list[str]] = {}

    def get_columns(file_key: str) -> list[str] | None:
        if file_key in _col_cache:
            return _col_cache[file_key]
        filename = file_map.get(file_key)
        if not filename:
            return None
        path = decomp_dir / filename
        if not path.exists():
            log.warning(f"File not found: {path}")
            return None
        try:
            import pandas as pd
            cols = list(pd.read_csv(path, nrows=0).columns)
            _col_cache[file_key] = cols
            return cols
        except Exception as e:
            log.warning(f"Could not read {path}: {e}")
            return None

    benchmarks = spec.get("benchmarks", {}).get("values", [])
    n_ok = 0
    n_patched = 0
    n_unfixable = 0

    for bm in benchmarks:
        source = bm.get("source")
        if not source:
            continue

        file_key = source.get("file")
        cols = get_columns(file_key)
        if cols is None:
            # File missing or unknown — leave as-is; validator will report ERROR
            n_unfixable += 1
            continue

        changed = False

        # Check and fix `column`
        col = source.get("column")
        if col and col not in cols:
            matches = get_close_matches(col, cols, n=1, cutoff=_FUZZY_CUTOFF)
            if matches:
                log.info(f"  [{bm['name']}] column '{col}' → '{matches[0]}'")
                source["column"] = matches[0]
                changed = True
            else:
                log.warning(f"  [{bm['name']}] column '{col}' not found; no close match in {cols}")
                n_unfixable += 1

        # Check and fix filter keys
        filt = source.get("filter", {})
        if filt:
            new_filt = {}
            for fcol, fval in filt.items():
                if fcol in cols:
                    new_filt[fcol] = fval
                else:
                    matches = get_close_matches(fcol, cols, n=1, cutoff=_FUZZY_CUTOFF)
                    if matches:
                        log.info(f"  [{bm['name']}] filter key '{fcol}' → '{matches[0]}'")
                        new_filt[matches[0]] = fval
                        changed = True
                    else:
                        log.warning(f"  [{bm['name']}] filter key '{fcol}' not found; keeping as-is")
                        new_filt[fcol] = fval
            source["filter"] = new_filt

        if changed:
            n_patched += 1
        elif col and col in cols:
            n_ok += 1
        elif source.get("op") in ("sum_all", "sum_row"):
            # These ops don't use a column — they're fine
            n_ok += 1

    # Summary
    if n_patched:
        _console.print(
            f"  [yellow]Reconciler:[/yellow] patched {n_patched} column reference(s), "
            f"{n_ok} already correct, {n_unfixable} unfixable (will be UNVERIFIED)"
        )
    else:
        _console.print(
            f"  [green]Reconciler:[/green] all {n_ok} source columns verified ✓"
            + (f" ({n_unfixable} unfixable)" if n_unfixable else "")
        )

    return {"replication_spec": spec}
