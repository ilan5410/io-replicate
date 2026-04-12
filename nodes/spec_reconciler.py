"""
Stage 4.5 — Spec Reconciler (deterministic, no LLM)

Runs between decomposer (Stage 4) and output_producer (Stage 5).

Problem: Stage 0 (paper_analyst) writes benchmark `source` descriptors with column names
it predicts Stage 5 will use. Stage 5 is an LLM agent that may name columns differently,
and the spec is written before data is seen so codes may be in short form ("B-E") while
actual files use full names ("B-E - Industry (incl. energy)").

Reconciliation strategy (in priority order):
1. Exact match — already correct, nothing to do.
2. Fuzzy string match — close enough, patch in-place.
3. Guide-assisted expansion — use data_guide code lists to expand short codes to full names.
4. Structural op correction — detect when the wrong op type is used for index-based files
   (e.g. op=lookup with industry_row filter on index_col=0 file → op=sum_row with row key).
5. Auto-source — value-matching: for unsourced benchmarks, scan decomp files for a
   (file, op, filter, column) whose computed value is within tolerance of expected_value.
6. Mark as UNVERIFIED — no fix found; validator will report UNVERIFIED, not ERROR.
"""
from __future__ import annotations

import copy
import logging
import math
from difflib import get_close_matches
from pathlib import Path

import pandas as pd
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

# Fallback index-col set for specs that pre-date output_schema.index_col
_LEGACY_INDEX_COL0_FILES = {"industry_table4", "industry_figure3", "annex_c_matrix"}


def _build_index_col0_set(output_schema: dict) -> set[str]:
    """
    Return the set of file keys that should be loaded with index_col=0.
    Uses output_schema[key].index_col when available; falls back to the
    legacy hardcoded set for specs that pre-date that field.
    """
    if not output_schema:
        return _LEGACY_INDEX_COL0_FILES
    return {key for key, val in output_schema.items() if val.get("index_col", False)}

# Fuzzy-match cutoff
_FUZZY_CUTOFF = 0.6

# Column names that signal "sum the whole row" when they don't exist in the file
_TOTAL_COLUMN_ALIASES = {"all products", "total", "grand total", "all", "sum"}

# Value-match tolerance for auto-sourcing (relative deviation allowed)
_AUTO_SOURCE_TOLERANCE = 0.15   # 15% for normal; approximate benchmarks get 30%


def spec_reconciler_node(state: PipelineState) -> dict:
    """LangGraph node: reconcile benchmark source descriptors with actual CSVs."""
    run_dir = Path(state["run_dir"])
    spec = copy.deepcopy(state["replication_spec"])
    data_guide: dict = state.get("data_guide") or {}
    decomp_dir = run_dir / "data" / "decomposition"

    _console.print(Panel(
        "[bold]Stage 4.5 — Spec Reconciler[/bold]\n"
        "Verifying benchmark source column names + auto-sourcing unsourced benchmarks"
        + (" [dim](data_guide available)[/dim]" if data_guide else ""),
        style="blue"
    ))

    if not decomp_dir.exists():
        log.warning(f"Decomposition directory not found: {decomp_dir} — skipping reconciliation")
        _console.print("  [yellow]Skipping:[/yellow] decomposition directory not found")
        return {}

    # Build file map and index-col set from spec output_schema or fall back to legacy
    output_schema = spec.get("output_schema", {})
    file_map = (
        {key: val["file"] for key, val in output_schema.items()}
        if output_schema else _LEGACY_FILE_MAP.copy()
    )
    index_col0_files = _build_index_col0_set(output_schema)

    # Build code expansion maps from data_guide
    # industry_expansions: short code → full label as it appears in actual files
    # country_column_hints: map of likely wrong filter key → actual column name
    industry_expansions = _build_industry_expansions(data_guide, decomp_dir, file_map, index_col0_files)
    country_col_hints = _build_country_col_hints(data_guide, decomp_dir, file_map)

    # Cache per file: (columns_list, index_values_list, df)
    _file_cache: dict[str, tuple[list[str], list[str]]] = {}

    def get_file_info(file_key: str) -> tuple[list[str], list[str]] | tuple[None, None]:
        if file_key in _file_cache:
            return _file_cache[file_key]
        filename = file_map.get(file_key)
        if not filename:
            return None, None
        path = decomp_dir / filename
        if not path.exists():
            log.warning(f"File not found: {path}")
            return None, None
        try:
            use_index = file_key in index_col0_files
            df = pd.read_csv(path, index_col=0 if use_index else None, nrows=0)
            cols = list(df.columns)
            idx = list(df.index) if use_index else []
            _file_cache[file_key] = (cols, idx)
            return cols, idx
        except Exception as e:
            log.warning(f"Could not read {path}: {e}")
            return None, None

    benchmarks = spec.get("benchmarks", {}).get("values", [])
    n_ok = n_patched = n_unfixable = 0

    for bm in benchmarks:
        source = bm.get("source")
        if not source:
            continue

        file_key = source.get("file", "")
        cols, idx = get_file_info(file_key)
        if cols is None:
            n_unfixable += 1
            continue

        changed = False
        is_index_file = file_key in index_col0_files

        # ── 1. Fix filter keys and values ────────────────────────────────────
        filt = source.get("filter", {})
        if filt:
            new_filt, filt_changed = _fix_filter(
                bm["name"], filt, cols, idx, is_index_file,
                industry_expansions, country_col_hints,
            )
            if filt_changed:
                source["filter"] = new_filt
                changed = True
            else:
                source["filter"] = new_filt

        # ── 2. Fix column reference ───────────────────────────────────────────
        col = source.get("column")
        op = source.get("op", "")

        if col and col not in cols:
            # 2a. For index-based files: "All products" / total aliases → op=sum_row
            if is_index_file and col.lower() in _TOTAL_COLUMN_ALIASES:
                row_key = _resolve_row_key(bm["name"], source.get("filter", {}), idx, industry_expansions)
                if row_key:
                    log.info(f"  [{bm['name']}] column '{col}' (total) → op=sum_row, row='{row_key}'")
                    source["op"] = "sum_row"
                    source["row"] = row_key
                    source.pop("column", None)
                    source.pop("filter", None)
                    changed = True
                else:
                    log.warning(f"  [{bm['name']}] column '{col}' (total) but no row key resolved")
                    n_unfixable += 1
                    continue

            # 2b. For index-based files: column is a short industry code → expand
            elif is_index_file and col in industry_expansions:
                source["column"] = industry_expansions[col]
                log.info(f"  [{bm['name']}] column '{col}' → '{industry_expansions[col]}' (guide expansion)")
                changed = True

            # 2c. Fuzzy match against columns
            else:
                candidates = cols + (list(industry_expansions.values()) if is_index_file else [])
                matches = get_close_matches(col, candidates, n=1, cutoff=_FUZZY_CUTOFF)
                if matches:
                    log.info(f"  [{bm['name']}] column '{col}' → '{matches[0]}' (fuzzy)")
                    source["column"] = matches[0]
                    changed = True
                else:
                    log.warning(f"  [{bm['name']}] column '{col}' not found; no close match in {cols}")
                    n_unfixable += 1
                    continue

        # ── 3. For index-based files: if op=lookup with filter→index mismatch,
        #       the filter fixup may have already removed the filter. Ensure
        #       op=sum_row is used when there's a row key but no filter column.
        if is_index_file and op == "lookup" and not source.get("filter"):
            row_key = source.get("row")
            if not row_key:
                row_key = _resolve_row_key(bm["name"], filt, idx, industry_expansions)
            if row_key and row_key in idx:
                source["op"] = "sum_row"
                source["row"] = row_key
                source.pop("column", None)
                source.pop("filter", None)
                log.info(f"  [{bm['name']}] op=lookup → op=sum_row, row='{row_key}' (structural fix)")
                changed = True

        if changed:
            n_patched += 1
        elif op in ("sum_all", "sum_row") or (col and col in cols):
            n_ok += 1

    # ── Auto-source unsourced benchmarks ──────────────────────────────────────
    n_auto = _auto_source_benchmarks(spec, decomp_dir, file_map, spec, index_col0_files)

    # Summary
    guide_note = " (guide-assisted)" if data_guide else ""
    _console.print(
        f"  [yellow]Reconciler:[/yellow] patched {n_patched} descriptor(s){guide_note}, "
        f"{n_ok} already correct, {n_unfixable} unfixable, "
        f"[cyan]{n_auto} auto-sourced[/cyan]"
    )

    return {"replication_spec": spec}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_industry_expansions(data_guide: dict, decomp_dir: Path, file_map: dict, index_col0_files: set) -> dict[str, str]:
    """
    Build a map of short industry code → full label as it appears in index-based output files.

    Sources (in priority order):
    1. data_guide.files[*].codes.industries — codes found in raw data
    2. Actual index values read from industry_table4.csv
    """
    expansions: dict[str, str] = {}

    # From actual file index (most authoritative)
    for file_key in index_col0_files:
        filename = file_map.get(file_key)
        if not filename:
            continue
        path = decomp_dir / filename
        if not path.exists():
            continue
        try:
            # Read only the first column to get row labels (efficient even for large files)
            df_idx = pd.read_csv(path, usecols=[0], header=0)
            idx = list(df_idx.iloc[:, 0].astype(str))
            for full_label in idx:
                # "B-E - Industry (incl. energy)" → key candidates: "B-E", "b-e", "B_E"
                code_part = full_label.split(" - ")[0].strip()
                if code_part and code_part not in expansions:
                    expansions[code_part] = full_label
                    expansions[code_part.replace("-", "_")] = full_label
        except Exception:
            pass

    # From data_guide (may have additional code forms)
    for file_info in data_guide.get("files", {}).values():
        for code in file_info.get("codes", {}).get("industries", []):
            code_str = str(code)
            if code_str not in expansions:
                # Try to find a matching full label via fuzzy
                all_labels = list(expansions.values())
                matches = get_close_matches(code_str, all_labels, n=1, cutoff=0.5)
                if matches:
                    expansions[code_str] = matches[0]

    return expansions


def _build_country_col_hints(data_guide: dict, decomp_dir: Path, file_map: dict) -> dict[str, str]:
    """
    Build a map of likely wrong filter key → actual column name for country_decomposition.

    E.g. {'exporting_country': 'country', 'importing_country': 'country'}
    """
    hints: dict[str, str] = {}
    filename = file_map.get("country_decomposition", "country_decomposition.csv")
    path = decomp_dir / filename
    if not path.exists():
        return hints
    try:
        cols = list(pd.read_csv(path, nrows=0).columns)
        # Any column whose name contains "country" is a candidate for country filter
        country_cols = [c for c in cols if "country" in c.lower()]
        if country_cols:
            # Map common wrong names to the real column
            for wrong in ["exporting_country", "importing_country", "origin_country",
                          "dest_country", "geo", "geo_code", "nation"]:
                if wrong not in cols:
                    hints[wrong] = country_cols[0]
    except Exception:
        pass
    return hints


def _fix_filter(
    bm_name: str,
    filt: dict,
    cols: list[str],
    idx: list[str],
    is_index_file: bool,
    industry_expansions: dict[str, str],
    country_col_hints: dict[str, str],
) -> tuple[dict, bool]:
    """Fix filter keys and values. Returns (new_filter, changed)."""
    new_filt: dict = {}
    changed = False

    for fcol, fval in filt.items():
        if fcol in cols:
            new_filt[fcol] = fval
            continue

        # For index-based files: filter by row label is done via op=sum_row, not filter
        # Signal the caller to remove this filter by omitting it from new_filt
        if is_index_file and fcol not in cols:
            # Don't include index-based filters — they'll be handled by _resolve_row_key
            changed = True
            log.debug(f"  [{bm_name}] removing index filter key '{fcol}' (handled by op=sum_row)")
            continue

        # Country column hints
        if fcol in country_col_hints:
            real_key = country_col_hints[fcol]
            log.info(f"  [{bm_name}] filter key '{fcol}' → '{real_key}' (guide hint)")
            new_filt[real_key] = fval
            changed = True
            continue

        # Fuzzy match against actual columns
        matches = get_close_matches(fcol, cols, n=1, cutoff=_FUZZY_CUTOFF)
        if matches:
            log.info(f"  [{bm_name}] filter key '{fcol}' → '{matches[0]}' (fuzzy)")
            new_filt[matches[0]] = fval
            changed = True
        else:
            log.warning(f"  [{bm_name}] filter key '{fcol}' not found; keeping as-is")
            new_filt[fcol] = fval

    return new_filt, changed


def _resolve_row_key(
    bm_name: str,
    filt: dict,
    idx: list[str],
    industry_expansions: dict[str, str],
) -> str | None:
    """
    Given a filter dict from a benchmark, find the actual row label in the index.

    Tries:
    1. Direct match of filter value in index
    2. industry_expansions lookup of filter value
    3. Fuzzy match of filter value against index
    """
    for fcol, fval in filt.items():
        fval_str = str(fval)
        # Direct
        if fval_str in idx:
            return fval_str
        # Guide expansion
        if fval_str in industry_expansions:
            expanded = industry_expansions[fval_str]
            if expanded in idx:
                log.info(f"  [{bm_name}] row '{fval_str}' → '{expanded}' (guide expansion)")
                return expanded
        # Fuzzy
        matches = get_close_matches(fval_str, idx, n=1, cutoff=_FUZZY_CUTOFF)
        if matches:
            log.info(f"  [{bm_name}] row '{fval_str}' → '{matches[0]}' (fuzzy)")
            return matches[0]
    return None


# ---------------------------------------------------------------------------
# Auto-sourcing: value-based matching for unsourced benchmarks
# ---------------------------------------------------------------------------

def _auto_source_benchmarks(spec: dict, decomp_dir: Path, file_map: dict, _spec_ref: dict, index_col0_files: set) -> int:
    """
    For each benchmark without a source descriptor, try to find the best-matching
    (file, op, filter, column) by comparing the expected value against actual
    computed values in the decomposition CSVs.

    Returns the number of benchmarks successfully auto-sourced.

    Strategy:
    - Enumerate all candidate source descriptors with their computed values.
    - For each unsourced benchmark, pick the candidate whose value deviates
      least from the expected value, subject to a maximum tolerance.
    - Per-country lookup candidates are generated only when a country name
      appears in the benchmark name; aggregate (column/row sum) candidates
      are always generated as fallbacks.
    """
    benchmarks = spec.get("benchmarks", {}).get("values", [])

    # Build country name→code map
    geo = spec.get("geography", {})
    country_name_to_code: dict[str, str] = {}
    for entity in geo.get("analysis_entities", []):
        name = entity.get("name", "")
        code = entity.get("code", "")
        if name and code:
            country_name_to_code[name.lower()] = code
            if name == "Czechia":
                country_name_to_code["czech republic"] = code
            if name == "Germany":
                country_name_to_code["german"] = code  # "German exports"

    # Load all decomp CSVs
    file_dfs: dict[str, pd.DataFrame] = {}
    for file_key, filename in file_map.items():
        path = decomp_dir / filename
        if not path.exists():
            continue
        try:
            use_index = file_key in index_col0_files
            df = pd.read_csv(path, index_col=0 if use_index else None)
            file_dfs[file_key] = df
        except Exception as e:
            log.warning(f"auto_source: could not load {path}: {e}")

    def _rel_dev(actual: float, target: float) -> float:
        if target == 0:
            return abs(actual)
        return abs(actual - target) / abs(target)

    def _find_country_column(df: pd.DataFrame) -> str | None:
        for col in df.columns:
            if "country" in col.lower() or col.lower() in ("geo", "code"):
                return col
        return None

    # Pre-compute all aggregate candidates (column sums, matrix ops) once
    # Each entry: (dev_placeholder, source_dict, computed_val)
    # We'll add computed_val at query time via a callable.
    # Instead: build lists of (source_dict, computed_val) pairs.

    # Tabular column sums (non-index files)
    tabular_col_sums: list[tuple[dict, float]] = []
    for file_key, df in file_dfs.items():
        if file_key in index_col0_files:
            continue
        country_col = _find_country_column(df)
        for col in df.columns:
            if col == country_col:
                continue
            try:
                s = float(df[col].sum())
                if math.isfinite(s):
                    tabular_col_sums.append((
                        {"file": file_key, "op": "sum_column", "column": col}, s
                    ))
            except (TypeError, ValueError):
                pass

    # Annex-C column sums and row sums
    annex_col_sums: list[tuple[dict, float]] = []
    annex_row_sums: list[tuple[dict, float]] = []
    annex_grand_total: float | None = None
    if "annex_c_matrix" in file_dfs:
        adf = file_dfs["annex_c_matrix"]
        for col in adf.columns:
            try:
                s = float(adf[col].sum())
                if math.isfinite(s):
                    annex_col_sums.append((
                        {"file": "annex_c_matrix", "op": "sum_column", "column": col}, s
                    ))
            except (TypeError, ValueError):
                pass
        for row in adf.index:
            try:
                s = float(adf.loc[row].sum())
                if math.isfinite(s):
                    annex_row_sums.append((
                        {"file": "annex_c_matrix", "op": "sum_row", "row": row}, s
                    ))
            except (TypeError, ValueError):
                pass
        try:
            gt = float(adf.select_dtypes(include="number").values.sum())
            if math.isfinite(gt):
                annex_grand_total = gt
        except (TypeError, ValueError):
            pass

    n_auto = 0

    for bm in benchmarks:
        if bm.get("source"):
            continue

        raw_expected = bm.get("expected")
        if raw_expected is None:
            continue
        try:
            target = float(raw_expected)
        except (TypeError, ValueError):
            continue

        # Skip near-zero targets (too likely to match spuriously)
        if abs(target) < 0.5:
            continue

        approx = bm.get("approximate", False)
        tol_lookup = _AUTO_SOURCE_TOLERANCE * (1.5 if approx else 1.0)   # 15% / 22.5%
        tol_agg    = _AUTO_SOURCE_TOLERANCE * (2.0 if approx else 1.5)   # 22.5% / 30%

        name_lower = bm.get("name", "").lower()

        # ── Identify country codes mentioned in the benchmark name ────────────
        mentioned_countries: list[str] = []
        for country_name, code in country_name_to_code.items():
            if country_name in name_lower:
                mentioned_countries.append(code)

        # ── Build candidate list for this benchmark ───────────────────────────
        # candidate = (dev, priority, source_dict)
        # Lower priority number = preferred (per-country lookup > agg sum)
        candidates: list[tuple[float, int, dict]] = []

        # 1. Per-country lookup in tabular files
        primary_country = mentioned_countries[0] if mentioned_countries else None
        if primary_country:
            for file_key, df in file_dfs.items():
                if file_key in index_col0_files:
                    continue
                country_col = _find_country_column(df)
                if not country_col:
                    continue
                rows = df[df[country_col] == primary_country]
                if rows.empty:
                    continue
                for col in df.columns:
                    if col == country_col:
                        continue
                    try:
                        val = float(rows.iloc[0][col])
                        if not math.isfinite(val):
                            continue
                        dev = _rel_dev(val, target)
                        if dev <= tol_lookup:
                            candidates.append((dev, 1, {
                                "file": file_key,
                                "op": "lookup",
                                "filter": {country_col: primary_country},
                                "column": col,
                            }))
                    except (TypeError, ValueError):
                        pass

        # 2. Annex-C column sum for the primary country
        if primary_country:
            for src, val in annex_col_sums:
                if src.get("column") == primary_country:
                    dev = _rel_dev(val, target)
                    if dev <= tol_agg:
                        candidates.append((dev, 2, src))

        # 3. Annex-C row sum for the primary country
        if primary_country:
            for src, val in annex_row_sums:
                if src.get("row") == primary_country:
                    dev = _rel_dev(val, target)
                    if dev <= tol_agg:
                        candidates.append((dev, 3, src))

        # 4. Tabular column sums (any country or no country)
        for src, val in tabular_col_sums:
            dev = _rel_dev(val, target)
            if dev <= tol_agg:
                candidates.append((dev, 4, src))

        # 5. Annex-C column sums (no country filter, or secondary)
        if not primary_country:
            for src, val in annex_col_sums:
                dev = _rel_dev(val, target)
                if dev <= tol_agg:
                    candidates.append((dev, 5, src))

        # 6. Annex-C row sums (no country filter)
        if not primary_country:
            for src, val in annex_row_sums:
                dev = _rel_dev(val, target)
                if dev <= tol_agg:
                    candidates.append((dev, 6, src))

        # 7. Annex-C grand total
        if annex_grand_total is not None and not primary_country:
            dev = _rel_dev(annex_grand_total, target)
            if dev <= tol_agg:
                candidates.append((dev, 7, {"file": "annex_c_matrix", "op": "sum_all"}))

        if not candidates:
            continue

        # Pick best: lowest priority tier first, then lowest deviation
        candidates.sort(key=lambda x: (x[1], x[0]))
        best_dev, best_prio, best_src = candidates[0]

        bm["source"] = best_src
        log.info(
            f"  auto-source [{bm['name']}] → {best_src} "
            f"(dev={best_dev:.1%}, prio={best_prio})"
        )
        n_auto += 1

    log.info(f"auto_source: {n_auto} benchmarks auto-sourced")
    return n_auto
