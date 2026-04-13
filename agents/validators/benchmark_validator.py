"""
Deterministic benchmark validator.

Reads `benchmarks.values` from the spec. Each entry may carry an optional `source`
descriptor that tells the validator how to extract the actual value from the
decomposition outputs — without any LLM involvement.

Generic source descriptor (all fields except `file` and `op` are op-specific):

    source:
      file: country_decomposition | industry_table4 | industry_figure3
      op:   sum_column | lookup
      column: <column name in the CSV>
      filter: {<col>: <val>, ...}   # for op=lookup

If `source` is absent from a benchmark entry the result status is "UNVERIFIED" and
the entry is passed to the LLM for narrative interpretation.

This design keeps the validator fully generic: any IO paper can define benchmarks with
or without sources, and the same code handles both.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

log = logging.getLogger("benchmark_validator")

# Fallback file map for specs that pre-date the output_schema feature
_LEGACY_FILE_MAP: dict[str, str] = {
    "country_decomposition": "country_decomposition.csv",
    "industry_table4":       "industry_table4.csv",
    "industry_figure3":      "industry_figure3.csv",
    "annex_c_matrix":        "annex_c_matrix.csv",
}

# Fallback index-col set (same as spec_reconciler) for specs without output_schema
_LEGACY_INDEX_COL0: set[str] = {"industry_table4", "industry_figure3", "annex_c_matrix"}


def _build_file_map(spec: dict) -> dict[str, str]:
    """Build file map from spec output_schema; falls back to legacy hardcoded map."""
    schema = spec.get("output_schema", {})
    if schema:
        return {key: val["file"] for key, val in schema.items()}
    return _LEGACY_FILE_MAP.copy()


def _build_index_col0_set(spec: dict) -> set[str]:
    """Return the set of file keys that must be loaded with index_col=0."""
    schema = spec.get("output_schema", {})
    if schema:
        return {key for key, val in schema.items() if val.get("index_col", False)}
    return _LEGACY_INDEX_COL0


def run_benchmark_checks(spec: dict, decomp_dir: Path) -> list[dict]:  # noqa: C901
    """
    Run all benchmarks defined in spec["benchmarks"]["values"].

    Returns a list of result dicts:
        {name, expected, unit, actual, deviation_pct, status, note}
    where status ∈ {PASS, WARN, FAIL, UNVERIFIED, ERROR}.
    """
    benchmarks = spec.get("benchmarks", {})
    tolerances = benchmarks.get("tolerances", {})
    warn_pct = float(tolerances.get("warning_pct", 10))
    error_pct = float(tolerances.get("error_pct", 25))

    file_map = _build_file_map(spec)
    index_col0 = _build_index_col0_set(spec)

    # Cache loaded DataFrames — avoids re-reading the same CSV for every benchmark
    _df_cache: dict[str, pd.DataFrame] = {}

    def _get_df(file_key: str) -> pd.DataFrame:
        if file_key not in _df_cache:
            filename = file_map.get(file_key)
            if not filename:
                raise ValueError(f"Unknown source file '{file_key}'. Valid: {list(file_map)}")
            path = decomp_dir / filename
            if not path.exists():
                raise FileNotFoundError(f"Decomposition file not found: {path}")
            use_index = file_key in index_col0
            _df_cache[file_key] = pd.read_csv(path, index_col=0 if use_index else None)
        return _df_cache[file_key]

    results: list[dict] = []
    for bm in benchmarks.get("values", []):
        name = bm.get("name", "<unnamed>")
        expected = bm.get("expected")
        unit = bm.get("unit", "")
        source = bm.get("source")

        if expected is None:
            results.append(_result(name, expected, unit, None, None, "UNVERIFIED",
                                   "No expected value defined"))
            continue

        if not source:
            results.append(_result(name, expected, unit, None, None, "UNVERIFIED",
                                   "No source descriptor — requires LLM verification"))
            continue

        try:
            actual = _resolve(source, _get_df)
        except Exception as e:
            log.warning(f"Benchmark '{name}' resolution failed: {e}")
            results.append(_result(name, expected, unit, None, None, "ERROR", str(e)))
            continue

        deviation_pct = (abs(actual - expected) / abs(expected) * 100) if expected != 0 else 0.0

        if deviation_pct >= error_pct:
            status = "FAIL"
        elif deviation_pct >= warn_pct:
            status = "WARN"
        else:
            status = "PASS"

        note = "approximate" if bm.get("approximate") else ""
        results.append(_result(name, expected, unit, round(actual, 2),
                                round(deviation_pct, 1), status, note))
        log.info(f"  {status:12s} {name}: expected={expected}, actual={actual:.1f}, "
                 f"dev={deviation_pct:.1f}%")

    return results


def format_benchmark_table(results: list[dict]) -> str:
    """Format results as a markdown table."""
    lines = [
        "| Check | Expected | Actual | Deviation | Status |",
        "|-------|----------|--------|-----------|--------|",
    ]
    for r in results:
        exp = f"{r['expected']:,}" if isinstance(r["expected"], (int, float)) else str(r["expected"])
        act = f"{r['actual']:,.1f}" if r["actual"] is not None else "—"
        dev = f"{r['deviation_pct']:.1f}%" if r["deviation_pct"] is not None else "—"
        note = f" _{r['note']}_" if r.get("note") else ""
        unit = f" {r['unit']}" if r.get("unit") else ""
        lines.append(f"| {r['name']}{note} | {exp}{unit} | {act}{unit} | {dev} | {r['status']} |")
    return "\n".join(lines)


def summarize(results: list[dict]) -> tuple[int, int, int, int]:
    """Returns (n_pass, n_warn, n_fail, n_unverified)."""
    statuses = [r["status"] for r in results]
    return (
        statuses.count("PASS"),
        statuses.count("WARN"),
        statuses.count("FAIL"),
        statuses.count("UNVERIFIED") + statuses.count("ERROR"),
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _result(name, expected, unit, actual, deviation_pct, status, note) -> dict:
    return {
        "name": name,
        "expected": expected,
        "unit": unit,
        "actual": actual,
        "deviation_pct": deviation_pct,
        "status": status,
        "note": note,
    }


def _resolve(source: dict[str, Any], get_df) -> float:
    """Resolve a source descriptor to a single float value.

    Args:
        source: The source descriptor dict from a benchmark.
        get_df: Callable(file_key) → pd.DataFrame — returns a cached DataFrame.
    """
    file_key = source.get("file")
    df = get_df(file_key)   # raises ValueError/FileNotFoundError if missing

    op = source.get("op")
    column = source.get("column")

    if op == "sum_column":
        if column not in df.columns:
            raise KeyError(f"Column '{column}' not in {file_key}. Available: {list(df.columns)}")
        filt: dict = source.get("filter", {})
        if filt:
            mask = pd.Series([True] * len(df), index=df.index)
            for col, val in filt.items():
                if col not in df.columns:
                    raise KeyError(f"Filter column '{col}' not in {file_key}")
                mask = mask & (df[col].astype(str) == str(val))
            return float(df.loc[mask, column].sum())
        return float(df[column].sum())

    elif op == "sum_row":
        # Sum across all columns for a named row (index value) — used for industry_table4
        row_key = source.get("row")
        if row_key is None:
            raise ValueError("op=sum_row requires a 'row' key")
        if row_key not in df.index:
            raise KeyError(f"Row '{row_key}' not in {file_key}. Available: {list(df.index)}")
        return float(df.loc[row_key].sum())

    elif op == "sum_all":
        # Grand total of the entire table
        return float(df.to_numpy(dtype=float, na_value=0.0).sum())

    elif op == "lookup":
        filt: dict = source.get("filter", {})
        mask = pd.Series([True] * len(df), index=df.index)
        for col, val in filt.items():
            if col not in df.columns:
                raise KeyError(f"Filter column '{col}' not in {file_key}")
            mask = mask & (df[col].astype(str) == str(val))
        rows = df[mask]
        if len(rows) == 0:
            raise ValueError(f"No rows in {file_key} matching filter {filt}")
        if column not in df.columns:
            raise KeyError(f"Column '{column}' not in {file_key}")
        return float(rows.iloc[0][column])

    else:
        raise ValueError(f"Unknown op '{op}'. Valid: sum_column, sum_row, sum_all, lookup")
