"""
OECD ICIO 2023 CSV parser.

File format (OECD, 2023):
  ICIO{version}_{year}.csv  — e.g. ICIO2023_2010.csv
  Comma-separated; first column = row labels, first row = column labels.

  Row/column code format:  {ISO3}_{ISIC4_code}
    e.g.  AUS_D01T02,  FRA_D20,  USA_D62T63

  Special row codes (bottom of file):
    {ISO3}_TAXSUB  — taxes less subsidies on products
    {ISO3}_VALU    — value added
    {ISO3}_OUTPUT  — total output (gross output)

  Final demand column codes:
    {ISO3}_HFCE    — household final consumption expenditure
    {ISO3}_NPISH   — non-profit institutions serving households
    {ISO3}_GGFC    — government final consumption
    {ISO3}_GFCF    — gross fixed capital formation
    {ISO3}_INVNT   — changes in inventories

Employment:
  NOT bundled with ICIO.  Optionally provide:
    icio_employment_{year}.csv  — two columns: code ({ISO3}_{ISIC4}), emp_THS_PER
  If absent, Em is set to zero with a logged warning.

Reference: OECD (2023), "OECD Inter-Country Input-Output Tables",
  https://www.oecd.org/sti/ind/inter-country-input-output-tables.htm
"""
import logging
import time
from pathlib import Path

import numpy as np
import pandas as pd

from skills.io_parsers.base import PreparedMatrices

log = logging.getLogger(__name__)

# Non-industry row suffixes to skip when parsing producing-sector rows
_VA_SUFFIXES = {"TAXSUB", "VALU", "OUTPUT"}

# Final demand column suffixes (not intermediate use)
_FD_SUFFIXES = {"HFCE", "NPISH", "GGFC", "GFCF", "INVNT"}


def _split_code(code: str) -> tuple[str, str]:
    """
    Split 'AUS_D01T02' → ('AUS', 'D01T02').
    Also handles codes with multiple underscores like 'AUS_D31T33'.
    The country code is always the first 3 characters before the first '_'.
    """
    parts = code.split("_", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return code, ""


def _is_industry_row(suffix: str) -> bool:
    return suffix not in _VA_SUFFIXES and suffix != ""


def _is_fd_col(suffix: str) -> bool:
    return suffix in _FD_SUFFIXES


def load(raw_dir: Path, spec: dict) -> PreparedMatrices:
    """
    Parse an OECD ICIO CSV file and return standardized matrices.

    Looks for files matching `ICIO*_{year}.csv` in raw_dir.
    """
    raw_dir = Path(raw_dir)
    year = spec["paper"]["reference_year"]

    # Find the ICIO file (version-agnostic: ICIO2021, ICIO2023, etc.)
    candidates = sorted(raw_dir.glob(f"ICIO*_{year}.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"No ICIO file found for year {year} in {raw_dir}. "
            f"Expected: ICIO{{version}}_{year}.csv"
        )
    icio_path = candidates[0]
    if len(candidates) > 1:
        log.warning(f"Multiple ICIO files found; using {icio_path.name}")

    analysis_countries = [e["code"] for e in spec["geography"]["analysis_entities"]]
    analysis_set = set(analysis_countries)
    industry_list = [i["code"] for i in spec["classification"]["industry_list"]]
    industry_set = set(industry_list)
    code_to_idx = {c: i for i, c in enumerate(industry_list)}
    ctry_to_idx = {c: i for i, c in enumerate(analysis_countries)}

    N_c = len(analysis_countries)
    N_i = len(industry_list)
    N = N_c * N_i

    log.info(f"OECD ICIO parser: {N_c} countries × {N_i} industries = {N}, year={year}")

    t0 = time.time()
    raw = pd.read_csv(icio_path, index_col=0, dtype=str)
    log.info(f"ICIO loaded: {raw.shape} in {time.time()-t0:.1f}s")

    # Parse row codes
    row_meta = pd.DataFrame(
        [_split_code(str(idx)) for idx in raw.index],
        index=raw.index,
        columns=["country", "industry"],
    )
    # Parse column codes
    col_meta = pd.DataFrame(
        [_split_code(str(c)) for c in raw.columns],
        index=raw.columns,
        columns=["country", "industry"],
    )

    # Convert values to float
    raw_vals = raw.apply(pd.to_numeric, errors="coerce").fillna(0.0)

    # Producing rows: analysis countries, known industries
    row_mask = (
        row_meta["country"].isin(analysis_set) &
        row_meta["industry"].isin(industry_set)
    )

    Z = _build_Z(raw_vals, row_meta, col_meta, row_mask, analysis_set,
                 industry_set, code_to_idx, ctry_to_idx, N_c, N_i, N)

    e = _build_e(raw_vals, row_meta, col_meta, row_mask, analysis_set,
                 code_to_idx, ctry_to_idx, N_c, N_i, N)

    x = _build_x(raw_vals, row_meta, col_meta, row_mask, code_to_idx,
                 ctry_to_idx, N_c, N_i, N)

    e = np.maximum(e, 0.0)
    x = np.maximum(x, 0.0)
    log.info(f"Z sum={Z.sum():,.0f}  e sum={e.sum():,.0f}  x sum={x.sum():,.0f}")

    Em = _build_Em(raw_dir, year, analysis_countries, analysis_set,
                   industry_list, code_to_idx, ctry_to_idx, N_c, N_i, N)
    log.info(f"Em sum={Em.sum():,.1f} thousand persons")

    labels = [f"{c}_{i}" for c in analysis_countries for i in industry_list]
    return PreparedMatrices(
        Z=Z, e=e, x=x, Em=Em,
        labels=labels, eu_codes=analysis_countries, cpa_codes=industry_list,
        year=year, source="oecd_icio",
    )


def _build_Z(raw, row_meta, col_meta, row_mask, analysis_set, industry_set,
             code_to_idx, ctry_to_idx, N_c, N_i, N):
    """Intra-analysis intermediate flows only."""
    col_mask = (
        col_meta["country"].isin(analysis_set) &
        col_meta["industry"].isin(industry_set)
    )
    sub = raw.loc[row_mask, col_mask]

    Z = np.zeros((N, N))
    if sub.empty:
        return Z

    r_ctry = row_meta.loc[row_mask, "country"].map(ctry_to_idx)
    r_ind = row_meta.loc[row_mask, "industry"].map(code_to_idx)
    row_flat = (r_ctry * N_i + r_ind).values

    c_ctry = col_meta.loc[col_mask, "country"].map(ctry_to_idx)
    c_ind = col_meta.loc[col_mask, "industry"].map(code_to_idx)
    col_flat = (c_ctry * N_i + c_ind).values

    vals = sub.values  # (n_rows, n_cols)
    for j, cf in enumerate(col_flat):
        np.add.at(Z, (row_flat, np.full(len(row_flat), cf, dtype=int)), vals[:, j])

    return Z


def _build_e(raw, row_meta, col_meta, row_mask, analysis_set,
             code_to_idx, ctry_to_idx, N_c, N_i, N):
    """All flows from analysis countries to destinations outside the analysis set."""
    col_mask = ~col_meta["country"].isin(analysis_set)
    sub = raw.loc[row_mask, col_mask]

    e = np.zeros(N)
    if sub.empty:
        return e

    r_ctry = row_meta.loc[row_mask, "country"].map(ctry_to_idx)
    r_ind = row_meta.loc[row_mask, "industry"].map(code_to_idx)
    row_flat = (r_ctry * N_i + r_ind).values
    np.add.at(e, row_flat, sub.sum(axis=1).values)
    return e


def _build_x(raw, row_meta, col_meta, row_mask, code_to_idx, ctry_to_idx, N_c, N_i, N):
    """Total output = sum of all use columns for each analysis-country row."""
    all_use_cols = [c for c in raw.columns]
    x = np.zeros(N)
    total = raw.loc[row_mask, all_use_cols].sum(axis=1).values
    r_ctry = row_meta.loc[row_mask, "country"].map(ctry_to_idx)
    r_ind = row_meta.loc[row_mask, "industry"].map(code_to_idx)
    row_flat = (r_ctry * N_i + r_ind).values
    np.add.at(x, row_flat, total)
    return x


def _build_Em(raw_dir, year, analysis_countries, analysis_set, industry_list,
              code_to_idx, ctry_to_idx, N_c, N_i, N):
    """
    Load employment from optional icio_employment_{year}.csv.
    Format: two columns — 'code' ({ISO3}_{ISIC4}) and 'emp_THS_PER'.
    If the file is absent, returns zeros with a warning.
    """
    emp_path = raw_dir / f"icio_employment_{year}.csv"
    Em = np.zeros(N)

    if not emp_path.exists():
        log.warning(
            f"No employment file found ({emp_path.name}). Em will be zero. "
            "Provide icio_employment_{year}.csv with columns: code, emp_THS_PER"
        )
        return Em

    emp = pd.read_csv(emp_path)
    for _, row in emp.iterrows():
        country, industry = _split_code(str(row["code"]))
        if country in ctry_to_idx and industry in code_to_idx:
            flat = ctry_to_idx[country] * N_i + code_to_idx[industry]
            Em[flat] += float(row["emp_THS_PER"])

    return Em
