"""
WIOD 2016 release parser.

File format (Timmer et al., 2015):
  WIOT{year}_Nov16_ROW.xlsx
    - Sheet "wiot{year}":
        Row 0: header labels (country codes repeated 56 times + final demand)
        Row 1: sector codes (56 NACE-like codes, repeated per country)
        Rows 2+: data (country-sector combinations)
        First 2 columns: (IndustryCode, Description) or (Country, IndustryCode)
        Last rows: VA components (ii=value added rows)
    - 44 countries × 56 sectors = 2,464 producing rows
    - 44 countries × 56 sectors (intermediate) + 44 × 5 (final demand) = columns
    - Total output in last column ("TOT")

  SEA.xlsx
    - Sheet "EA":
        Columns: country, year, variable, then one column per industry code
        Variable "EMP" = persons employed (thousands)

The parser normalises country and industry codes to those listed in
spec.geography.analysis_entities and spec.classification.industry_list,
restricting the MRIO to the analysis scope defined in the spec.
"""
import logging
import time
from pathlib import Path

import numpy as np
import pandas as pd

from skills.io_parsers.base import PreparedMatrices

log = logging.getLogger(__name__)

# WIOD 2016 final demand column suffixes (5 per country)
_FD_CODES = ["CONS_h", "CONS_np", "CONS_g", "GFCF", "INVEN"]

# WIOD 2016 value-added row codes
_VA_CODES = {"ii", "VA", "GO"}


def load(raw_dir: Path, spec: dict) -> PreparedMatrices:
    """
    Parse WIOD 2016 WIOT Excel file and SEA employment file.

    Expected files:
        {raw_dir}/WIOT{year}_Nov16_ROW.xlsx
        {raw_dir}/SEA.xlsx
    """
    raw_dir = Path(raw_dir)
    year = spec["paper"]["reference_year"]

    wiot_path = raw_dir / f"WIOT{year}_Nov16_ROW.xlsx"
    sea_path = raw_dir / "SEA.xlsx"
    assert wiot_path.exists(), f"WIOT file not found: {wiot_path}"
    assert sea_path.exists(), f"SEA file not found: {sea_path}"

    analysis_countries = [e["code"] for e in spec["geography"]["analysis_entities"]]
    analysis_set = set(analysis_countries)

    # Industry codes in spec (already normalised by the paper analyst)
    industry_list = [i["code"] for i in spec["classification"]["industry_list"]]
    industry_set = set(industry_list)
    code_to_idx = {c: i for i, c in enumerate(industry_list)}
    ctry_to_idx = {c: i for i, c in enumerate(analysis_countries)}

    N_c = len(analysis_countries)
    N_i = len(industry_list)
    N = N_c * N_i

    log.info(f"WIOD parser: {N_c} countries × {N_i} industries = {N}, year={year}")

    t0 = time.time()
    wiot_raw = _load_wiot(wiot_path, year)
    log.info(f"WIOT loaded in {time.time()-t0:.1f}s, shape={wiot_raw.shape}")

    # Filter to analysis scope
    row_mask = (
        wiot_raw["country"].isin(analysis_set) &
        wiot_raw["industry"].isin(industry_set)
    )

    Z = _extract_Z(wiot_raw, row_mask, analysis_countries, analysis_set,
                   industry_list, industry_set, code_to_idx, ctry_to_idx, N_c, N_i, N)

    e = _extract_e(wiot_raw, row_mask, analysis_set, industry_list,
                   code_to_idx, ctry_to_idx, N_c, N_i, N)

    x = _extract_x(wiot_raw, row_mask, code_to_idx, ctry_to_idx, N_c, N_i, N)

    e = np.maximum(e, 0.0)
    x = np.maximum(x, 0.0)
    log.info(f"Z sum={Z.sum():,.0f}  e sum={e.sum():,.0f}  x sum={x.sum():,.0f}")

    Em = _extract_Em(sea_path, year, analysis_countries, analysis_set,
                     industry_list, industry_set, code_to_idx, ctry_to_idx, N_c, N_i, N)
    log.info(f"Em sum={Em.sum():,.1f} thousand persons")

    labels = [f"{c}_{i}" for c in analysis_countries for i in industry_list]
    return PreparedMatrices(
        Z=Z, e=e, x=x, Em=Em,
        labels=labels, eu_codes=analysis_countries, cpa_codes=industry_list,
        year=year, source="wiod_mrio",
    )


# ── Internal helpers ──────────────────────────────────────────────────────────

def _load_wiot(path: Path, year: int) -> pd.DataFrame:
    """
    Load the WIOT sheet and return a long DataFrame with columns:
    country, industry, and one column per (dest_country, dest_industry_or_fd).

    WIOD 2016 layout:
      - Row 0: column description header
      - Row 1: country codes (repeated for each industry/FD column)
      - Row 2: industry or FD codes
      - Row 3+: data rows; col 0 = originating country, col 1 = industry code
      - Last rows (after all country-industry rows): value-added rows (skip)
    """
    sheet_name = f"wiot{year}"
    df = pd.read_excel(path, sheet_name=sheet_name, header=None, dtype=str)

    # Row 1 = dest country codes, Row 2 = dest sector/FD codes
    dest_countries = df.iloc[0, 2:].tolist()
    dest_sectors = df.iloc[1, 2:].tolist()

    # Data rows: skip first 2 header rows
    data = df.iloc[2:].copy()
    data.columns = ["country", "industry"] + [
        f"{c}||{s}" for c, s in zip(dest_countries, dest_sectors)
    ]

    # Drop value-added rows (country column looks like "ii" or empty)
    data = data[~data["country"].isin(_VA_CODES) & data["country"].notna()].copy()

    # Numeric conversion
    value_cols = [c for c in data.columns if "||" in c]
    for col in value_cols:
        data[col] = pd.to_numeric(data[col], errors="coerce").fillna(0.0)

    return data


def _extract_Z(wiot, row_mask, analysis_countries, analysis_set,
               industry_list, industry_set, code_to_idx, ctry_to_idx, N_c, N_i, N):
    """Extract the domestic intermediate-use block (analysis countries only)."""
    sub = wiot[row_mask]
    Z = np.zeros((N, N))

    # Intermediate-demand columns: dest country in analysis_set AND dest sector in industry_set
    int_cols = [
        col for col in sub.columns
        if "||" in col
        and col.split("||")[0] in analysis_set
        and col.split("||")[1] in industry_set
    ]

    for col in int_cols:
        dest_c, dest_i = col.split("||")
        dest_c_idx = ctry_to_idx[dest_c]
        dest_i_idx = code_to_idx[dest_i]
        flat_col = dest_c_idx * N_i + dest_i_idx

        row_idxs = (
            sub["country"].map(ctry_to_idx) * N_i
            + sub["industry"].map(code_to_idx)
        ).values
        np.add.at(Z, (row_idxs, np.full(len(row_idxs), flat_col, dtype=int)),
                  sub[col].values)

    return Z


def _extract_e(wiot, row_mask, analysis_set, industry_list,
               code_to_idx, ctry_to_idx, N_c, N_i, N):
    """
    Export vector: all flows from analysis countries to destinations outside
    the analysis set (both intermediate and final demand uses).
    """
    sub = wiot[row_mask]

    # All columns where destination country is NOT in the analysis set
    export_cols = [
        col for col in sub.columns
        if "||" in col and col.split("||")[0] not in analysis_set
    ]

    e = np.zeros(N)
    if not export_cols:
        return e

    export_vals = sub[export_cols].sum(axis=1).values
    row_idxs = (
        sub["country"].map(ctry_to_idx) * N_i
        + sub["industry"].map(code_to_idx)
    ).values
    np.add.at(e, row_idxs, export_vals)
    return e


def _extract_x(wiot, row_mask, code_to_idx, ctry_to_idx, N_c, N_i, N):
    """Total output: sum of all use columns for each analysis-country row."""
    sub = wiot[row_mask]
    all_value_cols = [c for c in sub.columns if "||" in c]
    x = np.zeros(N)
    total = sub[all_value_cols].sum(axis=1).values
    row_idxs = (
        sub["country"].map(ctry_to_idx) * N_i
        + sub["industry"].map(code_to_idx)
    ).values
    np.add.at(x, row_idxs, total)
    return x


def _extract_Em(sea_path, year, analysis_countries, analysis_set,
                industry_list, industry_set, code_to_idx, ctry_to_idx, N_c, N_i, N):
    """
    Load employment from WIOD SEA.xlsx.

    SEA.xlsx sheet "EA" layout:
      Columns: country, year, variable, {industry codes...}
      Variable "EMP" = persons employed (thousands).
    """
    try:
        sea = pd.read_excel(sea_path, sheet_name="EA", dtype=str)
    except Exception as e:
        log.warning(f"Could not read SEA.xlsx: {e} — employment will be zero")
        return np.zeros(N)

    # Filter to EMP, target year, analysis countries
    sea["year"] = sea["year"].astype(str).str.strip()
    emp = sea[
        (sea["variable"] == "EMP") &
        (sea["year"] == str(year)) &
        (sea["country"].isin(analysis_set))
    ].copy()

    if emp.empty:
        log.warning(f"No EMP data found in SEA.xlsx for year {year}")
        return np.zeros(N)

    Em = np.zeros(N)
    for _, row in emp.iterrows():
        c = row["country"]
        if c not in ctry_to_idx:
            continue
        c_idx = ctry_to_idx[c]
        for ind_code in industry_list:
            if ind_code in row.index:
                val = pd.to_numeric(row[ind_code], errors="coerce")
                if pd.notna(val):
                    flat = c_idx * N_i + code_to_idx[ind_code]
                    Em[flat] += float(val)

    return Em
