"""
EXIOBASE 3.x IOT parser.

File format:
  IOT_{year}_{variant}.zip  where variant is 'ixi' (industry×industry) or 'pxp'

  Inside the zip:
    Z.txt      — intermediate use, tab-delimited
    Y.txt      — final demand, tab-delimited
    x.txt      — total output, tab-delimited
    satellite/
      F.txt    — satellite extension matrix; rows = stressors, cols = sectors
                 Employment rows: stressor name contains "Employment"

  Z.txt / Y.txt column header structure (2 header rows):
    Row 0:  region codes  (e.g. AT AT ... ZA ZA)
    Row 1:  sector names  (full English names)
  Row index (2 index columns):
    Col 0:  region code
    Col 1:  sector name

  F.txt:
    Same 2-row column header structure.
    Row index (1 column): stressor name.

Sector mapping:
  EXIOBASE uses full English sector names, not standard ISIC/NACE codes.
  Each spec industry entry may carry an 'exiobase_name' field giving the
  exact EXIOBASE sector name to use.  If absent, the spec industry 'code'
  is tried as a direct name match.

Reference: Stadler et al. (2018), Journal of Industrial Ecology.
"""
import logging
import time
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

from skills.io_parsers.base import PreparedMatrices

log = logging.getLogger(__name__)


def load(raw_dir: Path, spec: dict) -> PreparedMatrices:
    """Parse an EXIOBASE 3 IOT zip archive and return standardized matrices."""
    raw_dir = Path(raw_dir)
    year = spec["paper"]["reference_year"]

    candidates = sorted(raw_dir.glob(f"IOT_{year}_*.zip"))
    if not candidates:
        raise FileNotFoundError(
            f"No EXIOBASE IOT file found for year {year} in {raw_dir}. "
            f"Expected: IOT_{year}_ixi.zip or IOT_{year}_pxp.zip"
        )
    zip_path = candidates[0]
    if len(candidates) > 1:
        log.warning(f"Multiple EXIOBASE zips found; using {zip_path.name}")

    analysis_countries = [e["code"] for e in spec["geography"]["analysis_entities"]]
    analysis_set = set(analysis_countries)
    industry_list = [i["code"] for i in spec["classification"]["industry_list"]]
    code_to_idx = {c: i for i, c in enumerate(industry_list)}
    ctry_to_idx = {c: i for i, c in enumerate(analysis_countries)}
    N_c, N_i = len(analysis_countries), len(industry_list)
    N = N_c * N_i

    # sector-name → spec-code lookup (exiobase_name field, or code itself)
    name_to_code: dict[str, str] = {}
    for item in spec["classification"]["industry_list"]:
        if "exiobase_name" in item:
            name_to_code[item["exiobase_name"]] = item["code"]
        name_to_code[item["code"]] = item["code"]

    log.info(f"EXIOBASE parser: {N_c} countries × {N_i} industries = {N}, year={year}")

    t0 = time.time()
    with zipfile.ZipFile(zip_path) as zf:
        Z_vals, Z_row_c, Z_row_s, Z_col_c, Z_col_s = _read_matrix(zf, "Z.txt")
        Y_vals, Y_row_c, Y_row_s, Y_col_c, _ = _read_matrix(zf, "Y.txt")
        x_vals, x_row_c, x_row_s = _read_x(zf)
        emp_vals, emp_col_c, emp_col_s = _read_employment(zf)

    log.info(f"EXIOBASE zip parsed in {time.time()-t0:.1f}s  Z={Z_vals.shape}")

    # Position → flat-index maps for row and column dimensions
    row_map = _pos_map(Z_row_c, Z_row_s, analysis_set, name_to_code,
                       ctry_to_idx, code_to_idx, N_i)
    col_map_Z = _pos_map(Z_col_c, Z_col_s, analysis_set, name_to_code,
                         ctry_to_idx, code_to_idx, N_i)
    col_map_emp = _pos_map(emp_col_c, emp_col_s, analysis_set, name_to_code,
                           ctry_to_idx, code_to_idx, N_i)

    Z = _scatter_Z(Z_vals, row_map, col_map_Z, N)
    e = _scatter_e(Z_vals, Z_col_c, Y_vals, Y_col_c, row_map, analysis_set, N)
    x = _scatter_vec(x_vals, _pos_map(x_row_c, x_row_s, analysis_set, name_to_code,
                                      ctry_to_idx, code_to_idx, N_i), N)
    Em = _scatter_vec(emp_vals, col_map_emp, N)

    e = np.maximum(e, 0.0)
    x = np.maximum(x, 0.0)
    log.info(f"Z sum={Z.sum():,.0f}  e sum={e.sum():,.0f}  x sum={x.sum():,.0f}")
    log.info(f"Em sum={Em.sum():,.1f} thousand persons")

    labels = [f"{c}_{i}" for c in analysis_countries for i in industry_list]
    return PreparedMatrices(
        Z=Z, e=e, x=x, Em=Em,
        labels=labels, eu_codes=analysis_countries, cpa_codes=industry_list,
        year=year, source="exiobase",
    )


# ── File readers ──────────────────────────────────────────────────────────────

def _read_matrix(zf: zipfile.ZipFile, name: str):
    """
    Read a tab-delimited matrix with 2 header rows and 2 index columns.
    Returns (values, row_countries, row_sectors, col_countries, col_sectors).
    """
    with zf.open(name) as f:
        df = pd.read_csv(f, sep="\t", header=[0, 1], index_col=[0, 1], dtype=str)

    row_countries = df.index.get_level_values(0).tolist()
    row_sectors = df.index.get_level_values(1).tolist()
    col_countries = df.columns.get_level_values(0).tolist()
    col_sectors = df.columns.get_level_values(1).tolist()
    values = df.apply(pd.to_numeric, errors="coerce").fillna(0.0).values
    return values, row_countries, row_sectors, col_countries, col_sectors


def _read_x(zf: zipfile.ZipFile):
    """
    Read total output vector from x.txt.
    Returns (values_1d, row_countries, row_sectors).
    """
    with zf.open("x.txt") as f:
        df = pd.read_csv(f, sep="\t", index_col=[0, 1], header=0, dtype=str)

    row_countries = df.index.get_level_values(0).tolist()
    row_sectors = df.index.get_level_values(1).tolist()
    # First numeric column = total output
    values = pd.to_numeric(df.iloc[:, 0], errors="coerce").fillna(0.0).values
    return values, row_countries, row_sectors


def _read_employment(zf: zipfile.ZipFile):
    """
    Read employment from satellite/F.txt (or F.txt at root).
    Returns (employment_1d, col_countries, col_sectors) summed over all
    stressor rows whose name contains 'Employment' (case-insensitive).
    """
    available = zf.namelist()
    fname = next(
        (n for n in ["satellite/F.txt", "F.txt"] if n in available),
        next((n for n in available if n.endswith("F.txt")), None),
    )
    if fname is None:
        log.warning("F.txt not found in EXIOBASE zip — employment will be zero")
        return np.array([]), [], []

    with zf.open(fname) as f:
        df = pd.read_csv(f, sep="\t", header=[0, 1], index_col=0, dtype=str)

    col_countries = df.columns.get_level_values(0).tolist()
    col_sectors = df.columns.get_level_values(1).tolist()

    emp_rows = [idx for idx in df.index if "employment" in str(idx).lower()]
    if not emp_rows:
        log.warning("No employment rows found in F.txt (looked for 'Employment')")
        n_cols = df.shape[1]
        return np.zeros(n_cols), col_countries, col_sectors

    emp = df.loc[emp_rows].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    return emp.sum(axis=0).values, col_countries, col_sectors


# ── Position maps and scatter ─────────────────────────────────────────────────

def _pos_map(countries: list, sectors: list, analysis_set: set,
             name_to_code: dict, ctry_to_idx: dict,
             code_to_idx: dict, N_i: int) -> dict[int, int]:
    """Map each position → flat PreparedMatrices index (skip non-analysis rows)."""
    result = {}
    for pos, (c, s) in enumerate(zip(countries, sectors)):
        if c not in analysis_set:
            continue
        spec_code = name_to_code.get(s)
        if spec_code is None or spec_code not in code_to_idx:
            continue
        result[pos] = ctry_to_idx[c] * N_i + code_to_idx[spec_code]
    return result


def _scatter_Z(Z_vals, row_map, col_map, N):
    Z = np.zeros((N, N))
    r_pos = sorted(row_map)
    c_pos = sorted(col_map)
    if not r_pos or not c_pos:
        return Z
    sub = Z_vals[np.ix_(r_pos, c_pos)]
    r_flat = [row_map[p] for p in r_pos]
    c_flat = [col_map[p] for p in c_pos]
    for j, cf in enumerate(c_flat):
        np.add.at(Z, (r_flat, np.full(len(r_flat), cf, dtype=int)), sub[:, j])
    return Z


def _scatter_e(Z_vals, Z_col_c, Y_vals, Y_col_c, row_map, analysis_set, N):
    """All analysis→non-analysis flows (intermediate via Z + final demand via Y)."""
    e = np.zeros(N)
    r_pos = sorted(row_map)
    if not r_pos:
        return e
    r_flat = np.array([row_map[p] for p in r_pos])

    # Z: columns where destination country is not in analysis_set
    z_nonanalysis = [j for j, c in enumerate(Z_col_c) if c not in analysis_set]
    if z_nonanalysis:
        np.add.at(e, r_flat, Z_vals[np.ix_(r_pos, z_nonanalysis)].sum(axis=1))

    # Y: columns where destination country is not in analysis_set
    y_nonanalysis = [j for j, c in enumerate(Y_col_c) if c not in analysis_set]
    if y_nonanalysis and Y_vals is not None:
        np.add.at(e, r_flat, Y_vals[np.ix_(r_pos, y_nonanalysis)].sum(axis=1))

    return e


def _scatter_vec(vals, pos_map, N):
    """Scatter a 1-D array into a flat N-element vector via pos_map."""
    out = np.zeros(N)
    if not pos_map or len(vals) == 0:
        return out
    for pos, flat in pos_map.items():
        if pos < len(vals):
            out[flat] += vals[pos]
    return out
