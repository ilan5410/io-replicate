"""
Eurostat FIGARO IC-IOT bulk TSV parser.

Reads two gzip-TSV files:
  - naio_10_fcp_ip1.tsv.gz  (IC-IOT, product-by-product)
  - nama_10_a64_e.tsv.gz    (employment by NACE A64)

Returns a PreparedMatrices with Z, e, x, Em ready for the Leontief skill.
"""
import logging
import re
import time
from pathlib import Path

import numpy as np
import pandas as pd

from skills.io_parsers.base import PreparedMatrices

log = logging.getLogger(__name__)

# ── Code normalisation ────────────────────────────────────────────────────────
# IC-IOT prd_ava / prd_use codes in the TSV start with "CPA_" and use "_"
# where the spec uses "-".  A handful of aggregate codes differ more severely.
_CPA_EXCEPTIONS = {
    'B': 'B05-09', 'F': 'F41-43', 'I': 'I55-56',
    'L': 'L68',    'T': 'T97-98', 'U': 'U99',
}

# Employment NACE leaf codes — one per spec industry (aggregates excluded).
_NACE_LEAF = {
    'A01', 'A02', 'A03', 'B',
    'C10-C12', 'C13-C15', 'C16', 'C17', 'C18', 'C19', 'C20', 'C21',
    'C22', 'C23', 'C24', 'C25', 'C26', 'C27', 'C28', 'C29', 'C30',
    'C31_C32', 'C33', 'D35', 'E36', 'E37-E39', 'F',
    'G45', 'G46', 'G47', 'H49', 'H50', 'H51', 'H52', 'H53', 'I',
    'J58', 'J59_J60', 'J61', 'J62_J63', 'K64', 'K65', 'K66', 'L68',
    'M69_M70', 'M71', 'M72', 'M73', 'M74_M75',
    'N77', 'N78', 'N79', 'N80-N82', 'O84', 'P85', 'Q86', 'Q87_Q88',
    'R90-R92', 'R93', 'S94', 'S95', 'S96', 'T', 'U99',
}
_NACE_EXCEPTIONS = {'B': 'B05-09', 'F': 'F41-43', 'I': 'I55-56', 'T': 'T97-98'}


def _norm_cpa(raw: str) -> str:
    """'CPA_C31_32' → 'C31-32',  'CPA_B' → 'B05-09',  'P3_S13' → 'P3_S13'."""
    if not raw.startswith('CPA_'):
        return raw
    code = raw[4:].replace('_', '-')
    return _CPA_EXCEPTIONS.get(code, code)


def _norm_nace(raw: str) -> str:
    """'C10-C12' → 'C10-12',  'C31_C32' → 'C31-32',  'B' → 'B05-09'."""
    if raw in _NACE_EXCEPTIONS:
        return _NACE_EXCEPTIONS[raw]
    code = raw.replace('_', '-')
    # Remove duplicate letter prefix in ranges: C10-C12 → C10-12
    code = re.sub(r'^([A-Z])(\d.*)-\1(\d.*)$', r'\1\2-\3', code)
    return code


def _load_wide_tsv(path: Path, year: int) -> pd.DataFrame:
    """
    Parse a Eurostat wide-format gzip-TSV into a long DataFrame.

    Header: 'dim1,dim2,...,dimN\\TIME_PERIOD\\t2010 \\t2011 ...'
    Each subsequent row has the dim values comma-joined in column 0,
    then one value per year (with possible Eurostat flags like 'b','e',':').
    """
    df = pd.read_csv(path, sep="\t", compression="gzip", dtype=str)
    key_col = df.columns[0]
    dim_names = key_col.split("\\")[0].split(",")
    split = df[key_col].str.split(",", n=len(dim_names) - 1, expand=True)
    split.columns = dim_names

    year_col = next(c for c in df.columns[1:] if c.strip() == str(year))
    val_series = df[year_col].str.strip().str.replace(r"[^0-9.\-]", "", regex=True)
    values = pd.to_numeric(val_series, errors="coerce").fillna(0.0)

    result = split.copy()
    result["value"] = values.values
    return result


def _build_Z(iot, eu_set, cpa_set, code_to_idx, ctry_to_idx, N_c, N_i, N):
    mask = (
        iot["c_orig"].isin(eu_set) &
        iot["c_dest"].isin(eu_set) &
        iot["prd_ava_n"].isin(cpa_set) &
        iot["prd_use_n"].isin(cpa_set)
    )
    df = iot[mask]
    row_idx = (df["c_orig"].map(ctry_to_idx) * N_i + df["prd_ava_n"].map(code_to_idx)).values
    col_idx = (df["c_dest"].map(ctry_to_idx) * N_i + df["prd_use_n"].map(code_to_idx)).values
    Z = np.zeros((N, N))
    np.add.at(Z, (row_idx, col_idx), df["value"].values)
    return Z


def _build_e(iot, eu_set, cpa_set, code_to_idx, ctry_to_idx, N_c, N_i, N):
    """
    Export vector (Arto 2015): all EU→non-EU flows, regardless of use type.

    Includes EU→non-EU intermediate flows and EU→non-EU final demand flows.
    Excludes intra-EU final demand (outside the model scope entirely).
    """
    mask = (
        iot["c_orig"].isin(eu_set) &
        ~iot["c_dest"].isin(eu_set) &
        iot["prd_ava_n"].isin(cpa_set)
    )
    df = iot[mask]
    row_idx = (df["c_orig"].map(ctry_to_idx) * N_i + df["prd_ava_n"].map(code_to_idx)).values
    e = np.zeros(N)
    np.add.at(e, row_idx, df["value"].values)
    return e


def _build_x(iot, eu_set, cpa_set, code_to_idx, ctry_to_idx, N_c, N_i, N):
    mask = (
        iot["c_orig"].isin(eu_set) &
        iot["prd_ava_n"].isin(cpa_set)
    )
    df = iot[mask]
    row_idx = (df["c_orig"].map(ctry_to_idx) * N_i + df["prd_ava_n"].map(code_to_idx)).values
    x = np.zeros(N)
    np.add.at(x, row_idx, df["value"].values)
    return x


def _build_Em(emp_path, year, eu_codes, eu_set, cpa_codes, code_to_idx,
              ctry_to_idx, N_c, N_i, N):
    emp = _load_wide_tsv(emp_path, year)
    emp = emp[
        (emp["unit"] == "THS_PER") &
        (emp["na_item"] == "EMP_DC") &
        (emp["geo"].isin(eu_set)) &
        (emp["nace_r2"].isin(_NACE_LEAF))
    ].copy()
    emp["spec_code"] = emp["nace_r2"].map(_norm_nace)
    emp = emp[emp["spec_code"].isin(code_to_idx)]
    Em = np.zeros(N)
    row_idx = (emp["geo"].map(ctry_to_idx) * N_i + emp["spec_code"].map(code_to_idx)).values
    np.add.at(Em, row_idx, emp["value"].values)
    return Em


# ── Public entry point ────────────────────────────────────────────────────────

def load(raw_dir: Path, spec: dict) -> PreparedMatrices:
    """
    Parse FIGARO IC-IOT TSV files and return standardized matrices.

    Expected files in raw_dir:
      - naio_10_fcp_ip1.tsv.gz
      - nama_10_a64_e.tsv.gz
    """
    raw_dir = Path(raw_dir)
    iot_path = raw_dir / "naio_10_fcp_ip1.tsv.gz"
    emp_path = raw_dir / "nama_10_a64_e.tsv.gz"
    assert iot_path.exists(), f"IC-IOT file not found: {iot_path}"
    assert emp_path.exists(), f"Employment file not found: {emp_path}"

    eu_codes = [e["code"] for e in spec["geography"]["analysis_entities"]]
    eu_set = set(eu_codes)
    cpa_codes = [_norm_cpa(i["code"]) for i in spec["classification"]["industry_list"]]
    cpa_set = set(cpa_codes)
    code_to_idx = {c: i for i, c in enumerate(cpa_codes)}
    ctry_to_idx = {c: i for i, c in enumerate(eu_codes)}
    year = spec["paper"]["reference_year"]
    N_c, N_i = len(eu_codes), len(cpa_codes)
    N = N_c * N_i

    log.info(f"FIGARO parser: {N_c} countries × {N_i} industries = {N}, year={year}")

    t0 = time.time()
    iot = _load_wide_tsv(iot_path, year)
    log.info(f"IC-IOT loaded: {len(iot):,} rows in {time.time()-t0:.1f}s")

    iot = iot[iot["unit"] == "MIO_EUR"].copy()
    iot["prd_ava_n"] = iot["prd_ava"].map(_norm_cpa)
    iot["prd_use_n"] = iot["prd_use"].map(_norm_cpa)

    Z = _build_Z(iot, eu_set, cpa_set, code_to_idx, ctry_to_idx, N_c, N_i, N)
    e = _build_e(iot, eu_set, cpa_set, code_to_idx, ctry_to_idx, N_c, N_i, N)
    x = _build_x(iot, eu_set, cpa_set, code_to_idx, ctry_to_idx, N_c, N_i, N)

    n_neg_e = int(np.sum(e < 0))
    n_neg_x = int(np.sum(x < 0))
    if n_neg_e:
        log.info(f"Clipping {n_neg_e} negative e values (min={e.min():.2f}) to 0")
    if n_neg_x:
        log.info(f"Clipping {n_neg_x} negative x values (min={x.min():.2f}) to 0")
    e = np.maximum(e, 0.0)
    x = np.maximum(x, 0.0)

    log.info(f"Z sum={Z.sum():,.0f}  e sum={e.sum():,.0f}  x sum={x.sum():,.0f}")

    Em = _build_Em(emp_path, year, eu_codes, eu_set, cpa_codes,
                   code_to_idx, ctry_to_idx, N_c, N_i, N)
    log.info(f"Em sum={Em.sum():,.1f} thousand persons")

    labels = [f"{c}_{p}" for c in eu_codes for p in cpa_codes]
    return PreparedMatrices(
        Z=Z, e=e, x=x, Em=Em,
        labels=labels, eu_codes=eu_codes, cpa_codes=cpa_codes,
        year=year, source="figaro_iciot",
    )
