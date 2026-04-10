"""
Stage 2: Data Preparer — deterministic TSV parser.

Reads two bulk Eurostat gzip-TSV files downloaded by Stage 1 and
produces the five matrices needed by Stage 3:
  Z_EU.csv, e_nonEU.csv, x_EU.csv, Em_EU.csv  +  metadata.json

No LLM involved — we know exactly what the format is.
"""
import json
import logging
import re
import time
from pathlib import Path

import numpy as np
import pandas as pd
from rich.console import Console
from rich.panel import Panel

from agents.state import PipelineState
from agents.validators import validate_prepared_data

log = logging.getLogger("data_preparer")
_console = Console()

# ── Code normalisation ────────────────────────────────────────────────────────
# IC-IOT prd_ava / prd_use codes in the TSV start with "CPA_" and use "_"
# where the spec uses "-".  A handful of aggregate codes differ more severely.
_CPA_EXCEPTIONS = {
    'B': 'B05-09', 'F': 'F41-43', 'I': 'I55-56',
    'L': 'L68',    'T': 'T97-98', 'U': 'U99',
}

# Employment NACE leaf codes (exactly 64) — one per spec industry.
# Aggregates (A, B-E, C, G-I, TOTAL, …) are excluded by this set.
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

VA_ROWS = {'B2A3G', 'D1', 'D21X31', 'D29X39', 'OP_NRES', 'OP_RES'}
FD_COLS = {'P3_S13', 'P3_S14', 'P3_S15', 'P51G', 'P5M'}


def _norm_cpa(raw: str) -> str:
    """'CPA_C31_32' → 'C31-32',  'CPA_B' → 'B05-09',  'P3_S13' → 'P3_S13'."""
    if not raw.startswith('CPA_'):
        return raw  # FD or VA code — pass through unchanged
    code = raw[4:].replace('_', '-')   # strip CPA_ and normalise separator
    return _CPA_EXCEPTIONS.get(code, code)


def _norm_nace(raw: str) -> str:
    """'C10-C12' → 'C10-12',  'C31_C32' → 'C31-32',  'B' → 'B05-09'."""
    if raw in _NACE_EXCEPTIONS:
        return _NACE_EXCEPTIONS[raw]
    code = raw.replace('_', '-')
    # Remove duplicate letter prefix in ranges: C10-C12 → C10-12
    code = re.sub(r'^([A-Z])(\d.*)-\1(\d.*)$', r'\1\2-\3', code)
    return code


# ── Node entry point ──────────────────────────────────────────────────────────

def data_preparer_node(state: PipelineState) -> dict:
    """LangGraph node: deterministic matrix construction + validation."""
    run_dir  = Path(state["run_dir"])
    spec     = state["replication_spec"]
    retry_count = state.get("retry_count", 0)

    _console.print(Panel(
        f"[bold]Stage 2 — Data Preparer[/bold]  (attempt {retry_count + 1})\n"
        "Deterministic TSV parser → Z, e, x, Em matrices\n"
        "[dim]Input: data/raw/*.tsv.gz   →   Output: data/prepared/[/dim]",
        style="blue"
    ))

    raw_dir     = run_dir / "data" / "raw"
    prepared_dir = run_dir / "data" / "prepared"
    prepared_dir.mkdir(parents=True, exist_ok=True)

    iot_path = raw_dir / "naio_10_fcp_ip1.tsv.gz"
    emp_path = raw_dir / "nama_10_a64_e.tsv.gz"
    assert iot_path.exists(), f"IC-IOT file not found: {iot_path}"
    assert emp_path.exists(), f"Employment file not found: {emp_path}"

    # Derive dimensions from spec
    eu_codes   = [e["code"] for e in spec["geography"]["analysis_entities"]]
    eu_set     = set(eu_codes)
    cpa_codes  = [i["code"] for i in spec["classification"]["industry_list"]]
    cpa_set    = set(cpa_codes)
    code_to_idx = {c: i for i, c in enumerate(cpa_codes)}
    ctry_to_idx = {c: i for i, c in enumerate(eu_codes)}
    year       = spec["paper"]["reference_year"]
    N_c, N_i   = len(eu_codes), len(cpa_codes)
    N          = N_c * N_i

    _console.print(f"  Dimensions: {N_c} countries × {N_i} industries = {N:,}")

    # ── Load IC-IOT ──────────────────────────────────────────────────────────
    _console.print(f"  Loading IC-IOT ({iot_path.name}) ...")
    t0 = time.time()
    iot = _load_wide_tsv(iot_path, year)
    _console.print(f"  {len(iot):,} rows loaded in {time.time()-t0:.1f}s")

    # Filter to MIO_EUR
    iot = iot[iot["unit"] == "MIO_EUR"].copy()
    # Normalise product codes
    iot["prd_ava_n"] = iot["prd_ava"].map(_norm_cpa)
    iot["prd_use_n"] = iot["prd_use"].map(_norm_cpa)

    # ── Build Z_EU ───────────────────────────────────────────────────────────
    _console.print("  Building Z_EU (intra-EU intermediate flows)...")
    Z = _build_Z(iot, eu_set, cpa_set, code_to_idx, ctry_to_idx, N_c, N_i, N)

    # ── Build e_nonEU (Arto 2015) ────────────────────────────────────────────
    _console.print("  Building e_nonEU (EU→non-EU intermediate + final demand)...")
    e = _build_e(iot, eu_set, cpa_set, code_to_idx, ctry_to_idx, N_c, N_i, N)

    # ── Build x_EU ───────────────────────────────────────────────────────────
    _console.print("  Building x_EU (total output)...")
    x = _build_x(iot, eu_set, cpa_set, code_to_idx, ctry_to_idx, N_c, N_i, N)

    # Clip negative values to 0 (standard IO practice — negatives arise from
    # inventory changes in P5M which are legitimately negative in national accounts
    # but Leontief model requires non-negative vectors)
    n_neg_e = int(np.sum(e < 0))
    n_neg_x = int(np.sum(x < 0))
    if n_neg_e:
        log.info(f"Clipping {n_neg_e} negative e_nonEU values (min={e.min():.2f} MIO_EUR) to 0")
    if n_neg_x:
        log.info(f"Clipping {n_neg_x} negative x_EU values (min={x.min():.2f} MIO_EUR) to 0")
    e = np.maximum(e, 0.0)
    x = np.maximum(x, 0.0)

    _console.print(f"  Z sum={Z.sum():,.0f}  e sum={e.sum():,.0f}  x sum={x.sum():,.0f}")

    # ── Load employment ──────────────────────────────────────────────────────
    _console.print(f"  Loading employment ({emp_path.name}) ...")
    Em = _build_Em(emp_path, year, eu_codes, eu_set, cpa_codes, code_to_idx,
                   ctry_to_idx, N_c, N_i, N)
    _console.print(f"  Em sum={Em.sum():,.1f} thousand persons")

    # ── Save outputs ─────────────────────────────────────────────────────────
    _console.print("  Saving matrices...")
    row_labels = [f"{c}_{p}" for c in eu_codes for p in cpa_codes]

    z_path = prepared_dir / "Z_EU.csv"
    pd.DataFrame(Z, index=row_labels, columns=row_labels).to_csv(z_path)

    e_path = prepared_dir / "e_nonEU.csv"
    pd.DataFrame({"e_nonEU_MIO_EUR": e}).to_csv(e_path, index=False)

    x_path = prepared_dir / "x_EU.csv"
    pd.DataFrame({"x_EU_MIO_EUR": x}).to_csv(x_path, index=False)

    em_path = prepared_dir / "Em_EU.csv"
    pd.DataFrame({"em_EU_THS_PER": Em}).to_csv(em_path, index=False)

    meta = {
        "eu_countries": eu_codes,
        "cpa_codes": cpa_codes,
        "n_countries": N_c,
        "n_industries": N_i,
        "n_total": N,
        "reference_year": year,
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

    # ── Validate ─────────────────────────────────────────────────────────────
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


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_wide_tsv(path: Path, year: int) -> pd.DataFrame:
    """
    Parse a Eurostat wide-format gzip-TSV into a long DataFrame.

    Header: 'dim1,dim2,...,dimN\\TIME_PERIOD\\t2010 \\t2011 ...'
    Each subsequent row has the dim values comma-joined in column 0,
    then one value per year (with possible Eurostat flags like 'b','e',':').
    """
    df = pd.read_csv(path, sep="\t", compression="gzip", dtype=str)
    key_col = df.columns[0]
    # Split packed dimensions
    dim_names = key_col.split("\\")[0].split(",")
    split = df[key_col].str.split(",", n=len(dim_names) - 1, expand=True)
    split.columns = dim_names

    # Find target-year column (header values have trailing spaces)
    year_col = next(c for c in df.columns[1:] if c.strip() == str(year))
    val_series = df[year_col].str.strip().str.replace(r"[^0-9.\-]", "", regex=True)
    values = pd.to_numeric(val_series, errors="coerce").fillna(0.0)

    result = split.copy()
    result["value"] = values.values
    return result


def _scatter_sum(N: int, rows, cols, vals) -> np.ndarray:
    """Vectorised scatter-add into a flat N-element vector."""
    out = np.zeros(N)
    np.add.at(out, rows, vals)
    return out


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

    Includes:
    - EU→non-EU intermediate flows (prd_use is a CPA code)
    - EU→non-EU final demand flows (prd_use is P3_S13, P3_S14, P3_S15, P51G, P5M)

    Does NOT include intra-EU final demand — that is domestic EU consumption and
    is excluded from the Leontief export calculation entirely. The spec note
    "intra-EU FD treated as exogenous" means it is excluded from Z, not that it
    is included in e.
    """
    mask = (
        iot["c_orig"].isin(eu_set) &
        ~iot["c_dest"].isin(eu_set) &
        iot["prd_ava_n"].isin(cpa_set)
        # prd_use_n unrestricted: captures both intermediate (CPA) and FD (P3_S13...) uses
    )
    df = iot[mask]
    row_idx = (df["c_orig"].map(ctry_to_idx) * N_i + df["prd_ava_n"].map(code_to_idx)).values
    e = np.zeros(N)
    np.add.at(e, row_idx, df["value"].values)
    return e


def _build_x(iot, eu_set, cpa_set, code_to_idx, ctry_to_idx, N_c, N_i, N):
    # Total output = sum of all uses (intermediate + FD + non-EU) of each EU product
    mask = (
        iot["c_orig"].isin(eu_set) &
        iot["prd_ava_n"].isin(cpa_set)
    )
    df = iot[mask]
    row_idx = (df["c_orig"].map(ctry_to_idx) * N_i + df["prd_ava_n"].map(code_to_idx)).values
    return _scatter_sum(N, row_idx, None, df["value"].values)


def _build_Em(emp_path, year, eu_codes, eu_set, cpa_codes, code_to_idx,
              ctry_to_idx, N_c, N_i, N):
    emp = _load_wide_tsv(emp_path, year)

    # Filter: THS_PER + EMP_DC + EU countries + leaf NACE codes
    emp = emp[
        (emp["unit"] == "THS_PER") &
        (emp["na_item"] == "EMP_DC") &
        (emp["geo"].isin(eu_set)) &
        (emp["nace_r2"].isin(_NACE_LEAF))
    ].copy()

    # Normalise NACE → spec code
    emp["spec_code"] = emp["nace_r2"].map(_norm_nace)

    # Keep only codes that actually appear in the spec
    emp = emp[emp["spec_code"].isin(code_to_idx)]

    Em = np.zeros(N)
    row_idx = (emp["geo"].map(ctry_to_idx) * N_i + emp["spec_code"].map(code_to_idx)).values
    np.add.at(Em, row_idx, emp["value"].values)
    return Em
