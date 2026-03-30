"""
Stage 3: Model Builder (DETERMINISTIC — no LLM)
Builds the Leontief model and computes employment content of exports.
Parameterized entirely by replication_spec — no hardcoded dimensions.
"""
import json
import logging
import time
from pathlib import Path

import numpy as np
import pandas as pd

from agents.state import PipelineState

log = logging.getLogger("model_builder")


def model_builder_node(state: PipelineState) -> dict:
    """LangGraph node: deterministic Leontief model construction."""
    spec = state["replication_spec"]
    run_dir = Path(state["run_dir"])
    prepared_paths = state["prepared_data_paths"]

    prepared_dir = Path(prepared_paths.get("metadata", "")).parent
    model_dir = run_dir / "data" / "model"
    model_dir.mkdir(parents=True, exist_ok=True)

    # Derive dimensions from spec
    eu_countries = [e["code"] for e in spec["geography"]["analysis_entities"]]
    cpa_codes = [i["code"] for i in spec["classification"]["industry_list"]]
    N = len(eu_countries) * len(cpa_codes)

    log.info(f"Model Builder: {len(eu_countries)} countries × {len(cpa_codes)} industries = {N}")

    # Load prepared matrices
    with open(prepared_paths["metadata"]) as f:
        meta = json.load(f)

    Z_EU = pd.read_csv(prepared_paths["Z_EU"], index_col=0).values.astype(np.float64)
    e_nonEU = pd.read_csv(prepared_paths["e_nonEU"])["e_nonEU_MIO_EUR"].values.astype(np.float64)
    x_EU = pd.read_csv(prepared_paths["x_EU"])["x_EU_MIO_EUR"].values.astype(np.float64)
    em_EU = pd.read_csv(prepared_paths["Em_EU"])["em_EU_THS_PER"].values.astype(np.float64)

    # Build model
    A = _build_technical_coefficients(Z_EU, x_EU)
    L = _build_leontief_inverse(A)
    d = _build_employment_coefficients(x_EU, em_EU)

    # Compute employment content
    results = _compute_employment_content(d, L, e_nonEU, eu_countries, cpa_codes)

    # Run validation checks
    checks = _validate_model(A, L)

    # Save outputs
    row_labels = [f"{c}_{p}" for c in eu_countries for p in cpa_codes]
    paths = _save_outputs(A, L, d, results, model_dir, eu_countries, cpa_codes, row_labels)

    model_valid = (
        checks["n_col_sums_ge1"] == 0
        and checks["n_negative_L"] == 0
        and checks["identity_residual"] < 1e-4
    )

    return {
        "model_paths": paths,
        "model_valid": model_valid,
        "model_checks": checks,
        "current_stage": 3,
    }


def _build_technical_coefficients(Z_EU: np.ndarray, x_EU: np.ndarray) -> np.ndarray:
    x_inv = np.where(x_EU > 0, 1.0 / x_EU, 0.0)
    A = Z_EU * x_inv[np.newaxis, :]
    col_sums = A.sum(axis=0)
    log.info(f"A: max col sum={col_sums.max():.6f}, cols>=1: {np.sum(col_sums >= 1.0)}")
    return A


def _build_leontief_inverse(A: np.ndarray) -> np.ndarray:
    N = A.shape[0]
    log.info(f"Computing Leontief inverse ({N}×{N})...")
    t0 = time.time()
    I_minus_A = np.eye(N) - A
    L = np.linalg.inv(I_minus_A)
    log.info(f"Leontief inverse computed in {time.time()-t0:.1f}s")
    return L


def _build_employment_coefficients(x_EU: np.ndarray, em_EU: np.ndarray) -> np.ndarray:
    d = np.where(x_EU > 0, em_EU / x_EU, 0.0)
    log.info(f"d: min={d.min():.6f}, max={d.max():.6f}, zeros={np.sum(d==0)}")
    return d


def _compute_employment_content(
    d: np.ndarray,
    L: np.ndarray,
    e_nonEU: np.ndarray,
    eu_countries: list,
    cpa_codes: list,
) -> dict:
    N = len(eu_countries)
    P = len(cpa_codes)

    em_exports_total = d * (L @ e_nonEU)
    log.info(f"Total EU export-supported employment: {em_exports_total.sum():.0f} thousand persons")

    em_country_matrix = np.zeros((N, N), dtype=np.float64)
    for s_idx in range(N):
        e_s = np.zeros(N * P, dtype=np.float64)
        e_s[s_idx * P:(s_idx + 1) * P] = e_nonEU[s_idx * P:(s_idx + 1) * P]
        if e_s.sum() == 0:
            continue
        Le_s = L @ e_s
        for r_idx in range(N):
            r_start = r_idx * P
            r_end = (r_idx + 1) * P
            em_country_matrix[r_idx, s_idx] = np.dot(d[r_start:r_end], Le_s[r_start:r_end])

    log.info(f"Country matrix total: {em_country_matrix.sum():.0f}")
    return {"em_exports_total": em_exports_total, "em_country_matrix": em_country_matrix}


def _validate_model(A: np.ndarray, L: np.ndarray) -> dict:
    N = A.shape[0]
    col_sums = A.sum(axis=0)
    I_minus_A = np.eye(N) - A
    identity_residual = float(np.max(np.abs(L @ I_minus_A - np.eye(N))))
    checks = {
        "max_col_sum": float(col_sums.max()),
        "n_col_sums_ge1": int(np.sum(col_sums >= 1.0)),
        "n_negative_L": int(np.sum(L < -1e-10)),
        "n_diag_lt1": int(np.sum(np.diag(L) < 1.0 - 1e-10)),
        "identity_residual": identity_residual,
    }
    log.info(f"Model checks: {checks}")
    return checks


def _save_outputs(A, L, d, results, model_dir, eu_countries, cpa_codes, row_labels) -> dict:
    paths = {}

    p = model_dir / "A_EU.csv"
    pd.DataFrame(A, index=row_labels, columns=row_labels).to_csv(p)
    paths["A_EU"] = str(p)

    p = model_dir / "L_EU.csv"
    pd.DataFrame(L, index=row_labels, columns=row_labels).to_csv(p)
    paths["L_EU"] = str(p)

    p = model_dir / "d_EU.csv"
    pd.DataFrame({"label": row_labels, "d_THS_PER_per_MIO_EUR": d}).to_csv(p, index=False)
    paths["d_EU"] = str(p)

    p = model_dir / "em_exports_total.csv"
    pd.DataFrame({"label": row_labels, "em_exports_THS_PER": results["em_exports_total"]}).to_csv(p, index=False)
    paths["em_exports_total"] = str(p)

    p = model_dir / "em_exports_country_matrix.csv"
    pd.DataFrame(results["em_country_matrix"], index=eu_countries, columns=eu_countries).to_csv(p)
    paths["em_exports_country_matrix"] = str(p)

    log.info(f"Model outputs saved to {model_dir}")
    return paths
