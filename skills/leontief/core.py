"""
Core Leontief input–output mathematics.

All functions are pure: they take numpy arrays / plain Python types and return
numpy arrays. No LangGraph, no pandas, no I/O.
"""
import logging
import time

import numpy as np

log = logging.getLogger(__name__)


# ── Model construction ────────────────────────────────────────────────────────

def build_technical_coefficients(Z_EU: np.ndarray, x_EU: np.ndarray) -> np.ndarray:
    """A = Z · diag(x)^-1.  Column j of A = input shares needed to produce 1 unit of j."""
    x_inv = np.where(x_EU > 0, 1.0 / x_EU, 0.0)
    A = Z_EU * x_inv[np.newaxis, :]
    col_sums = A.sum(axis=0)
    log.info(f"A: max col sum={col_sums.max():.6f}, cols>=1: {np.sum(col_sums >= 1.0)}")
    return A


def build_leontief_inverse(A: np.ndarray) -> np.ndarray:
    """L = (I - A)^-1.  Full inverse (not just solve) — reused across many demand vectors."""
    N = A.shape[0]
    log.info(f"Computing Leontief inverse ({N}×{N})...")
    t0 = time.time()
    L = np.linalg.inv(np.eye(N) - A)
    log.info(f"Leontief inverse computed in {time.time()-t0:.1f}s")
    return L


def build_employment_coefficients(x_EU: np.ndarray, em_EU: np.ndarray) -> np.ndarray:
    """d = Em / x  (employment per unit of output; zero where x=0)."""
    d = np.where(x_EU > 0, em_EU / x_EU, 0.0)
    log.info(f"d: min={d.min():.6f}, max={d.max():.6f}, zeros={np.sum(d==0)}")
    return d


# ── Employment content ────────────────────────────────────────────────────────

def compute_employment_content(
    d: np.ndarray,
    L: np.ndarray,
    e_nonEU: np.ndarray,
    eu_countries: list,
    cpa_codes: list,
) -> dict:
    """
    Compute employment content of exports: em = d * (L @ e).

    Returns:
        em_exports_total:    (NP,) array — jobs at each (country, industry) cell
        em_country_matrix:   (N, N) array — em_country_matrix[r, s] = jobs in
                             country r supported by exports from country s
    """
    N = len(eu_countries)
    P = len(cpa_codes)

    em_exports_total = d * (L @ e_nonEU)
    log.info(f"Total EU export-supported employment: {em_exports_total.sum():.0f} thousand persons")

    # Build E_mat: column s = country s's export vector (NP × N)
    E_mat = np.zeros((N * P, N), dtype=np.float64)
    for s_idx in range(N):
        E_mat[s_idx * P:(s_idx + 1) * P, s_idx] = e_nonEU[s_idx * P:(s_idx + 1) * P]

    LE = L @ E_mat  # (NP, N)

    D = d.reshape(N, P)                    # (N, P)
    LE_reshaped = LE.reshape(N, P, N)      # (r_country, r_prod, s_country)
    em_country_matrix = np.einsum('rp,rps->rs', D, LE_reshaped)

    log.info(f"Country matrix total: {em_country_matrix.sum():.0f}")
    return {"em_exports_total": em_exports_total, "em_country_matrix": em_country_matrix}


# ── Model validation ──────────────────────────────────────────────────────────

def validate_model(A: np.ndarray, L: np.ndarray) -> dict:
    """
    Sanity checks for the Leontief model.

    Returns a dict with:
        max_col_sum, n_col_sums_ge1, n_negative_L, n_diag_lt1, identity_residual
    """
    N = A.shape[0]
    col_sums = A.sum(axis=0)
    identity_residual = float(np.max(np.abs(L @ (np.eye(N) - A) - np.eye(N))))
    checks = {
        "max_col_sum": float(col_sums.max()),
        "n_col_sums_ge1": int(np.sum(col_sums >= 1.0)),
        "n_negative_L": int(np.sum(L < -1e-10)),
        "n_diag_lt1": int(np.sum(np.diag(L) < 1.0 - 1e-10)),
        "identity_residual": identity_residual,
    }
    log.info(f"Model checks: {checks}")
    return checks


# ── Decompositions ────────────────────────────────────────────────────────────

def compute_domestic_spillover(
    eu_countries: list,
    N: int,
    P: int,
    e: np.ndarray,
    em: np.ndarray,
    em_mat: np.ndarray,
    d: np.ndarray,
) -> list[dict]:
    """
    Country-level domestic / spillover decomposition.

    Returns a list of row dicts (one per country) with keys:
        country, total_employment_THS, domestic_effect_THS, spillover_received_THS,
        spillover_generated_THS, direct_effect_THS, indirect_effect_THS,
        total_in_country_THS, total_by_country_THS,
        export_emp_share_pct, domestic_share_pct, spillover_share_pct
    """
    rows = []
    for r_idx, r in enumerate(eu_countries):
        r_start, r_end = r_idx * P, (r_idx + 1) * P
        d_r = d[r_start:r_end]
        e_r = e[r_start:r_end]
        total_emp_r = em[r_start:r_end].sum()

        domestic = em_mat[r_idx, r_idx]
        spillover_received = em_mat[r_idx, :].sum() - domestic
        spillover_generated = em_mat[:, r_idx].sum() - domestic
        total_in_r = em_mat[r_idx, :].sum()
        total_by_r = em_mat[:, r_idx].sum()
        direct = float(np.dot(d_r, e_r))
        indirect = domestic - direct

        rows.append({
            "country": r,
            "total_employment_THS": total_emp_r,
            "domestic_effect_THS": domestic,
            "spillover_received_THS": spillover_received,
            "spillover_generated_THS": spillover_generated,
            "direct_effect_THS": direct,
            "indirect_effect_THS": indirect,
            "total_in_country_THS": total_in_r,
            "total_by_country_THS": total_by_r,
            "export_emp_share_pct": total_in_r / total_emp_r * 100 if total_emp_r > 0 else 0,
            "domestic_share_pct": domestic / total_by_r * 100 if total_by_r > 0 else 0,
            "spillover_share_pct": spillover_generated / total_by_r * 100 if total_by_r > 0 else 0,
        })
    return rows


def compute_industry_decomposition(
    L: np.ndarray,
    d: np.ndarray,
    e: np.ndarray,
    em_mat: np.ndarray,
    eu_countries: list,
    N: int,
    P: int,
    agg: dict,
) -> tuple[np.ndarray, list[dict]]:
    """
    Industry-level decomposition using a sector aggregation scheme.

    agg: dict mapping sector_name → list of 1-based product indices
         e.g. {"Manufacturing": [3,4,5,...], "Services": [45,46,...]}

    Returns:
        table4:    (n_sectors, n_sectors) ndarray — rows = producing sector, cols = exporting sector
        fig3_rows: list of dicts with sector, total_employment_THS, domestic_THS, spillover_THS
    """
    sector_names = list(agg.keys())
    n_sectors = len(sector_names)

    # Flat (0-based) column indices for each sector across all countries
    sector_flat: dict[str, np.ndarray] = {
        sec: np.array([c * P + (p - 1) for c in range(N) for p in prods], dtype=int)
        for sec, prods in agg.items()
    }

    # Table 4: em_by_sector[j] = d * (L[:, j_cols] @ e[j_cols])
    table4 = np.zeros((n_sectors, n_sectors), dtype=np.float64)
    em_by_sector: dict[str, np.ndarray] = {}
    for j_sec in sector_names:
        j_cols = sector_flat[j_sec]
        e_sub = e[j_cols]
        if e_sub.sum() == 0:
            em_by_sector[j_sec] = np.zeros(N * P)
        else:
            em_by_sector[j_sec] = d * (L[:, j_cols] @ e_sub)

    for j_idx, j_sec in enumerate(sector_names):
        em_j = em_by_sector[j_sec]
        for i_idx, i_sec in enumerate(sector_names):
            table4[i_idx, j_idx] = em_j[sector_flat[i_sec]].sum()

    # Figure 3: vectorized domestic vs spillover
    D = d.reshape(N, P)                                                     # (N, P)
    E_mat = e.reshape(N, P)
    L_diag_blocks = np.stack([L[c * P:(c + 1) * P, c * P:(c + 1) * P] for c in range(N)])  # (N, P, P)
    d_L_diag = np.einsum('cp,cpq->cq', D, L_diag_blocks)                   # (N, P)

    fig3_rows = []
    for j_idx, j_sec in enumerate(sector_names):
        col_total = table4[:, j_idx].sum()
        j_prods_0 = [p - 1 for p in agg[j_sec]]
        domestic_j = float((E_mat[:, j_prods_0] * d_L_diag[:, j_prods_0]).sum())
        fig3_rows.append({
            "sector": j_sec,
            "total_employment_THS": col_total,
            "domestic_THS": domestic_j,
            "spillover_THS": col_total - domestic_j,
        })

    return table4, fig3_rows
