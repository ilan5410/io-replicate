"""
Stage 4: Decomposer (DETERMINISTIC — no LLM)
Decomposes employment content into domestic/spillover and direct/indirect components.
Reads decomposition types from replication_spec — no hardcoded logic.
"""
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from rich.console import Console
from rich.panel import Panel

from agents.state import PipelineState

log = logging.getLogger("decomposer")
_console = Console()


def decomposer_node(state: PipelineState) -> dict:
    """LangGraph node: deterministic employment content decomposition."""
    spec = state["replication_spec"]
    run_dir = Path(state["run_dir"])
    model_paths = state["model_paths"]
    prepared_paths = state["prepared_data_paths"]
    decomp_dir = run_dir / "data" / "decomposition"
    decomp_dir.mkdir(parents=True, exist_ok=True)

    _console.print(Panel(
        "[bold]Stage 4 — Decomposer[/bold]  (deterministic)\n"
        "Domestic / spillover / direct / indirect employment decomposition",
        style="blue"
    ))

    eu_countries = [e["code"] for e in spec["geography"]["analysis_entities"]]
    cpa_codes = [i["code"] for i in spec["classification"]["industry_list"]]
    N = len(eu_countries)
    P = len(cpa_codes)

    # Load data
    e_nonEU = pd.read_csv(prepared_paths["e_nonEU"])["e_nonEU_MIO_EUR"].values.astype(np.float64)
    em_EU = pd.read_csv(prepared_paths["Em_EU"])["em_EU_THS_PER"].values.astype(np.float64)
    # L_EU stored as .npy (binary) — fast load, ~4x smaller than CSV
    L = np.load(model_paths["L_EU"])
    d = pd.read_csv(model_paths["d_EU"])["d_THS_PER_per_MIO_EUR"].values.astype(np.float64)
    em_mat = pd.read_csv(model_paths["em_exports_country_matrix"], index_col=0).values.astype(np.float64)

    paths = {}
    decomp_names = [dec["name"] for dec in spec.get("decompositions", [])]

    # Country domestic/spillover decomposition (always computed — needed for outputs)
    decomp_df = _compute_domestic_spillover(eu_countries, N, P, e_nonEU, em_EU, em_mat, d)
    p = decomp_dir / "country_decomposition.csv"
    decomp_df.to_csv(p, index=False)
    paths["country_decomposition"] = str(p)

    # 28×28 annex matrix
    p = decomp_dir / "annex_c_matrix.csv"
    pd.DataFrame(em_mat, index=eu_countries, columns=eu_countries).to_csv(p)
    paths["annex_c_matrix"] = str(p)

    # Industry decomposition (10-sector) — requires aggregations in spec
    agg_schemes = spec["classification"].get("aggregations", {})
    if agg_schemes:
        agg_name = list(agg_schemes.keys())[0]  # use first aggregation scheme
        agg = agg_schemes[agg_name]
        table4_df, fig3_df = _compute_industry_decomposition(
            L, d, e_nonEU, em_mat, eu_countries, N, P, agg
        )
        p = decomp_dir / "industry_table4.csv"
        table4_df.to_csv(p)
        paths["industry_table4"] = str(p)

        p = decomp_dir / "industry_figure3.csv"
        fig3_df.to_csv(p, index=False)
        paths["industry_figure3"] = str(p)

    log.info(f"Decomposition outputs saved to {decomp_dir}")
    _console.print(f"[green]✓[/green] Stage 4 complete — {len(paths)} decomposition files saved")
    return {
        "decomposition_paths": paths,
        "decomposition_valid": True,
        "current_stage": 4,
    }


def _compute_domestic_spillover(eu_countries, N, P, e, em, em_mat, d) -> pd.DataFrame:
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
    return pd.DataFrame(rows)


def _compute_industry_decomposition(L, d, e, em_mat, eu_countries, N, P, agg) -> tuple:
    sector_names = list(agg.keys())
    n_sectors = len(sector_names)

    # Precompute per-sector flat (0-based) column indices
    sector_flat: dict[str, np.ndarray] = {
        sec: np.array([c * P + (p - 1) for c in range(N) for p in prods], dtype=int)
        for sec, prods in agg.items()
    }

    # --- Table 4 ---
    # em_by_sector[j] = d * (L[:, j_cols] @ e[j_cols])  — one matmul per sector
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

    table4_df = pd.DataFrame(table4, index=sector_names, columns=sector_names)

    # --- Figure 3: domestic vs spillover ---
    # Precompute d_L_diag[c, p] = D[c] @ L_diag_block[c][:, p]
    # Represents the domestic employment generated by one unit of exports from (c, p).
    # Replaces the O(N * |j_prods|) per-sector matmul loop with a one-time (N,P,P) computation.
    D = d.reshape(N, P)
    E_mat = e.reshape(N, P)
    L_diag_blocks = np.stack([L[c * P:(c + 1) * P, c * P:(c + 1) * P] for c in range(N)])  # (N, P, P)
    d_L_diag = np.einsum('cp,cpq->cq', D, L_diag_blocks)  # (N, P)

    fig3_rows = []
    for j_idx, j_sec in enumerate(sector_names):
        col_total = table4[:, j_idx].sum()
        j_prods_0 = [p - 1 for p in agg[j_sec]]
        # domestic = sum over (country, product) of E_mat[c,p] * d_L_diag[c,p]
        domestic_j = float((E_mat[:, j_prods_0] * d_L_diag[:, j_prods_0]).sum())
        fig3_rows.append({
            "sector": j_sec,
            "total_employment_THS": col_total,
            "domestic_THS": domestic_j,
            "spillover_THS": col_total - domestic_j,
        })

    return table4_df, pd.DataFrame(fig3_rows)
