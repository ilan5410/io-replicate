"""
Stage 4: Decomposer (DETERMINISTIC — no LLM)
Decomposes employment content into domestic/spillover and direct/indirect components.
Reads decomposition types from replication_spec — no hardcoded logic.
"""
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from agents.state import PipelineState

log = logging.getLogger("decomposer")


def decomposer_node(state: PipelineState) -> dict:
    """LangGraph node: deterministic employment content decomposition."""
    spec = state["replication_spec"]
    run_dir = Path(state["run_dir"])
    model_paths = state["model_paths"]
    prepared_paths = state["prepared_data_paths"]
    decomp_dir = run_dir / "data" / "decomposition"
    decomp_dir.mkdir(parents=True, exist_ok=True)

    eu_countries = [e["code"] for e in spec["geography"]["analysis_entities"]]
    cpa_codes = [i["code"] for i in spec["classification"]["industry_list"]]
    N = len(eu_countries)
    P = len(cpa_codes)

    # Load data
    e_nonEU = pd.read_csv(prepared_paths["e_nonEU"])["e_nonEU_MIO_EUR"].values.astype(np.float64)
    em_EU = pd.read_csv(prepared_paths["Em_EU"])["em_EU_THS_PER"].values.astype(np.float64)
    L = pd.read_csv(model_paths["L_EU"], index_col=0).values.astype(np.float64)
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
    N_EU = N * P

    table4 = np.zeros((n_sectors, n_sectors), dtype=np.float64)

    for j_sec_idx, j_sec in enumerate(sector_names):
        j_prods = [idx - 1 for idx in agg[j_sec]]
        e_j = np.zeros(N_EU, dtype=np.float64)
        for c_idx in range(N):
            for p_idx in j_prods:
                e_j[c_idx * P + p_idx] = e[c_idx * P + p_idx]
        if e_j.sum() == 0:
            continue
        em_j = d * (L @ e_j)
        for i_sec_idx, i_sec in enumerate(sector_names):
            i_prods = [idx - 1 for idx in agg[i_sec]]
            total = sum(em_j[c_idx * P + p_idx] for c_idx in range(N) for p_idx in i_prods)
            table4[i_sec_idx, j_sec_idx] = total

    table4_df = pd.DataFrame(table4, index=sector_names, columns=sector_names)

    # Figure 3 data
    fig3_rows = []
    for j_sec_idx, j_sec in enumerate(sector_names):
        j_prods = [idx - 1 for idx in agg[j_sec]]
        col_total = table4[:, j_sec_idx].sum()
        domestic_j = 0.0
        for c_idx in range(N):
            for p_idx in j_prods:
                flat = c_idx * P + p_idx
                if e[flat] > 0:
                    c_start, c_end = c_idx * P, (c_idx + 1) * P
                    e_cp = np.zeros(N_EU)
                    e_cp[flat] = e[flat]
                    Le_cp = L @ e_cp
                    domestic_j += np.dot(d[c_start:c_end], Le_cp[c_start:c_end])
        fig3_rows.append({
            "sector": j_sec,
            "total_employment_THS": col_total,
            "domestic_THS": domestic_j,
            "spillover_THS": col_total - domestic_j,
        })

    return table4_df, pd.DataFrame(fig3_rows)
