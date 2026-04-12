"""
Tests for decomposer — verifies accounting identities on synthetic data.

Key identity: for every country and every sector,
  domestic + spillover = total

Also verifies the vectorized Figure 3 computation matches the naive loop.
"""
import numpy as np
import pandas as pd
import pytest

import pandas as pd

from skills.leontief import (
    compute_domestic_spillover as _compute_domestic_spillover_raw,
    compute_industry_decomposition as _compute_industry_decomposition_raw,
    build_employment_coefficients as _build_employment_coefficients,
    build_leontief_inverse as _build_leontief_inverse,
    build_technical_coefficients as _build_technical_coefficients,
)


def _compute_domestic_spillover(eu_countries, N, P, e, em, em_mat, d):
    return pd.DataFrame(_compute_domestic_spillover_raw(eu_countries, N, P, e, em, em_mat, d))


def _compute_industry_decomposition(L, d, e, em_mat, eu_countries, N, P, agg):
    sector_names = list(agg.keys())
    table4_arr, fig3_rows = _compute_industry_decomposition_raw(L, d, e, em_mat, eu_countries, N, P, agg)
    return (
        pd.DataFrame(table4_arr, index=sector_names, columns=sector_names),
        pd.DataFrame(fig3_rows),
    )

# ── Synthetic 2-country × 2-product system ────────────────────────────────────
Z = np.array([
    [1.0, 2.0, 0.5, 0.5],
    [0.5, 3.0, 1.0, 0.5],
    [0.5, 0.5, 1.5, 1.0],
    [1.0, 1.0, 0.5, 4.0],
], dtype=np.float64)
x = np.array([10.0, 20.0, 12.0, 16.0], dtype=np.float64)
em_vals = np.array([100.0, 150.0, 80.0, 200.0], dtype=np.float64)
e = np.array([1.0, 2.0, 0.5, 1.5], dtype=np.float64)

EU_COUNTRIES = ["X", "Y"]
CPA_CODES = ["1", "2"]
N, P = 2, 2

# Aggregation: 2 sectors, each covers 1 product (1-based indices)
AGG = {"S1": [1], "S2": [2]}


@pytest.fixture(scope="module")
def model():
    A = _build_technical_coefficients(Z, x)
    L = _build_leontief_inverse(A)
    d = _build_employment_coefficients(x, em_vals)
    em_country_matrix = np.zeros((N, N))
    for s in range(N):
        e_s = np.zeros(N * P)
        e_s[s * P:(s + 1) * P] = e[s * P:(s + 1) * P]
        Le_s = L @ e_s
        for r in range(N):
            em_country_matrix[r, s] = np.dot(d[r * P:(r + 1) * P], Le_s[r * P:(r + 1) * P])
    return L, d, em_country_matrix


# ── Country decomposition identity tests ─────────────────────────────────────

def test_domestic_plus_spillover_received_equals_total_in(model):
    """domestic + spillover_received == total_in_country for every country."""
    L, d, em_mat = model
    df = _compute_domestic_spillover(EU_COUNTRIES, N, P, e, em_vals, em_mat, d)
    for _, row in df.iterrows():
        expected = row["total_in_country_THS"]
        actual = row["domestic_effect_THS"] + row["spillover_received_THS"]
        assert abs(actual - expected) < 1e-6, (
            f"{row['country']}: domestic+spillover_received={actual:.4f} != total_in={expected:.4f}"
        )


def test_domestic_share_plus_spillover_share_eq_100(model):
    """domestic_share_pct + spillover_share_pct ≈ 100 for every country."""
    L, d, em_mat = model
    df = _compute_domestic_spillover(EU_COUNTRIES, N, P, e, em_vals, em_mat, d)
    for _, row in df.iterrows():
        total = row["domestic_share_pct"] + row["spillover_share_pct"]
        assert abs(total - 100.0) < 1e-6, (
            f"{row['country']}: domestic_share + spillover_share = {total:.4f} != 100"
        )


def test_direct_plus_indirect_equals_domestic(model):
    """direct + indirect == domestic_effect for every country."""
    L, d, em_mat = model
    df = _compute_domestic_spillover(EU_COUNTRIES, N, P, e, em_vals, em_mat, d)
    for _, row in df.iterrows():
        total = row["direct_effect_THS"] + row["indirect_effect_THS"]
        assert abs(total - row["domestic_effect_THS"]) < 1e-6, (
            f"{row['country']}: direct+indirect={total:.4f} != domestic={row['domestic_effect_THS']:.4f}"
        )


# ── Industry decomposition identity tests ─────────────────────────────────────

def test_table4_column_sum_matches_figure3_total(model):
    """table4 column sums should equal figure3 total_employment_THS for each sector."""
    L, d, em_mat = model
    table4_df, fig3_df = _compute_industry_decomposition(L, d, e, em_mat, EU_COUNTRIES, N, P, AGG)
    for sec in AGG:
        col_total = table4_df[sec].sum()
        fig3_total = fig3_df.loc[fig3_df["sector"] == sec, "total_employment_THS"].iloc[0]
        assert abs(col_total - fig3_total) < 1e-6, (
            f"Sector {sec}: table4 col sum={col_total:.4f} != fig3 total={fig3_total:.4f}"
        )


def test_domestic_plus_spillover_equals_total_sector(model):
    """For every sector: domestic_THS + spillover_THS == total_employment_THS."""
    L, d, em_mat = model
    _, fig3_df = _compute_industry_decomposition(L, d, e, em_mat, EU_COUNTRIES, N, P, AGG)
    for _, row in fig3_df.iterrows():
        total = row["domestic_THS"] + row["spillover_THS"]
        assert abs(total - row["total_employment_THS"]) < 1e-6, (
            f"Sector {row['sector']}: domestic+spillover={total:.4f} != total={row['total_employment_THS']:.4f}"
        )


def test_vectorized_figure3_matches_naive_loop(model):
    """Vectorized domestic computation must match the original per-column matmul."""
    L, d, em_mat = model
    _, fig3_vec = _compute_industry_decomposition(L, d, e, em_mat, EU_COUNTRIES, N, P, AGG)

    # Naive reference
    N_EU = N * P
    naive_domestic = {}
    for j_sec, j_prods in AGG.items():
        j_prods_0 = [p - 1 for p in j_prods]
        domestic_j = 0.0
        for c_idx in range(N):
            for p_idx in j_prods_0:
                flat = c_idx * P + p_idx
                if e[flat] > 0:
                    c_start, c_end = c_idx * P, (c_idx + 1) * P
                    e_cp = np.zeros(N_EU)
                    e_cp[flat] = e[flat]
                    Le_cp = L @ e_cp
                    domestic_j += np.dot(d[c_start:c_end], Le_cp[c_start:c_end])
        naive_domestic[j_sec] = domestic_j

    for _, row in fig3_vec.iterrows():
        sec = row["sector"]
        assert abs(row["domestic_THS"] - naive_domestic[sec]) < 1e-8, (
            f"Sector {sec}: vectorized={row['domestic_THS']:.6f} vs naive={naive_domestic[sec]:.6f}"
        )
