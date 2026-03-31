"""
Tests for model_builder — verifies Leontief math on a small synthetic system.

System: 2 countries (X, Y) × 2 products (1, 2)  →  NP = 4
All values chosen so that A column sums < 1 (productive economy).
"""
import numpy as np
import pytest

from nodes.model_builder import (
    _build_employment_coefficients,
    _build_leontief_inverse,
    _build_technical_coefficients,
    _compute_employment_content,
)

# ── Synthetic data ─────────────────────────────────────────────────────────────
# Z[i,j] = intermediate demand from sector i to sector j (MIO_EUR)
Z = np.array([
    [1.0, 2.0, 0.5, 0.5],
    [0.5, 3.0, 1.0, 0.5],
    [0.5, 0.5, 1.5, 1.0],
    [1.0, 1.0, 0.5, 4.0],
], dtype=np.float64)

x = np.array([10.0, 20.0, 12.0, 16.0], dtype=np.float64)   # total output
em = np.array([100.0, 150.0, 80.0, 200.0], dtype=np.float64)  # employment (THS)
e = np.array([1.0, 2.0, 0.5, 1.5], dtype=np.float64)          # exports to non-EU


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def model():
    A = _build_technical_coefficients(Z, x)
    L = _build_leontief_inverse(A)
    d = _build_employment_coefficients(x, em)
    return A, L, d


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_technical_coefficients_column_sums_lt1(model):
    A, _, _ = model
    col_sums = A.sum(axis=0)
    assert (col_sums < 1.0).all(), f"Some A column sums >= 1: {col_sums}"


def test_technical_coefficients_shape(model):
    A, _, _ = model
    assert A.shape == (4, 4)


def test_leontief_inverse_is_identity(model):
    A, L, _ = model
    I_minus_A = np.eye(4) - A
    residual = np.max(np.abs(L @ I_minus_A - np.eye(4)))
    assert residual < 1e-10, f"L · (I-A) ≈ I violated: max residual = {residual:.2e}"


def test_leontief_diagonal_ge1(model):
    _, L, _ = model
    diag = np.diag(L)
    assert (diag >= 1.0 - 1e-10).all(), f"Some L diagonal elements < 1: {diag}"


def test_leontief_non_negative(model):
    _, L, _ = model
    assert (L >= -1e-10).all(), f"Negative L elements found: min={L.min():.4f}"


def test_employment_coefficients(model):
    _, _, d = model
    expected = em / x
    np.testing.assert_allclose(d, expected, rtol=1e-10)


def test_employment_content_total_positive(model):
    A, L, d = model
    eu_countries = ["X", "Y"]
    cpa_codes = ["1", "2"]
    results = _compute_employment_content(d, L, e, eu_countries, cpa_codes)
    total = results["em_exports_total"].sum()
    assert total > 0, "Total export employment should be positive"


def test_country_matrix_sums_to_total(model):
    """em_country_matrix columns should sum to em_exports_total (rounded)."""
    A, L, d = model
    eu_countries = ["X", "Y"]
    cpa_codes = ["1", "2"]
    results = _compute_employment_content(d, L, e, eu_countries, cpa_codes)
    total_from_vector = results["em_exports_total"].sum()
    total_from_matrix = results["em_country_matrix"].sum()
    np.testing.assert_allclose(total_from_matrix, total_from_vector, rtol=1e-6,
                               err_msg="Country matrix total != em_exports_total sum")


def test_vectorized_matches_scalar():
    """Verify vectorized country matrix gives same result as naive loop."""
    A = _build_technical_coefficients(Z, x)
    L = _build_leontief_inverse(A)
    d = _build_employment_coefficients(x, em)
    eu_countries = ["X", "Y"]
    cpa_codes = ["1", "2"]
    N, P = 2, 2

    # Vectorized result
    results = _compute_employment_content(d, L, e, eu_countries, cpa_codes)
    em_mat_vec = results["em_country_matrix"]

    # Naive loop reference
    em_mat_ref = np.zeros((N, N))
    for s in range(N):
        e_s = np.zeros(N * P)
        e_s[s * P:(s + 1) * P] = e[s * P:(s + 1) * P]
        Le_s = L @ e_s
        for r in range(N):
            em_mat_ref[r, s] = np.dot(d[r * P:(r + 1) * P], Le_s[r * P:(r + 1) * P])

    np.testing.assert_allclose(em_mat_vec, em_mat_ref, rtol=1e-10,
                               err_msg="Vectorized country matrix differs from naive loop")
