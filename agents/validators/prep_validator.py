"""
Deterministic validator for prepared matrices (Stage 2 output).
Checks dimensions, non-negativity, balance, and employment totals.
"""
import json
from pathlib import Path

import numpy as np
import pandas as pd


def validate_prepared_data(
    prepared_data_paths: dict,
    replication_spec: dict,
) -> tuple[bool, list[str]]:
    """
    Validate that the prepared matrices satisfy structural requirements.

    Args:
        prepared_data_paths: dict with keys Z_EU, e_nonEU, x_EU, Em_EU, metadata
        replication_spec: the full parsed spec dict

    Returns:
        (is_valid, errors)
    """
    errors = []

    # Derive expected dimensions from spec
    n_countries = len(replication_spec["geography"]["analysis_entities"])
    n_industries = replication_spec["classification"]["n_industries"]
    expected_dim = n_countries * n_industries

    # Load metadata
    meta_path = Path(prepared_data_paths.get("metadata", ""))
    if not meta_path.exists():
        errors.append(f"metadata.json not found at {meta_path}")
        return False, errors

    with open(meta_path) as f:
        meta = json.load(f)

    actual_n_countries = len(meta.get("eu_countries", []))
    actual_n_industries = meta.get("n_industries", 0)
    actual_dim = meta.get("n_total", 0)

    if actual_n_countries != n_countries:
        errors.append(
            f"Country count mismatch: metadata has {actual_n_countries}, spec expects {n_countries}"
        )
    if actual_n_industries != n_industries:
        errors.append(
            f"Industry count mismatch: metadata has {actual_n_industries}, spec expects {n_industries}"
        )

    # Load matrices
    matrices = {}
    for key, col_name in [
        ("Z_EU", None),
        ("e_nonEU", "e_nonEU_MIO_EUR"),
        ("x_EU", "x_EU_MIO_EUR"),
        ("Em_EU", "em_EU_THS_PER"),
    ]:
        path = Path(prepared_data_paths.get(key, ""))
        if not path.exists():
            errors.append(f"{key} file not found at {path}")
            continue
        try:
            if col_name:
                arr = pd.read_csv(path)[col_name].values.astype(np.float64)
            else:
                arr = pd.read_csv(path, index_col=0).values.astype(np.float64)
            matrices[key] = arr
        except Exception as e:
            errors.append(f"Failed to load {key}: {e}")

    if errors:
        return False, errors

    Z = matrices["Z_EU"]
    e = matrices["e_nonEU"]
    x = matrices["x_EU"]
    Em = matrices["Em_EU"]

    # Dimension checks
    if Z.shape != (expected_dim, expected_dim):
        errors.append(
            f"Z_EU shape {Z.shape} != expected ({expected_dim}, {expected_dim}). "
            f"This usually means a country in the spec wasn't found in the raw data."
        )
    for name, arr in [("e_nonEU", e), ("x_EU", x), ("Em_EU", Em)]:
        if arr.shape[0] != expected_dim:
            errors.append(f"{name} length {arr.shape[0]} != expected {expected_dim}")

    # Non-negativity checks
    if np.any(Z < -1e-6):
        n_neg = np.sum(Z < -1e-6)
        errors.append(f"Z_EU has {n_neg} negative values (min={Z.min():.4f})")
    if np.any(e < -1e-6):
        errors.append(f"e_nonEU has {np.sum(e < -1e-6)} negative values")
    if np.any(x < -1e-6):
        errors.append(f"x_EU has {np.sum(x < -1e-6)} negative values")
    if np.any(Em < -1e-6):
        errors.append(f"Em_EU has {np.sum(Em < -1e-6)} negative values")

    # Balance check: total output ≈ total input (column sums of Z + value added ≈ x)
    # We check that x is consistent with Z: x should not be less than Z column sums
    if Z.shape == (expected_dim, expected_dim) and x.shape[0] == expected_dim:
        z_col_sums = Z.sum(axis=0)
        inconsistent = np.sum(z_col_sums > x + 1e-3)
        if inconsistent > 5:
            errors.append(
                f"{inconsistent} sectors have Z column sums > x (possible balance violation)"
            )

    # Employment benchmark check (50% tolerance — loose, just catches gross errors)
    benchmark_values = replication_spec.get("benchmarks", {}).get("values", [])
    for bv in benchmark_values:
        if "total employment" in bv["name"].lower() and bv["unit"] == "thousands":
            expected_emp = bv["expected"]
            actual_emp = Em.sum()
            rel_error = abs(actual_emp - expected_emp) / expected_emp
            if rel_error > 0.5:
                errors.append(
                    f"Employment total {actual_emp:.0f} is >50% off from benchmark {expected_emp:.0f}. "
                    f"Possible data loading error."
                )
            break

    return len(errors) == 0, errors
