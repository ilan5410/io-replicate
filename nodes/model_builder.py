"""
Stage 3: Model Builder (DETERMINISTIC — no LLM)
Builds the Leontief model and computes employment content of exports.
Parameterized entirely by replication_spec — no hardcoded dimensions.
"""
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from rich.console import Console
from rich.panel import Panel

from agents.state import PipelineState
from skills.leontief import (
    build_technical_coefficients,
    build_leontief_inverse,
    build_employment_coefficients,
    compute_employment_content,
    validate_model,
)

# Private aliases — preserved for backward compatibility with existing tests
_build_technical_coefficients = build_technical_coefficients
_build_leontief_inverse = build_leontief_inverse
_build_employment_coefficients = build_employment_coefficients
_compute_employment_content = compute_employment_content
_validate_model = validate_model

log = logging.getLogger("model_builder")
_console = Console()


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

    _console.print(Panel(
        f"[bold]Stage 3 — Model Builder[/bold]  (deterministic)\n"
        f"Building A and L matrices — {len(eu_countries)} countries × {len(cpa_codes)} industries = {N:,} cells",
        style="blue"
    ))
    log.info(f"Model Builder: {len(eu_countries)} countries × {len(cpa_codes)} industries = {N}")

    # Load prepared matrices
    with open(prepared_paths["metadata"]) as f:
        meta = json.load(f)

    Z_EU = pd.read_csv(prepared_paths["Z_EU"], index_col=0).values.astype(np.float64)
    e_nonEU = pd.read_csv(prepared_paths["e_nonEU"])["e_nonEU_MIO_EUR"].values.astype(np.float64)
    x_EU = pd.read_csv(prepared_paths["x_EU"])["x_EU_MIO_EUR"].values.astype(np.float64)
    em_EU = pd.read_csv(prepared_paths["Em_EU"])["em_EU_THS_PER"].values.astype(np.float64)

    # Build model
    _console.print("  Computing technical coefficients (A)...")
    A = _build_technical_coefficients(Z_EU, x_EU)
    _console.print("  Computing Leontief inverse (L = (I-A)⁻¹)...")
    L = _build_leontief_inverse(A)
    d = _build_employment_coefficients(x_EU, em_EU)

    # Compute employment content
    _console.print("  Computing employment content of exports...")
    results = _compute_employment_content(d, L, e_nonEU, eu_countries, cpa_codes)

    # Run validation checks
    checks = _validate_model(A, L)
    _console.print(
        f"[green]✓[/green] Stage 3 complete — "
        f"EU export employment: {results['em_exports_total'].sum():,.0f} thousand persons"
    )

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



def _save_outputs(A, L, d, results, model_dir, eu_countries, cpa_codes, row_labels) -> dict:
    paths = {}

    # Save large matrices as binary .npy — 10x faster I/O, ~4x smaller than CSV
    p = model_dir / "A_EU.npy"
    np.save(p, A)
    paths["A_EU"] = str(p)

    p = model_dir / "L_EU.npy"
    np.save(p, L)
    paths["L_EU"] = str(p)

    # Keep row labels for downstream reference
    pd.DataFrame({"label": row_labels}).to_csv(model_dir / "labels.csv", index=False)

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
