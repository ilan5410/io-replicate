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
from skills.leontief import compute_domestic_spillover, compute_industry_decomposition

# Private aliases — preserved for backward compatibility with existing tests
_compute_domestic_spillover = compute_domestic_spillover
_compute_industry_decomposition = compute_industry_decomposition

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

    # Resolve file paths: prefer state-provided paths, fall back to convention-based
    # paths under run_dir so that --start-stage 4 works without stages 2/3 in state.
    prepared_dir = run_dir / "data" / "prepared"
    model_dir = run_dir / "data" / "model"

    def _prepared(key: str, filename: str) -> Path:
        return Path(prepared_paths[key]) if key in prepared_paths else prepared_dir / filename

    def _model(key: str, filename: str) -> Path:
        return Path(model_paths[key]) if key in model_paths else model_dir / filename

    # Load data
    e_nonEU = pd.read_csv(_prepared("e_nonEU", "e_nonEU.csv"))["e_nonEU_MIO_EUR"].values.astype(np.float64)
    em_EU = pd.read_csv(_prepared("Em_EU", "Em_EU.csv"))["em_EU_THS_PER"].values.astype(np.float64)
    # L_EU stored as .npy (binary) — fast load, ~4x smaller than CSV
    L = np.load(_model("L_EU", "L_EU.npy"))
    d = pd.read_csv(_model("d_EU", "d_EU.csv"))["d_THS_PER_per_MIO_EUR"].values.astype(np.float64)
    em_mat = pd.read_csv(_model("em_exports_country_matrix", "em_exports_country_matrix.csv"), index_col=0).values.astype(np.float64)

    paths = {}
    decomp_names = [dec["name"] for dec in spec.get("decompositions", [])]

    # Country domestic/spillover decomposition (always computed — needed for outputs)
    decomp_rows = compute_domestic_spillover(eu_countries, N, P, e_nonEU, em_EU, em_mat, d)
    p = decomp_dir / "country_decomposition.csv"
    pd.DataFrame(decomp_rows).to_csv(p, index=False)
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
        sector_names = list(agg.keys())
        table4_arr, fig3_rows = compute_industry_decomposition(
            L, d, e_nonEU, em_mat, eu_countries, N, P, agg
        )
        p = decomp_dir / "industry_table4.csv"
        pd.DataFrame(table4_arr, index=sector_names, columns=sector_names).to_csv(p)
        paths["industry_table4"] = str(p)

        p = decomp_dir / "industry_figure3.csv"
        pd.DataFrame(fig3_rows).to_csv(p, index=False)
        paths["industry_figure3"] = str(p)

    log.info(f"Decomposition outputs saved to {decomp_dir}")
    _console.print(f"[green]✓[/green] Stage 4 complete — {len(paths)} decomposition files saved")
    return {
        "decomposition_paths": paths,
        "decomposition_valid": True,
        "current_stage": 4,
    }


