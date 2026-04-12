"""
Base types for IO parser skills.

Every parser returns a PreparedMatrices object so Stage 2 can save it
identically regardless of which database was used.
"""
from dataclasses import dataclass

import numpy as np


@dataclass
class PreparedMatrices:
    """Standardized output of any IO parser.

    All arrays are ordered (country_0_industry_0, ..., country_0_industry_P,
    country_1_industry_0, ...) — i.e. countries vary slowest, industries fastest.
    """
    Z: np.ndarray        # (N, N) intermediate-use matrix, domestic EU flows only
    e: np.ndarray        # (N,)   export vector (per methodology.export_definition)
    x: np.ndarray        # (N,)   total output
    Em: np.ndarray       # (N,)   employment (or chosen satellite factor)
    labels: list         # length N  ["AT_A01", "AT_A02", ...]
    eu_codes: list       # length N_c
    cpa_codes: list      # length N_i (normalized, e.g. "A01" not "CPA_A01")
    year: int
    source: str          # e.g. "figaro_iciot", "wiod_mrio"
