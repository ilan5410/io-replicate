"""
skills.leontief — Pure Leontief input–output math.

Importable independently of the agent framework:

    from skills.leontief import (
        build_technical_coefficients,
        build_leontief_inverse,
        build_employment_coefficients,
        compute_employment_content,
        validate_model,
        compute_domestic_spillover,
        compute_industry_decomposition,
    )
"""
from .core import (
    build_technical_coefficients,
    build_leontief_inverse,
    build_employment_coefficients,
    compute_employment_content,
    validate_model,
    compute_domestic_spillover,
    compute_industry_decomposition,
)

__all__ = [
    "build_technical_coefficients",
    "build_leontief_inverse",
    "build_employment_coefficients",
    "compute_employment_content",
    "validate_model",
    "compute_domestic_spillover",
    "compute_industry_decomposition",
]
