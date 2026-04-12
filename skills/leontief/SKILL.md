# Skill: leontief

Pure Leontief input–output mathematics. No LLM, no agent framework.

## When to use

Call these functions when you need to:
- Build the technical coefficients matrix A from Z and x
- Invert (I−A) to get the Leontief inverse L
- Compute employment content of exports: `d · L · e`
- Decompose employment into domestic vs spillover components
- Aggregate employment to sector groups

## API

```python
from skills.leontief import (
    build_technical_coefficients,   # A = Z · diag(x)^-1
    build_leontief_inverse,         # L = (I-A)^-1
    build_employment_coefficients,  # d = Em / x
    compute_employment_content,     # returns {em_exports_total, em_country_matrix}
    validate_model,                 # sanity checks on A and L
    compute_domestic_spillover,     # country-level domestic/spillover split
    compute_industry_decomposition, # sector-level table + figure data
)
```

## Input requirements

- All arrays are ordered `(country_0_industry_0, ..., country_0_industry_P, country_1_industry_0, ...)` — countries vary slowest.
- `Z`, `x`, `Em` must cover the same scope (e.g. EU-only; US/RoW excluded).
- `e` is the export vector under whichever export definition the paper uses.

## Notes

- The full L matrix is computed (not just L·e) because it is reused across many demand vectors in the decomposition.
- Negative x values should be clipped to 0 before calling (handled by IO parsers).
- `compute_industry_decomposition` expects 1-based product indices in the `agg` dict.
