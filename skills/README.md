# io-replication-skills

Standalone Python library for Input-Output economics analysis. No agent framework, no LLM dependencies — just pure math and data parsing.

```
pip install io-replication-skills
```

## What's included

### `skills.leontief` — Leontief analysis

```python
from skills.leontief import (
    build_technical_coefficients,  # A = Z · diag(x)^-1
    build_leontief_inverse,         # L = (I - A)^-1
    build_employment_coefficients,  # d = Em / x
    compute_employment_content,     # diag(d)' · L · e
    validate_model,                 # column sums, L ≥ 0, diag ≥ 1, L(I-A)≈I
    compute_domestic_spillover,     # domestic vs spillover decomposition
    compute_industry_decomposition, # jobs attributed to producing industries
)
```

See [leontief/SKILL.md](leontief/SKILL.md) for full API reference.

---

### `skills.io_parsers` — IO database parsers

Parse raw database files into a unified `PreparedMatrices` object:

```python
from skills.io_parsers import load_parser, PreparedMatrices

load = load_parser("figaro_iciot")   # or wiod_mrio | oecd_icio | exiobase
matrices = load(raw_dir, spec)
# matrices.Z  (N×N)  intra-analysis intermediate use
# matrices.e  (N,)   exports to non-analysis regions
# matrices.x  (N,)   total output
# matrices.Em (N,)   employment or chosen satellite factor
# matrices.labels    ["AT_C20", "AT_C27", ...]
```

| Parser type | Database | Files expected |
|---|---|---|
| `figaro_iciot` | Eurostat FIGARO IC-IOT | `naio_10_fcp_ip1.tsv.gz`, `nama_10_a64_e.tsv.gz` |
| `wiod_mrio` | WIOD 2016 | `WIOT{year}_Nov16_ROW.xlsx`, `SEA.xlsx` |
| `oecd_icio` | OECD ICIO 2021/2023 | `ICIO{version}_{year}.csv` |
| `exiobase` | EXIOBASE 3.x | `IOT_{year}_ixi.zip` or `IOT_{year}_pxp.zip` |

See [io_parsers/SKILL.md](io_parsers/SKILL.md) for full API reference.

---

### `skills.classification_mapping` — Industrial classification concordance

```python
from skills.classification_mapping import (
    load_concordance,       # NACE ↔ ISIC, NACE ↔ CPA, NACE ↔ WIOD56, ...
    search_by_description,  # fuzzy-match codes by text
    expand_prefix,          # all children of C27 → [C27.1, C27.11, ...]
    validate_mapping,       # check codes exist, warn on parent+child overlap
)
```

Bundled concordances: NACE Rev. 2 ↔ CPA 2008, NACE Rev. 2 ↔ ISIC Rev. 4, NACE Rev. 2 ↔ WIOD56. No external data files required.

See [classification_mapping/SKILL.md](classification_mapping/SKILL.md) for full API reference.

---

## Install from source (monorepo)

```bash
# From the io-replicator repo root — installs just the skills package
pip install skills/

# Full pipeline (agents + LLM dependencies)
pip install .
```

## Requirements

- Python ≥ 3.10
- numpy ≥ 1.24
- pandas ≥ 2.0
- openpyxl ≥ 3.1 (required by the WIOD parser)
