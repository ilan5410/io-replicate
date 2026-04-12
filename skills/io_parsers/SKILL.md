# Skill: io_parsers

Parsers for widely-used multi-regional IO databases. Each parser returns
a `PreparedMatrices` object in the same standardized format regardless of
the source database.

## When to use

Call `load_parser(type)` when Stage 2 needs to parse raw downloaded files
into the Z, e, x, Em matrices that Stage 3 (Model Builder) expects.

The parser type comes from `spec["data_sources"]["io_table"]["type"]`.

## API

```python
from skills.io_parsers import load_parser, PreparedMatrices

load = load_parser("figaro_iciot")   # or "wiod_mrio"
matrices = load(raw_dir, spec)
# matrices.Z, matrices.e, matrices.x, matrices.Em, matrices.labels, ...
```

## Supported parsers

| type | Database | Files expected in raw_dir |
|------|----------|--------------------------|
| `figaro_iciot` | Eurostat FIGARO IC-IOT 2010–2013 | `naio_10_fcp_ip1.tsv.gz`, `nama_10_a64_e.tsv.gz` |
| `wiod_mrio` | WIOD 2016 release | `WIOT{year}_Nov16_ROW.xlsx`, `SEA.xlsx` |
| `oecd_icio` | OECD ICIO 2021/2023 | `ICIO{version}_{year}.csv`; optionally `icio_employment_{year}.csv` |
| `exiobase` | EXIOBASE 3.x IOT | `IOT_{year}_ixi.zip` or `IOT_{year}_pxp.zip` |

### EXIOBASE sector mapping note
EXIOBASE uses full English sector names ("Chemicals nec"), not standard codes.
Add an `exiobase_name` field to each spec industry entry:
```yaml
- code: C20
  label: Chemicals
  exiobase_name: "Chemicals nec"
```

## Adding a new parser

1. Create `skills/io_parsers/{type}/core.py` with a `load(raw_dir, spec)` function
2. Create `skills/io_parsers/{type}/__init__.py` that exports `load`
3. Register it in `skills/io_parsers/__init__.py` → `load_parser()`
4. Add a synthetic-data test in `tests/test_{type}_parser.py`

## PreparedMatrices contract

All arrays ordered `(country_0_industry_0, ..., country_N_industry_P)`.
- `Z`: (N, N) intra-analysis intermediate use matrix
- `e`: (N,) export vector (all analysis→non-analysis flows)
- `x`: (N,) total output (all uses)
- `Em`: (N,) employment or chosen satellite factor (thousands of persons)
- `labels`: `["{country}_{industry}", ...]` length N
