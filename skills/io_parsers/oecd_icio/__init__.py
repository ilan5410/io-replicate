"""
skills.io_parsers.oecd_icio — parser for the OECD ICIO 2023 release (CSV format).

Entry point:
    from skills.io_parsers.oecd_icio import load
    matrices = load(raw_dir, spec)

Expected files in raw_dir:
    ICIO{version}_{year}.csv   e.g. ICIO2023_2010.csv
    (employment is not bundled with ICIO; Em will be zero unless a separate
     TiM employment file is provided as icio_employment_{year}.csv)
"""
from .core import load

__all__ = ["load"]
