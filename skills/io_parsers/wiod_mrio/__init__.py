"""
skills.io_parsers.wiod_mrio — parser for the WIOD 2016 release (Excel format).

Entry point:
    from skills.io_parsers.wiod_mrio import load
    matrices = load(raw_dir, spec)

Expected files in raw_dir:
    WIOT{year}_Nov16_ROW.xlsx   — World IO table for the target year
    SEA.xlsx                    — Socio-Economic Accounts (employment + VA)
"""
from .core import load

__all__ = ["load"]
