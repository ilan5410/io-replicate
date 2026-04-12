"""
skills.io_parsers.figaro_iciot — parser for Eurostat FIGARO IC-IOT bulk TSV files.

Entry point:
    from skills.io_parsers.figaro_iciot import load
    matrices = load(raw_dir, spec)
"""
from .core import load

__all__ = ["load"]
