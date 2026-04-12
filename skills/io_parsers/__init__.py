"""
skills.io_parsers — dispatcher for IO database parsers.

Usage:
    from skills.io_parsers import load_parser
    load = load_parser("figaro_iciot")
    matrices = load(raw_dir, spec)
"""
from .base import PreparedMatrices


def load_parser(parser_type: str):
    """Return the load() function for the given parser type.

    Supported types: figaro_iciot, wiod_mrio, oecd_icio, exiobase
    """
    if parser_type == "figaro_iciot":
        from .figaro_iciot import load
        return load
    if parser_type == "wiod_mrio":
        from .wiod_mrio import load
        return load
    if parser_type == "oecd_icio":
        from .oecd_icio import load
        return load
    if parser_type == "exiobase":
        from .exiobase import load
        return load
    raise ValueError(
        f"Unknown IO parser type: {parser_type!r}. "
        f"Supported: figaro_iciot, wiod_mrio, oecd_icio, exiobase"
    )


__all__ = ["PreparedMatrices", "load_parser"]
