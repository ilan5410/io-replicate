"""
skills.io_parsers — dispatcher for IO database parsers.

Usage:
    from skills.io_parsers import load_parser
    load = load_parser("figaro_iciot")
    matrices = load(raw_dir, spec)
"""
from .base import PreparedMatrices


_ALIASES: dict[str, str] = {
    # Common short-forms the paper_analyst LLM may produce
    "figaro":       "figaro_iciot",
    "figaro_icio":  "figaro_iciot",
    "figaro_io":    "figaro_iciot",
    "naio_10":      "figaro_iciot",
    "wiod":         "wiod_mrio",
    "wiod_io":      "wiod_mrio",
    "oecd":         "oecd_icio",
    "oecd_io":      "oecd_icio",
    "icio":         "oecd_icio",
}


def load_parser(parser_type: str):
    """Return the load() function for the given parser type.

    Supported types: figaro_iciot, wiod_mrio, oecd_icio, exiobase
    Common aliases (e.g. 'figaro', 'wiod', 'oecd') are also accepted.
    """
    canonical = _ALIASES.get(parser_type.lower(), parser_type)
    if canonical == "figaro_iciot":
        from .figaro_iciot import load
        return load
    if canonical == "wiod_mrio":
        from .wiod_mrio import load
        return load
    if canonical == "oecd_icio":
        from .oecd_icio import load
        return load
    if canonical == "exiobase":
        from .exiobase import load
        return load
    raise ValueError(
        f"Unknown IO parser type: {parser_type!r}. "
        f"Supported: figaro_iciot, wiod_mrio, oecd_icio, exiobase"
    )


__all__ = ["PreparedMatrices", "load_parser"]
