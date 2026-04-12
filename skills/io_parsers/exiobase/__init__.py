"""
skills.io_parsers.exiobase — parser for EXIOBASE 3.x IOT zip archives.

Entry point:
    from skills.io_parsers.exiobase import load
    matrices = load(raw_dir, spec)

Expected files in raw_dir:
    IOT_{year}_ixi.zip   or   IOT_{year}_pxp.zip
    (industry-by-industry or product-by-product variant)

The zip must contain:
    Z.txt            — intermediate use matrix (tab-delimited, 2-level header)
    Y.txt            — final demand matrix
    x.txt            — total output vector
    satellite/F.txt  — extension/satellite matrix (employment lives here)
"""
from .core import load

__all__ = ["load"]
