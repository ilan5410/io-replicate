"""
skills.classification_mapping — concordance tables and code search.

Usage:
    from skills.classification_mapping import (
        load_concordance,
        search_by_description,
        expand_prefix,
        validate_mapping,
    )
"""
from .core import load_concordance, search_by_description, expand_prefix, validate_mapping

__all__ = [
    "load_concordance",
    "search_by_description",
    "expand_prefix",
    "validate_mapping",
]
