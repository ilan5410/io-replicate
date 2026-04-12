"""
Classification mapping skill — pure Python, no LLM dependency.

Provides:
  load_concordance    — list of (from_code, to_code) pairs between two classifications
  search_by_description — fuzzy text search within a classification
  expand_prefix       — all codes under a parent prefix
  validate_mapping    — check codes exist, warn on parent+child overlap
"""
from __future__ import annotations

import difflib
import re
from dataclasses import dataclass

from .concordances import CLASSIFICATION_LABELS, REGISTERED


# ── Public API ────────────────────────────────────────────────────────────────

def load_concordance(from_cls: str, to_cls: str) -> list[tuple[str, str]]:
    """
    Return a list of (from_code, to_code) pairs for the given classification pair.

    Supported pairs: see concordances.REGISTERED keys.
    Raises KeyError if the pair is not registered.

    Example:
        pairs = load_concordance("NACE_R2_64", "CPA_2008_64")
        # [("A01", "A01"), ("A02", "A02"), ...]
    """
    key = (from_cls, to_cls)
    if key not in REGISTERED:
        available = [f"{f} → {t}" for f, t in REGISTERED]
        raise KeyError(
            f"No concordance for {from_cls} → {to_cls}. "
            f"Available: {available}"
        )
    return REGISTERED[key]()


def search_by_description(text: str, classification: str, top_k: int = 10) -> list[Code]:
    """
    Return up to top_k codes whose label best matches text (case-insensitive).

    Uses difflib SequenceMatcher — good for finding "motor vehicles" in
    a classification without knowing the exact code.

    Example:
        results = search_by_description("electric vehicles", "NACE_R2_64")
    """
    if classification not in CLASSIFICATION_LABELS:
        raise KeyError(f"Unknown classification: {classification!r}. "
                       f"Available: {list(CLASSIFICATION_LABELS)}")
    labels = CLASSIFICATION_LABELS[classification]
    query = text.lower()

    scored = []
    for code, label in labels.items():
        label_lower = label.lower()
        # Exact substring match → score 1.0
        if query in label_lower:
            score = 1.0
        else:
            score = difflib.SequenceMatcher(None, query, label_lower).ratio()
        scored.append(Code(code=code, label=label, score=score))

    scored.sort(key=lambda c: c.score, reverse=True)
    return scored[:top_k]


def expand_prefix(prefix: str, classification: str) -> list[Code]:
    """
    Return all codes in the classification that start with the given prefix.

    Example:
        expand_prefix("C2", "NACE_R2_64")
        # → C20, C21, C22, C23, C24, C25, C26, C27, C28, C29
    """
    if classification not in CLASSIFICATION_LABELS:
        raise KeyError(f"Unknown classification: {classification!r}")
    labels = CLASSIFICATION_LABELS[classification]
    return [
        Code(code=code, label=label, score=1.0)
        for code, label in labels.items()
        if code.startswith(prefix)
    ]


@dataclass
class ValidationReport:
    valid: bool
    unknown_codes: list[str]
    parent_child_overlaps: list[tuple[str, str]]
    warnings: list[str]


def validate_mapping(codes: list[str], classification: str) -> ValidationReport:
    """
    Check that all codes exist in the classification and warn on parent/child overlap.

    Parent/child overlap: e.g. ["C", "C10", "C11"] — C already covers C10 and C11.

    Example:
        report = validate_mapping(["C20", "C27", "D35"], "NACE_R2_64")
    """
    if classification not in CLASSIFICATION_LABELS:
        raise KeyError(f"Unknown classification: {classification!r}")
    known = set(CLASSIFICATION_LABELS[classification].keys())
    unknown = [c for c in codes if c not in known]

    # Detect parent/child overlaps: code A is a parent of code B if B starts with A
    # and len(A) < len(B)
    overlaps = []
    code_set = set(codes)
    for c in codes:
        for other in codes:
            if other != c and other.startswith(c) and len(c) < len(other):
                overlaps.append((c, other))

    warnings = []
    if unknown:
        warnings.append(f"{len(unknown)} unknown code(s): {unknown[:5]}")
    if overlaps:
        warnings.append(
            f"{len(overlaps)} parent/child overlap(s): "
            + ", ".join(f"{p}⊃{ch}" for p, ch in overlaps[:3])
        )

    return ValidationReport(
        valid=len(unknown) == 0,
        unknown_codes=unknown,
        parent_child_overlaps=overlaps,
        warnings=warnings,
    )


# ── Helper type ───────────────────────────────────────────────────────────────

@dataclass
class Code:
    code: str
    label: str
    score: float   # relevance score from search_by_description; 1.0 for expand_prefix

    def __repr__(self) -> str:
        return f"Code({self.code!r}, score={self.score:.2f})"
