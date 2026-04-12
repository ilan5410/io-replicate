"""
LangChain tool wrappers for the classification_mapping skill.

These tools are given to the Classification Mapper agent so it can search
and validate industrial classification codes without needing web access.
"""
import json

from langchain_core.tools import tool

from skills.classification_mapping import (
    load_concordance,
    search_by_description,
    expand_prefix,
    validate_mapping,
)
from skills.classification_mapping.concordances import CLASSIFICATION_LABELS


@tool
def tool_search_by_description(text: str, classification: str, top_k: int = 10) -> str:
    """
    Find classification codes matching a text description.

    Args:
        text:           Description to search for, e.g. "electric vehicles manufacturing"
        classification: One of NACE_R2_64, CPA_2008_64, ISIC_R4, WIOD56
        top_k:          Max results to return (default 10)

    Returns JSON list of {code, label, score}.
    """
    try:
        results = search_by_description(text, classification, top_k=top_k)
        return json.dumps([
            {"code": r.code, "label": r.label, "score": round(r.score, 3)}
            for r in results
        ], indent=2)
    except KeyError as e:
        return f"ERROR: {e}"


@tool
def tool_expand_prefix(prefix: str, classification: str) -> str:
    """
    List all codes in a classification that start with a given prefix.

    Args:
        prefix:         Code prefix to expand, e.g. "C2" or "K"
        classification: One of NACE_R2_64, CPA_2008_64, ISIC_R4, WIOD56

    Returns JSON list of {code, label}.
    """
    try:
        results = expand_prefix(prefix, classification)
        return json.dumps([{"code": r.code, "label": r.label} for r in results], indent=2)
    except KeyError as e:
        return f"ERROR: {e}"


@tool
def tool_load_concordance(from_cls: str, to_cls: str) -> str:
    """
    Return a mapping between two classification systems.

    Args:
        from_cls: Source classification, e.g. NACE_R2_64
        to_cls:   Target classification, e.g. ISIC_R4

    Returns JSON list of [from_code, to_code] pairs.
    """
    try:
        pairs = load_concordance(from_cls, to_cls)
        return json.dumps([[f, t] for f, t in pairs], indent=2)
    except KeyError as e:
        return f"ERROR: {e}. Available: NACE_R2_64, CPA_2008_64, ISIC_R4, WIOD56"


@tool
def tool_validate_mapping(codes: list, classification: str) -> str:
    """
    Check that a list of codes are valid in the given classification.
    Warns on parent/child overlaps (e.g. ["C", "C10"]).

    Args:
        codes:          List of code strings to validate
        classification: One of NACE_R2_64, CPA_2008_64, ISIC_R4, WIOD56

    Returns JSON with valid, unknown_codes, parent_child_overlaps, warnings.
    """
    try:
        report = validate_mapping(codes, classification)
        return json.dumps({
            "valid": report.valid,
            "unknown_codes": report.unknown_codes,
            "parent_child_overlaps": report.parent_child_overlaps,
            "warnings": report.warnings,
        }, indent=2)
    except KeyError as e:
        return f"ERROR: {e}"


@tool
def tool_list_classifications() -> str:
    """List all available classification systems and their code counts."""
    return json.dumps({
        cls: len(codes)
        for cls, codes in CLASSIFICATION_LABELS.items()
    }, indent=2)


@tool
def tool_write_concept_mapping(
    run_dir: str,
    concept_id: str,
    codes: list,
    reasoning: str,
    sources: list,
    confidence: str,
    caveats: list = None,
) -> str:
    """
    Persist one concept mapping to concept_mappings.json in the run directory.
    Call once per concept after you have validated the codes.

    Args:
        run_dir:    Run directory path (from the initial message)
        concept_id: Concept identifier from spec.concepts_to_map[].id
        codes:      Final list of classification codes for this concept
        reasoning:  One paragraph explaining the mapping decision
        sources:    List of source references (concordance name, paper section, URL)
        confidence: "high", "medium", or "low"
        caveats:    Optional list of ambiguity notes
    """
    import json
    from pathlib import Path

    mappings_path = Path(run_dir) / "data" / "concept_mappings.json"
    mappings_path.parent.mkdir(parents=True, exist_ok=True)

    existing = {}
    if mappings_path.exists():
        existing = json.loads(mappings_path.read_text())

    existing[concept_id] = {
        "codes": codes,
        "reasoning": reasoning,
        "sources": sources,
        "confidence": confidence,
        "caveats": caveats or [],
    }
    mappings_path.write_text(json.dumps(existing, indent=2))
    return f"Mapping saved: {concept_id} → {codes} (confidence: {confidence})"
