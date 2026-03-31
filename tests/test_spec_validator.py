"""
Tests for spec_validator — verifies that the FIGARO spec passes schema validation
and that malformed specs are correctly rejected.
"""
from pathlib import Path

import pytest
import yaml

from agents.validators.spec_validator import validate_spec, validate_spec_file

FIGARO_SPEC = Path(__file__).parents[1] / "specs" / "figaro_2019" / "replication_spec.yaml"


def test_figaro_spec_is_valid():
    is_valid, errors = validate_spec_file(str(FIGARO_SPEC))
    assert is_valid, f"FIGARO spec failed validation:\n" + "\n".join(errors)


def test_figaro_spec_has_expected_countries():
    with open(FIGARO_SPEC) as f:
        spec = yaml.safe_load(f)
    codes = {e["code"] for e in spec["geography"]["analysis_entities"]}
    assert "DE" in codes
    assert "LU" in codes
    assert len(codes) == 28, f"Expected 28 EU countries, got {len(codes)}"


def test_figaro_spec_has_expected_industries():
    with open(FIGARO_SPEC) as f:
        spec = yaml.safe_load(f)
    assert len(spec["classification"]["industry_list"]) == 64


def test_figaro_spec_benchmarks_have_sources():
    """All benchmark entries in the FIGARO spec should have source descriptors."""
    with open(FIGARO_SPEC) as f:
        spec = yaml.safe_load(f)
    for bm in spec["benchmarks"]["values"]:
        assert "source" in bm, f"Benchmark '{bm['name']}' is missing a source descriptor"


def test_missing_required_field_fails():
    spec = {"geography": {}, "classification": {}, "data_sources": {},
            "methodology": {}, "decompositions": [], "outputs": {},
            "benchmarks": {}, "limitations": []}
    is_valid, errors = validate_spec(spec)
    assert not is_valid
    assert any("paper" in e for e in errors)


def test_null_doi_and_journal_are_valid():
    """doi and journal accept null — regression test for schema fix."""
    with open(FIGARO_SPEC) as f:
        spec = yaml.safe_load(f)
    spec["paper"]["doi"] = None
    spec["paper"]["journal"] = None
    is_valid, errors = validate_spec(spec)
    assert is_valid, f"null doi/journal should be valid but got errors: {errors}"
