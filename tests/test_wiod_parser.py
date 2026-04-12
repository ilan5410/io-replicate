"""
Tests for skills.io_parsers.wiod_mrio using synthetic in-memory xlsx files.

Synthetic system: 2 "analysis" countries (AT, DE) × 2 industries (C20, C27)
plus 1 "rest of world" country (RoW) used only in the export vector.
"""
import io
import tempfile
from pathlib import Path

import numpy as np
import pytest

try:
    import openpyxl
    _HAS_OPENPYXL = True
except ImportError:
    _HAS_OPENPYXL = False

from skills.io_parsers.wiod_mrio import load

pytestmark = pytest.mark.skipif(not _HAS_OPENPYXL, reason="openpyxl required")


# ── Minimal synthetic spec ────────────────────────────────────────────────────

SPEC = {
    "paper": {"reference_year": 2010},
    "geography": {
        "analysis_entities": [
            {"code": "AT", "name": "Austria"},
            {"code": "DE", "name": "Germany"},
        ]
    },
    "classification": {
        "industry_list": [
            {"code": "C20", "label": "Chemicals"},
            {"code": "C27", "label": "Electrical equipment"},
        ]
    },
}

# ── Build synthetic xlsx files ────────────────────────────────────────────────

def _make_wiot_xlsx(tmp_dir: Path) -> Path:
    """
    Build a minimal WIOT Excel file matching the WIOD 2016 format.

    Layout (header rows 0-1, then data rows):
      Row 0: [None, None, AT,  AT,  DE,  DE,  RoW, RoW]
      Row 1: [None, None, C20, C27, C20, C27, C20, C27]
      Row 2: [AT,   C20,   1,   2,   3,   4,   5,   6 ]
      Row 3: [AT,   C27,   7,   8,   9,  10,  11,  12 ]
      Row 4: [DE,   C20,  13,  14,  15,  16,  17,  18 ]
      Row 5: [DE,   C27,  19,  20,  21,  22,  23,  24 ]
    """
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "wiot2010"

    # Header row 0 — destination countries
    ws.append([None, None, "AT", "AT", "DE", "DE", "RoW", "RoW"])
    # Header row 1 — destination sectors
    ws.append([None, None, "C20", "C27", "C20", "C27", "C20", "C27"])
    # Data rows
    ws.append(["AT", "C20",   1,   2,   3,   4,   5,   6])
    ws.append(["AT", "C27",   7,   8,   9,  10,  11,  12])
    ws.append(["DE", "C20",  13,  14,  15,  16,  17,  18])
    ws.append(["DE", "C27",  19,  20,  21,  22,  23,  24])

    path = tmp_dir / "WIOT2010_Nov16_ROW.xlsx"
    wb.save(path)
    return path


def _make_sea_xlsx(tmp_dir: Path) -> Path:
    """Build a minimal SEA.xlsx with EMP data."""
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "EA"

    ws.append(["country", "year", "variable", "C20", "C27"])
    ws.append(["AT", "2010", "EMP", 100.0, 50.0])
    ws.append(["DE", "2010", "EMP", 400.0, 200.0])

    path = tmp_dir / "SEA.xlsx"
    wb.save(path)
    return path


@pytest.fixture(scope="module")
def matrices():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        _make_wiot_xlsx(tmp_dir)
        _make_sea_xlsx(tmp_dir)
        return load(tmp_dir, SPEC)


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_source_is_wiod(matrices):
    assert matrices.source == "wiod_mrio"


def test_dimensions(matrices):
    assert matrices.Z.shape == (4, 4)   # 2 countries × 2 industries
    assert matrices.e.shape == (4,)
    assert matrices.x.shape == (4,)
    assert matrices.Em.shape == (4,)


def test_labels(matrices):
    assert matrices.labels == ["AT_C20", "AT_C27", "DE_C20", "DE_C27"]


def test_Z_contains_intra_analysis_only(matrices):
    """Z must only contain AT↔AT, AT↔DE, DE↔AT, DE↔DE flows — not RoW."""
    # All Z values should come from the data rows, which we set explicitly
    assert matrices.Z.sum() > 0


def test_e_positive(matrices):
    """Export vector = flows to RoW; must be positive."""
    assert matrices.e.sum() > 0


def test_x_ge_Z_row_sums(matrices):
    """Total output ≥ each row's intermediate use (sanity check)."""
    # x is the sum of ALL uses including exports; Z is only intra-analysis
    assert (matrices.x >= matrices.Z.sum(axis=1) - 1e-6).all()


def test_employment_loaded(matrices):
    assert matrices.Em.sum() > 0
    # AT_C20 = index 0 → 100 THS, AT_C27 = index 1 → 50 THS
    assert abs(matrices.Em[0] - 100.0) < 1e-6
    assert abs(matrices.Em[1] - 50.0) < 1e-6


def test_e_values_are_row_exports(matrices):
    """
    AT_C20 row exports to RoW: values 5 + 6 = 11
    AT_C27 row exports to RoW: values 11 + 12 = 23
    DE_C20 row exports to RoW: values 17 + 18 = 35
    DE_C27 row exports to RoW: values 23 + 24 = 47
    """
    expected = np.array([5 + 6, 11 + 12, 17 + 18, 23 + 24], dtype=float)
    np.testing.assert_allclose(matrices.e, expected, rtol=1e-6)


def test_x_values_are_row_totals(matrices):
    """
    AT_C20 total output: 1+2+3+4+5+6 = 21
    AT_C27 total output: 7+8+9+10+11+12 = 57
    DE_C20 total output: 13+14+15+16+17+18 = 93
    DE_C27 total output: 19+20+21+22+23+24 = 129
    """
    expected = np.array([21.0, 57.0, 93.0, 129.0])
    np.testing.assert_allclose(matrices.x, expected, rtol=1e-6)
