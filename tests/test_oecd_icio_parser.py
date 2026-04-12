"""
Tests for skills.io_parsers.oecd_icio using synthetic CSV files.

Synthetic system: 2 analysis countries (FRA, DEU) × 2 industries (D20, D27)
plus 1 rest-of-world country (ROW) used only in the export vector.
"""
import io
import tempfile
from pathlib import Path

import numpy as np
import pytest

from skills.io_parsers.oecd_icio import load

SPEC = {
    "paper": {"reference_year": 2015},
    "geography": {
        "analysis_entities": [
            {"code": "FRA", "name": "France"},
            {"code": "DEU", "name": "Germany"},
        ]
    },
    "classification": {
        "industry_list": [
            {"code": "D20", "label": "Chemicals"},
            {"code": "D27", "label": "Electrical equipment"},
        ]
    },
}


def _make_icio_csv(tmp_dir: Path) -> Path:
    """
    Build a minimal ICIO2023_2015.csv.

    Columns (intermediate): FRA_D20, FRA_D27, DEU_D20, DEU_D27, ROW_D20, ROW_D27
    Columns (final demand):  FRA_HFCE, DEU_HFCE, ROW_HFCE
    Rows (producing):        FRA_D20, FRA_D27, DEU_D20, DEU_D27
    Rows (VA):               FRA_VALU, DEU_VALU

    Values are simple integers for easy manual checking.
    """
    header = ",".join([
        "",                               # row label column
        "FRA_D20", "FRA_D27",            # intra-analysis intermediate
        "DEU_D20", "DEU_D27",
        "ROW_D20", "ROW_D27",            # export intermediate
        "FRA_HFCE", "DEU_HFCE",          # intra-analysis final demand
        "ROW_HFCE",                       # export final demand
    ])
    rows = [
        "FRA_D20,1,2,3,4,5,6,7,8,9",    # row sums: 1+2+3+4+5+6+7+8+9 = 45
        "FRA_D27,10,11,12,13,14,15,16,17,18",  # sum = 126
        "DEU_D20,19,20,21,22,23,24,25,26,27",  # sum = 207
        "DEU_D27,28,29,30,31,32,33,34,35,36",  # sum = 288
        "FRA_VALU,0,0,0,0,0,0,0,0,0",
        "DEU_VALU,0,0,0,0,0,0,0,0,0",
    ]
    content = header + "\n" + "\n".join(rows) + "\n"
    path = tmp_dir / "ICIO2023_2015.csv"
    path.write_text(content)
    return path


def _make_emp_csv(tmp_dir: Path) -> Path:
    """Build icio_employment_2015.csv."""
    content = "code,emp_THS_PER\nFRA_D20,200.0\nFRA_D27,80.0\nDEU_D20,600.0\nDEU_D27,250.0\n"
    path = tmp_dir / "icio_employment_2015.csv"
    path.write_text(content)
    return path


@pytest.fixture(scope="module")
def matrices():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        _make_icio_csv(tmp_dir)
        _make_emp_csv(tmp_dir)
        return load(tmp_dir, SPEC)


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_source(matrices):
    assert matrices.source == "oecd_icio"


def test_dimensions(matrices):
    assert matrices.Z.shape == (4, 4)
    assert matrices.e.shape == (4,)
    assert matrices.x.shape == (4,)
    assert matrices.Em.shape == (4,)


def test_labels(matrices):
    assert matrices.labels == ["FRA_D20", "FRA_D27", "DEU_D20", "DEU_D27"]


def test_Z_intra_analysis(matrices):
    """Z should contain only analysis↔analysis intermediate flows."""
    # FRA_D20 → FRA_D20=1, FRA_D20 → FRA_D27=2, FRA_D20 → DEU_D20=3, FRA_D20 → DEU_D27=4
    assert matrices.Z.sum() > 0
    expected_FRA_D20_row = np.array([1, 2, 3, 4], dtype=float)
    np.testing.assert_allclose(matrices.Z[0, :], expected_FRA_D20_row, rtol=1e-6)


def test_e_exports(matrices):
    """
    Export vector = flows to ROW (intermediate + final demand).
    FRA_D20: ROW_D20=5 + ROW_D27=6 + ROW_HFCE=9 = 20
    FRA_D27: 14+15+18 = 47
    DEU_D20: 23+24+27 = 74
    DEU_D27: 32+33+36 = 101
    """
    expected = np.array([20.0, 47.0, 74.0, 101.0])
    np.testing.assert_allclose(matrices.e, expected, rtol=1e-6)


def test_x_total_output(matrices):
    """x = sum of all use columns for each row."""
    expected = np.array([45.0, 126.0, 207.0, 288.0])
    np.testing.assert_allclose(matrices.x, expected, rtol=1e-6)


def test_employment(matrices):
    """Employment loaded from icio_employment_{year}.csv."""
    expected = np.array([200.0, 80.0, 600.0, 250.0])
    np.testing.assert_allclose(matrices.Em, expected, rtol=1e-6)


def test_missing_icio_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load(tmp_path, SPEC)
