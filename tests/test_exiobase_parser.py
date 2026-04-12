"""
Tests for skills.io_parsers.exiobase using synthetic in-memory zip files.

Synthetic system: 2 analysis countries (AT, DE) × 2 industries
  - EXIOBASE sector names: "Chemicals nec" and "Electrical machinery"
  - Spec maps these via 'exiobase_name' to codes 'C20' and 'C27'
Plus 1 non-analysis region (RoW) for export flows.
"""
import io
import tempfile
import zipfile
from pathlib import Path

import numpy as np
import pytest

from skills.io_parsers.exiobase import load

SPEC = {
    "paper": {"reference_year": 2011},
    "geography": {
        "analysis_entities": [
            {"code": "AT", "name": "Austria"},
            {"code": "DE", "name": "Germany"},
        ]
    },
    "classification": {
        "industry_list": [
            {"code": "C20", "label": "Chemicals", "exiobase_name": "Chemicals nec"},
            {"code": "C27", "label": "Electrical", "exiobase_name": "Electrical machinery"},
        ]
    },
}


def _tsv_matrix(header_regions, header_sectors, row_regions, row_sectors, data):
    """Build a tab-delimited string with 2 header rows and 2 index columns."""
    lines = []
    # Header row 0: index placeholders + column regions
    lines.append("\t\t" + "\t".join(header_regions))
    # Header row 1: index placeholders + column sectors
    lines.append("\t\t" + "\t".join(header_sectors))
    # Data rows
    for r_region, r_sector, row_vals in zip(row_regions, row_sectors, data):
        lines.append(f"{r_region}\t{r_sector}\t" + "\t".join(str(v) for v in row_vals))
    return "\n".join(lines) + "\n"


def _make_exiobase_zip(tmp_dir: Path) -> Path:
    """
    Build a minimal IOT_2011_ixi.zip.

    6 sectors total: AT×2 + DE×2 + RoW×2 (analysis_countries + 1 RoW for exports)
    """
    regions = ["AT", "AT", "DE", "DE", "RoW", "RoW"]
    sectors = ["Chemicals nec", "Electrical machinery",
               "Chemicals nec", "Electrical machinery",
               "Chemicals nec", "Electrical machinery"]
    fd_regions = ["AT", "DE", "RoW"]
    fd_cats = ["CONS_h", "CONS_h", "CONS_h"]

    # Z matrix (6×6): simple integer values
    Z_data = [
        [1, 2, 3, 4, 5, 6],    # AT / Chemicals nec
        [7, 8, 9, 10, 11, 12],  # AT / Electrical
        [13, 14, 15, 16, 17, 18],
        [19, 20, 21, 22, 23, 24],
        [25, 26, 27, 28, 29, 30],
        [31, 32, 33, 34, 35, 36],
    ]
    Z_txt = _tsv_matrix(regions, sectors, regions, sectors, Z_data)

    # Y matrix (6×3 final demand): one column per region
    Y_data = [
        [100, 101, 102],
        [200, 201, 202],
        [300, 301, 302],
        [400, 401, 402],
        [500, 501, 502],
        [600, 601, 602],
    ]
    Y_txt = _tsv_matrix(fd_regions, fd_cats, regions, sectors, Y_data)

    # x.txt: one column (indout)
    x_lines = ["\t\tindout"]  # header row
    x_values = [1000, 2000, 3000, 4000, 5000, 6000]
    for r, s, v in zip(regions, sectors, x_values):
        x_lines.append(f"{r}\t{s}\t{v}")
    x_txt = "\n".join(x_lines) + "\n"

    # satellite/F.txt: 2 stressor rows
    F_data_emp1 = [10, 20, 30, 40, 50, 60]
    F_data_emp2 = [1, 2, 3, 4, 5, 6]
    F_data_co2 = [100, 200, 300, 400, 500, 600]
    F_header0 = "\t" + "\t".join(regions)
    F_header1 = "\t" + "\t".join(sectors)
    F_lines = [
        F_header0, F_header1,
        "Employment: Total (person)\t" + "\t".join(str(v) for v in F_data_emp1),
        "Employment: Paid (person)\t" + "\t".join(str(v) for v in F_data_emp2),
        "CO2 emissions\t" + "\t".join(str(v) for v in F_data_co2),
    ]
    F_txt = "\n".join(F_lines) + "\n"

    zip_path = tmp_dir / "IOT_2011_ixi.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("Z.txt", Z_txt)
        zf.writestr("Y.txt", Y_txt)
        zf.writestr("x.txt", x_txt)
        zf.writestr("satellite/F.txt", F_txt)
    return zip_path


@pytest.fixture(scope="module")
def matrices():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        _make_exiobase_zip(tmp_dir)
        return load(tmp_dir, SPEC)


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_source(matrices):
    assert matrices.source == "exiobase"


def test_dimensions(matrices):
    assert matrices.Z.shape == (4, 4)   # 2 countries × 2 sectors
    assert matrices.e.shape == (4,)
    assert matrices.x.shape == (4,)
    assert matrices.Em.shape == (4,)


def test_labels(matrices):
    assert matrices.labels == ["AT_C20", "AT_C27", "DE_C20", "DE_C27"]


def test_Z_intra_analysis_only(matrices):
    """Z must only contain AT↔AT, AT↔DE, DE↔AT, DE↔DE flows."""
    # AT/C20 → AT/C20=1, AT/C20 → AT/C27=2, AT/C20 → DE/C20=3, AT/C20 → DE/C27=4
    np.testing.assert_allclose(matrices.Z[0, :], [1, 2, 3, 4], rtol=1e-6)
    np.testing.assert_allclose(matrices.Z[1, :], [7, 8, 9, 10], rtol=1e-6)


def test_e_includes_Z_and_Y_exports(matrices):
    """
    AT/C20 exports to RoW:
      Z:     row=0, cols 4,5 (RoW×2) = 5 + 6 = 11
      Y:     row=0, col 2 (RoW CONS_h) = 102
      total: 113
    """
    assert matrices.e[0] == pytest.approx(5 + 6 + 102, abs=1e-6)


def test_e_positive(matrices):
    assert (matrices.e > 0).all()


def test_x_from_x_txt(matrices):
    """Total output from x.txt: AT/C20=1000, AT/C27=2000, DE/C20=3000, DE/C27=4000."""
    np.testing.assert_allclose(matrices.x, [1000, 2000, 3000, 4000], rtol=1e-6)


def test_employment_sums_two_rows(matrices):
    """
    F.txt has two employment rows summed.
    AT/C20: 10+1=11, AT/C27: 20+2=22, DE/C20: 30+3=33, DE/C27: 40+4=44
    """
    np.testing.assert_allclose(matrices.Em, [11, 22, 33, 44], rtol=1e-6)


def test_missing_zip_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load(tmp_path, SPEC)
