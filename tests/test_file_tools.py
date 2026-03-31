"""
Tests for file_tools — verifies safety constraints:
  - write_file is restricted to CWD or /tmp
  - read_file is capped at 5,000 characters
  - read_file blocks known large matrix files
"""
import os
import tempfile
from pathlib import Path

import pytest

from agents.tools.file_tools import read_file, write_file, list_files


# ── write_file ────────────────────────────────────────────────────────────────

def test_write_file_to_tmp_succeeds():
    tmp = Path(tempfile.mktemp(suffix=".txt", dir="/tmp"))
    result = write_file.invoke({"path": str(tmp), "content": "hello"})
    assert result.startswith("OK"), f"Expected OK, got: {result}"
    assert tmp.read_text() == "hello"
    tmp.unlink(missing_ok=True)


def test_write_file_outside_cwd_blocked():
    # /etc/test_io_replicator.txt is outside both CWD and /tmp
    result = write_file.invoke({"path": "/etc/test_io_replicator.txt", "content": "bad"})
    assert "ERROR" in result
    assert not Path("/etc/test_io_replicator.txt").exists()


def test_write_file_inside_cwd_succeeds(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    target = tmp_path / "subdir" / "out.txt"
    result = write_file.invoke({"path": str(target), "content": "ok"})
    assert result.startswith("OK"), f"Expected OK, got: {result}"
    assert target.read_text() == "ok"


# ── read_file ─────────────────────────────────────────────────────────────────

def test_read_file_truncates_at_5000_chars():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", dir="/tmp",
                                    delete=False) as f:
        f.write("x" * 10_000)
        path = f.name
    result = read_file.invoke({"path": path})
    assert "TRUNCATED" in result, "Expected truncation notice"
    # Content before truncation marker should be 5000 chars of 'x'
    assert result[:5000] == "x" * 5000
    Path(path).unlink(missing_ok=True)


def test_read_file_short_file_not_truncated():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", dir="/tmp",
                                    delete=False) as f:
        f.write("hello world")
        path = f.name
    result = read_file.invoke({"path": path})
    assert result == "hello world"
    Path(path).unlink(missing_ok=True)


def test_read_file_missing_returns_error():
    result = read_file.invoke({"path": "/tmp/definitely_does_not_exist_io_replicator.csv"})
    assert "ERROR" in result


@pytest.mark.parametrize("blocked_name", [
    "Z_EU.csv", "z_eu.csv", "L_EU.csv", "l_eu.csv",
    "A_EU.csv", "a_eu.csv", "L_EU.npy", "l_eu.npy",
    "em_exports_country_matrix.csv",
])
def test_read_file_blocks_large_matrix(blocked_name, tmp_path):
    target = tmp_path / blocked_name
    target.write_text("data")
    result = read_file.invoke({"path": str(target)})
    assert "BLOCKED" in result, f"{blocked_name} should be blocked but got: {result}"


# ── list_files ────────────────────────────────────────────────────────────────

def test_list_files_missing_dir():
    result = list_files.invoke({"directory": "/tmp/no_such_dir_io_replicator"})
    assert "ERROR" in result


def test_list_files_returns_files(tmp_path):
    (tmp_path / "a.csv").write_text("1")
    (tmp_path / "b.csv").write_text("2")
    result = list_files.invoke({"directory": str(tmp_path), "pattern": "*.csv"})
    assert "a.csv" in result
    assert "b.csv" in result
