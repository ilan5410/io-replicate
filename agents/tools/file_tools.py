"""
Tools: read_file, write_file, list_files
"""
import os
from pathlib import Path

from langchain_core.tools import tool


@tool
def read_file(path: str) -> str:
    """
    Read and return the contents of a file.

    Args:
        path: Absolute or relative path to the file.
    """
    p = Path(path)
    if not p.exists():
        return f"ERROR: File not found: {path}"
    try:
        return p.read_text()
    except Exception as e:
        return f"ERROR reading {path}: {e}"


@tool
def write_file(path: str, content: str) -> str:
    """
    Write content to a file, creating parent directories as needed.

    Args:
        path: Absolute or relative path to write to.
        content: The text content to write.
    """
    p = Path(path)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)
        return f"OK: Written {len(content)} bytes to {path}"
    except Exception as e:
        return f"ERROR writing {path}: {e}"


@tool
def list_files(directory: str, pattern: str = "*") -> str:
    """
    List files in a directory matching a glob pattern.

    Args:
        directory: Directory to list.
        pattern: Glob pattern, e.g. '*.csv' or '**/*.yaml'. Defaults to '*'.
    """
    d = Path(directory)
    if not d.exists():
        return f"ERROR: Directory not found: {directory}"
    try:
        matches = sorted(d.glob(pattern))
        if not matches:
            return f"No files matching '{pattern}' in {directory}"
        return "\n".join(str(p) for p in matches)
    except Exception as e:
        return f"ERROR listing {directory}: {e}"
