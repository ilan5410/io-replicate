"""
Tools: read_file, write_file, list_files
read_file is capped at 5,000 characters to prevent accidentally loading
huge matrix files (Z_EU, L_EU, A_EU are 1792×1792 = millions of tokens).
"""
from pathlib import Path

from langchain_core.tools import tool

_READ_LIMIT = 5_000  # characters


@tool
def read_file(path: str) -> str:
    """
    Read and return the contents of a file (capped at 5,000 characters).
    For large files, returns the first 5,000 chars and a truncation notice.
    Never use this to read matrix CSV files (Z_EU, L_EU, A_EU, em_exports) —
    use the decomposition summary files instead.

    Args:
        path: Absolute or relative path to the file.
    """
    p = Path(path)
    if not p.exists():
        return f"ERROR: File not found: {path}"

    # Block reads of known large matrix files
    name = p.name.lower()
    blocked = ["z_eu.csv", "l_eu.csv", "a_eu.csv", "em_exports_country_matrix.csv"]
    if any(name == b for b in blocked):
        size_mb = p.stat().st_size / 1_048_576
        return (
            f"BLOCKED: {p.name} is a large matrix file ({size_mb:.1f} MB). "
            f"Reading it would cost too much. Use the summary files in data/decomposition/ instead."
        )

    try:
        text = p.read_text()
    except Exception as e:
        return f"ERROR reading {path}: {e}"

    if len(text) > _READ_LIMIT:
        return text[:_READ_LIMIT] + f"\n\n[TRUNCATED — file is {len(text):,} chars, showing first {_READ_LIMIT:,}]"
    return text


@tool
def write_file(path: str, content: str) -> str:
    """
    Write content to a file, creating parent directories as needed.
    Restricted to the project directory or /tmp.

    Args:
        path: Absolute or relative path to write to.
        content: The text content to write.
    """
    import os as _os
    p = Path(path).resolve()
    cwd = Path(_os.getcwd()).resolve()
    if not (str(p).startswith(str(cwd)) or str(p).startswith("/tmp")):
        return f"ERROR: write_file is restricted to the project directory. Cannot write to {path}"
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)
        return f"OK: Written {len(content)} chars to {path}"
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
        lines = []
        for p in matches:
            size = p.stat().st_size if p.is_file() else 0
            size_str = f"{size/1024:.0f}KB" if size > 1024 else f"{size}B"
            lines.append(f"{p}  ({size_str})")
        return "\n".join(lines)
    except Exception as e:
        return f"ERROR listing {directory}: {e}"
