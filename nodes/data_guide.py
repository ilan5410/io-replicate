"""
Stage 1.5: Data Guide Agent (AGENTIC)

Runs after data_acquirer (Stage 1), before data_preparer (Stage 2).

Caching strategy (three layers, cheapest first):
  1. Run-local: data/raw/data_guide.yaml already exists for this run → load & validate
  2. Repo cache: data_guides/{fingerprint}/data_guide.yaml committed to git → copy & validate
  3. LLM agent: profile only the files NOT already covered (incremental update)

After a successful LLM run, the guide is written to BOTH the run directory AND the repo
cache. Committing + pushing data_guides/ to GitHub makes guides reusable across machines.

Fingerprint = SHA-256(sorted manifest keys + filenames + sizes), first 16 hex chars.
This is stable across reruns that download the same dataset version.
"""
import hashlib
import logging
import shutil
from pathlib import Path

import yaml
from rich.console import Console
from rich.panel import Panel

from agents.agent_runner import run_agent_loop
from agents.llm import get_llm
from agents.prompts import DATA_GUIDE_SYSTEM_PROMPT
from agents.state import PipelineState
from agents.tools import make_execute_python_tool, read_file, write_file, list_files

log = logging.getLogger("data_guide")
_console = Console()
_DEFAULT_MAX_ITERATIONS = 12

# Root of the repo cache (relative to this file: nodes/ → repo root → data_guides/)
_REPO_ROOT = Path(__file__).parents[1]
_CACHE_DIR = _REPO_ROOT / "data_guides"


# ---------------------------------------------------------------------------
# Public node
# ---------------------------------------------------------------------------

def data_guide_node(state: PipelineState) -> dict:
    """LangGraph node: profile downloaded data and produce data_guide.yaml."""
    run_dir = Path(state["run_dir"])
    config = state.get("config", {})
    spec = state.get("replication_spec", {})
    manifest: dict = state.get("data_manifest", {})

    raw_dir = run_dir / "data" / "raw"
    guide_path = raw_dir / "data_guide.yaml"
    fingerprint = _fingerprint(manifest)
    cache_path = _CACHE_DIR / fingerprint / "data_guide.yaml"

    # ── Layer 1: run-local ────────────────────────────────────────────────────
    existing_guide, missing_keys = _load_and_validate(guide_path, manifest)
    if existing_guide is not None and not missing_keys:
        _console.print(Panel(
            "[bold]Stage 1.5 — Data Guide[/bold]\n"
            f"[green]✓ Loaded from run directory[/green] — "
            f"{len(existing_guide.get('files', {}))} file(s) covered\n"
            f"[dim]Fingerprint: {fingerprint}[/dim]",
            style="blue"
        ))
        return {"data_guide": existing_guide, "current_stage": 1}

    # ── Layer 2: repo cache ───────────────────────────────────────────────────
    cached_guide, cache_missing = _load_and_validate(cache_path, manifest)
    if cached_guide is not None and not cache_missing:
        _console.print(Panel(
            "[bold]Stage 1.5 — Data Guide[/bold]\n"
            f"[green]✓ Loaded from repo cache[/green] — "
            f"{len(cached_guide.get('files', {}))} file(s) covered\n"
            f"[dim]Fingerprint: {fingerprint}[/dim]",
            style="blue"
        ))
        # Copy to run directory so this run has a local copy too
        raw_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(cache_path, guide_path)
        log.info(f"Copied cached guide to {guide_path}")
        return {"data_guide": cached_guide, "current_stage": 1}

    # ── Layer 3: LLM agent ────────────────────────────────────────────────────
    uncovered = cache_missing if cached_guide else list(manifest.keys())
    partial_guide = cached_guide or {}

    _console.print(Panel(
        "[bold]Stage 1.5 — Data Guide[/bold]\n"
        + (
            f"[yellow]Partial cache hit[/yellow] — profiling {len(uncovered)} missing file(s): "
            f"{', '.join(uncovered)}"
            if partial_guide else
            f"Profiling {len(uncovered)} file(s) → data_guide.yaml\n"
            "[dim]Identifies columns, codes, quirks for all downstream stages[/dim]"
        )
        + f"\n[dim]Fingerprint: {fingerprint}[/dim]",
        style="blue"
    ))

    raw_dir.mkdir(parents=True, exist_ok=True)
    initial_message = _build_initial_message(
        manifest, uncovered, partial_guide, spec, run_dir
    )

    execute_python = make_execute_python_tool(str(run_dir))
    tools = [execute_python, read_file, write_file, list_files]
    llm = get_llm("data_guide", config).bind_tools(tools)

    pipeline_cfg = config.get("pipeline", {})
    max_iter = pipeline_cfg.get("data_guide_max_iterations", _DEFAULT_MAX_ITERATIONS)
    max_cost = pipeline_cfg.get("max_cost_per_stage", 2.0)

    run_agent_loop(
        llm=llm,
        tools=tools,
        system_prompt=DATA_GUIDE_SYSTEM_PROMPT,
        initial_message=initial_message,
        stage_name="data_guide",
        max_iterations=max_iter,
        max_cost_usd=max_cost,
    )

    # Load what the agent wrote
    data_guide, _ = _load_and_validate(guide_path, manifest)
    if data_guide is None:
        data_guide = {}
        log.warning("Agent did not write data_guide.yaml — downstream stages will use defaults")
        _console.print("  [yellow]⚠[/yellow]  data_guide.yaml not produced")
    else:
        covered = len(data_guide.get("files", {}))
        _console.print(
            f"  [green]✓[/green] Data guide written — {covered} file(s) profiled"
        )
        # Write-through to repo cache
        _write_to_cache(cache_path, guide_path, fingerprint, manifest, spec)

    # Patch spec parser if the profiled file format doesn't match what spec says
    spec_patch = _detect_parser_mismatch(raw_dir, spec)
    result: dict = {"data_guide": data_guide, "current_stage": 1}
    if spec_patch:
        result["replication_spec"] = spec_patch
    return result


# ---------------------------------------------------------------------------
# Parser mismatch detection
# ---------------------------------------------------------------------------

# Same signatures as data_preparer._autodetect_parser — kept in sync manually
_FILE_SIGNATURES: list[tuple[str, str]] = [
    ("naio_10_fcp_ip1",   "figaro_iciot"),
    ("figaro",            "figaro_iciot"),
    ("wiot",              "wiod_mrio"),
    ("wiod",              "wiod_mrio"),
    ("icio",              "oecd_icio"),
    ("mriot",             "exiobase"),
    ("exiobase",          "exiobase"),
]


def _detect_parser_mismatch(raw_dir: Path, spec: dict) -> dict | None:
    """
    Check if the actual raw files imply a different parser than spec says.

    Returns an updated copy of spec (with corrected io_table.type) if a
    mismatch is found, or None if spec is already correct / undetectable.
    """
    if not raw_dir.exists():
        return None
    filenames = [f.name.lower() for f in raw_dir.iterdir()]
    spec_type = spec.get("data_sources", {}).get("io_table", {}).get("type", "")
    for sig, detected in _FILE_SIGNATURES:
        if any(sig in fn for fn in filenames):
            if detected != spec_type:
                import copy
                patched = copy.deepcopy(spec)
                patched.setdefault("data_sources", {}).setdefault("io_table", {})["type"] = detected
                log.warning(
                    f"Parser mismatch detected: spec says '{spec_type}' but raw files "
                    f"match '{detected}' — patching replication_spec in state"
                )
                _console.print(
                    f"  [yellow]⚠[/yellow]  Parser corrected: "
                    f"[red]{spec_type}[/red] → [green]{detected}[/green] "
                    f"(based on raw file names)"
                )
                return patched
            return None  # matched — spec already correct
    return None


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _fingerprint(manifest: dict) -> str:
    """
    Stable 16-char hex fingerprint derived from the manifest.

    Uses sorted (key, filename_stem, size_bytes) tuples so the fingerprint is
    the same across reruns that download identical data, regardless of timestamps.
    """
    parts = []
    for key in sorted(manifest.keys()):
        entry = manifest[key]
        filename = Path(entry.get("path", "")).name if entry.get("path") else key
        size = entry.get("size_bytes", 0)
        parts.append(f"{key}:{filename}:{size}")
    digest = hashlib.sha256("|".join(parts).encode()).hexdigest()
    return digest[:16]


def _load_and_validate(
    path: Path, manifest: dict
) -> tuple[dict | None, list[str]]:
    """
    Load a data_guide.yaml and check which manifest keys are NOT covered.

    Returns (guide_dict, missing_keys).
    If the file doesn't exist or is unreadable, returns (None, all_manifest_keys).
    """
    if not path.exists():
        return None, list(manifest.keys())
    try:
        guide = yaml.safe_load(path.read_text()) or {}
    except Exception as e:
        log.warning(f"Could not parse {path}: {e}")
        return None, list(manifest.keys())

    covered = set(guide.get("files", {}).keys())
    missing = [k for k in manifest if k not in covered]
    return guide, missing


def _write_to_cache(
    cache_path: Path,
    guide_path: Path,
    fingerprint: str,
    manifest: dict,
    spec: dict,
) -> None:
    """Copy the generated guide to the repo cache with a README."""
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(guide_path, cache_path)

        # Write a human-readable README alongside the guide
        readme_path = cache_path.parent / "README.md"
        dataset_ids = sorted(manifest.keys())
        paper_title = spec.get("paper", {}).get("title", "unknown")
        ref_year = spec.get("paper", {}).get("reference_year", "unknown")
        readme_path.write_text(
            f"# Data Guide Cache\n\n"
            f"**Fingerprint**: `{fingerprint}`  \n"
            f"**Paper**: {paper_title}  \n"
            f"**Reference year**: {ref_year}  \n"
            f"**Datasets**: {', '.join(dataset_ids)}  \n\n"
            f"## Files covered\n\n"
            + "".join(
                f"- `{k}`: {manifest[k].get('path', 'unknown')}\n"
                for k in dataset_ids
            )
            + "\n## Usage\n\n"
            f"Any pipeline run whose manifest produces fingerprint `{fingerprint}` "
            f"will automatically load this guide instead of calling the LLM.\n\n"
            f"To share: `git add data_guides/ && git commit -m 'cache: add data guide "
            f"{fingerprint}' && git push`\n"
        )
        log.info(f"Guide cached at {cache_path} (fingerprint: {fingerprint})")
        _console.print(
            f"  [dim]Cached → data_guides/{fingerprint}/ "
            f"(git add + push to share)[/dim]"
        )
    except Exception as e:
        log.warning(f"Failed to write repo cache: {e}")


# ---------------------------------------------------------------------------
# LLM message builder
# ---------------------------------------------------------------------------

def _build_initial_message(
    manifest: dict,
    uncovered: list[str],
    partial_guide: dict,
    spec: dict,
    run_dir: Path,
) -> str:
    manifest_text = yaml.dump(manifest, default_flow_style=False)
    spec_geo = spec.get("geography", {})
    spec_cls = spec.get("classification", {})
    spec_ds = spec.get("data_sources", {})

    partial_section = ""
    if partial_guide:
        already_covered = list(partial_guide.get("files", {}).keys())
        partial_section = (
            f"\n## Existing partial guide\n"
            f"The following files are ALREADY profiled — do not re-profile them:\n"
            + "".join(f"  - {k}\n" for k in already_covered)
            + f"\nThe current guide content:\n```yaml\n"
            + yaml.dump(partial_guide, default_flow_style=False)[:2000]
            + "\n```\n"
            f"Merge your new profiling results into this existing guide before writing.\n"
        )

    return f"""Profile the following raw data files and produce (or update) `data/raw/data_guide.yaml`.

## Files to profile NOW
{', '.join(uncovered)}
{partial_section}
## Full data manifest
```yaml
{manifest_text}
```

## Spec context (for alignment cross-check)
- Analysis entities: {spec_geo.get('analysis_entities', 'unknown')}
- External entities: {spec_geo.get('external_entities', 'unknown')}
- Classification system: {spec_cls.get('system', 'unknown')}
- Industry codes: {spec_cls.get('industry_codes', 'unknown')}
- Reference year: {spec.get('paper', {}).get('reference_year', 'unknown')}
- Data sources: {list(spec_ds.keys()) if spec_ds else 'unknown'}

## Instructions
1. Run one profiling script per file listed under "Files to profile NOW".
2. Cross-check codes found against the spec.
3. Write the complete merged `data/raw/data_guide.yaml` (all files, old + new).

Run directory: {run_dir}
All script paths must be relative to the run directory.
"""
