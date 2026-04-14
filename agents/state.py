"""
PipelineState — shared state for the LangGraph IO Replicator pipeline.
"""
from typing import TypedDict, Optional


class PipelineState(TypedDict, total=False):
    # ── Run metadata ──────────────────────────────────────────────────────────
    run_id: str                    # Timestamp-based unique ID, e.g. "20250330_142301"
    run_dir: str                   # Path to runs/{run_id}/
    config: dict                   # Loaded infrastructure config (config.yaml)

    # ── Input ─────────────────────────────────────────────────────────────────
    paper_pdf_path: Optional[str]  # Path to the paper PDF (None if spec provided directly)
    user_hints: Optional[str]      # Free-text hints from the user to the Paper Analyst

    # ── Stage 0 output ────────────────────────────────────────────────────────
    replication_spec: dict         # Parsed replication_spec.yaml (the shared context)
    replication_spec_path: str     # Path to the written spec file
    spec_approved: bool            # Set to True after human_approval node

    # ── Stage 0.5 output (Classification Mapper) ─────────────────────────────
    concept_mappings: dict         # {concept_id: {codes, reasoning, sources, confidence, caveats}}

    # ── Stage 1 output ────────────────────────────────────────────────────────
    data_manifest: dict            # {table_code: {path, rows, checksum}, ...}
    acquisition_complete: bool

    # ── Stage 1.5 output (Data Guide) ─────────────────────────────────────────
    data_guide: dict               # Structured profile of all raw files (data_guide.yaml)

    # ── Stage 2 output ────────────────────────────────────────────────────────
    prepared_data_paths: dict      # {Z_EU: path, e_nonEU: path, x_EU: path, Em_EU: path, metadata: path}
    preparation_valid: bool
    preparation_errors: list       # Validation error messages from prep_validator

    # ── Stage 3 output ────────────────────────────────────────────────────────
    model_paths: dict              # {A_EU: path, L_EU: path, d_EU: path, em_exports_total: path, em_country_matrix: path}
    model_valid: bool
    model_checks: dict             # {max_col_sum, n_negative_L, identity_residual, ...}

    # ── Stage 4 output ────────────────────────────────────────────────────────
    decomposition_paths: dict      # {country_decomposition: path, annex_c_matrix: path, industry_table4: path, industry_figure3: path}
    decomposition_valid: bool

    # ── Stage 5 output ────────────────────────────────────────────────────────
    output_paths: dict             # {table_1: path, figure_1: path, ...}

    # ── Stage 6 output ────────────────────────────────────────────────────────
    review_report_path: str
    review_passed: bool
    review_warnings: list
    review_errors: list

    # ── Manual data upload gate ───────────────────────────────────────────────
    manual_download_required: bool       # True when data_acquirer cannot auto-download
    manual_download_instructions: dict   # {reason, files_needed: [{filename, source_url, place_at}]}

    # ── Control flow ─────────────────────────────────────────────────────────
    current_stage: int
    retry_count: int
    error_log: list
