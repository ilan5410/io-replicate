# Non-Crashing Replication Failure Modes

This document captures the repo's **silent**, **partial**, or **false-success** modes: cases where the pipeline may finish, or even report success, while still failing to reproduce the paper faithfully.

Reviewed against the current codebase on **2026-04-01**.

## Verification used for this review

- `./.venv/bin/python -m pytest tests -q` → **45 passed**
- Local reviewer reproduction with an empty run directory returned:
  - `review_passed=True`
  - `PASS: 0 | WARN: 0 | FAIL: 0 | UNVERIFIED: 7`
- Static review of prompts, validators, and node routing

## The biggest false-success risks

| ID | Failure mode | Why it can pass silently | Evidence |
|---|---|---|---|
| R1 | Reviewer treats `UNVERIFIED` and `ERROR` benchmarks as non-failures | `review_passed = n_fail == 0`; missing decomposition files or missing `source` descriptors do not block success | `nodes/reviewer.py:43-63`, `agents/validators/benchmark_validator.py:63-73,109-117` |
| R2 | Tables/figures are not part of pass/fail logic | Reviewer validates benchmarks only; it never checks whether requested outputs were generated or whether they match the paper | `nodes/output_producer.py:53-74`, `nodes/reviewer.py:41-63` |
| R3 | Output Producer has no completeness gate | It collects whatever files happen to exist and returns normally even if many spec outputs were never produced | `nodes/output_producer.py:53-74`, `agents/orchestrator.py:125-130` |
| R4 | Paper Analyst prompt does not ask for benchmark `source` descriptors | New paper specs can contain benchmarks that are narratively described but not deterministically checkable | `agents/prompts/paper_analyst.py:42-45`, `README.md:145-160` |
| R5 | Prepared-data validation checks shape and magnitude, not exact ordering/labels | A misaligned matrix can pass structural checks while still producing wrong economics | `agents/validators/prep_validator.py:84-127` |
| R6 | Model/decomposition validity is not a routing gate | `model_valid` and `decomposition_valid` are computed, but the graph never stops on them | `nodes/model_builder.py:53-70`, `nodes/decomposer.py:69-74`, `agents/orchestrator.py:123-130` |
| R7 | The repo claims multi-source / generic-paper support, but the prompts are heavily FIGARO/Eurostat/Arto-specific | New papers may "run" with guessed logic that is methodologically wrong rather than explicitly blocked | `README.md:3-6`, `cli/main.py:25-35`, `agents/prompts/data_acquirer.py:26-135`, `agents/prompts/data_preparer.py:23-47` |
| R8 | WARN-level deviations still count as success | A result can be 10-24.9% off and still be reported as a passing replication | `agents/validators/benchmark_validator.py:77-82`, `nodes/reviewer.py:56-63` |

## Detailed non-crashing failure catalogue

### R1. The reviewer can report success even when nothing was actually validated

**What happens**
- Benchmarks without a `source` become `UNVERIFIED`.
- Benchmarks whose source file is missing become `ERROR`.
- `summarize()` groups both into the `UNVERIFIED` bucket.
- `reviewer_node()` marks the run as passed as long as `n_fail == 0`.

**Why this matters**
- A broken `--only reviewer` run on an empty directory can still end with `review_passed=True`.
- A new-paper run whose spec lacks source descriptors can also end "successfully" without deterministic verification.

### R2. The reviewer does not inspect paper outputs

**What happens**
- Stage 5 produces tables and figures.
- Stage 6 ignores them and reads only benchmark inputs from decomposition outputs.
- There is no check that the requested outputs were produced, complete, correctly named, or visually faithful.

**Why this matters**
- The pipeline can say "replication passed" even if the tables/figures are missing or wrong.
- This is the clearest gap between "the benchmark numbers are acceptable" and "the paper was replicated".

### R3. Output generation is best-effort, not validated

**What happens**
- `output_producer_node()` runs one agent loop for all requested outputs.
- It does not enforce that every table/figure in the spec was created.
- It returns a partial `output_paths` dict without raising.

**Why this matters**
- A long outputs section can be only partially produced.
- The misleading comment `per output item` on `MAX_ITERATIONS` is not true in implementation; the limit is global for the whole stage (`nodes/output_producer.py:15,47-50`).

### R4. New paper specs are likely to be under-instrumented for deterministic review

**What happens**
- The manual FIGARO spec contains `source` descriptors for benchmarks.
- The Paper Analyst prompt asks for benchmark names/values/units, but not for `source` descriptors.
- The README describes deterministic benchmark checks as if that capability were generally present.

**Why this matters**
- The best-case path (deterministic review) exists mainly for hand-authored specs.
- Auto-generated specs for new papers are likely to degrade into narrative review only.

### R5. Matrix order/content errors can slip through Stage 2 validation

**What happens**
- `prep_validator` verifies dimensions, non-negativity, rough balance, and one loose employment total check.
- It does **not** verify the exact country/product ordering, exact row labels, or CPA↔NACE mapping quality.

**Why this matters**
- An off-by-one country block, duplicated mapping, or wrong row order can still satisfy shape checks.
- The downstream model then produces reproducible but economically wrong outputs.

### R6. Suspicious model results do not stop the pipeline

**What happens**
- `model_builder` computes diagnostics (`n_col_sums_ge1`, `n_negative_L`, `identity_residual`) and a `model_valid` flag.
- The orchestrator never branches on `model_valid`.
- `decomposer` likewise returns `decomposition_valid=True` without any identity checks in-node.

**Why this matters**
- A mathematically dubious model can still flow into decomposition, output generation, and review.
- The current reviewer does not consume `model_checks` either.

### R7. "Generic paper replication" is broader than the implemented guidance

**What happens**
- README and CLI surface support for FIGARO, WIOD, OECD ICIO, and EXIOBASE.
- But the acquisition prompt is almost entirely Eurostat JSON-stat guidance.
- The preparation prompt is strongly tied to FIGARO IC-IOT structure and the Arto 2015 export definition.

**Why this matters**
- Non-FIGARO papers may not crash immediately; instead, the agent can improvise unsupported logic.
- That is worse than a hard failure because it produces plausible-looking but methodologically wrong results.

### R8. WARN-level benchmark misses still count as successful replication

**What happens**
- Deviations under `error_pct` but over `warning_pct` become `WARN`.
- The run still passes.

**Why this matters**
- For a research replication workflow, a 10-25% miss may still be unacceptable depending on the paper.
- The repo currently treats that as a successful overall run.

### R9. The first aggregation scheme wins, even if the paper needs several

**What happens**
- `decomposer_node()` uses only `list(agg_schemes.keys())[0]`.
- Any second or third aggregation scheme in the spec is ignored.

**Why this matters**
- Multi-aggregation papers can be only partially replicated.
- The pipeline may silently produce the wrong sector grouping for industry outputs.

### R10. Output specs and materialized files are not fully aligned

**What happens**
- The FIGARO spec requests `source_data: [employment_vector, export_vector]` for `table_1`.
- The output prompt references `employment_vector.csv` and `export_vector.csv` as if they exist.
- No deterministic node actually materializes those files under those names.

**Why this matters**
- Stage 5 depends on the LLM inferring how to synthesize those sources from prepared/model data.
- That can fail silently, especially for tables that are not benchmark-checked later.

### R11. Benchmark lookup semantics can hide ambiguous matches

**What happens**
- `benchmark_validator._resolve(..., op="lookup")` returns the first matched row.
- It does not assert uniqueness.

**Why this matters**
- Duplicate rows or under-specified filters can yield the wrong comparison value without a crash.

## Practical takeaway

Today, a run can look "green" while still failing replication in at least four important ways:

1. **Nothing meaningful was validated** (`UNVERIFIED` / `ERROR` benchmarks only).
2. **The right tables/figures were not actually produced**.
3. **The prepared matrices are structurally valid but semantically misaligned**.
4. **The paper's methodology is outside the FIGARO/Eurostat/Arto template, so the agents improvise**.

For this repo, "pipeline completed" and "paper replicated" are not yet equivalent statements.
