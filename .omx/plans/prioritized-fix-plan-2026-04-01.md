# Prioritized Fix Plan — Pipeline Reliability and Replication Fidelity

Created: 2026-04-01

## Requirements Summary

Turn the repo review docs into an execution-ready fix plan that prioritizes:
1. failures that make advertised workflows unusable,
2. false-success paths that can claim replication when validation is weak or absent,
3. replication-fidelity gaps that let materially wrong outputs pass.

Primary evidence sources:
- `docs/pipeline-crash-modes.md`
- `docs/replication-failure-modes.md`
- `cli/main.py:76-185`
- `run_agentic.py:49-111`
- `agents/orchestrator.py:27-40, 173-235`
- `nodes/output_producer.py:18-74`
- `nodes/reviewer.py:30-63`
- `agents/validators/prep_validator.py:84-127`
- `nodes/model_builder.py:53-70, 82-91`
- `nodes/decomposer.py:53-69`
- `nodes/paper_analyst.py:90-95`
- `pyproject.toml:10-25`

## Prioritization Principles

1. Fix **broken advertised workflows** before optimization.
2. Fix **false-success / false-green** paths before expanding scope.
3. Prefer **deterministic gates** over stronger prompts where possible.
4. Make replication failure **loud and actionable**, not silent and narrative.
5. Update docs only after code behavior matches the docs.

## Acceptance Criteria

### P0 acceptance criteria
- `run_agentic.py --spec <spec>` either prompts/auto-approves correctly or fails immediately with a targeted usage error instead of dying inside the graph. (`run_agentic.py:88-104`, `agents/orchestrator.py:27-40`)
- `io-replicate run --spec <spec> --start-stage 3` and `--only reviewer` work against an explicit existing run directory, or are removed from docs/CLI help until implemented. (`cli/main.py:52-57, 166-181`, `README.md:66-76`)
- Reviewer returns `review_passed=False` whenever all benchmarks are `UNVERIFIED`/`ERROR`, or when required decomposition inputs are missing. (`nodes/reviewer.py:43-63`, `agents/validators/benchmark_validator.py:63-73, 109-117`)
- Stage 1 fails closed when `data_manifest.yaml` is absent. (`nodes/data_acquirer.py:50-64`)

### P1 acceptance criteria
- Output stage fails if any required table/figure from `spec["outputs"]` is missing. (`nodes/output_producer.py:53-74`)
- Run path performs schema validation before any downstream stage executes. (`cli/main.py:120-125`, `run_agentic.py:88-92`, `agents/validators/spec_validator.py:22-37`)
- Prepared-data validation checks exact row/column ordering against spec metadata, not just shape. (`agents/validators/prep_validator.py:84-127`)
- Model/decomposition validity becomes a routing gate, not just logging. (`nodes/model_builder.py:53-70`, `agents/orchestrator.py:123-130`)

### P2 acceptance criteria
- PDF mode works after standard install because `pypdf` is declared in package dependencies. (`nodes/paper_analyst.py:90-95`, `pyproject.toml:10-25`)
- Benchmark `lookup` operations fail on ambiguous matches instead of silently taking the first row. (`agents/validators/benchmark_validator.py:163-175`)
- Aggregation handling is explicit when multiple schemes are present. (`nodes/decomposer.py:53-58`)
- Docs no longer imply broader support than the implemented prompts actually provide. (`README.md:3-6`, `agents/prompts/data_acquirer.py:26-135`, `agents/prompts/data_preparer.py:23-47`)

## Implementation Steps

### Step 1 — Fix broken execution entrypoints and resume semantics (P0)
**Why first:** the repo currently advertises resume/programmatic paths that do not work.

**Changes**
- Add `--run-dir` to `io-replicate run` and `run_agentic.py` for stage-3+ resume and `--only` flows.
- When `--start-stage >= 3` or `--only` is used, hydrate state from the specified run directory instead of creating empty `prepared_data_paths` / `model_paths`.
- For `run_agentic.py --spec` without `--auto-approve`, either:
  - set `spec_approved=True` for non-interactive spec-driven runs, or
  - reject the invocation before graph execution with a direct CLI error.
- Update `build_graph_from_stage` call sites to enforce the run-dir requirement for late-stage resumes.

**Files**
- `cli/main.py:45-185`
- `run_agentic.py:20-111`
- `agents/orchestrator.py:173-235`
- possibly add a shared run-state loader module, e.g. `agents/run_state.py`

**Tests / verification**
- Add CLI/programmatic tests for:
  - `--spec` without `--auto-approve`
  - `--start-stage 3 --run-dir <existing run>`
  - `--only reviewer --run-dir <existing run>`
- Manual smoke run against `runs/test_phase0` or a prepared fixture.

### Step 2 — Make Stage 1 and Stage 6 fail closed instead of succeeding vaguely (P0)
**Why second:** these are the most dangerous false-success paths.

**Changes**
- In `data_acquirer_node`, raise when `data_manifest.yaml` is missing or empty instead of logging a warning and continuing.
- In `reviewer_node`, compute failure conditions beyond `n_fail == 0`, for example:
  - fail if all benchmarks are `UNVERIFIED`/`ERROR`
  - fail if any benchmark resolution hit missing files
  - optionally fail if deterministic coverage is below a threshold
- Distinguish `ERROR` from `UNVERIFIED` in summary/reporting.

**Files**
- `nodes/data_acquirer.py:50-64`
- `nodes/reviewer.py:43-63, 113-133`
- `agents/validators/benchmark_validator.py:63-73, 109-117`

**Tests / verification**
- Add regression test proving reviewer on an empty run directory yields `review_passed=False`.
- Add test proving Stage 1 without manifest cannot proceed to Stage 2.

### Step 3 — Add deterministic completeness gates for outputs and specs (P1)
**Why here:** finishing without requested outputs is replication failure, not partial success.

**Changes**
- Validate the loaded spec inside the `run` path before graph execution.
- In `output_producer_node`, compare required output IDs from `spec["outputs"]` to materialized files and raise on missing outputs.
- Consider splitting output generation into per-output iterations or per-output subtasks so one bad figure does not consume the whole stage budget.

**Files**
- `cli/main.py:120-125, 166-185`
- `run_agentic.py:88-104`
- `nodes/output_producer.py:18-74`
- `agents/validators/spec_validator.py:22-37`

**Tests / verification**
- Add tests for invalid spec rejection during `run`.
- Add tests for missing output files causing stage failure.

### Step 4 — Strengthen semantic validation for prepared data and model health (P1)
**Why here:** wrong matrix ordering can pass shape checks and poison everything downstream.

**Changes**
- Extend `prep_validator` to verify exact country and industry order from `metadata.json` and CSV labels against spec ordering.
- Add stronger economic sanity checks: exact expected index cardinality, duplicate-code detection, and explicit CPA↔NACE mapping coverage diagnostics.
- Route on `model_valid`; do not continue to decomposition if the Leontief checks fail.
- Add decomposition validity checks instead of always returning `True`.

**Files**
- `agents/validators/prep_validator.py:28-127`
- `nodes/model_builder.py:53-70, 82-91`
- `nodes/decomposer.py:17-74`
- `agents/orchestrator.py:74-86, 123-130`

**Tests / verification**
- Add validator tests for row-order mismatch and duplicate mappings.
- Add model-builder test for singular / near-singular systems producing a controlled error.
- Add decomposition identity tests at node level, not only helper-function level.

### Step 5 — Close documented source/output mismatches that currently rely on LLM improvisation (P1)
**Why here:** some spec/output combinations are only working if the model infers missing artifacts.

**Changes**
- Materialize deterministic `employment_vector` / `export_vector` artifacts if the spec/output layer expects them.
- Or, change the FIGARO spec and Output Producer contract to reference actual generated files only.
- Ensure the output prompt and implementation agree on available source files.

**Files**
- `specs/figaro_2019/replication_spec.yaml:186-235`
- `agents/prompts/output_producer.py:12-21`
- `nodes/model_builder.py:165-171`
- `nodes/output_producer.py:36-74`

**Tests / verification**
- Add a test asserting every `source_data` token in the spec maps to a real file or a deterministic resolver.

### Step 6 — Fix installation and narrow the support contract (P2)
**Why here:** these improve trust and reduce unsupported "generic" behavior.

**Changes**
- Add `pypdf` to `pyproject.toml` dependencies.
- Reconcile README claims with implemented support:
  - either narrow docs to FIGARO-first support,
  - or add deterministic adapters for WIOD / OECD / EXIOBASE before claiming parity.
- Make prompt wording explicit about what is supported vs aspirational.

**Files**
- `pyproject.toml:10-25`
- `README.md:3-6, 32-76`
- `docs/user_guide.md`
- `agents/prompts/data_acquirer.py:26-135`
- `agents/prompts/data_preparer.py:23-47`

**Tests / verification**
- Fresh-env install smoke test for `--paper` mode.
- Documentation review to ensure all claimed workflows are executable.

### Step 7 — Tighten benchmark semantics and aggregation behavior (P2)
**Why here:** these are correctness refinements after the main fail-stop gaps are closed.

**Changes**
- Change benchmark `lookup` to require exactly one match.
- Add explicit policy for `WARN`: configurable pass/fail threshold or stricter paper-specific tolerances.
- Support named aggregation selection rather than silently choosing `list(agg_schemes.keys())[0]`.

**Files**
- `agents/validators/benchmark_validator.py:163-175`
- `nodes/reviewer.py:56-63`
- `nodes/decomposer.py:53-58`
- schema/docs/spec examples as needed

**Tests / verification**
- Add lookup-ambiguity regression tests.
- Add multi-aggregation spec test.
- Add reviewer behavior tests for WARN policy.

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Resume fixes spread across CLI, graph construction, and state hydration | Introduce a single helper for loading late-stage state from `run_dir`; test it directly before wiring CLI paths |
| Tightening Stage 6 may suddenly make existing runs fail | Roll out explicit report statuses (`PASS`, `WARN`, `FAIL`, `UNVERIFIED`, `ERROR`) and update docs/tests in the same change |
| Stronger validators may reject borderline historical runs | Start with exact ordering/uniqueness checks first; keep economic-threshold checks configurable |
| Output completeness gates may expose latent stage-5 brittleness | Land deterministic missing-file reporting and per-output logging before making it hard-fail |
| Narrowing support claims may feel like regression | Prefer truth in docs now; expand support later with deterministic adapters |

## Verification Steps

1. Run unit tests: `./.venv/bin/python -m pytest tests -q`
2. Add focused regression tests for each P0 item before changing behavior.
3. Verify fresh-run path:
   - `io-replicate run --spec specs/figaro_2019/replication_spec.yaml --auto-approve`
4. Verify resume path with explicit run dir:
   - `io-replicate run --spec specs/figaro_2019/replication_spec.yaml --start-stage 3 --run-dir <existing_run> --auto-approve`
5. Verify reviewer hard-fails on empty/missing decomposition inputs.
6. Verify output stage hard-fails when a required figure/table is absent.
7. Verify `--paper` works in a clean editable install.

## Recommended Execution Order

- **Sprint / PR 1:** Step 1 + Step 2
- **Sprint / PR 2:** Step 3 + Step 4
- **Sprint / PR 3:** Step 5 + Step 6 + Step 7

## Suggested First PR Scope

If you want the smallest high-value first patch, do this first:
1. Fix `run_agentic.py --spec` approval semantics.
2. Implement `--run-dir` for stage-3+ / `--only` flows.
3. Make reviewer fail when nothing was actually validated.
4. Make Stage 1 fail if the manifest is missing.

That PR would remove the two reproduced hard failures and the worst reproduced false-success path.
