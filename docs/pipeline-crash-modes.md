# Pipeline Crash Modes and Hard-Failure Paths

This document captures the repo's **fail-stop** modes: things that can crash the pipeline, force human escalation, or make the advertised resume flows unusable.

Reviewed against the current codebase on **2026-04-01**.

## Verification used for this review

- `./.venv/bin/python -m pytest tests -q` → **45 passed**
- Reproduced `run_agentic.py --spec specs/figaro_2019/replication_spec.yaml` → fails at human approval gate
- Reproduced `run_agentic.py --spec specs/figaro_2019/replication_spec.yaml --start-stage 3 --auto-approve` → crashes with `KeyError: 'metadata'`

## Highest-priority crash paths

| ID | Failure mode | Why it fails hard | Evidence |
|---|---|---|---|
| C1 | `run_agentic.py --spec ...` without `--auto-approve` | `run_agentic.py` loads the spec but leaves `spec_approved=False`, then the graph hits `human_approval_node` and raises immediately | `run_agentic.py:88-104`, `agents/orchestrator.py:27-40` |
| C2 | CLI / programmatic "resume from stage 3+" creates a new run directory instead of reusing the old one | `cli/main.py` and `run_agentic.py` always create a fresh `runs/<timestamp>/` and never accept `--run-dir`, but stages 3-6 assume prepared/model files already exist inside the current `run_dir` | `cli/main.py:76-92`, `cli/main.py:166-181`, `run_agentic.py:49-63`, `agents/orchestrator.py:199-201`, `nodes/model_builder.py:37-43` |
| C3 | Stage 0 PDF flow can crash after `pip install -e .` | `paper_analyst.py` imports `pypdf`, but `pyproject.toml` does not declare `pypdf` as a package dependency | `nodes/paper_analyst.py:90-95`, `pyproject.toml:10-25` |
| C4 | Invalid or missing API keys crash agentic stages | `get_llm()` and `_call_opus()` raise `EnvironmentError` when the required provider key is missing and fallback is unavailable | `agents/llm.py:58-104`, `nodes/paper_analyst.py:135-146` |
| C5 | Malformed specs can crash later nodes instead of failing early | `run` loads specs directly and most nodes index required fields with `spec[...]`; there is no mandatory schema validation in the run path | `cli/main.py:120-125`, `run_agentic.py:88-92`, `nodes/data_acquirer.py:33-40`, `nodes/model_builder.py:21-43` |
| C6 | Missing `data_manifest.yaml` in Stage 1 does not stop the graph, but Stage 2 then walks into a broken run | Data acquisition only logs a warning when the manifest is absent; the graph always continues to `data_preparer` | `nodes/data_acquirer.py:50-64`, `agents/orchestrator.py:118-123` |
| C7 | Long-running download/parse scripts can hard-timeout | `execute_python` kills any generated script after 600s, with no stage-specific timeout override | `agents/tools/execute_python.py:45-52` |
| C8 | Singular or oversized Leontief systems can crash in deterministic math | `np.linalg.inv(I - A)` is called directly, with no pre-check for singularity, conditioning, or model-size guardrails | `nodes/model_builder.py:82-91` |

## Detailed crash catalogue

### C1. Programmatic `--spec` flow is not self-approving

**What happens**
- `run_agentic.py` loads a spec into state.
- It sets `spec_approved` to `args.auto_approve`.
- If `--auto-approve` is omitted, the full graph still starts.
- `human_approval_node()` immediately raises.

**Observed reproduction**
```bash
.venv/bin/python run_agentic.py --spec specs/figaro_2019/replication_spec.yaml
```
This currently fails before Stage 1.

**Impact**
- The advertised programmatic entrypoint is unusable unless callers know to pass `--auto-approve`.
- This is especially easy to hit in automation.

### C2. "Resume from stage" is broken in the CLI and in `run_agentic.py`

**What happens**
- The README advertises `io-replicate run --spec ... --start-stage 3` and `--only reviewer` as resume flows.
- But both `cli/main.py` and `run_agentic.py` always create a fresh `run_dir`.
- Stages 3-6 expect data to already exist in the *current* `run_dir`.
- Fresh state contains empty `prepared_data_paths` / `model_paths` dicts.

**Observed reproduction**
```bash
.venv/bin/python run_agentic.py \
  --spec specs/figaro_2019/replication_spec.yaml \
  --start-stage 3 --auto-approve
```
Current result: `KeyError: 'metadata'` from `nodes/model_builder.py`.

**Impact**
- Resume from stage 3+ is broken unless the user switches to `run_deterministic.py --run-dir ...`.
- The README currently over-promises this path (`README.md:66-76`).

### C3. PDF mode can fail on a clean editable install

**What happens**
- Stage 0 requires `pypdf`.
- `requirements.txt` includes it, but `pyproject.toml` does not.
- `pip install -e .` therefore does not guarantee that PDF runs work.

**Impact**
- A user following the README can get a runtime import crash the first time they use `--paper`.

### C4. Missing credentials fail hard at the point of first model call

**What happens**
- `paper_analyst` requires Anthropic directly.
- Other agentic stages route through `get_llm()` and may fallback only if the alternative provider is configured *and* its key exists.

**Impact**
- Invalid/missing API keys are not soft-degraded into skipped stages.
- The pipeline stops immediately when the first required model call is attempted.

### C5. Invalid specs are allowed into the run path

**What happens**
- The repo has a schema validator.
- The `validate` CLI command uses it.
- The normal `run` flow does **not** require successful schema validation before executing downstream nodes.
- Downstream nodes index required sections like `spec["data_sources"]`, `spec["paper"]["reference_year"]`, etc.

**Impact**
- A malformed generated spec can survive Stage 0 and fail much later with a less actionable `KeyError` / `TypeError` / missing-column error.

### C6. Stage 1 can fail "softly" and cause a later hard failure

**What happens**
- `data_acquirer_node()` logs a warning if `data_manifest.yaml` is missing.
- It still returns normally.
- The graph always proceeds to `data_preparer`.
- Stage 2 then tries to parse raw data that may not exist.

**Impact**
- The true Stage 1 failure is delayed and obscured.
- Users debug Stage 2 even when the root cause was Stage 1 acquisition.

### C7. Generated scripts have a hard 600s wall-clock limit

**What happens**
- Every generated Python script runs through `subprocess.run(..., timeout=600)`.
- There is no acquisition-specific extension for slow APIs, retries, or big downloads.

**Impact**
- Slow APIs, exponential backoff, or large-source downloads can abort even when the logic is otherwise correct.

### C8. Deterministic math can still hard-fail on bad or large data

**What happens**
- `model_builder` computes a full dense inverse with `np.linalg.inv`.
- There is no guard for singular systems, bad conditioning, or model-size limits.

**Impact**
- Bad prepared data can raise linear algebra errors.
- Larger-than-FIGARO systems can hit RAM / runtime blowups before the repo can produce a review.

## Secondary hard-failure paths

- **Checkpointing may be unavailable**: the graph catches missing `langgraph.checkpoint.sqlite` and falls back to no checkpointing rather than crashing, but resume robustness drops (`agents/orchestrator.py:133-142`).
- **Old run directories are format-sensitive**: `run_deterministic.py` expects `A_EU.npy` / `L_EU.npy`; older runs with CSV-only model artifacts will fail to resume (`run_deterministic.py:71-77`).
- **Subprocess-generated code is not sandboxed**: agent-written scripts can still delete files, consume resources, or damage the run in ways that manifest as crashes (`agents/tools/execute_python.py:21-63`).

## Practical takeaway

Today, the most reliable execution paths are:

1. `io-replicate run --paper ...` or `io-replicate run --spec ... --auto-approve` for a fresh run.
2. `python run_deterministic.py --run-dir <existing run> --start-stage 3` for reusing an existing run.

The least reliable / currently misleading paths are:

- `run_agentic.py --spec ...` without `--auto-approve`
- `io-replicate run --spec ... --start-stage 3`
- `io-replicate run --spec ... --only reviewer`
