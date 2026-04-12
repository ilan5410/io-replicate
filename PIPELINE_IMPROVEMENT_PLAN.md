# Pipeline Improvement Plan

> **Pipeline:** IO Replicator — `io-replicator/`
> **Reviewed:** 2026-04-12
> **Reviewer:** Claude (pipeline-reviewer skill)
> **Materials reviewed:** `agents/orchestrator.py`, `agents/state.py`, `agents/agent_runner.py`, `agents/message_utils.py`, `agents/token_tracker.py`, `agents/validators/benchmark_validator.py`, `nodes/output_producer.py`, `nodes/spec_reconciler.py`, `nodes/data_guide.py`, `nodes/reviewer.py`, `agents/prompts/paper_analyst.py`, `agents/prompts/output_producer.py`, `config.yaml`, `run_agentic.py`

---

## Executive Summary

The IO Replicator is a well-structured 10-node LangGraph pipeline that turns an IO economics paper PDF into validated tables, figures, and a benchmark report. The overall architecture is sound: agentic nodes are isolated, bounded by token circuit-breakers, and message history is trimmed. Three issues need immediate attention: `benchmark_validator._resolve` has a FIGARO-specific hardcode that silently breaks `sum_row` for every paper other than FIGARO; LLM-generated scripts execute unsandboxed as the current OS user; and `output_producer` failure does not halt the pipeline, causing the reviewer to run against empty outputs and report misleading results. Beyond those, cost visibility is incomplete (two nodes bypass LangChain so their token usage is not tracked), and several config parameters are hardcoded rather than read from `config.yaml`.

---

## Pipeline Map

```
PDF
 └─ paper_analyst (LLM, direct call, streaming)
     └─ classification_mapper (LLM, tool loop)
         └─ human_approval (gate — blocks until spec_approved)
             └─ data_acquirer (LLM, tool loop)
                 └─ data_guide (LLM, tool loop, 3-layer cache)
                     └─ data_preparer (LLM, tool loop)
                         └─[validation gate]─ model_builder (deterministic, numpy)
                             └─ decomposer (deterministic, numpy)
                                 └─ spec_reconciler (deterministic, pandas)
                                     └─ output_producer (LLM, direct call × 2 scripts)
                                         └─ reviewer (deterministic + LLM interpretation)
                                             └─[review gate]─ END
                                                           └─ human_escalation
```

**Nodes (10):**
- `paper_analyst` — reads PDF, writes `replication_spec.yaml` via direct streaming Anthropic call
- `classification_mapper` — maps spec concepts to standard industry/product codes
- `human_approval` — mandatory gate; blocks until `spec_approved` flag is set
- `data_acquirer` — downloads IO tables and employment data from Eurostat REST API
- `data_guide` — profiles raw data files; 3-layer cache (run-local → repo cache → LLM)
- `data_preparer` — parses raw CSVs into cleaned numpy matrices
- `model_builder` — builds Leontief model (A, L, d matrices)
- `decomposer` — decomposes employment content of exports by country and industry
- `spec_reconciler` — patches benchmark `source` descriptors; auto-sources unsourced ones
- `output_producer` — generates tables/figures via two direct LLM calls (tables + figures)
- `reviewer` — runs deterministic benchmark checks + single LLM interpretation call

**External dependencies:** Eurostat REST API, Anthropic API, LangChain/LangGraph, SQLite (checkpoint), local filesystem (`runs/`, `data_guides/`)

**Identified gaps:** `concept_mappings` state field is populated by `classification_mapper` but no downstream node visibly consumes it — possible dead field. The `data_preparer` node was not read in full; its validation gate logic was inferred from the orchestrator.

---

## P1 – Fix Now 🔴

### 1. `benchmark_validator._resolve` hardcodes `industry_table4`

- **Dimension:** Tool and prompt design / State management
- **Location:** `agents/validators/benchmark_validator.py:161`
- **Problem:** The validator uses a hardcoded `if file_key == "industry_table4"` check to decide whether to load with `index_col=0`. This is the exact FIGARO-specific pattern already fixed in `spec_reconciler`. For any other paper whose spec defines an index file under a different key, `op=sum_row` will fail with a `KeyError` because the index is not loaded correctly — a silent generalisation failure.
- **Fix:**
  ```python
  # Before (benchmark_validator.py:160-164)
  if file_key == "industry_table4":
      df = pd.read_csv(path, index_col=0)
  else:
      df = pd.read_csv(path)

  # After — mirror the same pattern as spec_reconciler
  schema = spec.get("output_schema", {})
  _LEGACY_INDEX_COL0 = {"industry_table4", "industry_figure3", "annex_c_matrix"}
  index_col0 = (
      {k for k, v in schema.items() if v.get("index_col")}
      if schema else _LEGACY_INDEX_COL0
  )
  # Then in _resolve, pass index_col0 and use:
  use_index = file_key in index_col0
  df = pd.read_csv(path, index_col=0 if use_index else None)
  ```
  Note: `_resolve` also needs `spec` passed in, or `index_col0` pre-built in `run_benchmark_checks` and threaded through.

---

### 2. `output_producer` failure does not halt the pipeline

- **Dimension:** Error handling and resilience / Termination and safety
- **Location:** `nodes/output_producer.py:192-234` (`_run_script_with_fix`)
- **Problem:** When both script-generation attempts fail, `_run_script_with_fix` logs, prints `[red]✗[/red]`, and returns `False` — but `output_producer_node` ignores the return value and continues to return normally. The reviewer then runs against an empty `outputs/` directory and reports all benchmarks as UNVERIFIED, which looks like a successful run with no data rather than a failed stage.
- **Fix:**
  ```python
  # In output_producer_node, after _run_script_with_fix calls:
  tables_ok = _run_script_with_fix(..., "generate_tables")
  figures_ok = _run_script_with_fix(..., "generate_figures")

  if not tables_ok and n_tables > 0:
      raise RuntimeError("Table generation failed after fix attempt — see logs")
  if not figures_ok and n_figures > 0:
      raise RuntimeError("Figure generation failed after fix attempt — see logs")
  ```
  This lets the pipeline route to `human_escalation` rather than silently continuing.

---

### 3. LLM-generated scripts execute unsandboxed

- **Dimension:** Security and data handling
- **Location:** `nodes/output_producer.py:250-266` (`_execute`), `agents/tools/execute_python.py`
- **Problem:** LLM-generated Python scripts are written to disk and executed via `subprocess.run` as the current OS user with full filesystem access. While the LLM is trusted and the timeout (120s) limits damage from infinite loops, there is no restriction on what the script can read, write, delete, or call. A prompt injection in a paper PDF could trigger unexpected file system operations.
- **Fix (short term):** Add a script content check that rejects imports of `os`, `subprocess`, `shutil`, `socket`, and `requests` outside a known whitelist before execution:
  ```python
  _FORBIDDEN_IMPORTS = {"os", "subprocess", "shutil", "socket", "requests", "urllib"}

  def _check_script_safety(script: str) -> None:
      for forbidden in _FORBIDDEN_IMPORTS:
          if re.search(rf"\bimport\s+{forbidden}\b", script):
              raise ValueError(f"Generated script imports forbidden module '{forbidden}'")
  ```
  **Long term:** Run generated scripts in a restricted subprocess with `--isolated` or inside a container/venv with no network access.

---

## P2 – Fix Soon 🟡

### 4. `paper_analyst` and `output_producer` token usage is invisible

- **Dimension:** Observability and debuggability / Cost and latency
- **Location:** `nodes/paper_analyst.py`, `nodes/output_producer.py` (both use `anthropic.Anthropic` SDK directly, not LangChain)
- **Problem:** These two nodes bypass `run_agent_loop` and `TokenTracker`. Their LLM costs are not surfaced in logs or the run summary. Since `paper_analyst` can use 32k output tokens and `output_producer` calls the API 2–4 times, this can represent a significant and invisible cost.
- **Fix:** After each `stream.get_final_message()` call, log the usage metadata:
  ```python
  response = stream.get_final_message()
  usage = response.usage
  log.info(
      f"{stage_name} LLM call: {usage.input_tokens} in + {usage.output_tokens} out tokens"
      f" (~${(usage.input_tokens * 3 + usage.output_tokens * 15) / 1_000_000:.3f})"
  )
  ```

---

### 5. `max_cost_per_stage` config is not wired to `run_agent_loop`

- **Dimension:** Cost and latency / Termination and safety
- **Location:** `agents/agent_runner.py:26`, `config.yaml:26`
- **Problem:** `config.yaml` defines `max_cost_per_stage: 2.0` but `run_agent_loop`'s `max_cost_usd` parameter defaults to `5.0` and is only sometimes set by calling nodes. The config value is not systematically read.
- **Fix:** Every node that calls `run_agent_loop` should pass the config value:
  ```python
  max_cost = config.get("pipeline", {}).get("max_cost_per_stage", 2.0)
  run_agent_loop(..., max_cost_usd=max_cost)
  ```

---

### 6. `data_guide` `MAX_ITERATIONS` is hardcoded

- **Dimension:** Termination and safety / Cost and latency
- **Location:** `nodes/data_guide.py:34`
- **Problem:** `MAX_ITERATIONS = 8` is hardcoded. For a paper with 15+ data files, 8 iterations may be insufficient to profile all files, leaving the guide incomplete. This should be configurable.
- **Fix:**
  ```python
  # In data_guide_node:
  max_iter = config.get("pipeline", {}).get("data_guide_max_iterations", 12)
  run_agent_loop(..., max_iterations=max_iter, ...)
  ```
  And add `data_guide_max_iterations: 12` to `config.yaml`.

---

### 7. Reviewer interpretation prompt grows with unverified count

- **Dimension:** Context management / Cost and latency
- **Location:** `nodes/reviewer.py:107-114`
- **Problem:** The reviewer sends the full list of UNVERIFIED benchmarks to the LLM (potentially 200+ entries). With 243 unverified benchmarks at ~60 chars each, this adds ~15k characters (~4k tokens) to the interpretation prompt every single run — even after auto-sourcing reduces the number.
- **Fix:** Cap the unverified list in the prompt:
  ```python
  MAX_UNVERIFIED_IN_PROMPT = 20
  if len(unverified) > MAX_UNVERIFIED_IN_PROMPT:
      shown = unverified[:MAX_UNVERIFIED_IN_PROMPT]
      unverified_section += f"\n... and {len(unverified) - MAX_UNVERIFIED_IN_PROMPT} more (omitted for brevity)"
  ```

---

### 8. `replication_spec_path` state field is populated but never consumed

- **Dimension:** State management
- **Location:** `agents/state.py:19`, `run_agentic.py:91`
- **Problem:** `initial_state["replication_spec_path"]` is set from `args.spec` but no node reads `state["replication_spec_path"]`. It is a dead field that adds noise to state inspection.
- **Fix:** Remove from `PipelineState` and from `run_agentic.py` initial state, or wire it to the reviewer (which could embed the spec path in the report).

---

## P3 – Nice to Have 🟢

| # | Dimension | Location | Finding | Recommendation |
|---|-----------|----------|---------|----------------|
| 1 | Cost and latency | `agents/validators/benchmark_validator.py:80` | `_resolve` re-reads CSVs from disk for every benchmark (up to 256 reads against 4 files) | Cache loaded DataFrames in `run_benchmark_checks` keyed by `file_key` |
| 2 | State management | `agents/state.py:7` | `TypedDict(total=False)` makes all fields optional; required fields like `run_dir`, `config`, `run_id` get no runtime enforcement | Split into a required base class and an optional fields class, or validate required keys at pipeline entry |
| 3 | Observability | `run_agentic.py:104` | No total pipeline cost summary written at the end of the run | Aggregate per-stage token logs and write a `outputs/run_summary.json` with total cost and stage durations |
| 4 | Tool and prompt design | `agents/prompts/paper_analyst.py:120` | The quality reference `specs/figaro_2019/replication_spec.yaml` is FIGARO-specific; for any other paper this instruction misleads the LLM | Replace with a general instruction: "Use the schema above as your quality reference; the field completeness matters more than any specific paper's values" |
| 5 | Termination and safety | `run_agentic.py:21` | No `--timeout` CLI argument; a hung data_acquirer (waiting on Eurostat) can block indefinitely | Add `--timeout <minutes>` with a `signal.alarm` or `threading.Timer` wrapper |
| 6 | Observability | All nodes | No LangSmith / trace store integration; agent loop reasoning steps are only visible in stdout | Add optional `LANGSMITH_API_KEY` env var check; if present, enable tracing via `langsmith.traceable` |
| 7 | State management | `agents/state.py:23` | `concept_mappings` field populated by `classification_mapper` but no downstream node appears to read it | Either wire it into `data_acquirer` initial prompt, or remove if unused |

---

## Quick-Win Checklist (P1 items)

```
P1 Fixes:
- [ ] Fix benchmark_validator._resolve: replace hardcoded `industry_table4` check
      with dynamic index_col0 set derived from output_schema (mirrors spec_reconciler fix)
- [ ] Make output_producer_node raise RuntimeError when _run_script_with_fix returns False
      so the pipeline routes to human_escalation instead of continuing silently
- [ ] Add _check_script_safety() guard in _execute to reject forbidden imports
      (os, subprocess, shutil, socket) before running LLM-generated scripts
```

---

## Longer-Term Recommendations

1. **Unify LLM calls under a single client abstraction.** Two nodes (`paper_analyst`, `output_producer`) use the raw Anthropic SDK while all others use LangChain. This creates two separate observability, retry, and cost-tracking stacks. Wrapping both behind a thin `call_llm(config, agent_name, system, user, max_tokens)` helper would centralise token logging, cost tracking, and model routing in one place.

2. **Add a pipeline-level cost budget and post-run summary.** Each stage currently tracks its own tokens in isolation. A `PipelineState.cost_log: list[dict]` accumulator (stage, tokens_in, tokens_out, cost_usd) written at each node exit would enable a `outputs/run_summary.json` with total cost and a `--max-total-cost` CLI circuit breaker.

3. **Script execution sandbox.** The current architecture relies on the LLM not generating harmful scripts. Consider running generated scripts in a subprocess with a read-only view of `data/decomposition/` and write access only to `outputs/`. On macOS this can be done with `sandbox-exec`; in CI, a Docker bind-mount gives the same guarantee without OS-specific tooling.

4. **Make `spec_reconciler` auto-sourcing iterative.** The current auto-source runs once with fixed tolerances. An iterative approach — tight tolerance first, then progressively loosen only for still-unmatched benchmarks — would reduce false positives while still reaching most matches. This would also make the tolerance logic easier to tune per paper.

5. **Introduce a `spec_validator` gate after `paper_analyst`.** Before the human reviews the spec, a deterministic check could flag missing required fields (no `output_schema`, no benchmarks, no geography entities) so the human can immediately see what needs fixing. `agents/validators/spec_validator.py` already exists but is not wired into the graph.

---

## What Looks Good

1. **Token circuit-breaker + message trimming.** `TokenTracker` raises before a runaway agent bankrupts a stage, and `trim_messages` keeps history bounded to 40 messages — both are clean, composable, and applied consistently in `run_agent_loop`.

2. **Three-layer data guide cache.** The fingerprint-based run-local → repo-cache → LLM cascade is elegant: first runs are expensive but subsequent runs on the same dataset are near-instant, and the guide can be committed to Git for team reuse without any external infrastructure.

3. **Deterministic decomposition + hybrid review.** Stages 3–4 are pure numpy, stages 4.5 and 6-phase-1 are pure pandas — no LLM involvement where it isn't needed. The benchmark validator is fully deterministic and free to run; the LLM is called only for the narrative interpretation. This is the right separation of concerns.

---

*Generated by the pipeline-reviewer skill. Review findings with the team before acting on P1 items in production.*
