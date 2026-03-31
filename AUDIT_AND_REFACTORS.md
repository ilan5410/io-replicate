# IO Replicator — Technical Audit & Implementation Plan

> **Generated**: 2026-03-31  
> **Scope**: Full codebase review of `io-replicate` (commit at HEAD)  
> **Target**: Claude Code implementation of all actionable items

---

## 1. Executive Summary

The codebase is well-architected for an early-stage project — clean separation between agentic and deterministic stages, a shared spec as single source of truth, and sensible guardrails (file read caps, blocked matrix reads). However, several issues need fixing before production use:

1. **🔥 Agentic tool loops have no per-iteration token/cost tracking** — a single bad agent run can silently burn $50–200+ in API calls with no circuit breaker.
2. **🔥 `execute_python` is an unsandboxed arbitrary code execution tool** — the LLM can write and run any Python, including `os.system`, `subprocess`, network calls, or file deletion outside the run directory.
3. **🔥 Message history accumulates unboundedly inside agent loops** — every tool call result is appended and re-sent on the next `llm.invoke()`, causing quadratic token growth.
4. **🔥 The `decomposer._compute_industry_decomposition` Figure 3 loop** does `L @ e_cp` (a 1792×1792 matrix-vector multiply) once per country-per-product — up to 28×64 = 1,792 times. This is O(N³) when O(N²) suffices.
5. **Retry logic in `data_preparer` re-runs the entire agent from scratch** including re-sending the full system prompt and re-invoking the LLM, rather than feeding errors back into the existing conversation.
6. **No timeout or cost ceiling on agentic stages** — `MAX_ITERATIONS` caps loop count but not wall-clock time or token spend.
7. **`Anthropic()` client is re-instantiated on every call** in `paper_analyst._call_opus` — should be cached.
8. **`SqliteSaver.from_conn_string` is called without `check_same_thread=False`** — will crash in async/threaded contexts.
9. **Schema is reloaded from disk on every `validate_spec` call** — should be cached at module level.
10. **No `.gitignore`** — `runs/`, `__pycache__/`, `.env` will be committed.

---

## 2. High-Risk Issues (🔥)

### 2.1 🔥 Unbounded Token Growth in Agent Loops

**Files**: `nodes/data_acquirer.py:51-66`, `nodes/data_preparer.py:65-80`, `nodes/output_producer.py:54-69`, `nodes/reviewer.py:57-72`

**Problem**: Every agentic node follows the same pattern:
```python
for iteration in range(MAX_ITERATIONS):
    response = llm.invoke(messages)  # sends ALL accumulated messages
    messages.append(response)
    for tool_call in response.tool_calls:
        result = tool_map[tool_name].invoke(tool_call["args"])
        messages.append(ToolMessage(content=str(result), tool_call_id=tool_id))
```

Each iteration appends the assistant response + all tool results, then re-sends the entire history. By iteration 10 of `data_acquirer` (MAX=20), the message list can easily contain 50+ messages with large stdout/stderr payloads from `execute_python`.

**Worst case**: `data_acquirer` with 20 iterations, each executing a script that prints 8KB of stdout. Input tokens grow as `sum(1..20) * ~2K ≈ 420K input tokens` per run. At Claude Sonnet pricing ($3/M input), that's ~$1.26 per node invocation just for the accumulation effect. With Opus for `paper_analyst`, it would be much more.

**Fix**: Implement a sliding window or summarization strategy for message history.

```
ACTION:
- Create a helper `agents/message_utils.py` with a function `trim_messages(messages, max_tokens=50000)` that:
  1. Always preserves the SystemMessage and first HumanMessage
  2. Always preserves the last 4 message pairs (assistant + tool results)
  3. Summarizes or drops middle messages when total estimated tokens exceed max_tokens
  4. Truncates individual ToolMessage content to 2000 chars (down from current implicit unlimited)
- Apply `messages = trim_messages(messages)` before each `llm.invoke(messages)` call in all 4 agentic nodes
```

---

### 2.2 🔥 Unsandboxed Code Execution

**File**: `agents/tools/execute_python.py:39-44`

**Problem**: `subprocess.run(["python3", str(script_path)])` runs arbitrary LLM-generated code with full filesystem and network access. The LLM could:
- Delete files outside `run_dir`
- Read environment variables (API keys)
- Make arbitrary network requests
- Install packages or modify the system

**Fix**: Add a `cwd` restriction and basic safety checks.

```
ACTION in agents/tools/execute_python.py:
1. Add `cwd=str(Path(run_dir))` to subprocess.run() so scripts run from within the run directory
2. Add a pre-execution safety scan that rejects scripts containing:
   - `os.environ` (except os.environ.get for safe keys)
   - `subprocess` or `os.system` 
   - `shutil.rmtree` on paths outside run_dir
   - `open()` calls with absolute paths outside run_dir
   - `requests` or `urllib` calls (data_acquirer gets an explicit exemption)
3. Add an `allow_network: bool = False` parameter to `make_execute_python_tool`
   - Set `allow_network=True` only for `data_acquirer_node`
   - When False, the safety scan also blocks `requests`, `urllib`, `http`
4. Add `env` parameter to subprocess.run that strips API keys:
   ```python
   import os
   safe_env = {k: v for k, v in os.environ.items() 
               if not any(secret in k.upper() for secret in ['API_KEY', 'SECRET', 'TOKEN', 'PASSWORD'])}
   result = subprocess.run(["python3", str(script_path)], 
                          capture_output=True, text=True, timeout=600,
                          cwd=str(Path(run_dir)), env=safe_env)
   ```
```

---

### 2.3 🔥 O(N³) Decomposer Figure 3 Computation

**File**: `nodes/decomposer.py:133-146`

**Problem**: The Figure 3 domestic computation does:
```python
for c_idx in range(N):        # 28 countries
    for p_idx in j_prods:     # ~6-7 products per sector
        e_cp = np.zeros(N_EU) # N_EU = 1792
        e_cp[flat] = e[flat]
        Le_cp = L @ e_cp      # 1792×1792 matmul — called up to 1792 times!
        domestic_j += np.dot(d[c_start:c_end], Le_cp[c_start:c_end])
```

This is doing `L @ e_cp` for each individual non-zero element of e. Since `e_cp` is a one-hot-like vector, `L @ e_cp` is just extracting a column of L scaled by `e[flat]`. The entire loop can be replaced with vectorized numpy.

**Fix**:

```
ACTION in nodes/decomposer.py — replace _compute_industry_decomposition's Figure 3 loop:

Replace the inner loop (lines ~133-146) with:
```python
for j_sec_idx, j_sec in enumerate(sector_names):
    j_prods = [idx - 1 for idx in agg[j_sec]]
    col_total = table4[:, j_sec_idx].sum()
    
    # Vectorized domestic computation — no per-element matmul
    domestic_j = 0.0
    for c_idx in range(N):
        c_start, c_end = c_idx * P, (c_idx + 1) * P
        # Extract this country's product indices for sector j
        flat_indices = [c_idx * P + p_idx for p_idx in j_prods]
        # Build sector export vector for this country (only sector j products)
        e_cj = np.zeros(N_EU)
        for flat in flat_indices:
            e_cj[flat] = e[flat]
        if e_cj.sum() == 0:
            continue
        # Single matmul per country (not per product)
        Le_cj = L @ e_cj
        domestic_j += np.dot(d[c_start:c_end], Le_cj[c_start:c_end])
    
    fig3_rows.append({...})
```

This reduces from N*P matmuls to N matmuls per sector (28 instead of ~180 per sector).

Even better — fully vectorized:
```python
# Pre-compute all domestic effects in one pass
for j_sec_idx, j_sec in enumerate(sector_names):
    j_prods = [idx - 1 for idx in agg[j_sec]]
    col_total = table4[:, j_sec_idx].sum()
    domestic_j = 0.0
    for c_idx in range(N):
        c_start, c_end = c_idx * P, (c_idx + 1) * P
        flat_indices = [c_idx * P + p_idx for p_idx in j_prods]
        e_slice = e[flat_indices]
        if e_slice.sum() == 0:
            continue
        # L columns for these indices, restricted to country c's rows
        L_block = L[c_start:c_end, :][:, flat_indices]  # (P, len(j_prods))
        domestic_j += float(d[c_start:c_end] @ L_block @ e_slice)
    fig3_rows.append({
        "sector": j_sec,
        "total_employment_THS": col_total,
        "domestic_THS": domestic_j,
        "spillover_THS": col_total - domestic_j,
    })
```
```

---

### 2.4 🔥 No Cost/Token Tracking or Circuit Breaker

**Files**: All agentic nodes, `agents/llm.py`

**Problem**: There is no mechanism to:
1. Track cumulative token usage across iterations
2. Abort if a cost threshold is exceeded
3. Log per-call token counts for post-hoc analysis

A misbehaving agent (e.g., data_preparer stuck in a retry loop with MAX_RETRIES=3 × MAX_AGENT_ITERATIONS=12 = 36 LLM calls per stage) can rack up significant costs silently.

**Fix**:

```
ACTION:
1. Add to config.yaml:
   ```yaml
   pipeline:
     max_tokens_per_stage: 200000   # abort if cumulative input+output tokens exceed this
     max_cost_per_run_usd: 10.0     # abort entire run if estimated cost exceeds this
   ```

2. Create `agents/token_tracker.py`:
   ```python
   class TokenTracker:
       def __init__(self, max_tokens: int = 200_000):
           self.max_tokens = max_tokens
           self.total_input = 0
           self.total_output = 0
           self.calls = []
       
       def record(self, response):
           """Extract token usage from LangChain response metadata."""
           usage = getattr(response, 'usage_metadata', None) or {}
           inp = usage.get('input_tokens', 0)
           out = usage.get('output_tokens', 0)
           self.total_input += inp
           self.total_output += out
           self.calls.append({'input': inp, 'output': out})
           if self.total_input + self.total_output > self.max_tokens:
               raise RuntimeError(
                   f"Token budget exceeded: {self.total_input + self.total_output} > {self.max_tokens}. "
                   f"Aborting to prevent cost overrun."
               )
           return inp, out
       
       def summary(self) -> dict:
           return {
               'total_input_tokens': self.total_input,
               'total_output_tokens': self.total_output,
               'n_calls': len(self.calls),
           }
   ```

3. Use in every agentic node:
   ```python
   from agents.token_tracker import TokenTracker
   max_tokens = config.get("pipeline", {}).get("max_tokens_per_stage", 200_000)
   tracker = TokenTracker(max_tokens)
   
   for iteration in range(MAX_ITERATIONS):
       response = llm.invoke(messages)
       tracker.record(response)
       ...
   
   log.info(f"Token usage: {tracker.summary()}")
   ```
```

---

### 2.5 🔥 Data Preparer Retry Wastes All Prior Agent Work

**File**: `nodes/data_preparer.py`, `agents/orchestrator.py:74-80`

**Problem**: When preparation validation fails and `retry_count < max_retries`, the orchestrator routes back to `data_preparer`, which starts a **completely new agent conversation** — new system prompt, new initial message, new LLM context. All the work from the previous attempt (the script it wrote, the errors it diagnosed) is lost except for a brief `error_context` string.

This means:
- The agent will likely write the same bad script again
- Each retry burns the full token budget of a fresh agent run
- The error context is appended as text but the agent has no memory of its previous code

**Fix**:

```
ACTION in nodes/data_preparer.py:
1. Save the generated script path from each attempt in state:
   Add `last_script_path: Optional[str]` to the state return
   
2. On retry, include the previous script content in the initial message:
   ```python
   if prior_errors:
       # Find the most recent script from generated_scripts/
       scripts = sorted((run_dir / "generated_scripts").glob("*.py"), key=lambda p: p.stat().st_mtime)
       if scripts:
           last_script = scripts[-1].read_text()
           error_context = (
               f"\n\nPrevious attempt FAILED validation. Here is the script you wrote:\n"
               f"```python\n{last_script[:3000]}\n```\n\n"
               f"Validation errors:\n" + "\n".join(f"- {e}" for e in prior_errors) +
               f"\n\nFix these errors. Do NOT rewrite from scratch — modify the approach."
           )
   ```
```

---

## 3. API Cost Analysis

### 3.1 Cost Model per Pipeline Run

| Stage | Model | Est. Input Tokens | Est. Output Tokens | Est. Cost |
|-------|-------|-------------------|--------------------|-----------| 
| 0: Paper Analyst | Opus | ~30K (paper) + 5K (schema/prompt) | ~5K (spec YAML) | ~$0.90 |
| 1: Data Acquirer | GPT-4o-mini | ~5K × 10 iterations avg | ~2K × 10 | ~$0.01 |
| 2: Data Preparer | Sonnet | ~5K × 8 iterations avg (with accumulation: ~80K total) | ~3K × 8 | ~$0.35 |
| 3: Model Builder | None (numpy) | 0 | 0 | $0.00 |
| 4: Decomposer | None (numpy) | 0 | 0 | $0.00 |
| 5: Output Producer | GPT-4o-mini | ~3K × 8 iterations avg | ~2K × 8 | ~$0.01 |
| 6: Reviewer | Sonnet | ~3K × 5 iterations avg | ~2K × 5 | ~$0.08 |

**Typical run**: ~$1.35  
**Worst case (all retries, all max iterations)**: ~$8-15  
**Catastrophic case (infinite retry loop bug)**: Unbounded — needs circuit breaker

### 3.2 Cost Explosion Scenarios

1. **Data Preparer retry loop**: 3 retries × 12 iterations × growing message history = ~$3-5 for one stage
2. **Paper Analyst with huge PDF**: A 100-page paper → ~100K input tokens to Opus at $15/M = $1.50 for one call
3. **Message accumulation**: Without trimming, iteration 20 of data_acquirer sends ~200K tokens

### 3.3 Cost Reduction Strategies

```
ACTION — implement all of these:

1. Message history trimming (see §2.1) — saves ~40% on agentic stages

2. In agents/llm.py, cache the LLM client:
   Add module-level cache:
   ```python
   _llm_cache: dict[str, BaseChatModel] = {}
   
   def get_llm(agent_name: str, config: dict):
       cache_key = f"{agent_name}:{id(config)}"
       if cache_key in _llm_cache:
           return _llm_cache[cache_key]
       ...
       _llm_cache[cache_key] = llm
       return llm
   ```

3. In nodes/paper_analyst.py _call_opus: 
   - Cache the anthropic.Anthropic() client at module level
   - Consider using Sonnet instead of Opus for the paper_analyst when 
     papers are short (<20 pages). Add to config:
     ```yaml
     llm:
       routing:
         paper_analyst_short: anthropic/claude-sonnet-4-6  # <20 pages
         paper_analyst_long: anthropic/claude-opus-4-6     # >=20 pages
     ```

4. In nodes/reviewer.py:
   - The reviewer only needs to read 2-3 small CSV files and write a report.
   - Consider making it deterministic (no LLM) for benchmark comparisons,
     and only use LLM for the interpretation section.
```

---

## 4. Detailed Findings

### 4.1 API Usage

#### 4.1.1 `paper_analyst._call_opus` creates a new Anthropic client per call

**File**: `nodes/paper_analyst.py:135`

```
ACTION: Cache the client.
```python
_anthropic_client = None

def _call_opus(prompt, config):
    global _anthropic_client
    ...
    if _anthropic_client is None:
        _anthropic_client = anthropic.Anthropic(api_key=api_key)
    response = _anthropic_client.messages.create(...)
```
```

#### 4.1.2 `get_llm` has no temperature/seed control

**File**: `agents/llm.py:97-102`

The LLM factories don't set `temperature`. For deterministic-ish stages (reviewer benchmarking), temperature=0 would reduce variance.

```
ACTION in agents/llm.py:
Add temperature config:
```python
def _make_anthropic(model_id, api_key, temperature=None):
    kwargs = {"model": model_id, "api_key": api_key, "max_tokens": 8192}
    if temperature is not None:
        kwargs["temperature"] = temperature
    return ChatAnthropic(**kwargs)
```

And in get_llm:
```python
temperature = config.get("llm", {}).get("temperatures", {}).get(agent_name, None)
```

Add to config.yaml:
```yaml
llm:
  temperatures:
    reviewer: 0.0
    paper_analyst: 0.2
```
```

#### 4.1.3 LLM fallback logic may silently switch models

**File**: `agents/llm.py:51-78`

If ANTHROPIC_API_KEY is missing, the code silently falls back to OpenAI. This could cause unexpected behavior (different model capabilities) and unexpected costs.

```
ACTION in agents/llm.py:
Add a log.warning when falling back:
```python
log.warning(f"Falling back from {provider}/{model_id} to {fallback_provider}/{fallback_model} "
            f"(API key for {provider} not found)")
```
```

### 4.2 Loops & Control Flow

#### 4.2.1 `route_after_prep_validator` retry loop re-enters the full agent

Already covered in §2.5.

#### 4.2.2 Model builder country matrix loop is O(N²·P) but could be vectorized

**File**: `nodes/model_builder.py:112-122`

```python
for s_idx in range(N):
    e_s = np.zeros(N * P)
    e_s[s_idx * P:(s_idx + 1) * P] = e_nonEU[s_idx * P:(s_idx + 1) * P]
    Le_s = L @ e_s  # 1792×1792 matmul
    for r_idx in range(N):
        em_country_matrix[r_idx, s_idx] = np.dot(d[r_start:r_end], Le_s[r_start:r_end])
```

This does N=28 full matrix-vector multiplies. The inner loop over `r_idx` is fast (just dot products). The 28 matmuls are unavoidable for the decomposition but could be done as a single `L @ E_block` where E_block is N×P reshaped.

```
ACTION in nodes/model_builder.py — vectorize _compute_employment_content:
```python
def _compute_employment_content(d, L, e_nonEU, eu_countries, cpa_codes):
    N = len(eu_countries)
    P = len(cpa_codes)
    NP = N * P
    
    em_exports_total = d * (L @ e_nonEU)
    
    # Reshape e into (NP, N) where each column is one country's exports
    E_mat = np.zeros((NP, N))
    for s_idx in range(N):
        E_mat[s_idx*P:(s_idx+1)*P, s_idx] = e_nonEU[s_idx*P:(s_idx+1)*P]
    
    # Single matrix multiply: L @ E_mat → (NP, N)
    LE = L @ E_mat
    
    # Compute country matrix: em_country_matrix[r, s] = d[r_block] · LE[r_block, s]
    D = d.reshape(N, P)  # (N, P)
    LE_reshaped = LE.reshape(N, P, N)  # (r, p, s)
    em_country_matrix = np.einsum('rp,rps->rs', D, LE_reshaped)
    
    return {"em_exports_total": em_exports_total, "em_country_matrix": em_country_matrix}
```
This replaces 28 sequential matmuls with 1 matrix multiply, ~10x faster.
```

#### 4.2.3 Missing `else` clause when agent loop exhausts MAX_ITERATIONS

**Files**: All agentic nodes

When the `for` loop completes without `break` (i.e., the agent never stopped making tool calls), there's no warning or error — it silently continues.

```
ACTION: Add `else` clause to all agentic for-loops:
```python
for iteration in range(MAX_ITERATIONS):
    ...
    if not response.tool_calls:
        log.info(f"Agent finished after {iteration+1} iterations")
        break
    ...
else:
    log.warning(f"Agent hit MAX_ITERATIONS ({MAX_ITERATIONS}) without finishing — results may be incomplete")
```
Apply to: data_acquirer.py, data_preparer.py, output_producer.py, reviewer.py
```

### 4.3 Async / Concurrency

#### 4.3.1 SqliteSaver thread safety

**File**: `agents/orchestrator.py:138`

`SqliteSaver.from_conn_string(db_path)` uses default SQLite threading mode. If the pipeline is ever run in async or threaded contexts, this will crash.

```
ACTION in agents/orchestrator.py:
```python
checkpointer = SqliteSaver.from_conn_string(db_path + "?check_same_thread=False")
```
```

#### 4.3.2 No parallelization opportunity currently, but future-proofing needed

The pipeline is strictly sequential. However, within `output_producer`, multiple tables/figures could be generated in parallel. This is a future optimization, not a current bug.

```
ACTION (future): Add a note in output_producer.py:
# TODO: outputs could be generated in parallel with asyncio.gather() 
# since each table/figure script is independent
```

### 4.4 Error Handling

#### 4.4.1 Silent failure when `nodes/__init__.py` can't import agentic nodes

**File**: `nodes/__init__.py:8-14`

```python
try:
    from .paper_analyst import paper_analyst_node
    ...
except ImportError:
    _agentic_available = False
```

But `_agentic_available` is never checked! If imports fail, the code will crash with an unhelpful `NameError` when `orchestrator.py` tries to use the nodes.

```
ACTION in nodes/__init__.py:
1. Define stubs that raise clear errors:
```python
except ImportError as _import_err:
    _agentic_available = False
    _agentic_error = str(_import_err)
    
    def _agentic_unavailable(*args, **kwargs):
        raise ImportError(
            f"Agentic nodes require langchain dependencies: {_agentic_error}\n"
            f"Install with: pip install langchain-anthropic langchain-openai langgraph"
        )
    
    paper_analyst_node = _agentic_unavailable
    data_acquirer_node = _agentic_unavailable
    data_preparer_node = _agentic_unavailable
    output_producer_node = _agentic_unavailable
    reviewer_node = _agentic_unavailable
```
```

#### 4.4.2 `paper_analyst` doesn't handle YAML parse failures gracefully

**File**: `nodes/paper_analyst.py:57`

```python
spec = yaml.safe_load(spec_yaml)
```

If the LLM produces invalid YAML, this will raise `yaml.YAMLError` with no retry or helpful error message.

```
ACTION in nodes/paper_analyst.py:
```python
try:
    spec = yaml.safe_load(spec_yaml)
except yaml.YAMLError as e:
    log.error(f"LLM produced invalid YAML. First 500 chars:\n{spec_yaml[:500]}")
    # Save the raw output for debugging
    (run_dir / "replication_spec_raw.txt").write_text(spec_yaml)
    raise ValueError(
        f"Paper Analyst produced invalid YAML. Raw output saved to {run_dir}/replication_spec_raw.txt. "
        f"YAML error: {e}"
    ) from e
```
```

#### 4.4.3 `execute_python` doesn't log script content on failure

**File**: `agents/tools/execute_python.py`

When a script fails, only stdout/stderr are returned. The script content itself isn't in the error response (it's on disk, but the agent can't easily cross-reference).

```
ACTION: Already handled well — script is saved to disk and path is returned. No change needed.
```

#### 4.4.4 `reviewer_node` review parsing is fragile

**File**: `nodes/reviewer.py:82-91`

```python
fail_match = re.search(r"FAIL[:\s]+(\d+)", report_text)
```

This regex will match ANY occurrence of "FAIL" followed by a number in the report, including in benchmark descriptions. If the report contains "FAIL rate: 5%" in prose, it'll misparse.

```
ACTION in nodes/reviewer.py:
Make the regex more specific to the summary line format:
```python
# Match the specific summary format: "PASS: X | WARN: Y | FAIL: Z"
summary_match = re.search(r"PASS:\s*(\d+)\s*\|\s*WARN:\s*(\d+)\s*\|\s*FAIL:\s*(\d+)", report_text)
if summary_match:
    n_passes = int(summary_match.group(1))
    n_warns = int(summary_match.group(2))
    n_fails = int(summary_match.group(3))
    review_passed = n_fails == 0
else:
    # Fallback: count individual PASS/WARN/FAIL lines in the benchmark table
    n_fails = len(re.findall(r"\|\s*FAIL\s*\|", report_text))
    n_warns = len(re.findall(r"\|\s*WARN\s*\|", report_text))
    review_passed = n_fails == 0
```
```

### 4.5 Architecture

#### 4.5.1 Duplicated agent loop pattern across 4 nodes

All 4 agentic nodes (data_acquirer, data_preparer, output_producer, reviewer) have the identical ~20-line tool-calling loop. This should be a shared utility.

```
ACTION: Create agents/agent_runner.py:
```python
"""Shared agent execution loop with token tracking and message trimming."""
import logging
from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage
from agents.token_tracker import TokenTracker
from agents.message_utils import trim_messages

log = logging.getLogger("agent_runner")


def run_agent_loop(
    llm,
    tools: list,
    system_prompt: str,
    initial_message: str,
    max_iterations: int,
    max_tokens: int = 200_000,
    stage_name: str = "agent",
) -> list:
    """
    Run the standard tool-calling loop.
    
    Returns the final message list.
    """
    tool_map = {t.name: t for t in tools}
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=initial_message),
    ]
    tracker = TokenTracker(max_tokens)
    
    for iteration in range(max_iterations):
        messages = trim_messages(messages)
        response = llm.invoke(messages)
        tracker.record(response)
        messages.append(response)
        
        if not response.tool_calls:
            log.info(f"{stage_name} finished after {iteration+1} iterations")
            break
        
        for tool_call in response.tool_calls:
            tool_name = tool_call["name"]
            tool_id = tool_call["id"]
            if tool_name not in tool_map:
                result = f"ERROR: Unknown tool '{tool_name}'"
            else:
                try:
                    result = tool_map[tool_name].invoke(tool_call["args"])
                except Exception as e:
                    result = f"ERROR executing {tool_name}: {e}"
            messages.append(ToolMessage(content=str(result)[:4000], tool_call_id=tool_id))
    else:
        log.warning(f"{stage_name} hit MAX_ITERATIONS ({max_iterations}) without finishing")
    
    log.info(f"{stage_name} token usage: {tracker.summary()}")
    return messages
```

Then refactor each agentic node to use it:
```python
# In data_acquirer.py:
from agents.agent_runner import run_agent_loop

messages = run_agent_loop(
    llm=llm, tools=tools, system_prompt=system_prompt,
    initial_message=initial_message, max_iterations=MAX_ITERATIONS,
    stage_name="data_acquirer",
)
```
```

#### 4.5.2 `PipelineState` uses `TypedDict` but many fields are optional at runtime

**File**: `agents/state.py`

`TypedDict` provides no runtime validation. Fields like `paper_pdf_path`, `user_hints` are `Optional[str]` but others like `data_manifest`, `prepared_data_paths` are plain `dict` even though they're empty `{}` at pipeline start and only populated later.

```
ACTION in agents/state.py:
Add Optional to fields that are legitimately empty at certain stages:
```python
from typing import TypedDict, Optional

class PipelineState(TypedDict, total=False):
    # Required (always present)
    run_id: str
    run_dir: str
    config: dict
    current_stage: int
    retry_count: int
    error_log: list
    
    # Optional (populated by stages)
    paper_pdf_path: Optional[str]
    user_hints: Optional[str]
    replication_spec: dict
    replication_spec_path: str
    spec_approved: bool
    # ... etc
```
Note: LangGraph requires `total=False` for partial state updates to work. 
Verify this doesn't break LangGraph's state merging — test after change.
```

### 4.6 Security

#### 4.6.1 No `.gitignore`

```
ACTION: Create .gitignore at repo root:
```
runs/
__pycache__/
*.pyc
.env
*.egg-info/
dist/
build/
*.sqlite
.venv/
```
```

#### 4.6.2 API keys are properly handled via env vars ✅

The codebase correctly uses `os.environ.get()` for API keys. No hardcoded secrets found.

#### 4.6.3 `write_file` tool has no path restriction

**File**: `agents/tools/file_tools.py:49-60`

The `write_file` tool can write to any path on the filesystem. An LLM could write to `/etc/crontab` or overwrite system files.

```
ACTION in agents/tools/file_tools.py:
Add path validation to write_file:
```python
@tool
def write_file(path: str, content: str) -> str:
    """Write content to a file, creating parent directories as needed."""
    p = Path(path).resolve()
    # Safety: only allow writes under the current working directory or /tmp
    cwd = Path.cwd().resolve()
    if not (str(p).startswith(str(cwd)) or str(p).startswith("/tmp")):
        return f"ERROR: write_file is restricted to the project directory. Cannot write to {path}"
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)
        return f"OK: Written {len(content)} chars to {path}"
    except Exception as e:
        return f"ERROR writing {path}: {e}"
```

Do the same for read_file — add path validation.
```

### 4.7 Performance

#### 4.7.1 Schema loaded from disk on every validation call

**File**: `agents/validators/spec_validator.py:13-15`

```python
def validate_spec(spec):
    schema = load_schema()  # reads from disk every time
```

```
ACTION: Cache at module level:
```python
_cached_schema = None

def load_schema():
    global _cached_schema
    if _cached_schema is None:
        with open(_SCHEMA_PATH) as f:
            _cached_schema = yaml.safe_load(f)
    return _cached_schema
```
```

#### 4.7.2 Leontief inverse uses `np.linalg.inv` — consider `np.linalg.solve`

**File**: `nodes/model_builder.py:87`

`np.linalg.inv(I_minus_A)` computes the full inverse. If only `L @ e` is needed, `np.linalg.solve(I_minus_A, e)` is ~2x faster and more numerically stable. However, since L is used in multiple subsequent multiplications (country matrix, decomposer), the full inverse IS needed. No change required, but add a comment explaining this.

```
ACTION in nodes/model_builder.py:
Add comment:
```python
# Full inverse is needed (not just solve) because L is reused in decomposer 
# for country-level and industry-level decomposition with different e vectors
L = np.linalg.inv(I_minus_A)
```
```

#### 4.7.3 Large CSV writes for A_EU and L_EU

**File**: `nodes/model_builder.py:146-152`

Writing 1792×1792 matrices as CSV produces ~50MB files. These are never read by the reviewer (blocked in `read_file`), only used by the decomposer which loads them with pandas.

```
ACTION in nodes/model_builder.py:
Switch to numpy binary format for large matrices:
```python
# Save large matrices as .npy (binary) — 10x faster I/O, 4x smaller files
p = model_dir / "A_EU.npy"
np.save(p, A)
paths["A_EU"] = str(p)

p = model_dir / "L_EU.npy"
np.save(p, L)
paths["L_EU"] = str(p)

# Keep label mapping as a separate small file
pd.DataFrame({"label": row_labels}).to_csv(model_dir / "labels.csv", index=False)
```

Update decomposer.py to load .npy:
```python
L = np.load(model_paths["L_EU"])
```

Update file_tools.py blocked list to include .npy files.
Update run_deterministic.py paths to use .npy extension.
```

---

## 5. Quick Wins (⚡)

These are changes that take <5 minutes each and have immediate impact:

| # | Change | File | Impact |
|---|--------|------|--------|
| 1 | Add `.gitignore` | `.gitignore` (new) | Prevents accidental secret/data commits |
| 2 | Add `else` clause to all agent for-loops | 4 agentic nodes | Warns when agent doesn't finish |
| 3 | Cache schema in `spec_validator.py` | `agents/validators/spec_validator.py` | Eliminates redundant disk I/O |
| 4 | Add `cwd=run_dir` to `subprocess.run` | `agents/tools/execute_python.py` | Scripts run from expected directory |
| 5 | Add fallback logging in `llm.py` | `agents/llm.py` | Visible when model silently switches |
| 6 | Cap ToolMessage content to 4000 chars | All agentic nodes | Reduces token waste from large outputs |
| 7 | Add YAML parse error handling in paper_analyst | `nodes/paper_analyst.py` | Clear error instead of stack trace |
| 8 | Add path validation to write_file | `agents/tools/file_tools.py` | Prevents writes outside project dir |
| 9 | Strip API keys from subprocess env | `agents/tools/execute_python.py` | LLM scripts can't leak secrets |
| 10 | Add comment explaining `np.linalg.inv` choice | `nodes/model_builder.py` | Documents intentional design decision |

```
ACTION: Implement all 10 quick wins. They are independent and can be done in any order.
```

---

## 6. Suggested Refactors (Larger Changes)

### 6.1 Extract shared agent loop → `agents/agent_runner.py`

See §4.5.1. Eliminates ~80 lines of duplicated code across 4 files.

**Priority**: High  
**Effort**: 30 min  
**Files touched**: `agents/agent_runner.py` (new), `agents/message_utils.py` (new), `agents/token_tracker.py` (new), all 4 agentic nodes

### 6.2 Make reviewer partially deterministic

The benchmark comparison in the reviewer is purely mechanical (read CSV, compare numbers, compute deviations). Only the "interpretation" section benefits from LLM prose.

```
ACTION:
1. Create `agents/validators/benchmark_validator.py` that does the comparison deterministically
2. Have the reviewer node:
   a. Run deterministic benchmark checks first
   b. Only call LLM for the interpretation section of the report
   c. Combine into final review_report.md
This cuts reviewer LLM costs by ~60% and makes benchmark results reproducible.
```

**Priority**: Medium  
**Effort**: 1 hour

### 6.3 Switch large matrix storage to .npy

See §4.7.3. Reduces model_builder disk I/O time from ~10s to ~1s and file sizes from ~50MB to ~12MB each.

**Priority**: Medium  
**Effort**: 20 min  
**Files touched**: `nodes/model_builder.py`, `nodes/decomposer.py`, `run_deterministic.py`, `agents/tools/file_tools.py`

### 6.4 Vectorize model_builder country matrix computation

See §4.2.2. Replaces 28 sequential matrix-vector multiplies with 1 matrix-matrix multiply.

**Priority**: Medium  
**Effort**: 15 min  
**Files touched**: `nodes/model_builder.py`

### 6.5 Vectorize decomposer Figure 3 computation

See §2.3. The highest-impact performance fix — reduces from ~1800 matmuls to ~280.

**Priority**: High  
**Effort**: 20 min  
**Files touched**: `nodes/decomposer.py`

### 6.6 Add integration test harness

Currently there are zero tests. Add at minimum:

```
ACTION: Create tests/ directory with:
1. tests/test_spec_validator.py — validate the example spec passes
2. tests/test_model_builder.py — small 3×3 Leontief test case with known results  
3. tests/test_decomposer.py — verify domestic + spillover = total
4. tests/test_file_tools.py — verify path restrictions and read limits
5. tests/test_message_utils.py — verify message trimming preserves system/first messages
```

**Priority**: High  
**Effort**: 2 hours

---

## 7. Implementation Order

Recommended order for Claude Code to implement:

1. **Quick wins** (§5) — all 10 items, ~20 min total
2. **`.gitignore`** — create immediately
3. **Agent runner extraction** (§6.1) — includes message_utils.py and token_tracker.py
4. **Decomposer vectorization** (§2.3 + §6.5) — biggest perf win
5. **Model builder vectorization** (§6.4) — easy follow-up  
6. **Execute_python sandboxing** (§2.2) — security priority
7. **Data preparer retry improvement** (§2.5)
8. **Large matrix .npy storage** (§6.3)
9. **Reviewer partial determinism** (§6.2)
10. **Tests** (§6.6)

---

## 8. Files to Create (New)

| File | Purpose |
|------|---------|
| `.gitignore` | Standard Python gitignore + runs/ |
| `agents/agent_runner.py` | Shared agent execution loop |
| `agents/message_utils.py` | Message history trimming |
| `agents/token_tracker.py` | Per-stage token/cost tracking |
| `agents/validators/benchmark_validator.py` | Deterministic benchmark comparison |
| `tests/test_spec_validator.py` | Spec validation tests |
| `tests/test_model_builder.py` | Leontief model tests |
| `tests/test_decomposer.py` | Decomposition identity tests |
| `tests/test_file_tools.py` | File tool safety tests |
| `tests/test_message_utils.py` | Message trimming tests |

## 9. Files to Modify

| File | Changes |
|------|---------|
| `agents/llm.py` | Add LLM caching, temperature config, fallback logging |
| `agents/tools/execute_python.py` | Add cwd, env stripping, safety scan, allow_network param |
| `agents/tools/file_tools.py` | Add path validation to write_file and read_file, add .npy to blocklist |
| `agents/validators/spec_validator.py` | Cache schema at module level |
| `nodes/__init__.py` | Add clear error stubs for missing agentic deps |
| `nodes/paper_analyst.py` | Cache Anthropic client, handle YAML parse errors |
| `nodes/data_acquirer.py` | Use agent_runner, add for-else warning |
| `nodes/data_preparer.py` | Use agent_runner, improve retry context with prior script |
| `nodes/output_producer.py` | Use agent_runner, add for-else warning |
| `nodes/reviewer.py` | Use agent_runner, fix summary regex, add for-else warning |
| `nodes/model_builder.py` | Vectorize country matrix, switch to .npy, add comments |
| `nodes/decomposer.py` | Vectorize Figure 3 loop, load .npy |
| `nodes/decomposer.py` | Load .npy format for L matrix |
| `agents/orchestrator.py` | Add check_same_thread to SQLite |
| `run_deterministic.py` | Update paths for .npy format |
| `config.yaml` | Add max_tokens_per_stage, temperatures, cost ceiling |
| `agents/state.py` | Consider total=False for TypedDict |
