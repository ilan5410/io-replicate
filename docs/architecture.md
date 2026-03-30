# IO Replicator — Architecture

## Design Philosophy

Paper-specific knowledge lives in the spec; agents are generic IO analysis tools.

The existing `figaro-replication` repo hardwires every layer to one paper. The IO Replicator inverts this: the paper-specific knowledge is **context** (the `replication_spec.yaml`), while the agents are **tools** that know how to do IO analysis in general.

## The ReplicationSpec

`replication_spec.yaml` is the single source of truth. Schema: `schemas/replication_spec_schema.yaml`.

Key sections:
- `geography`: which countries are inside vs outside the Leontief system
- `classification`: industry codes and aggregation schemes
- `data_sources`: which tables to download and from where
- `methodology`: Leontief system scope, export definition
- `outputs`: every table and figure to produce (spec-driven, not hardcoded)
- `benchmarks`: expected numerical results from the paper

## Pipeline

```
Paper PDF → [Paper Analyst] → replication_spec.yaml
                                      ↓
                            [Human Approval checkpoint]
                                      ↓
                            [Data Acquirer] → data/raw/
                                      ↓
                     [Data Preparer] + [Deterministic Validator]
                                      ↓ (retry loop, max 3)
                             [Model Builder] → A, L, d
                                      ↓
                              [Decomposer] → domestic/spillover
                                      ↓
                           [Output Producer] → tables/figures
                                      ↓
                              [Reviewer] → review_report.md
```

## Agent Types

| Stage | Type | Why |
|-------|------|-----|
| Paper Analyst | Agentic (LLM) | Natural language understanding of academic papers |
| Data Acquirer | Agentic (LLM) | Adapts to different APIs and handles quirks |
| Data Preparer | Agentic + deterministic validator | Code generation; validator catches bugs |
| Model Builder | Deterministic | Leontief math is exact; LLM adds no value |
| Decomposer | Deterministic | Standard IO accounting; same reason |
| Output Producer | Agentic (LLM) | Spec-driven chart/table generation |
| Reviewer | Agentic (LLM) | Judgment-based benchmark analysis and prose |

## State Flow

```
paper_pdf_path ──► paper_analyst ──► replication_spec (shared by all agents)
                                           │
                              ┌────────────┼────────────┐
                              │    READ BY ALL AGENTS    │
                              └────────────┬────────────┘
                                           │
data_acquirer ──► data_manifest
data_preparer ──► prepared_data_paths
model_builder ──► model_paths
decomposer ──────► decomposition_paths
output_producer ─► output_paths
reviewer ────────► review_report_path
```

Agents don't talk to each other directly — they all read from `replication_spec` and write to the shared `PipelineState`.

## LLM Routing

Default routing from `config.yaml`:
- Paper Analyst → Claude Opus (best reasoning for PDF analysis)
- Data Acquirer → GPT-4o-mini (simple tool-calling, cost-effective)
- Data Preparer → Claude Sonnet (code quality matters)
- Output Producer → GPT-4o-mini (routine matplotlib code)
- Reviewer → Claude Sonnet (judgment matters)

Override per-agent in `config.yaml` under `llm.routing`.

## Reproducibility

Each run produces `runs/{timestamp}/`:
- `replication_spec.yaml` — frozen spec used for this run
- `generated_scripts/` — all Python scripts the agents wrote
- `data/` — all intermediate matrices
- `outputs/` — final tables and figures
- `pipeline.log` — full trace

To reproduce a run without LLM cost: execute the scripts in `generated_scripts/` directly.

## Key Files

| File | Role |
|------|------|
| `agents/state.py` | PipelineState TypedDict |
| `agents/orchestrator.py` | LangGraph graph definition |
| `agents/llm.py` | Anthropic/OpenAI abstraction |
| `agents/tools/` | Shared tools (execute_python, file I/O) |
| `agents/prompts/` | System prompts for each agentic node |
| `agents/validators/` | Deterministic validators |
| `nodes/` | Node implementations (agentic + deterministic) |
| `schemas/` | JSON Schema for the spec |
| `specs/` | Pre-written specs (FIGARO ground truth) |
| `cli/main.py` | `io-replicate` CLI |

## Ground Truth

`specs/figaro_2019/replication_spec.yaml` is the hand-crafted spec for Rémond-Tiedrez et al. (2019). It serves as:
1. Ground truth for testing the Paper Analyst agent
2. A working example for users learning the spec format
3. The input for running the deterministic stages (3-4) without LLM

The original pipeline it was derived from: `figaro-replication` repo.
