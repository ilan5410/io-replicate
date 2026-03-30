# CLAUDE.md

## Project Purpose

**IO Replicator** is a generic multi-agent system for replicating Input-Output economics papers. Given a paper PDF, it produces a structured `replication_spec.yaml`, then runs a 7-stage LangGraph pipeline (data acquisition → Leontief model → decomposition → outputs → review).

The design philosophy: paper-specific knowledge lives in the spec; agents are generic IO analysis tools.

Full architecture: `docs/architecture.md`

## Commands

```bash
# Install
pip install -e .

# Replicate a paper (full pipeline)
io-replicate run --paper paper.pdf

# Resume from a specific stage (spec already approved)
io-replicate run --spec specs/figaro_2019/replication_spec.yaml --start-stage 3

# Run only one stage
io-replicate run --spec specs/figaro_2019/replication_spec.yaml --only reviewer

# Validate a spec without running
io-replicate validate --spec specs/figaro_2019/replication_spec.yaml

# List available data source connectors
io-replicate sources --list

# Run programmatically
python run_agentic.py --paper paper.pdf --config config.yaml
```

## Architecture

7-stage LangGraph pipeline. Stages 0, 1, 2, 5, 6 are agentic (LLM-powered). Stages 3, 4 are deterministic (pure numpy).

| Stage | Node | Type | Role |
|-------|------|------|------|
| 0 | `paper_analyst` | Agentic | PDF → `replication_spec.yaml` |
| — | `human_approval` | Checkpoint | Human reviews spec |
| 1 | `data_acquirer` | Agentic | Downloads raw data |
| 2 | `data_preparer` | Agentic + validator | Parses → Z, e, x, Em matrices |
| 3 | `model_builder` | Deterministic | A, L, d, employment content |
| 4 | `decomposer` | Deterministic | Domestic/spillover, direct/indirect |
| 5 | `output_producer` | Agentic | Tables and figures from spec |
| 6 | `reviewer` | Agentic | Benchmark validation → review_report.md |

The `replication_spec.yaml` is the shared context: all agents read it, none are hardwired to a specific paper.

## Key Files

- `specs/figaro_2019/replication_spec.yaml` — ground truth spec for Rémond-Tiedrez et al. (2019)
- `schemas/replication_spec_schema.yaml` — JSON Schema for the spec
- `agents/state.py` — PipelineState TypedDict
- `agents/orchestrator.py` — LangGraph graph definition
- `agents/llm.py` — Anthropic/OpenAI dual-provider abstraction
- `nodes/model_builder.py` — deterministic Leontief model
- `nodes/decomposer.py` — deterministic decomposition

## Development Conventions

Write multi-line logic as script files, then execute. Single-line commands are fine inline.
