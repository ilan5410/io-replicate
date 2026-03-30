# IO Replicator

Generic multi-agent system for replicating Input-Output economics papers.

Given a paper PDF, it produces a structured `replication_spec.yaml`, then runs a 7-stage LangGraph pipeline: data acquisition → Leontief model → decomposition → outputs → review.

**The key design**: paper-specific knowledge lives in the spec; agents are generic IO analysis tools. The same pipeline can replicate any IO paper — not just FIGARO.

## Quick start

```bash
pip install -e .
export ANTHROPIC_API_KEY=sk-ant-...

# Full pipeline from a paper PDF
io-replicate run --paper my_paper.pdf

# Use existing FIGARO spec (skip Paper Analyst)
io-replicate run --spec specs/figaro_2019/replication_spec.yaml

# Deterministic-only run (no LLM, stages 3-6)
python run_deterministic.py --spec specs/figaro_2019/replication_spec.yaml --run-dir runs/my_run
```

## Architecture

| Stage | Type | Role |
|-------|------|------|
| 0 — Paper Analyst | Agentic (Claude Opus) | PDF → replication_spec.yaml |
| 1 — Data Acquirer | Agentic (GPT-4o-mini) | Download raw IO tables + satellite data |
| 2 — Data Preparer | Agentic + validator | Parse → Z, e, x, Em matrices |
| 3 — Model Builder | **Deterministic** | A, L, d, employment content |
| 4 — Decomposer | **Deterministic** | Domestic/spillover, direct/indirect |
| 5 — Output Producer | Agentic (GPT-4o-mini) | Tables + figures from spec |
| 6 — Reviewer | Agentic (Claude Sonnet) | Benchmark validation → review_report.md |

See `docs/architecture.md` for full details, `docs/user_guide.md` for usage.

## Ground truth

`specs/figaro_2019/replication_spec.yaml` is the hand-crafted spec for:
> Rémond-Tiedrez, Valderas-Jaramillo, Amores & Rueda-Cantuche (2019), *The employment content of EU exports: an application of FIGARO tables*, EURONA.

Use it to test the pipeline without needing a paper PDF.
