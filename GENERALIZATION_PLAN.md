# Generalization Plan — IO Replicator

**Status**: In progress — April 2026
**Context**: The current pipeline successfully replicates Rémond-Tiedrez et al. (2019) FIGARO employment-in-exports paper end-to-end (Industry B-E: 2.9% deviation, PASS). It is now time to lift it into a **general-purpose agentic framework** that can replicate any input–output analysis paper, not just FIGARO.

---

## 1. Goals

1. **Any paper, any database.** Accept a PDF of an IO paper → produce a faithful replication, regardless of whether the underlying data is FIGARO, WIOD, OECD ICIO, EXIOBASE, or a national SUT.
2. **Reusable skills.** Extract three first-class, standalone skills the broader community can use without the full pipeline:
   - **IO database parsers** (one per commonly-used database)
   - **Leontief analysis** (core math + standard decompositions)
   - **Classification mapping** (the "desk research" step — assigning paper-specific concepts to standardized industrial codes)
3. **Clean agent definitions.** Each agent has a crisp role, a short system prompt, a well-defined tool surface, and a documented success/failure mode.
4. **Graceful degradation.** When a paper uses non-public data or a method the framework doesn't support, the reviewer flags it explicitly rather than silently returning wrong numbers.

---

## 2. Current State Assessment

Stage-by-stage verdict based on the FIGARO run (`runs/20260411_224628`):

| # | Stage | Node | Generic today? | Blocker for generalization |
|---|---|---|---|---|
| 0 | Paper Analyst | `nodes/paper_analyst.py` | ✓ Yes | None — reads any PDF, emits replication_spec |
| 1 | Data Acquirer | `nodes/data_acquirer.py` | ~ Mostly | Prompt is Eurostat-tuned; tool surface is generic |
| 2 | Data Preparer | `nodes/data_preparer.py` | ✗ **No** | Hardcoded to Eurostat TSV format + FIGARO column names |
| 3 | Model Builder | `nodes/model_builder.py` | ✓ Yes | Pure linear algebra on matrices |
| 4 | Decomposer | `nodes/decomposer.py` | ~ Mostly | Domestic vs spillover is generic; sector aggregation uses a 10-sector FIGARO grouping |
| 5 | Output Producer | `nodes/output_producer.py` | ✓ Yes | Spec-driven + `output_schema` contract |
| 5.5 | Spec Reconciler | `nodes/spec_reconciler.py` | ✓ Yes | Fully generic (fuzzy-match) |
| 6 | Reviewer | `nodes/reviewer.py` | ✓ Yes | Reads benchmarks from spec, file-map from `output_schema` |

**Missing stage**: there is no explicit **Classification Mapper** today. The paper analyst captures what codes the paper uses, but when a paper says *"we analyze the solar PV supply chain"* or *"employment in the renewable energy sector"*, someone needs to decide that this maps to `C27 + C33 + F42 + M71`. Currently that decision is buried in whatever the paper analyst guesses, with no desk-research, no web lookup, and no traceable reasoning.

The Industry B-E sectoral result being 2.9% off is the best evidence that the core math + orchestration is already sound. The larger country-level deviations are all attributable to **data vintage + LU/MT confidential-data zero-filling**, not to pipeline bugs.

---

## 3. Architectural Vision

```
                                   SKILLS (reusable libraries)
                                   ┌─────────────────────────┐
                                   │ io_parsers/             │
                                   │   ├── figaro_tsv        │
                                   │   ├── wiod_xlsx         │
                                   │   ├── oecd_icio_csv     │
                                   │   └── exiobase_zip      │
                                   │ leontief/               │
                                   │   ├── build_model       │
                                   │   ├── decompose         │
                                   │   └── benchmarks        │
                                   │ classification_mapping/ │
                                   │   ├── concordance_tables│
                                   │   ├── fuzzy_match       │
                                   │   └── web_research      │
                                   └─────────────────────────┘
                                           ▲         ▲
                                           │         │
PDF ─► Paper Analyst ─► Classification Mapper ─► Data Acquirer ─► Data Preparer
                                                        │              │
                                                        ▼              ▼
                                                   (uses skill)  (dispatches to parser skill)
                                                                       │
                                                                       ▼
                                              Model Builder ─► Decomposer ─► Output Producer
                                                                                   │
                                                                                   ▼
                                                                          Spec Reconciler ─► Reviewer
```

Three big ideas:

1. **Skills are pure Python libraries**, importable outside this repo. Agents call them as tools.
2. **Agents are orchestration + judgment**, not computation. They decide *which* skill to call and *what* the paper wants — they don't reimplement math.
3. **The replication_spec is the single source of truth** across stages. Everything is authored by the paper analyst (+ classification mapper) and consumed downstream.

---

## 4. The NEW Classification Mapper Stage

This is the most important addition and the one the user explicitly flagged as missing.

### 4.1 Why it's needed

Papers rarely say "we use NACE code C27". They say things like:

- *"employment in green industries"*
- *"the automotive value chain"*
- *"ICT-producing sectors"*
- *"final demand for tourism services"*
- *"Just Transition regions dependent on coal mining"*

Each of these requires a **human-equivalent desk research step**:
1. Read what the paper actually means by the concept (sometimes defined in an annex, sometimes implied).
2. Consult standard concordance tables (e.g., NACE Rev. 2 ↔ CPA 2008 ↔ ISIC Rev. 4 ↔ HS).
3. Resolve ambiguity (is "automotive" just `C29` or also `C30.9 + G45 + parts of C27/C22`?).
4. Produce a **justified mapping** with sources and caveats.

In the current pipeline, the paper analyst silently guesses, and there is no separate artifact recording the reasoning. The FIGARO replication happens to dodge this because FIGARO uses the paper's native NACE-level detail directly — but any paper studying a *custom aggregate* (green jobs, digital economy, critical raw materials, etc.) will break.

### 4.2 Inputs and outputs

**Input** (from Paper Analyst):
```yaml
concepts_to_map:
  - id: green_industries
    description: "Industries producing environmental goods and services"
    source: "Section 2.1, footnote 4"
    target_classification: NACE Rev. 2
  - id: automotive_value_chain
    description: "Manufacturing of motor vehicles and directly supplying sectors"
    source: "Table 3 header"
    target_classification: NACE Rev. 2
```

**Output** (patched into replication_spec):
```yaml
concept_mappings:
  green_industries:
    codes: [C20.13, C27.1, C28.11, C33.14, E38.32, ...]
    reasoning: |
      Based on Eurostat EGSS classification (2009) and OECD Environmental Goods
      List. C27.1 (electric motors) per OECD pillar "Renewable energy".
      Excluded C35 (electricity generation) because the paper treats it as
      downstream user, not producer.
    sources:
      - https://ec.europa.eu/eurostat/.../egss
      - Paper annex Table A.1
    confidence: high
    caveats:
      - "E38.32 (recycling) is borderline; paper footnote 11 is ambiguous"
```

### 4.3 How it works

Agent loop with these tools:

| Tool | Purpose |
|---|---|
| `read_pdf_section(pdf, section_name_or_range)` | Re-read paper annexes, footnotes, method sections |
| `load_concordance(from_cls, to_cls)` | Load an official concordance table (Eurostat RAMON, UNSD) |
| `search_concordance(text)` | Fuzzy-search a classification by description |
| `web_search(query)` | For non-obvious mappings, find the official definition (e.g., "OECD digital economy sectors NACE") |
| `fetch_url(url)` | Pull an authoritative source (Eurostat glossary, OECD methodology note) |
| `list_codes(classification, prefix)` | Enumerate children of a prefix in a standard classification |
| `write_mapping(id, codes, reasoning, sources, caveats)` | Emit one mapping into the spec |

System prompt essence:

> You are a classification specialist. For each concept, find the authoritative definition, map it to the target classification with explicit reasoning, and cite your sources. If the paper is ambiguous, record a caveat and pick the most defensible interpretation. Never guess silently.

### 4.4 When it's a no-op

If the paper analyst reports `concepts_to_map: []` (i.e., the paper uses native classification codes directly, as FIGARO 2019 does), the mapper stage is a pass-through. This is the common case, so the stage must be cheap when there's nothing to do.

### 4.5 Where it sits in the graph

```
paper_analyst → classification_mapper → data_acquirer → data_preparer → ...
```

Must run **before** data acquisition so that the acquirer knows which codes to download, and **before** data preparation so that the preparer knows which rows/columns to keep.

### 4.6 Desk-research realism

This stage genuinely benefits from an LLM because:
- Concordance tables are sparse and don't cover custom aggregates.
- Papers often cite mappings by reference ("using the Eurostat EGSS 2009 definition"), which a language model can resolve by web search + retrieval.
- Disambiguation requires reading prose and judging intent.

Open question: should web access be **mandatory** or **optional** for this stage? See §8.

---

## 5. Skills Library Design

Skills live in a new `skills/` top-level package, importable as `from io_replicator.skills.leontief import build_model`. Each skill has:

- A narrow, typed Python API (no agent dependencies).
- A README with examples.
- Its own tests.
- A `SKILL.md` describing when an agent should call it.

### 5.1 `skills/io_parsers/`

One submodule per widely-used IO database. Each exposes the same contract:

```python
def load(path: Path, spec: dict) -> IOTables:
    """Return standardized IOTables from a raw database dump."""

@dataclass
class IOTables:
    Z: pd.DataFrame   # intermediate use, MultiIndex (country, industry) × (country, industry)
    Y: pd.DataFrame   # final demand, MultiIndex rows × (country, use)
    x: pd.Series      # total output, MultiIndex (country, industry)
    Em: pd.Series     # employment (or chosen factor), MultiIndex (country, industry)
    classification: str  # "NACE_R2_64", "ISIC_R4_35", etc.
    year: int
    source: str       # "figaro_2019", "wiod_2016", ...
```

Parsers to build, in priority order:

| # | Parser | Format | Comments |
|---|---|---|---|
| 1 | `figaro_tsv` | Eurostat bulk TSV | Already implemented in `data_preparer.py`; lift it out |
| 2 | `wiod_xlsx` | WIOD 2016 release xlsx | Used by ~40% of IO papers |
| 3 | `oecd_icio_csv` | OECD ICIO 2023 csv | Single-file, easier than FIGARO |
| 4 | `exiobase_zip` | EXIOBASE 3.x multi-file zip | Richer environmental extensions |
| 5 | `national_sut` | National SUTs (Eurostat `naio_10_cp15`-ish) | Single-country papers |

**Migration path for Stage 2**: `data_preparer.py` becomes a thin dispatcher:
```python
parser = load_parser(spec["data_sources"]["io_table"]["type"])
tables = parser.load(raw_dir, spec)
save_to_prepared_dir(tables, prepared_dir)
```
The FIGARO-specific code moves into `skills/io_parsers/figaro_tsv/`, preserving all existing fixes (CPA normalization, ESA imputation handling, etc.).

### 5.2 `skills/leontief/`

```python
def build_model(tables: IOTables, eu_only: bool = False) -> LeontiefModel:
    """A = Z·diag(x)^-1;  L = (I-A)^-1;  d = Em/x."""

def employment_content(model, e: pd.Series) -> pd.Series:
    """d' · L · e — jobs attributable to a given demand vector."""

def decompose_domestic_spillover(model, e, country) -> DecompositionResult:
    """Split jobs into domestic vs spillover components."""

def decompose_by_industry(model, e, aggregation=None) -> pd.DataFrame:
    """Attribute jobs to producing industries; optionally aggregate to N sectors."""

def check_model(model) -> list[Check]:
    """Column sums < 1, L ≥ 0, diag ≥ 1, L·(I-A) ≈ I."""

STANDARD_BENCHMARKS = {
    "figaro_2010_employment": {...}  # paper benchmarks we want to ship
}
```

This is mostly **already implemented** in `nodes/model_builder.py` and `nodes/decomposer.py`. The refactor is to lift the math into `skills/leontief/` and have the nodes become thin wrappers. Benefits:
- Other projects can `pip install` the skill and use it without the agent framework.
- Unit testable without mocking LangGraph state.
- The 10-sector FIGARO aggregation becomes one named aggregation among several (plus user-custom via spec).

### 5.3 `skills/classification_mapping/`

```python
def load_concordance(from_cls: str, to_cls: str) -> pd.DataFrame:
    """Load e.g. NACE Rev. 2 ↔ ISIC Rev. 4. Ships with bundled data."""

def search_by_description(text: str, classification: str, top_k: int = 10) -> list[Code]:
    """Return best-matching codes by text similarity."""

def expand_prefix(prefix: str, classification: str) -> list[Code]:
    """All codes under a parent (e.g., 'C27' → C27.1, C27.11, ...)."""

def validate_mapping(codes: list[str], classification: str) -> ValidationReport:
    """Check codes exist, warn on parent+child overlap, flag deprecated codes."""
```

Bundled concordance data to include (all public):
- NACE Rev. 2 ↔ ISIC Rev. 4 (UNSD)
- NACE Rev. 2 ↔ CPA 2008 (Eurostat RAMON)
- NACE Rev. 2 ↔ HS 2012 (for trade papers)
- NACE Rev. 2 ↔ Exiobase 163-sector
- NACE Rev. 2 ↔ WIOD 56-sector

This is the skill with the **highest external demand** — no good Python library exists for classification concordance today. It's worth shipping even if the rest of the pipeline weren't useful.

---

## 6. Agent Definitions

Each agent = `(role, system_prompt, tools, success_criterion, failure_mode)`. Prompts should be short, directive, and in the "prehistoric" style that worked for the output producer rewrite.

### 6.1 Paper Analyst

**Role**: Read a PDF, emit a complete `replication_spec.yaml`.
**Tools**: `read_pdf(pages)`, `write_spec(spec)`, `search_pdf(regex)`.
**Key outputs**:
- `classification` (what coding system the paper uses)
- `concepts_to_map` (any non-standard aggregates — hand off to Classification Mapper)
- `data_sources` (which DB + year + version)
- `methodology.export_definition`, `methodology.employment_measure`, etc.
- `outputs` (tables/figures to produce)
- `output_schema` (exact columns + file names for Stage 5/6 contract)
- `benchmarks` (numbers to validate against)
**Success**: schema validates against `schemas/replication_spec.schema.json`.
**Failure mode**: escalate to reviewer with `status: insufficient_information`.

### 6.2 Classification Mapper *(NEW)*

**Role**: Resolve `concepts_to_map` into concrete code lists with reasoning.
**Tools**: see §4.3.
**Success**: every concept has codes + reasoning + sources + confidence; spec re-validates.
**Failure mode**: emit concept with `confidence: low` and detailed caveats — do not guess.
**No-op**: if `concepts_to_map` is empty, return immediately.

### 6.3 Data Acquirer

**Role**: Download raw data files per `spec.data_sources` into `data/raw/`.
**Tools**: `http_get`, `bulk_download`, `write_file`, `list_files`.
**Generalization change**: prompt should be **database-agnostic**; route database-specific quirks into `skills/io_parsers/<x>/fetch.py` (each parser ships its own downloader).
**Success**: all required files exist on disk and pass checksum sanity checks.

### 6.4 Data Preparer *(now a dispatcher)*

**Role**: Load raw files via the appropriate parser skill, save standardized matrices to `data/prepared/`.
**Tools**: `execute_python` (to call the parser skill).
**Generalization change**: the current FIGARO-specific code moves into `skills/io_parsers/figaro_tsv/`. The node becomes ~30 lines.
**Success**: `Z.parquet`, `Y.parquet`, `x.parquet`, `Em.parquet` exist with expected shapes.

### 6.5 Model Builder

**Role**: Call `skills.leontief.build_model`, run `check_model`, save `A.parquet`, `L.parquet`, `d.parquet`.
**Already generic** — just refactor to import from the skill.

### 6.6 Decomposer

**Role**: Call `skills.leontief.employment_content` and the decomposition helpers per `spec.outputs.decompositions`.
**Already mostly generic** — the one change is making sector aggregation configurable via `spec.aggregations` rather than hardcoded FIGARO 10-sector.

### 6.7 Output Producer

**Role**: Produce tables/figures per `spec.outputs` + `spec.output_schema`.
**Already generic** after the recent rewrite. No changes.

### 6.8 Spec Reconciler

**Role**: Fuzzy-match Stage 5 output files/columns against `output_schema` and patch the spec so Stage 6 can find them.
**Already generic**. No changes.

### 6.9 Reviewer

**Role**: Run benchmark checks, write `review_report.md`, render the PASS/WARN/FAIL table + narrative.
**Already generic**. One small addition: surface `concept_mappings` confidence levels in the report so users see which paper concepts were mapped with low confidence.

---

## 7. Stage-by-Stage Migration Plan

Ordered by dependency / risk. Each step is independently shippable.

### Step 1 — Extract `skills/leontief/` ✅ DONE
- 7 pure-math functions in `skills/leontief/core.py`; nodes are thin wrappers.
- Private `_xxx` aliases kept in nodes for backward compat; tests updated to import from skill.
- 9 unit tests passing on synthetic 2×2 system.
- `skills/leontief/SKILL.md` written.

### Step 2 — Extract `skills/io_parsers/figaro_iciot/` ✅ DONE
- All FIGARO TSV logic moved to `skills/io_parsers/figaro_iciot/core.py`.
- `nodes/data_preparer.py` reduced to 70-line dispatcher: `load_parser(type)(raw_dir, spec)`.
- Parser type key `figaro_iciot` matches `spec.data_sources.io_table.type`.
- `skills/io_parsers/SKILL.md` written.

### Step 3 — Add `skills/classification_mapping/` core ✅ DONE
- Bundled label dicts for NACE_R2_64, CPA_2008_64, ISIC_R4, WIOD56 (in-process, no external files).
- `load_concordance`, `search_by_description`, `expand_prefix`, `validate_mapping` implemented.
- 14 unit tests passing.
- `skills/classification_mapping/SKILL.md` written.

### Step 4 — Classification Mapper agent stage ✅ DONE
- `nodes/classification_mapper.py` + `agents/prompts/classification_mapper.py`.
- Inserted between `paper_analyst` and `human_approval` in the LangGraph.
- No-op when `spec.concepts_to_map` is empty (FIGARO run unaffected).
- LangChain tool wrappers in `agents/tools/classification_tools.py`.
- `concept_mappings` field added to `PipelineState`.
- Config routing: `classification_mapper: claude-sonnet-4-6`.

### Step 5 — Add WIOD parser ✅ DONE
- `skills/io_parsers/wiod_mrio/core.py` — parses `WIOT{year}_Nov16_ROW.xlsx` + `SEA.xlsx`.
- Registered in dispatcher as `wiod_mrio`.
- 9 unit tests passing on synthetic 2-country × 2-industry xlsx (built in-memory with openpyxl).

### Step 6 — Add OECD ICIO and EXIOBASE parsers ✅ DONE
- `skills/io_parsers/oecd_icio/` — parses `ICIO{version}_{year}.csv`; optional `icio_employment_{year}.csv` for Em.
- `skills/io_parsers/exiobase/` — parses `IOT_{year}_*.zip` (Z.txt, Y.txt, x.txt, satellite/F.txt).
- EXIOBASE sector mapping via `exiobase_name` field in spec industry_list entries.
- Employment from F.txt by summing all stressor rows containing "Employment".
- 8 + 9 = 17 synthetic tests passing (in-memory CSV / zip, no real data required).

### Step 7 — Decomposer generalization ✅ DONE (was already generic)
- Confirmed: aggregation reads `spec["classification"]["aggregations"]` dynamically.
- No hardcoded sector names or counts in decomposer or Leontief skill.

### Step 8 — Polish + package
- Publish `skills/` as a standalone pip-installable package (`pip install io-replication-skills`).
- Keep agent framework in this repo; skills become a dependency.
- Per-skill SKILL.md files written (leontief, io_parsers, classification_mapping).

---

## 8. Open Questions

1. **Web access for Classification Mapper**: does the agent get real web search, or only a curated set of pre-fetched concordance tables? Web search is strictly more powerful but less reproducible. Proposed default: **bundled concordances + opt-in web search** via a config flag.

2. **Concept mapping confidence threshold**: at what `confidence: low` level should we halt the pipeline vs. continue with warnings? Proposed: halt only if `confidence == "low"` AND the concept is load-bearing for a benchmark (reviewer decides).

3. **Multi-year vs single-year**: FIGARO 2019 is 2010 only. Papers like WIOD-based growth decompositions span 1995–2014. Do we load all years at once or one-at-a-time? Proposed: parser skill returns a *year-indexed* `IOTables` dict; Stage 3 iterates.

4. **Non-IO extensions (SDA, GMRIO, MRIO-environmental)**: structural decomposition analysis and environmental extensions are natural next steps. Out of scope for v1 but the skills layout should not preclude them. Propose `skills/sda/` and `skills/environmental/` as future companions to `skills/leontief/`.

5. **Reproducibility of LLM stages**: the Paper Analyst and Classification Mapper are inherently non-deterministic. Ship every run with a `lockfile.yaml` capturing (model, temperature, prompt hash, tool call traces) so downstream users can reproduce exactly.

6. **Paper-analyst hallucinations**: today the analyst sometimes invents benchmark numbers. Should we enforce that every `benchmarks[].value` has a `page` citation and the reviewer verifies the cited page actually contains that number? Proposed: yes, make citation mandatory.

7. **Currency / price year**: cross-year comparisons need deflators. Parser skills should surface the price year; Leontief skill should not silently mix nominals from different years.

8. **Employment vs other factor inputs**: the framework is built around employment (`Em`), but CO₂, water, materials, value-added all work identically. Generalize to a named **factor** (`factor: employment` | `factor: co2` | ...) and let parsers expose multiple factors.

---

## 9. What Ships First

If we can only do one thing in the next iteration, do **Step 1 (`skills/leontief/` extraction)** because:
- Zero behavior change risk.
- Produces an immediately useful standalone library.
- Forces us to nail the `IOTables` + `LeontiefModel` dataclass contracts that every other step depends on.
- Unblocks Steps 2, 5, 6, 7 simultaneously.

**Do not** start Step 4 (Classification Mapper agent) until Step 3 (concordance skill) is done — the agent without the skill is just web-search-and-pray.
