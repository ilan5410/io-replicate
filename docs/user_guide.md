# IO Replicator — User Guide

## How to replicate a paper in 5 minutes

1. **Install**
   ```bash
   pip install -e .
   ```

2. **Set API keys** (at least one provider required)
   ```bash
   export ANTHROPIC_API_KEY=sk-ant-...
   export OPENAI_API_KEY=sk-...   # optional, used for cheaper stages
   ```

3. **Run**
   ```bash
   io-replicate run --paper my_paper.pdf
   ```
   The pipeline will:
   - Analyse the paper and produce `replication_spec.yaml`
   - Show you a summary and ask for approval
   - Download data, build the Leontief model, produce outputs
   - Write a review report comparing results to the paper's benchmarks

4. **Find your outputs** in `runs/{timestamp}/outputs/`

---

## Using an existing spec (skip Paper Analyst)

If you already have a spec (or want to use the FIGARO ground truth):

```bash
io-replicate run --spec specs/figaro_2019/replication_spec.yaml
```

---

## Resuming from a specific stage

If the pipeline failed partway through:

```bash
# Resume from data preparation (stage 2) onward
io-replicate run --spec specs/figaro_2019/replication_spec.yaml --start-stage 2

# Re-run only the reviewer (e.g. after editing the spec's benchmarks)
io-replicate run --spec specs/figaro_2019/replication_spec.yaml --only reviewer
```

---

## Reviewing and editing the replication spec

The spec is the most important artifact. After the Paper Analyst runs, review it carefully:

- **`geography.analysis_entities`**: Are all the right countries listed?
- **`methodology.export_definition`**: Does this match the paper's description?
- **`outputs`**: Are all tables and figures from the paper listed?
- **`benchmarks.values`**: Are the expected values extracted correctly from the paper?

Edit the YAML directly, then re-run from the appropriate stage.

---

## Handling common failures

### Data download fails
```
Error: HTTP 429 Too Many Requests
```
The Eurostat API rate-limits requests. Re-run from stage 1:
```bash
io-replicate run --spec my_spec.yaml --start-stage 1
```

### Data preparation dimension mismatch
```
Z_EU shape (1728, 1728) != expected (1792, 1792)
```
A country in your spec wasn't found in the raw data. Check which countries are missing:
```bash
io-replicate validate --spec my_spec.yaml
```
Then either remove the missing country from `geography.analysis_entities` or fix the download.

### Review FAIL
The review report lists which benchmarks failed. Common causes:
- Using product-by-product tables when the paper used industry-by-industry (expected ~2-5% deviation)
- Employment data vintage mismatch (expected ~1-3% deviation)
- Missing confidential data for small countries (LU, MT)

These are documented in `limitations` and are expected. A FAIL (>25% deviation) usually indicates a bug.

---

## Adding a new data source

1. Add the data source to `data_sources.io_table.type` in your spec
2. The Data Acquirer agent will look for matching API patterns in its system prompt
3. If the source isn't known to the agent, add API documentation to `agents/prompts/data_acquirer.py`
4. List the source in `cli/main.py`'s `SUPPORTED_SOURCES`

---

## Cost per run

Estimated API costs (mixed Anthropic/OpenAI routing as per `config.yaml`):

| Stage | Model | Estimated cost |
|-------|-------|----------------|
| Paper Analyst | Claude Opus | ~$0.50 |
| Data Acquirer | GPT-4o-mini | ~$0.01 |
| Data Preparer | Claude Sonnet | ~$0.10 |
| Model Builder | — (deterministic) | $0.00 |
| Decomposer | — (deterministic) | $0.00 |
| Output Producer | GPT-4o-mini | ~$0.02 |
| Reviewer | Claude Sonnet | ~$0.07 |
| **Total** | | **~$0.70** |

To reduce costs: use `--start-stage 3` when data is already downloaded (skips the expensive download stages).

After a successful run, all generated scripts are saved in `runs/{timestamp}/generated_scripts/`. For subsequent runs of the same paper, you can execute these scripts directly without any LLM cost.
