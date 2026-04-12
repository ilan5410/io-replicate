# Data Guide Cache

Pre-computed data profiles for known datasets. Each subdirectory holds one
cached `data_guide.yaml` for a specific dataset fingerprint.

## How it works

When Stage 1.5 (Data Guide) runs, it computes a **fingerprint** from the
data manifest (sorted dataset keys + filenames + file sizes). Before calling
the LLM, it checks here for a matching fingerprint. If found, the guide is
loaded instantly at zero LLM cost.

After generating a new guide, the pipeline automatically writes it here so
future runs (on any machine) can reuse it.

## Sharing guides via GitHub

```bash
# After a new guide is generated:
git add data_guides/
git commit -m "cache: add data guide <fingerprint>"
git push

# On another machine, pull before running:
git pull
io-replicate run --paper my_paper.pdf   # Stage 1.5 will find the cached guide
```

## Directory structure

```
data_guides/
  <fingerprint>/          # 16-char SHA-256 hex
    data_guide.yaml       # the cached profile
    README.md             # human-readable metadata (paper, datasets, year)
```

## Cache invalidation

The fingerprint changes automatically when:
- A different dataset is downloaded (different filename or Eurostat code)
- The file size changes (new data vintage was published)

It does NOT change based on run timestamps or run IDs, so the same dataset
downloaded twice produces the same fingerprint.
