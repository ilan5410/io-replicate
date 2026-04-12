# Skill: classification_mapping

Concordance tables and code search for industrial classification systems.
No LLM, no I/O — pure Python.

## When to use

Use when the Classification Mapper agent needs to:
- Find classification codes for a concept description
- Expand a code prefix to its children
- Map between two classification systems (NACE ↔ ISIC, etc.)
- Validate that a code list is self-consistent

## API

```python
from skills.classification_mapping import (
    load_concordance,        # list of (from_code, to_code) pairs
    search_by_description,   # fuzzy text → ranked Code list
    expand_prefix,           # all codes starting with prefix
    validate_mapping,        # check codes + warn on parent/child overlap
)
```

## Bundled classifications

| name | scope | codes |
|------|-------|-------|
| `NACE_R2_64` | EU activity classification, 64-sector detail | 64 |
| `CPA_2008_64` | EU product classification, 64-sector detail | 64 |
| `ISIC_R4` | UN global activity classification | ~80 |
| `WIOD56` | WIOD 2016 56-sector codes | 56 |

## Bundled concordances

| from → to | pairs |
|-----------|-------|
| NACE_R2_64 → CPA_2008_64 | direct 1-to-1 at 64-sector level |
| NACE_R2_64 → ISIC_R4 | 1-to-many for range codes |
| NACE_R2_64 → WIOD56 | 1-to-1 for overlapping codes |

## Adding a classification or concordance

Edit `skills/classification_mapping/concordances.py`:
1. Add a dict `{code: label}` for the new classification
2. Add it to `CLASSIFICATION_LABELS`
3. Add a function returning `list[tuple[str, str]]` pairs
4. Register it in `REGISTERED`
