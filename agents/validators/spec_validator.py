"""
Validates a replication_spec dict against the JSON Schema in schemas/replication_spec_schema.yaml.
"""
from pathlib import Path

import jsonschema
import yaml


_SCHEMA_PATH = Path(__file__).parents[2] / "schemas" / "replication_spec_schema.yaml"
_cached_schema: dict | None = None


def load_schema() -> dict:
    global _cached_schema
    if _cached_schema is None:
        with open(_SCHEMA_PATH) as f:
            _cached_schema = yaml.safe_load(f)
    return _cached_schema


def validate_spec(spec: dict) -> tuple[bool, list[str]]:
    """
    Validate a parsed replication_spec dict against the schema.

    Returns:
        (is_valid, errors) where errors is a list of human-readable error strings.
    """
    schema = load_schema()
    validator = jsonschema.Draft7Validator(schema)
    errors = [
        f"{' > '.join(str(p) for p in e.absolute_path) or 'root'}: {e.message}"
        for e in sorted(validator.iter_errors(spec), key=lambda e: e.absolute_path)
    ]
    return len(errors) == 0, errors


def validate_spec_file(path: str) -> tuple[bool, list[str]]:
    """Convenience wrapper that loads YAML from disk before validating."""
    with open(path) as f:
        spec = yaml.safe_load(f)
    return validate_spec(spec)
