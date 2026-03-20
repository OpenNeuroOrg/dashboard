"""Dashboard pipeline utilities."""

import json
from datetime import UTC, datetime
from pathlib import Path

SCHEMA_VERSION = "1.1.0"


def format_timestamp() -> str:
    """Format current UTC time in the standard dashboard timestamp format."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def write_json(path: Path, data: dict) -> None:
    """Write JSON to file with pretty formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def load_json(path: Path) -> dict:
    """Load JSON from file."""
    with open(path) as f:
        return json.load(f)


def load_json_safe(path: Path) -> dict | None:
    """Load JSON from file if it exists, otherwise return None."""
    if not path.exists():
        return None
    return load_json(path)
