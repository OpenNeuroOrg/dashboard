"""Typed JSON I/O using cattrs + generated dataclasses."""

import json
from pathlib import Path
from typing import TypeVar

import cattrs

T = TypeVar("T")

converter = cattrs.Converter()

# Enums: structure/unstructure as their string value
# (cattrs handles str-subclass enums natively)

# datetime: keep as ISO format strings (no conversion needed
# since the schema uses string range for datetime fields)


def load_typed(path: Path, cls: type[T]) -> T:
    """Load a JSON file and structure into a dataclass."""
    with open(path) as f:
        data = json.load(f)
    return converter.structure(data, cls)


def dump_typed(path: Path, instance: object) -> None:
    """Unstructure a dataclass and write as JSON."""
    data = converter.unstructure(instance)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
