"""Generate Python dataclasses + enums from the LinkML schema.

Usage:
    python scripts/gen_models.py <schema.yaml> <output.py>

The generated module depends ONLY on the stdlib (dataclasses, enum, typing).
"""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

from linkml_runtime.utils.schemaview import SchemaView

# LinkML type -> Python type name
TYPE_MAP: dict[str, str] = {
    "string": "str",
    "integer": "int",
    "boolean": "bool",
    "datetime": "str",
    "float": "float",
    "uri": "str",
}


def _is_map_class(sv: SchemaView, class_name: str) -> bool:
    """Detect key-value map classes (exactly 2 simple string attributes).

    Map classes have exactly 2 attributes, both with range 'string',
    neither required nor multivalued.
    """
    cls = sv.get_class(class_name)
    if cls is None:
        return False
    attrs = list(sv.class_induced_slots(class_name))
    if len(attrs) != 2:
        return False
    for attr in attrs:
        rng = attr.range or "string"
        if rng != "string" or attr.multivalued or attr.required:
            return False
    return True


def _python_type(sv: SchemaView, attr, enum_names: set[str]) -> str:
    """Return the Python type annotation for an attribute."""
    rng = attr.range or "string"

    # Check if range is a map-type class
    if rng in [c.name for c in sv.all_classes().values()] and _is_map_class(sv, rng):
        base = "dict[str, str]"
    elif rng in enum_names:
        base = rng
    elif rng in [c.name for c in sv.all_classes().values()]:
        base = rng
    elif rng in TYPE_MAP:
        base = TYPE_MAP[rng]
    else:
        base = "str"

    if attr.multivalued:
        base = f"list[{base}]"

    if not attr.required:
        base = f"{base} | None"

    return base


def _default(attr) -> str:
    """Return the default value expression for an attribute."""
    if attr.multivalued:
        return " = field(default_factory=list)" if attr.required else " = None"
    if not attr.required:
        return " = None"
    return ""


def generate(schema_path: str, output_path: str) -> None:
    sv = SchemaView(schema_path)

    enum_names = set(sv.all_enums().keys())
    map_classes = {
        c.name for c in sv.all_classes().values() if _is_map_class(sv, c.name)
    }

    lines: list[str] = [
        '"""Auto-generated dataclasses from LinkML schema.',
        "",
        "Do not edit manually. Re-generate with:",
        f"    python scripts/gen_models.py <schema.yaml> <output.py>",
        '"""',
        "",
        "from __future__ import annotations",
        "",
        "from dataclasses import dataclass, field",
        "from enum import Enum",
        "",
    ]

    # Enums
    for enum_name, enum_def in sv.all_enums().items():
        desc = enum_def.description or ""
        lines.append("")
        lines.append(f"class {enum_name}(str, Enum):")
        if desc:
            lines.append(f'    """{desc}"""')
        lines.append("")
        for pv_name, pv in enum_def.permissible_values.items():
            # Use the text as-is for value; Python name replaces hyphens/spaces
            py_name = pv_name.replace("-", "_").replace(" ", "_")
            lines.append(f'    {py_name} = "{pv_name}"')
        lines.append("")

    # Classes (skip map-type classes)
    for class_name, class_def in sv.all_classes().items():
        if class_name in map_classes:
            continue
        desc = class_def.description or ""

        lines.append("")
        lines.append("@dataclass")
        lines.append(f"class {class_name}:")
        if desc:
            # Replace inner double quotes with single to avoid docstring issues
            desc_safe = desc.replace('"', "'")
            wrapped = textwrap.fill(desc_safe, width=76)
            lines.append(f'    """{wrapped}"""')
        lines.append("")

        attrs = list(sv.class_induced_slots(class_name))
        # Sort: required fields first (no default), then optional
        required_attrs = [a for a in attrs if a.required and not a.multivalued]
        optional_attrs = [a for a in attrs if not a.required or a.multivalued]

        for attr in required_attrs + optional_attrs:
            py_type = _python_type(sv, attr, enum_names)
            default = _default(attr)
            lines.append(f"    {attr.name}: {py_type}{default}")

        if not attrs:
            lines.append("    pass")
        lines.append("")

    # Write output
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines))
    print(f"Generated {out}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <schema.yaml> <output.py>", file=sys.stderr)
        sys.exit(1)
    generate(sys.argv[1], sys.argv[2])
