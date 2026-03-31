"""Unit tests for openneuro_dashboard.utils."""

from __future__ import annotations

import json
import re

from openneuro_dashboard.utils import (
    SCHEMA_VERSION,
    format_timestamp,
    load_json,
    load_json_safe,
    write_json,
)


def test_write_json_load_json_roundtrip(tmp_path):
    """write_json then load_json returns the same dict."""
    data = {"key": "value", "nested": {"a": 1}}
    path = tmp_path / "test.json"
    write_json(path, data)

    result = load_json(path)
    assert result == data


def test_write_json_creates_directories(tmp_path):
    """write_json creates intermediate parent directories."""
    path = tmp_path / "deep" / "nested" / "dir" / "file.json"
    write_json(path, {"x": 1})

    assert path.exists()
    assert load_json(path) == {"x": 1}


def test_write_json_format(tmp_path):
    """Output uses indent=2 and ends with a trailing newline."""
    data = {"a": 1}
    path = tmp_path / "fmt.json"
    write_json(path, data)

    raw = path.read_text()
    assert raw.endswith("\n")
    # Verify indent by checking the structure matches json.dumps(indent=2)
    expected = json.dumps(data, indent=2) + "\n"
    assert raw == expected


def test_load_json_safe_missing(tmp_path):
    """load_json_safe returns None for a missing file."""
    result = load_json_safe(tmp_path / "nonexistent.json")
    assert result is None


def test_load_json_safe_existing(tmp_path):
    """load_json_safe returns dict for an existing file."""
    path = tmp_path / "existing.json"
    write_json(path, {"hello": "world"})

    result = load_json_safe(path)
    assert result == {"hello": "world"}


def test_format_timestamp():
    """format_timestamp returns ISO 8601 matching YYYY-MM-DDTHH:MM:SS.000Z."""
    ts = format_timestamp()
    pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.000Z$"
    assert re.match(pattern, ts), f"Timestamp {ts!r} does not match expected pattern"


def test_schema_version():
    """SCHEMA_VERSION is '2.2.0'."""
    assert SCHEMA_VERSION == "2.2.0"
