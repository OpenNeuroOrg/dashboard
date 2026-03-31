"""Unit tests for openneuro_dashboard.timestamps."""

from __future__ import annotations

from pathlib import Path

from openneuro_dashboard.timestamps import (
    GITHUB_MANIFEST,
    S3_FILES_MANIFEST,
    S3_VERSION_MANIFEST,
    load_timestamp_manifest,
    save_timestamp_manifest,
)


def test_load_missing_returns_empty(tmp_path):
    """Loading a nonexistent manifest returns an empty dict."""
    result = load_timestamp_manifest(tmp_path / "nonexistent.json")
    assert result == {}


def test_save_and_load_roundtrip(tmp_path):
    """Saved manifest round-trips through load."""
    manifest = {"ds000001": "2026-01-01T00:00:00.000Z", "ds000002": "2026-01-02T00:00:00.000Z"}
    path = tmp_path / "timestamps" / "test.json"
    save_timestamp_manifest(path, manifest)
    result = load_timestamp_manifest(path)
    assert result == manifest


def test_save_creates_parent_dirs(tmp_path):
    """save_timestamp_manifest creates intermediate directories."""
    path = tmp_path / "deep" / "nested" / "manifest.json"
    save_timestamp_manifest(path, {"ds000001": "2026-01-01T00:00:00.000Z"})
    assert path.exists()


def test_manifest_path_constants():
    """Manifest path constants are relative Path objects."""
    assert GITHUB_MANIFEST == Path("timestamps/github.json")
    assert S3_VERSION_MANIFEST == Path("timestamps/s3-version.json")
    assert S3_FILES_MANIFEST == Path("timestamps/s3-files.json")
