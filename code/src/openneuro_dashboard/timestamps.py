"""Timestamp manifest I/O for per-stage check timestamps."""

from pathlib import Path

from .utils import load_json_safe, write_json

# Relative paths from the output directory
GITHUB_MANIFEST = Path("timestamps/github.json")
S3_VERSION_MANIFEST = Path("timestamps/s3-version.json")
S3_FILES_MANIFEST = Path("timestamps/s3-files.json")


def load_timestamp_manifest(path: Path) -> dict[str, str]:
    """Load a timestamp manifest, returning empty dict if missing."""
    return load_json_safe(path) or {}


def save_timestamp_manifest(path: Path, manifest: dict[str, str]) -> None:
    """Write a timestamp manifest as sorted JSON."""
    write_json(path, dict(sorted(manifest.items())))
