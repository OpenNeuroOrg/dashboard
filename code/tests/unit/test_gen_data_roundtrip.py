"""Test that gen-data output round-trips through load_typed."""

import subprocess
import sys
from pathlib import Path

import pytest

from openneuro_dashboard.converter import load_typed
from openneuro_dashboard.models import (
    DatasetsRegistry,
    FileList,
    GitHubStatus,
    S3FileDiff,
    S3Version,
    SnapshotIndex,
    SnapshotMetadata,
)


@pytest.fixture(scope="module")
def gen_data_dir(tmp_path_factory):
    """Generate synthetic data to a temp directory."""
    out = tmp_path_factory.mktemp("gen-data")
    result = subprocess.run(
        [sys.executable, "-m", "openneuro_dashboard", "gen-data",
         "--output-dir", str(out), "--seed", "42", "--num-datasets", "5"],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, f"gen-data failed: {result.stderr}"
    return out


def test_registry_roundtrips(gen_data_dir):
    obj = load_typed(gen_data_dir / "datasets-registry.json", DatasetsRegistry)
    assert obj.totalCount == 5
    assert len(obj.latestSnapshots) == 5


def test_snapshots_roundtrip(gen_data_dir):
    for ds_dir in sorted((gen_data_dir / "datasets").iterdir()):
        obj = load_typed(ds_dir / "snapshots.json", SnapshotIndex)
        assert len(obj.tags) > 0


def test_metadata_roundtrip(gen_data_dir):
    for ds_dir in sorted((gen_data_dir / "datasets").iterdir()):
        snaps = load_typed(ds_dir / "snapshots.json", SnapshotIndex)
        for tag in snaps.tags:
            meta_path = ds_dir / "snapshots" / tag / "metadata.json"
            if meta_path.exists():
                obj = load_typed(meta_path, SnapshotMetadata)
                assert obj.hexsha is not None


def test_github_roundtrip(gen_data_dir):
    for ds_dir in sorted((gen_data_dir / "datasets").iterdir()):
        path = ds_dir / "github.json"
        if path.exists():
            obj = load_typed(path, GitHubStatus)
            assert obj.head is not None


def test_s3_version_roundtrip(gen_data_dir):
    for ds_dir in sorted((gen_data_dir / "datasets").iterdir()):
        path = ds_dir / "s3-version.json"
        if path.exists():
            obj = load_typed(path, S3Version)
            assert obj.lastChecked is not None


def test_s3_diff_roundtrip(gen_data_dir):
    for ds_dir in sorted((gen_data_dir / "datasets").iterdir()):
        path = ds_dir / "s3-diff.json"
        if path.exists():
            obj = load_typed(path, S3FileDiff)
            assert obj.datasetId is not None


def test_file_list_roundtrip(gen_data_dir):
    registry = load_typed(gen_data_dir / "datasets-registry.json", DatasetsRegistry)
    for ds_id, tag in registry.latestSnapshots.items():
        path = gen_data_dir / "datasets" / ds_id / "snapshots" / tag / "files.json"
        if path.exists():
            obj = load_typed(path, FileList)
            assert obj.count == len(obj.files)
