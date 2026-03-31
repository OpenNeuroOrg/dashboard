"""Test that gen-data output round-trips through load_typed."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from openneuro_dashboard.converter import load_typed
from openneuro_dashboard.utils import load_json
from openneuro_dashboard.models import (
    DatasetsRegistry,
    FileList,
    GitHubStatus,
    S3FileDiff,
    S3Version,
    SnapshotIndex,
    SnapshotMetadata,
)


def _assert_linkml_valid(linkml_validator, data, target_class):
    """Validate data against the LinkML schema and assert no errors."""
    errors = linkml_validator.validate(data, target_class=target_class)
    assert not errors, f"LinkML validation errors for {target_class}: {errors}"


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


def test_registry_roundtrips(gen_data_dir, linkml_validator):
    path = gen_data_dir / "datasets-registry.json"
    obj = load_typed(path, DatasetsRegistry)
    assert obj.totalCount == 5
    assert len(obj.latestSnapshots) == 5
    # LinkML validation
    with open(path) as f:
        data = json.load(f)
    _assert_linkml_valid(linkml_validator, data, "DatasetsRegistry")


def test_snapshots_roundtrip(gen_data_dir, linkml_validator):
    for ds_dir in sorted((gen_data_dir / "datasets").iterdir()):
        path = ds_dir / "snapshots.json"
        obj = load_typed(path, SnapshotIndex)
        assert len(obj.tags) > 0
        # LinkML validation
        with open(path) as f:
            data = json.load(f)
        _assert_linkml_valid(linkml_validator, data, "SnapshotIndex")


def test_metadata_roundtrip(gen_data_dir, linkml_validator):
    for ds_dir in sorted((gen_data_dir / "datasets").iterdir()):
        snaps = load_typed(ds_dir / "snapshots.json", SnapshotIndex)
        for tag in snaps.tags:
            meta_path = ds_dir / "snapshots" / tag / "metadata.json"
            if meta_path.exists():
                obj = load_typed(meta_path, SnapshotMetadata)
                assert obj.hexsha is not None
                # LinkML validation
                with open(meta_path) as f:
                    data = json.load(f)
                _assert_linkml_valid(linkml_validator, data, "SnapshotMetadata")


def test_github_roundtrip(gen_data_dir, linkml_validator):
    for ds_dir in sorted((gen_data_dir / "datasets").iterdir()):
        path = ds_dir / "github.json"
        if path.exists():
            obj = load_typed(path, GitHubStatus)
            assert obj.head is not None
            # LinkML validation
            with open(path) as f:
                data = json.load(f)
            _assert_linkml_valid(linkml_validator, data, "GitHubStatus")


def test_s3_version_roundtrip(gen_data_dir, linkml_validator):
    for ds_dir in sorted((gen_data_dir / "datasets").iterdir()):
        path = ds_dir / "s3-version.json"
        if path.exists():
            obj = load_typed(path, S3Version)
            assert obj.lastChecked is None
            # LinkML validation
            with open(path) as f:
                data = json.load(f)
            _assert_linkml_valid(linkml_validator, data, "S3Version")


def test_s3_diff_roundtrip(gen_data_dir, linkml_validator):
    for ds_dir in sorted((gen_data_dir / "datasets").iterdir()):
        path = ds_dir / "s3-diff.json"
        if path.exists():
            obj = load_typed(path, S3FileDiff)
            assert obj.datasetId is not None
            # LinkML validation
            with open(path) as f:
                data = json.load(f)
            _assert_linkml_valid(linkml_validator, data, "S3FileDiff")


def test_file_list_roundtrip(gen_data_dir, linkml_validator):
    registry = load_typed(gen_data_dir / "datasets-registry.json", DatasetsRegistry)
    for ds_id, tag in registry.latestSnapshots.items():
        path = gen_data_dir / "datasets" / ds_id / "snapshots" / tag / "files.json"
        if path.exists():
            obj = load_typed(path, FileList)
            assert obj.count == len(obj.files)
            # LinkML validation
            with open(path) as f:
                data = json.load(f)
            _assert_linkml_valid(linkml_validator, data, "FileList")


def test_timestamp_manifests_exist(gen_data_dir):
    """gen-data produces timestamp manifest files."""
    for name in ("github.json", "s3-version.json", "s3-files.json"):
        path = gen_data_dir / "timestamps" / name
        assert path.exists(), f"Missing manifest: {path}"
        data = load_json(path)
        assert isinstance(data, dict)
        assert len(data) > 0
        for key, value in data.items():
            assert key.startswith("ds"), f"Bad key: {key}"
            assert "T" in value, f"Bad timestamp: {value}"
