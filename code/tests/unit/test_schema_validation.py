"""Test that generated data files validate against the LinkML schema."""

import json

import pytest

from openneuro_dashboard.converter import load_typed
from openneuro_dashboard.models import (
    AllDatasetsSummary,
    GitHubStatus,
    S3FileDiff,
    S3Version,
)


def _assert_linkml_valid(linkml_validator, data, target_class):
    """Validate data against the LinkML schema and assert no errors."""
    errors = linkml_validator.validate(data, target_class=target_class)
    assert not errors, f"LinkML validation errors for {target_class}: {errors}"


def test_github_status_validates(datasets_dir, dataset_id, linkml_validator):
    path = datasets_dir / dataset_id / "github.json"
    if not path.exists():
        pytest.skip(f"No github.json for {dataset_id}")
    # cattrs validation
    obj = load_typed(path, GitHubStatus)
    assert obj.lastChecked is not None
    # LinkML validation
    with open(path) as f:
        data = json.load(f)
    _assert_linkml_valid(linkml_validator, data, "GitHubStatus")


def test_s3_version_validates(datasets_dir, dataset_id, linkml_validator):
    path = datasets_dir / dataset_id / "s3-version.json"
    if not path.exists():
        pytest.skip(f"No s3-version.json for {dataset_id}")
    # cattrs validation
    obj = load_typed(path, S3Version)
    assert obj.lastChecked is not None
    # LinkML validation
    with open(path) as f:
        data = json.load(f)
    _assert_linkml_valid(linkml_validator, data, "S3Version")


def test_s3_diff_validates(datasets_dir, dataset_id, linkml_validator):
    path = datasets_dir / dataset_id / "s3-diff.json"
    if not path.exists():
        pytest.skip(f"No s3-diff.json for {dataset_id}")
    # cattrs validation
    obj = load_typed(path, S3FileDiff)
    assert obj.datasetId is not None
    # LinkML validation
    with open(path) as f:
        data = json.load(f)
    _assert_linkml_valid(linkml_validator, data, "S3FileDiff")


def test_all_datasets_summary_validates(fixtures_dir, linkml_validator):
    """Validate the summary fixture against AllDatasetsSummary."""
    path = fixtures_dir / "all-datasets.json"
    if not path.exists():
        pytest.skip("No all-datasets.json fixture")
    # cattrs validation
    obj = load_typed(path, AllDatasetsSummary)
    assert len(obj.datasets) > 0
    for ds in obj.datasets:
        assert ds.id is not None
        assert ds.status is not None
    # LinkML validation
    with open(path) as f:
        data = json.load(f)
    _assert_linkml_valid(linkml_validator, data, "AllDatasetsSummary")
