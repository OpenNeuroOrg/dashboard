"""Test that generated data files validate against the LinkML schema."""

import pytest

from openneuro_dashboard.converter import load_typed
from openneuro_dashboard.models import (
    AllDatasetsSummary,
    GitHubStatus,
    S3FileDiff,
    S3Version,
)


def test_github_status_validates(datasets_dir, dataset_id):
    path = datasets_dir / dataset_id / "github.json"
    if not path.exists():
        pytest.skip(f"No github.json for {dataset_id}")
    obj = load_typed(path, GitHubStatus)
    assert obj.lastChecked is not None


def test_s3_version_validates(datasets_dir, dataset_id):
    path = datasets_dir / dataset_id / "s3-version.json"
    if not path.exists():
        pytest.skip(f"No s3-version.json for {dataset_id}")
    obj = load_typed(path, S3Version)
    assert obj.lastChecked is not None


def test_s3_diff_validates(datasets_dir, dataset_id):
    path = datasets_dir / dataset_id / "s3-diff.json"
    if not path.exists():
        pytest.skip(f"No s3-diff.json for {dataset_id}")
    obj = load_typed(path, S3FileDiff)
    assert obj.datasetId is not None


def test_all_datasets_summary_validates(fixtures_dir):
    """Validate the summary fixture against AllDatasetsSummary."""
    path = fixtures_dir / "all-datasets.json"
    if not path.exists():
        pytest.skip("No all-datasets.json fixture")
    obj = load_typed(path, AllDatasetsSummary)
    assert len(obj.datasets) > 0
    for ds in obj.datasets:
        assert ds.id is not None
        assert ds.status is not None
