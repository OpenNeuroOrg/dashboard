"""Integration test: run summarize against hand-crafted fixture data."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from openneuro_dashboard.summarize import generate_summary

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"

EXPECTED_STATUSES = {
    "ds000001": "ok",
    "ds000002": "error",
    "ds000003": "version-mismatch",
    "ds000004": "warning",
    "ds000005": "error",
}

EXPECTED_CHECKS = {
    "ds000001": {"github": "ok", "s3Version": "ok", "s3Files": "ok"},
    "ds000002": {"github": "ok", "s3Version": "error", "s3Files": "error"},
    "ds000003": {"github": "ok", "s3Version": "version-mismatch", "s3Files": "ok"},
    "ds000004": {"github": "warning", "s3Version": "warning", "s3Files": "ok"},
    "ds000005": {"github": "ok", "s3Version": "ok", "s3Files": "error"},
}


def _setup_data(tmp_path: Path) -> Path:
    """Copy fixtures into a tmp_path/data directory and return it."""
    data_dir = tmp_path / "data"
    shutil.copytree(FIXTURES_DIR, data_dir, dirs_exist_ok=True)
    return data_dir


def test_full_pipeline_summary(tmp_path):
    """Run summarize against fixture data and verify statuses."""
    data_dir = _setup_data(tmp_path)
    generate_summary(data_dir)

    result = json.loads((data_dir / "all-datasets.json").read_text())

    assert result["schemaVersion"] == "1.1.0"
    assert len(result["datasets"]) == 5

    for ds in result["datasets"]:
        expected = EXPECTED_STATUSES[ds["id"]]
        assert ds["status"] == expected, (
            f"{ds['id']}: expected status {expected!r}, got {ds['status']!r}"
        )


def test_per_check_statuses(tmp_path):
    """Verify individual check statuses for every dataset."""
    data_dir = _setup_data(tmp_path)
    generate_summary(data_dir)

    result = json.loads((data_dir / "all-datasets.json").read_text())

    for ds in result["datasets"]:
        expected = EXPECTED_CHECKS[ds["id"]]
        assert ds["checks"] == expected, (
            f"{ds['id']}: expected checks {expected}, got {ds['checks']}"
        )


def test_blocked_dataset_has_flag(tmp_path):
    """Verify s3Blocked flag is set for blocked datasets."""
    data_dir = _setup_data(tmp_path)
    generate_summary(data_dir)

    result = json.loads((data_dir / "all-datasets.json").read_text())
    ds002 = next(d for d in result["datasets"] if d["id"] == "ds000002")
    assert ds002.get("s3Blocked") is True

    # Non-blocked datasets should not have s3Blocked
    for ds in result["datasets"]:
        if ds["id"] != "ds000002":
            assert "s3Blocked" not in ds, (
                f"{ds['id']} should not have s3Blocked flag"
            )


def test_last_checked_timestamps_present(tmp_path):
    """Verify lastChecked sub-object is populated from fixture timestamps."""
    data_dir = _setup_data(tmp_path)
    generate_summary(data_dir)

    result = json.loads((data_dir / "all-datasets.json").read_text())

    for ds in result["datasets"]:
        assert "lastChecked" in ds, f"{ds['id']} missing lastChecked"
        lc = ds["lastChecked"]
        # All fixtures that have check files should have github timestamp
        assert "github" in lc, f"{ds['id']} missing lastChecked.github"


def test_expected_fixture_matches(tmp_path):
    """Cross-check generated summary against the expected fixture file."""
    data_dir = _setup_data(tmp_path)
    generate_summary(data_dir)

    result = json.loads((data_dir / "all-datasets.json").read_text())
    expected = json.loads((FIXTURES_DIR / "all-datasets.json").read_text())

    # Compare structural fields (ignore lastUpdated since it uses wallclock)
    assert result["schemaVersion"] == expected["schemaVersion"]
    assert len(result["datasets"]) == len(expected["datasets"])

    result_by_id = {ds["id"]: ds for ds in result["datasets"]}
    expected_by_id = {ds["id"]: ds for ds in expected["datasets"]}

    for ds_id in expected_by_id:
        assert result_by_id[ds_id]["status"] == expected_by_id[ds_id]["status"]
        assert result_by_id[ds_id]["checks"] == expected_by_id[ds_id]["checks"]
        if "s3Blocked" in expected_by_id[ds_id]:
            assert result_by_id[ds_id].get("s3Blocked") == expected_by_id[ds_id]["s3Blocked"]
