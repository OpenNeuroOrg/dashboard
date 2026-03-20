"""Unit tests for openneuro_dashboard.summarize (status aggregation)."""

from __future__ import annotations

from openneuro_dashboard.models import DatasetSummary
from openneuro_dashboard.summarize import summarize_dataset
from openneuro_dashboard.utils import write_json


def _write_fixtures(dataset_dir, github=None, s3_version=None, s3_diff=None):
    """Write JSON fixture files into *dataset_dir*."""
    if github is not None:
        write_json(dataset_dir / "github.json", github)
    if s3_version is not None:
        write_json(dataset_dir / "s3-version.json", s3_version)
    if s3_diff is not None:
        write_json(dataset_dir / "s3-diff.json", s3_diff)


GITHUB_OK = {
    "lastChecked": "2026-01-01T00:00:00.000Z",
    "head": "main",
    "branches": {"main": "abc123"},
    "tags": {"1.0.2": "abc123"},
}

S3_VERSION_DOI_OK = {
    "lastChecked": "2026-01-01T00:00:00.000Z",
    "accessible": True,
    "datasetDescriptionDOI": "10.18112/openneuro.ds000001.v1.0.2",
    "extractedVersion": "1.0.2",
    "versionSource": "doi",
}

S3_DIFF_OK = {
    "checkedAt": "2026-01-01T00:00:00.000Z",
    "status": "ok",
    "added": [],
    "removed": [],
}


# ------------------------------------------------------------------


def test_all_ok(tmp_path):
    """All checks pass -> overall status 'ok'."""
    dd = tmp_path / "datasets" / "ds000001"
    _write_fixtures(dd, github=GITHUB_OK, s3_version=S3_VERSION_DOI_OK, s3_diff=S3_DIFF_OK)

    result = summarize_dataset("ds000001", "1.0.2", dd)

    assert isinstance(result, DatasetSummary)
    assert result.status.value == "ok"
    assert result.checks.github.value == "ok"
    assert result.checks.s3Version.value == "ok"
    assert result.checks.s3Files.value == "ok"


def test_github_error(tmp_path):
    """Latest tag missing from GitHub -> github 'error', overall 'error'."""
    dd = tmp_path / "datasets" / "ds000001"
    github_missing_tag = {
        "lastChecked": "2026-01-01T00:00:00.000Z",
        "head": "main",
        "branches": {"main": "abc123"},
        "tags": {},
    }
    _write_fixtures(dd, github=github_missing_tag, s3_version=S3_VERSION_DOI_OK, s3_diff=S3_DIFF_OK)

    result = summarize_dataset("ds000001", "1.0.2", dd)

    assert result.checks.github.value == "error"
    assert result.status.value == "error"


def test_s3_blocked_403(tmp_path):
    """S3 inaccessible (403) -> s3Blocked=True, overall 'error'."""
    dd = tmp_path / "datasets" / "ds000001"
    s3_blocked = {
        "lastChecked": "2026-01-01T00:00:00.000Z",
        "accessible": False,
        "httpStatus": 403,
    }
    _write_fixtures(dd, github=GITHUB_OK, s3_version=s3_blocked)

    result = summarize_dataset("ds000001", "1.0.2", dd)

    assert result.s3Blocked is True
    assert result.checks.s3Version.value == "error"
    assert result.checks.s3Files.value == "error"
    assert result.status.value == "error"


def test_version_mismatch(tmp_path):
    """DOI version != latest -> s3Version 'version-mismatch'."""
    dd = tmp_path / "datasets" / "ds000001"
    s3_old = {
        "lastChecked": "2026-01-01T00:00:00.000Z",
        "accessible": True,
        "extractedVersion": "1.0.1",
        "versionSource": "doi",
    }
    _write_fixtures(dd, github=GITHUB_OK, s3_version=s3_old, s3_diff=S3_DIFF_OK)

    result = summarize_dataset("ds000001", "1.0.2", dd)

    assert result.checks.s3Version.value == "version-mismatch"
    assert result.status.value == "version-mismatch"


def test_pending_missing_files(tmp_path):
    """Missing check files -> those checks are 'pending'."""
    dd = tmp_path / "datasets" / "ds000001"
    dd.mkdir(parents=True, exist_ok=True)

    result = summarize_dataset("ds000001", "1.0.2", dd)

    assert result.checks.github.value == "pending"
    assert result.checks.s3Version.value == "pending"
    assert result.checks.s3Files.value == "pending"
    assert result.status.value == "pending"


def test_precedence(tmp_path):
    """ok < warning < version-mismatch < error < pending."""
    dd = tmp_path / "datasets" / "ds000001"
    # github ok, s3Version version-mismatch, s3Files pending (no s3-diff)
    s3_old = {
        "lastChecked": "2026-01-01T00:00:00.000Z",
        "accessible": True,
        "extractedVersion": "1.0.1",
        "versionSource": "doi",
    }
    _write_fixtures(dd, github=GITHUB_OK, s3_version=s3_old)
    # s3-diff absent -> s3Files pending

    result = summarize_dataset("ds000001", "1.0.2", dd)

    assert result.checks.github.value == "ok"
    assert result.checks.s3Version.value == "version-mismatch"
    assert result.checks.s3Files.value == "pending"
    # pending > version-mismatch > ok
    assert result.status.value == "pending"
