"""Unit tests for openneuro_dashboard.summarize (status aggregation)."""

from __future__ import annotations

from openneuro_dashboard.converter import dump_typed
from openneuro_dashboard.models import (
    CheckStatus,
    DatasetSummary,
    GitHubStatus,
    S3FileDiff,
    S3Version,
    VersionSource,
)
from openneuro_dashboard.summarize import summarize_dataset


def _write_fixtures(
    dataset_dir,
    github: GitHubStatus | None = None,
    s3_version: S3Version | None = None,
    s3_diff: S3FileDiff | None = None,
):
    """Write typed fixture files into *dataset_dir*."""
    if github is not None:
        dump_typed(dataset_dir / "github.json", github)
    if s3_version is not None:
        dump_typed(dataset_dir / "s3-version.json", s3_version)
    if s3_diff is not None:
        dump_typed(dataset_dir / "s3-diff.json", s3_diff)


GITHUB_OK = GitHubStatus(
    lastChecked="2026-01-01T00:00:00.000Z",
    head="main",
    branches={"main": "abc123"},
    tags={"1.0.2": "abc123"},
)

S3_VERSION_DOI_OK = S3Version(
    schemaVersion="2.0.0",
    lastChecked="2026-01-01T00:00:00.000Z",
    accessible=True,
    datasetDescriptionDOI="10.18112/openneuro.ds000001.v1.0.2",
    extractedVersion="1.0.2",
    versionSource=VersionSource.doi,
)

S3_DIFF_OK = S3FileDiff(
    schemaVersion="2.0.0",
    datasetId="ds000001",
    snapshotTag="1.0.2",
    s3Version="1.0.2",
    checkedAt="2026-01-01T00:00:00.000Z",
    status=CheckStatus.ok,
    totalS3Files=0,
    totalGitFiles=0,
    added=[],
    removed=[],
)


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
    github_missing_tag = GitHubStatus(
        lastChecked="2026-01-01T00:00:00.000Z",
        head="main",
        branches={"main": "abc123"},
        tags={},
    )
    _write_fixtures(dd, github=github_missing_tag, s3_version=S3_VERSION_DOI_OK, s3_diff=S3_DIFF_OK)

    result = summarize_dataset("ds000001", "1.0.2", dd)

    assert result.checks.github.value == "error"
    assert result.status.value == "error"


def test_s3_blocked_403(tmp_path):
    """S3 inaccessible (403) -> s3Blocked=True, overall 'error'."""
    dd = tmp_path / "datasets" / "ds000001"
    s3_blocked = S3Version(
        schemaVersion="2.0.0",
        lastChecked="2026-01-01T00:00:00.000Z",
        accessible=False,
        httpStatus=403,
    )
    _write_fixtures(dd, github=GITHUB_OK, s3_version=s3_blocked)

    result = summarize_dataset("ds000001", "1.0.2", dd)

    assert result.s3Blocked is True
    assert result.checks.s3Version.value == "error"
    assert result.checks.s3Files.value == "error"
    assert result.status.value == "error"


def test_version_mismatch(tmp_path):
    """DOI version != latest -> s3Version 'version-mismatch'."""
    dd = tmp_path / "datasets" / "ds000001"
    s3_old = S3Version(
        schemaVersion="2.0.0",
        lastChecked="2026-01-01T00:00:00.000Z",
        accessible=True,
        extractedVersion="1.0.1",
        versionSource=VersionSource.doi,
    )
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
    s3_old = S3Version(
        schemaVersion="2.0.0",
        lastChecked="2026-01-01T00:00:00.000Z",
        accessible=True,
        extractedVersion="1.0.1",
        versionSource=VersionSource.doi,
    )
    _write_fixtures(dd, github=GITHUB_OK, s3_version=s3_old)
    # s3-diff absent -> s3Files pending

    result = summarize_dataset("ds000001", "1.0.2", dd)

    assert result.checks.github.value == "ok"
    assert result.checks.s3Version.value == "version-mismatch"
    assert result.checks.s3Files.value == "pending"
    # pending > version-mismatch > ok
    assert result.status.value == "pending"
