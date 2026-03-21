"""Unit tests for issue-subtype derivation in summarize_dataset().

Each test loads real fixture data from tests/fixtures/datasets/ and verifies
that summarize_dataset() returns a DatasetSummary with the expected checks,
issueSubtypes, and derived fields.
"""

from openneuro_dashboard.models import (
    CheckStatus,
    DatasetSummary,
    GithubIssueSubtype,
    S3FilesIssueSubtype,
    S3VersionIssueSubtype,
)
from openneuro_dashboard.summarize import summarize_dataset


def test_ds000001_healthy(datasets_dir):
    """ds000001: all checks pass, no issue subtypes."""
    result = summarize_dataset("ds000001", "1.0.2", datasets_dir / "ds000001")

    assert isinstance(result, DatasetSummary)
    assert result.status == CheckStatus.ok
    assert result.checks.github == CheckStatus.ok
    assert result.checks.s3Version == CheckStatus.ok
    assert result.checks.s3Files == CheckStatus.ok
    assert result.issueSubtypes is None or (
        result.issueSubtypes.github is None
        and result.issueSubtypes.s3Version is None
        and result.issueSubtypes.s3Files is None
    )


def test_ds000002_blocked(datasets_dir):
    """ds000002: S3 blocked (403) -> s3Blocked, blocked subtype."""
    result = summarize_dataset("ds000002", "2.0.0", datasets_dir / "ds000002")

    assert isinstance(result, DatasetSummary)
    assert result.s3Blocked is True
    assert result.checks.s3Version == CheckStatus.error
    assert result.issueSubtypes is not None
    assert result.issueSubtypes.s3Version == S3VersionIssueSubtype.blocked


def test_ds000003_version_mismatch(datasets_dir):
    """ds000003: DOI version != latest -> version-mismatch + doi-mismatch."""
    result = summarize_dataset("ds000003", "1.1.0", datasets_dir / "ds000003")

    assert isinstance(result, DatasetSummary)
    assert result.checks.s3Version == CheckStatus.version_mismatch
    assert result.issueSubtypes is not None
    assert result.issueSubtypes.s3Version == S3VersionIssueSubtype.doi_mismatch


def test_ds000004_head_mismatch(datasets_dir):
    """ds000004: HEAD SHA != tag SHA -> warning + head-mismatch."""
    result = summarize_dataset("ds000004", "1.0.0", datasets_dir / "ds000004")

    assert isinstance(result, DatasetSummary)
    assert result.checks.github == CheckStatus.warning
    assert result.issueSubtypes is not None
    assert result.issueSubtypes.github == GithubIssueSubtype.head_mismatch


def test_ds000005_export_missing(datasets_dir):
    """ds000005: exportMissing -> s3Files error + export-missing subtype."""
    result = summarize_dataset("ds000005", "1.0.0", datasets_dir / "ds000005")

    assert isinstance(result, DatasetSummary)
    assert result.checks.s3Files == CheckStatus.error
    assert result.issueSubtypes is not None
    assert result.issueSubtypes.s3Files == S3FilesIssueSubtype.export_missing
    # When exportMissing, s3FilesAdded should NOT be populated
    assert result.s3FilesAdded is None


def test_ds000005_tag_missing_for_unknown_snapshot(datasets_dir):
    """ds000005 with latest_snapshot=2.0.0 (not in tags) -> tag-missing."""
    result = summarize_dataset("ds000005", "2.0.0", datasets_dir / "ds000005")

    assert isinstance(result, DatasetSummary)
    assert result.checks.github == CheckStatus.error
    assert result.issueSubtypes is not None
    assert result.issueSubtypes.github == GithubIssueSubtype.tag_missing
