"""Unit tests for openneuro_dashboard.check_s3_files (diff computation)."""

from __future__ import annotations

from unittest.mock import patch

from openneuro_dashboard.check_s3_files import compute_context, compute_diff
from openneuro_dashboard.models import CheckStatus, S3FileDiff


# ------------------------------------------------------------------
# compute_diff tests
# ------------------------------------------------------------------


@patch("openneuro_dashboard.check_s3_files.format_timestamp", return_value="2026-01-01T00:00:00.000Z")
def test_identical_sets(_ts):
    """Identical git and S3 file sets -> status 'ok', no diffs."""
    files = {"a.txt", "b.txt", "c.txt"}
    result = compute_diff("ds000001", "1.0.2", "1.0.2", files, files)

    assert isinstance(result, S3FileDiff)
    assert result.status == CheckStatus.ok
    assert result.added == []
    assert result.removed == []
    assert result.exportMissing is False


@patch("openneuro_dashboard.check_s3_files.format_timestamp", return_value="2026-01-01T00:00:00.000Z")
def test_added_files(_ts):
    """Files in git but not S3 appear in 'added'."""
    git_files = {"a.txt", "b.txt", "new.txt"}
    s3_files = {"a.txt", "b.txt"}
    result = compute_diff("ds000001", "1.0.2", "1.0.2", git_files, s3_files)

    assert result.status == CheckStatus.error
    assert "new.txt" in result.added
    assert result.removed == []


@patch("openneuro_dashboard.check_s3_files.format_timestamp", return_value="2026-01-01T00:00:00.000Z")
def test_removed_files(_ts):
    """Files in S3 but not git appear in 'removed'."""
    git_files = {"a.txt"}
    s3_files = {"a.txt", "old.txt"}
    result = compute_diff("ds000001", "1.0.2", "1.0.2", git_files, s3_files)

    assert result.status == CheckStatus.error
    assert "old.txt" in result.removed
    assert result.added == []


@patch("openneuro_dashboard.check_s3_files.format_timestamp", return_value="2026-01-01T00:00:00.000Z")
def test_export_missing(_ts):
    """S3 has zero files -> exportMissing=True."""
    git_files = {"a.txt", "b.txt"}
    s3_files: set[str] = set()
    result = compute_diff("ds000001", "1.0.2", "1.0.2", git_files, s3_files)

    assert result.exportMissing is True
    assert result.status == CheckStatus.error
    assert sorted(result.added) == ["a.txt", "b.txt"]


@patch("openneuro_dashboard.check_s3_files.format_timestamp", return_value="2026-01-01T00:00:00.000Z")
def test_both_empty(_ts):
    """Both sets empty -> status 'ok'."""
    result = compute_diff("ds000001", "1.0.2", "1.0.2", set(), set())

    assert result.status == CheckStatus.ok
    assert result.added == []
    assert result.removed == []
    # exportMissing is True because len(s3_files)==0
    assert result.exportMissing is True


# ------------------------------------------------------------------
# compute_context tests
# ------------------------------------------------------------------


def test_context_neighbours():
    """Changed files have +/-3 neighbours included as context."""
    sorted_files = [f"file_{i:02d}.txt" for i in range(10)]
    changed = {sorted_files[5]}  # file_05.txt

    context = compute_context(sorted_files, changed, radius=3)

    # Neighbours at indices 2,3,4 and 6,7,8 (not 5 itself)
    expected = [sorted_files[i] for i in (2, 3, 4, 6, 7, 8)]
    assert context == expected


def test_context_edge():
    """Changed file at index 0 only has right neighbours."""
    sorted_files = ["a.txt", "b.txt", "c.txt", "d.txt", "e.txt"]
    changed = {"a.txt"}

    context = compute_context(sorted_files, changed, radius=3)

    assert "a.txt" not in context
    assert context == ["b.txt", "c.txt", "d.txt"]


def test_context_no_changes():
    """No changed files -> empty context."""
    sorted_files = ["a.txt", "b.txt", "c.txt"]
    context = compute_context(sorted_files, set(), radius=3)
    assert context == []
