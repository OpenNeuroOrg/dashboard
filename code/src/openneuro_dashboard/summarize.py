"""Stage 6: Summarize check results."""

import re
from pathlib import Path

from .converter import dump_typed, load_typed, load_typed_safe
from .models import (
    AllDatasetsSummary,
    CheckResults,
    CheckStatus,
    CheckTimestamps,
    DatasetsRegistry,
    DatasetSummary,
    GitHubStatus,
    GithubIssueSubtype,
    IssueSubtypes,
    S3FileDiff,
    S3FilesIssueSubtype,
    S3Version,
    S3VersionIssueSubtype,
)
from .utils import format_timestamp


def _tag_generation(tag: str) -> tuple[int, str]:
    if re.match(r'\d+\.\d+\.\d+', tag):
        # Current, Version tag (e.g., "1.0.0")
        return (2, tag)
    if re.match(r'0\d{4}', tag):
        # Legacy tag (e.g., "01234")
        return (1, tag)
    if re.match(r'[0-9a-f]+', tag):
        # Checksum tag (e.g., "a1b2c3d")
        return (0, tag)
    raise ValueError(f"Unrecognized tag format: {tag}")


def summarize_dataset(dataset_id, latest_snapshot, dataset_dir) -> DatasetSummary:
    """Summarize check results for a single dataset."""
    github_check = CheckStatus.pending
    s3version_check = CheckStatus.pending
    s3files_check = CheckStatus.pending

    github_subtype: GithubIssueSubtype | None = None
    s3version_subtype: S3VersionIssueSubtype | None = None
    s3files_subtype: S3FilesIssueSubtype | None = None

    last_checked = CheckTimestamps()
    s3_blocked = False
    github_version: str | None = None
    s3_version_str: str | None = None
    s3_files_added: int | None = None
    s3_files_removed: int | None = None

    # GitHub check
    github = load_typed_safe(dataset_dir / "github.json", GitHubStatus)
    if github:
        last_checked.github = github.lastChecked

        if github.error:
            if github.error == "command-failed":
                github_check = CheckStatus.pending
            elif github.error == "repo-empty":
                github_check = CheckStatus.error
                github_subtype = GithubIssueSubtype.repo_empty
            else:  # repo-not-found or unknown
                github_check = CheckStatus.error
                github_subtype = GithubIssueSubtype.repo_not_found
        else:
            tags = github.tags

            if latest_snapshot not in tags:
                github_check = CheckStatus.error
                github_subtype = GithubIssueSubtype.tag_missing
            elif github.branches.get(github.head) != tags.get(latest_snapshot):
                github_check = CheckStatus.warning
                github_subtype = GithubIssueSubtype.head_mismatch
            else:
                github_check = CheckStatus.ok

            # Extract the version from github tags matching latest_snapshot
            if tags:
                try:
                    github_version = sorted(tags.keys(), key=_tag_generation, reverse=True)[0]
                except ValueError:
                    print(f"Warning: Unrecognized tag format in GitHub tags for dataset {dataset_id}. Tags: {tags}")

    # S3 version check
    s3_version = load_typed_safe(dataset_dir / "s3-version.json", S3Version)
    if not s3_version:
        s3version_check = CheckStatus.pending
        s3files_check = CheckStatus.pending
    elif not s3_version.accessible:
        s3version_check = CheckStatus.error
        s3files_check = CheckStatus.error
        s3_blocked = True
        s3version_subtype = S3VersionIssueSubtype.blocked
        last_checked.s3Version = s3_version.lastChecked
    else:
        s3_version_str = s3_version.extractedVersion

        if s3_version.versionSource == "doi":
            if s3_version.extractedVersion == latest_snapshot:
                s3version_check = CheckStatus.ok
            else:
                s3version_check = CheckStatus.version_mismatch
                s3version_subtype = S3VersionIssueSubtype.doi_mismatch
        else:
            s3version_check = CheckStatus.warning
            s3version_subtype = S3VersionIssueSubtype.no_doi
        last_checked.s3Version = s3_version.lastChecked

        s3_diff = load_typed_safe(dataset_dir / "s3-diff.json", S3FileDiff)
        if not s3_diff:
            s3files_check = CheckStatus.pending
        elif s3_diff.exportMissing:
            s3files_check = CheckStatus.error
            s3files_subtype = S3FilesIssueSubtype.export_missing
            last_checked.s3Files = s3_diff.checkedAt
            # Do NOT populate s3FilesAdded/Removed when exportMissing
        else:
            s3files_check = CheckStatus(s3_diff.status)
            last_checked.s3Files = s3_diff.checkedAt

            added = s3_diff.added
            removed = s3_diff.removed
            if added:
                s3_files_added = len(added)
                s3files_subtype = S3FilesIssueSubtype.files_missing
            if removed:
                s3_files_removed = len(removed)
                if s3files_subtype is None:
                    s3files_subtype = S3FilesIssueSubtype.files_orphaned

    checks = CheckResults(
        github=github_check,
        s3Version=s3version_check,
        s3Files=s3files_check,
    )

    status_priority = {
        CheckStatus.ok: 0,
        CheckStatus.warning: 1,
        CheckStatus.version_mismatch: 2,
        CheckStatus.error: 3,
        CheckStatus.pending: 4,
    }
    overall_status = max(
        [github_check, s3version_check, s3files_check],
        key=lambda s: status_priority[s],
    )

    # Build issue subtypes only if there are any
    issue_subtypes: IssueSubtypes | None = None
    if github_subtype or s3version_subtype or s3files_subtype:
        issue_subtypes = IssueSubtypes(
            github=github_subtype,
            s3Version=s3version_subtype,
            s3Files=s3files_subtype,
        )

    # Build timestamps only if any are set
    timestamps: CheckTimestamps | None = None
    if last_checked.github or last_checked.s3Version or last_checked.s3Files:
        timestamps = last_checked

    return DatasetSummary(
        id=dataset_id,
        status=overall_status,
        checks=checks,
        s3Blocked=True if s3_blocked else None,
        lastChecked=timestamps,
        s3FilesAdded=s3_files_added,
        s3FilesRemoved=s3_files_removed,
        issueSubtypes=issue_subtypes,
        githubVersion=github_version,
        s3Version=s3_version_str,
    )


def generate_summary(output_dir: Path):
    """Generate summary from all check files."""
    print("Generating summary...")
    registry = load_typed(output_dir / "datasets-registry.json", DatasetsRegistry)
    datasets_dict = registry.latestSnapshots

    datasets = []
    for dataset_id, latest_snapshot in datasets_dict.items():
        dataset_dir = output_dir / "datasets" / dataset_id
        summary = summarize_dataset(dataset_id, latest_snapshot, dataset_dir)
        datasets.append(summary)

    summary_doc = AllDatasetsSummary(
        lastUpdated=format_timestamp(),
        datasets=datasets,
    )
    dump_typed(output_dir / "all-datasets.json", summary_doc)

    # Print statistics
    status_counts: dict[CheckStatus, int] = {}
    s3_blocked_count = 0
    for ds in datasets:
        status_counts[ds.status] = status_counts.get(ds.status, 0) + 1
        if ds.s3Blocked:
            s3_blocked_count += 1

    print(f"\nSummary generation complete ({len(datasets)} datasets)")
    print("\nDataset status breakdown:")
    for status in [
        CheckStatus.ok,
        CheckStatus.warning,
        CheckStatus.version_mismatch,
        CheckStatus.error,
        CheckStatus.pending,
    ]:
        count = status_counts.get(status, 0)
        if count > 0:
            print(f"  {status.value}: {count}")
    if s3_blocked_count > 0:
        print(f"\nS3 blocked (403): {s3_blocked_count} datasets")
