"""Check GitHub mirror status for all datasets.

Reads:
- data/datasets-registry.json
- data/datasets/{id}/snapshots.json
- data/datasets/{id}/snapshots/{tag}/metadata.json

Writes:
- data/datasets/{id}/github.json
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from ondiagnostics.tasks.git import list_refs

from .utils import SCHEMA_VERSION, format_timestamp, load_json, write_json


async def check_github_mirror(
    dataset_id: str, output_dir: Path, verbose: bool = False
) -> dict | None:
    """Check GitHub mirror status for a single dataset.

    Parameters
    ----------
    dataset_id
        Dataset ID to check.
    output_dir
        Base output directory.
    verbose
        Enable verbose logging.

    Returns
    -------
    dict or None
        GitHub status dict, or None if the check failed.
    """
    repo_url = f"https://github.com/OpenNeuroDatasets/{dataset_id}.git"

    refs = await list_refs(repo_url)
    if refs is None:
        print(f"  {dataset_id}: failed to list refs")
        return None

    head = refs.head
    if head is None:
        # Fallback to common defaults
        if "master" in refs.branches:
            head = "master"
        elif "main" in refs.branches:
            head = "main"
        else:
            head = "unknown"

    github_data = {
        "schemaVersion": SCHEMA_VERSION,
        "lastChecked": format_timestamp(),
        "head": head,
        "branches": refs.branches,
        "tags": refs.tags,
    }

    if verbose:
        print(
            f"  {dataset_id}: "
            f"{len(refs.branches)} branches, "
            f"{len(refs.tags)} tags, "
            f"HEAD={head}"
        )

    return github_data


async def check_all_datasets(
    output_dir: Path, concurrency: int = 10, verbose: bool = False
) -> None:
    """Check GitHub mirrors for all datasets.

    Parameters
    ----------
    output_dir
        Base output directory.
    concurrency
        Maximum number of concurrent git operations.
    verbose
        Enable verbose logging.
    """
    print("Checking GitHub mirrors...")

    registry = load_json(output_dir / "datasets-registry.json")
    datasets = list(registry["latestSnapshots"].keys())
    total = len(datasets)

    print(f"Found {total} datasets to check")

    semaphore = asyncio.Semaphore(concurrency)

    async def check_with_semaphore(
        dataset_id: str, index: int
    ) -> tuple[str, dict | None]:
        async with semaphore:
            result = await check_github_mirror(dataset_id, output_dir, verbose)

            if not verbose and (index + 1) % 100 == 0:
                print(f"  Progress: {index + 1}/{total}")

            return dataset_id, result

    tasks = [
        check_with_semaphore(dataset_id, i) for i, dataset_id in enumerate(datasets)
    ]

    results = await asyncio.gather(*tasks)

    success_count = 0
    failed_count = 0

    for dataset_id, github_data in results:
        if github_data is None:
            failed_count += 1
            continue

        dataset_dir = output_dir / "datasets" / dataset_id
        write_json(dataset_dir / "github.json", github_data)
        success_count += 1

    print("\nGitHub check complete")
    print(f"  Success: {success_count}/{total}")
    print(f"  Failed: {failed_count}/{total}")


async def validate_github_data(output_dir: Path, verbose: bool = False) -> None:
    """Validate GitHub data against expected snapshots.

    Checks that:
    - All expected tags exist on GitHub
    - HEAD points to the latest snapshot
    - Git SHAs match expected values

    Parameters
    ----------
    output_dir
        Base output directory.
    verbose
        Enable verbose logging.
    """
    print("\nValidating GitHub data...")

    registry = load_json(output_dir / "datasets-registry.json")

    issues: dict[str, list[str]] = {
        "missing_tags": [],
        "sha_mismatch": [],
        "head_mismatch": [],
    }

    for dataset_id, latest_snapshot in registry["latestSnapshots"].items():
        dataset_dir = output_dir / "datasets" / dataset_id

        github_path = dataset_dir / "github.json"
        if not github_path.exists():
            continue

        github_data = load_json(github_path)

        snapshots_data = load_json(dataset_dir / "snapshots.json")
        tags = snapshots_data["tags"]

        # Check all tags exist
        for tag in tags:
            if tag not in github_data["tags"]:
                issues["missing_tags"].append(f"{dataset_id}: {tag}")

        # Check latest tag SHA matches
        if latest_snapshot in github_data["tags"]:
            metadata_path = (
                dataset_dir / "snapshots" / latest_snapshot / "metadata.json"
            )
            metadata = load_json(metadata_path)

            if github_data["tags"][latest_snapshot] != metadata["hexsha"]:
                issues["sha_mismatch"].append(
                    f"{dataset_id}: {latest_snapshot} "
                    f"(GitHub: {github_data['tags'][latest_snapshot][:7]}, "
                    f"Expected: {metadata['hexsha'][:7]})"
                )

        # Check HEAD points to latest
        head_branch = github_data["head"]
        if head_branch in github_data["branches"]:
            if latest_snapshot in github_data["tags"]:
                expected_sha = github_data["tags"][latest_snapshot]
                actual_sha = github_data["branches"][head_branch]

                if actual_sha != expected_sha:
                    issues["head_mismatch"].append(
                        f"{dataset_id}: HEAD ({head_branch}) -> {actual_sha[:7]}, "
                        f"expected {expected_sha[:7]}"
                    )

    # Print validation results
    if verbose or any(len(v) > 0 for v in issues.values()):
        print("\nValidation results:")

        if issues["missing_tags"]:
            print(f"\n  Missing tags ({len(issues['missing_tags'])}):")
            for issue in issues["missing_tags"][:10]:
                print(f"    {issue}")
            if len(issues["missing_tags"]) > 10:
                print(f"    ... and {len(issues['missing_tags']) - 10} more")

        if issues["sha_mismatch"]:
            print(f"\n  SHA mismatches ({len(issues['sha_mismatch'])}):")
            for issue in issues["sha_mismatch"][:10]:
                print(f"    {issue}")
            if len(issues["sha_mismatch"]) > 10:
                print(f"    ... and {len(issues['sha_mismatch']) - 10} more")

        if issues["head_mismatch"]:
            print(f"\n  HEAD mismatches ({len(issues['head_mismatch'])}):")
            for issue in issues["head_mismatch"][:10]:
                print(f"    {issue}")
            if len(issues["head_mismatch"]) > 10:
                print(f"    ... and {len(issues['head_mismatch']) - 10} more")

        if not any(len(v) > 0 for v in issues.values()):
            print("  No issues found!")
