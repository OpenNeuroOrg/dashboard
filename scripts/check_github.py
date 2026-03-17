#!/usr/bin/env python3
"""
Check GitHub mirror status for all datasets.

Reads:
- data/datasets-registry.json
- data/datasets/{id}/snapshots.json
- data/datasets/{id}/snapshots/{tag}/metadata.json

Writes:
- data/datasets/{id}/github.json
"""

import argparse
import asyncio
from asyncio.subprocess import PIPE
from pathlib import Path
from datetime import datetime, UTC
from dataclasses import dataclass, field

from utils import SCHEMA_VERSION, write_json, load_json


@dataclass
class SubprocessResult:
    args: tuple[str, ...]
    returncode: int
    stdout: bytes = field(repr=False)
    stderr: bytes = field(repr=False)


async def git(*args: str) -> SubprocessResult:
    """Run a git command and return the exit code, stdout, and stderr."""
    args_tuple = ("git", *args)

    proc = await asyncio.create_subprocess_exec(*args_tuple, stdout=PIPE, stderr=PIPE)
    stdout, stderr = await proc.communicate()
    assert proc.returncode is not None
    return SubprocessResult(
        args=args_tuple, returncode=proc.returncode, stdout=stdout, stderr=stderr
    )


async def check_github_mirror(
    dataset_id: str, output_dir: Path, verbose: bool = False
) -> dict | None:
    """
    Check GitHub mirror status for a single dataset.

    Args:
        dataset_id: Dataset ID to check
        output_dir: Base output directory
        verbose: Enable verbose logging

    Returns:
        GitHub status dict or None if check failed
    """
    repo = f"https://github.com/OpenNeuroDatasets/{dataset_id}.git"

    # Run git ls-remote to get all refs
    result = await git("ls-remote", "--symref", repo)

    if result.returncode != 0:
        if b"Repository not found" in result.stderr:
            print(f"✗ {dataset_id}: Repository not found on GitHub")
        else:
            print(f"✗ {dataset_id}: git ls-remote failed: {result.stderr.decode()}")
        return None

    if not result.stdout.strip():
        print(f"✗ {dataset_id}: Empty response from git ls-remote")
        return None

    # Parse output
    lines = result.stdout.decode().strip().split("\n")

    head_ref = None
    branches = {}
    tags = {}

    for line in lines:
        parts = line.split()

        # Handle symref (HEAD)
        if parts[0] == "ref:":
            # Format: "ref: refs/heads/master	HEAD"
            ref_target = parts[1]  # e.g., "refs/heads/master"
            head_ref = ref_target.split("/")[-1]  # Extract "master"
            continue

        # Handle normal refs
        if len(parts) != 2:
            continue

        sha, ref = parts

        if ref == "HEAD":
            continue  # Skip HEAD SHA line (we got symref above)
        elif ref.startswith("refs/heads/"):
            branch_name = ref.replace("refs/heads/", "")
            branches[branch_name] = sha
        elif ref.startswith("refs/tags/"):
            tag_name = ref.replace("refs/tags/", "")
            tags[tag_name] = sha

    if head_ref is None:
        print(f"⚠ {dataset_id}: Could not determine HEAD ref")
        # Default to common values if not found
        if "master" in branches:
            head_ref = "master"
        elif "main" in branches:
            head_ref = "main"
        else:
            head_ref = "unknown"

    github_data = {
        "schemaVersion": SCHEMA_VERSION,
        "lastChecked": datetime.now(UTC).isoformat(),
        "head": head_ref,
        "branches": branches,
        "tags": tags,
    }

    if verbose:
        print(
            f"✓ {dataset_id}: {len(branches)} branches, {len(tags)} tags, HEAD={head_ref}"
        )

    return github_data


async def check_all_datasets(
    output_dir: Path, concurrency: int = 10, verbose: bool = False
) -> None:
    """
    Check GitHub mirrors for all datasets with concurrency control.

    Args:
        output_dir: Base output directory
        concurrency: Maximum number of concurrent git operations
        verbose: Enable verbose logging
    """
    print("Checking GitHub mirrors...")

    # Load registry
    registry = load_json(output_dir / "datasets-registry.json")
    datasets = list(registry["latestSnapshots"].keys())
    total = len(datasets)

    print(f"Found {total} datasets to check")

    # Semaphore to limit concurrency
    semaphore = asyncio.Semaphore(concurrency)

    async def check_with_semaphore(
        dataset_id: str, index: int
    ) -> tuple[str, dict | None]:
        """Check a dataset with semaphore for rate limiting."""
        async with semaphore:
            result = await check_github_mirror(dataset_id, output_dir, verbose)

            if not verbose and (index + 1) % 100 == 0:
                print(f"  Progress: {index + 1}/{total}")

            return dataset_id, result

    # Launch all checks concurrently (but semaphore limits actual parallelism)
    tasks = [
        check_with_semaphore(dataset_id, i) for i, dataset_id in enumerate(datasets)
    ]

    results = await asyncio.gather(*tasks)

    # Write results
    success_count = 0
    failed_count = 0

    for dataset_id, github_data in results:
        if github_data is None:
            failed_count += 1
            continue

        dataset_dir = output_dir / "datasets" / dataset_id
        write_json(dataset_dir / "github.json", github_data)
        success_count += 1

    print("\n✓ GitHub check complete")
    print(f"  Success: {success_count}/{total}")
    print(f"  Failed: {failed_count}/{total}")


async def validate_github_data(output_dir: Path, verbose: bool = False) -> None:
    """
    Validate GitHub data against expected snapshots.

    This checks that:
    - All expected tags exist on GitHub
    - HEAD points to the latest snapshot
    - Git SHAs match expected values
    """
    print("\nValidating GitHub data...")

    registry = load_json(output_dir / "datasets-registry.json")

    issues = {
        "missing_tags": [],
        "sha_mismatch": [],
        "head_mismatch": [],
    }

    for dataset_id, latest_snapshot in registry["latestSnapshots"].items():
        dataset_dir = output_dir / "datasets" / dataset_id

        # Load GitHub data
        github_path = dataset_dir / "github.json"
        if not github_path.exists():
            continue

        github_data = load_json(github_path)

        # Load snapshots
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


def main():
    parser = argparse.ArgumentParser(
        description="Check GitHub mirror status for OpenNeuro datasets"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data"),
        help="Output directory (default: data)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Maximum concurrent git operations (default: 10)",
    )
    parser.add_argument(
        "--validate", action="store_true", help="Validate results after checking"
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    async def run():
        await check_all_datasets(args.output_dir, args.concurrency, args.verbose)

        if args.validate:
            await validate_github_data(args.output_dir, args.verbose)

    asyncio.run(run())


if __name__ == "__main__":
    main()
