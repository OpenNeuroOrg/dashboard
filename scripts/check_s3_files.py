#!/usr/bin/env python3
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "aioboto3>=13.4.0",
#     "pygit2>=1.17.0",
# ]
# ///
"""
Stage 4: Compare S3 file listing against git tree.

Manages a bare repo cache for git trees. Clones on cache miss,
fetches missing tags from existing repos.

Reads:
- data/datasets-registry.json
- data/datasets/{id}/s3-version.json
- data/datasets/{id}/github.json

Writes:
- data/datasets/{id}/s3-diff.json
"""

import argparse
import asyncio
from asyncio.subprocess import PIPE
from dataclasses import dataclass, field
from datetime import datetime, UTC, timedelta
from pathlib import Path

import aioboto3
import pygit2
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig

from utils import SCHEMA_VERSION, write_json, load_json, format_timestamp


S3_BUCKET = "openneuro.org"
S3_REGION = "us-east-1"
GITHUB_BASE = "https://github.com/OpenNeuroDatasets"
STALE_DAYS = 7


@dataclass
class SubprocessResult:
    args: tuple[str, ...]
    returncode: int
    stdout: bytes = field(repr=False)
    stderr: bytes = field(repr=False)


async def git(*args: str, cwd: Path | None = None) -> SubprocessResult:
    """Run a git command and return the result."""
    args_tuple = ("git", *args)
    proc = await asyncio.create_subprocess_exec(
        *args_tuple, stdout=PIPE, stderr=PIPE, cwd=cwd
    )
    stdout, stderr = await proc.communicate()
    assert proc.returncode is not None
    return SubprocessResult(
        args=args_tuple, returncode=proc.returncode, stdout=stdout, stderr=stderr
    )


def load_json_safe(path: Path) -> dict | None:
    """Load JSON file if it exists, return None otherwise."""
    if path.exists():
        return load_json(path)
    return None


def is_eligible(
    dataset_id: str,
    s3_version: dict | None,
    github: dict | None,
    existing_diff: dict | None,
) -> bool:
    """Check preconditions for running the S3 file diff."""
    if not s3_version:
        return False

    # Skip if S3 is blocked (403) or has no extracted version
    if not s3_version.get("accessible", True):
        return False
    extracted = s3_version.get("extractedVersion")
    if not extracted:
        return False

    # Skip if tag not available on GitHub
    if not github:
        return False
    if extracted not in github.get("tags", {}):
        return False

    # Incremental skip logic
    if existing_diff and existing_diff.get("s3Version") == extracted:
        checked_at = existing_diff.get("checkedAt", "")
        try:
            checked_time = datetime.fromisoformat(checked_at.replace("Z", "+00:00"))
            age = datetime.now(UTC) - checked_time
            if age < timedelta(days=STALE_DAYS):
                return False
            # Older than STALE_DAYS: re-run regardless
        except (ValueError, TypeError):
            pass  # Can't parse timestamp — re-run

    return True


async def ensure_tag_cached(
    dataset_id: str, tag: str, cache_dir: Path, semaphore: asyncio.Semaphore
) -> bool:
    """Ensure the bare repo cache has the needed tag. Returns True on success."""
    repo_path = cache_dir / f"{dataset_id}.git"

    async with semaphore:
        if not repo_path.exists():
            # Clone bare repo
            result = await git(
                "clone", "--bare", "--filter=blob:none", "--depth=1",
                "--branch", tag,
                f"{GITHUB_BASE}/{dataset_id}.git",
                str(repo_path),
            )
            if result.returncode != 0:
                print(f"✗ {dataset_id}: git clone failed: {result.stderr.decode().strip()}")
                return False
            return True

        # Repo exists — check if tag is present
        try:
            repo = await asyncio.to_thread(pygit2.Repository, str(repo_path))
            ref_name = f"refs/tags/{tag}"
            has_tag = await asyncio.to_thread(lambda: ref_name in repo.references)
            if has_tag:
                return True
        except Exception as e:
            print(f"⚠ {dataset_id}: Error checking repo: {e}")

        # Fetch the missing tag
        result = await git(
            "fetch", "--refetch", "--filter=blob:none", "--depth=1",
            "origin", "tag", tag,
            cwd=repo_path,
        )
        if result.returncode != 0:
            print(f"✗ {dataset_id}: git fetch failed: {result.stderr.decode().strip()}")
            return False

        return True


def walk_git_tree(repo_path: Path, tag: str) -> set[str]:
    """Walk the git tree at the given tag and return all file paths as a set."""
    repo = pygit2.Repository(str(repo_path))
    ref = repo.references.get(f"refs/tags/{tag}")
    if ref is None:
        raise ValueError(f"Tag {tag} not found in {repo_path}")

    commit = ref.peel(pygit2.Commit)
    tree = commit.tree

    files = set()

    def _walk(tree_obj, prefix=""):
        for entry in tree_obj:
            path = entry.name if not prefix else f"{prefix}/{entry.name}"
            if entry.type == pygit2.GIT_OBJECT_TREE:
                _walk(repo.get(entry.id), path)
            else:
                files.add(path)

    _walk(tree)
    return files


async def list_s3_files(
    dataset_id: str, semaphore: asyncio.Semaphore
) -> set[str] | None:
    """List all S3 objects under the dataset prefix. Returns file paths as a set."""
    prefix = f"{dataset_id}/"

    async with semaphore:
        session = aioboto3.Session()
        async with session.client(
            "s3",
            region_name=S3_REGION,
            config=BotoConfig(signature_version=UNSIGNED),
        ) as s3:
            files = set()
            paginator = s3.get_paginator("list_objects_v2")
            try:
                async for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
                    for obj in page.get("Contents", []):
                        key = obj["Key"]
                        # Strip the dataset prefix
                        rel_path = key[len(prefix):]
                        if rel_path:
                            files.add(rel_path)
            except Exception as e:
                print(f"✗ {dataset_id}: S3 listing failed: {e}")
                return None

            return files


def compute_context(sorted_files: list[str], changed: set[str], radius: int = 3) -> list[str]:
    """Compute context files within `radius` sorted positions of any changed file."""
    context = set()
    for i, f in enumerate(sorted_files):
        if f in changed:
            for j in range(max(0, i - radius), min(len(sorted_files), i + radius + 1)):
                neighbor = sorted_files[j]
                if neighbor not in changed:
                    context.add(neighbor)
    return sorted(context)


def compute_diff(
    dataset_id: str, tag: str, s3_version: str,
    git_files: set[str], s3_files: set[str]
) -> dict:
    """Compute the S3 file diff and return the s3-diff.json data."""
    added = sorted(git_files - s3_files)
    removed = sorted(s3_files - git_files)

    changed = set(added) | set(removed)
    sorted_git = sorted(git_files)
    context = compute_context(sorted_git, changed) if changed else []

    export_missing = len(s3_files) == 0
    status = "ok" if not added and not removed else "error"

    return {
        "schemaVersion": SCHEMA_VERSION,
        "datasetId": dataset_id,
        "snapshotTag": tag,
        "s3Version": s3_version,
        "checkedAt": format_timestamp(),
        "status": status,
        "exportMissing": export_missing,
        "totalS3Files": len(s3_files),
        "totalGitFiles": len(git_files),
        "added": added,
        "removed": removed,
        "context": context,
    }


async def process_dataset(
    dataset_id: str,
    s3_version_data: dict,
    output_dir: Path,
    cache_dir: Path,
    git_semaphore: asyncio.Semaphore,
    s3_semaphore: asyncio.Semaphore,
    verbose: bool = False,
) -> bool:
    """Process a single dataset: ensure cache, get git tree, list S3, compute diff."""
    tag = s3_version_data["extractedVersion"]

    # Step 1: Ensure git cache
    if not await ensure_tag_cached(dataset_id, tag, cache_dir, git_semaphore):
        return False

    # Step 2: Get git file tree (blocking I/O in thread)
    repo_path = cache_dir / f"{dataset_id}.git"
    try:
        git_files = await asyncio.to_thread(walk_git_tree, repo_path, tag)
    except Exception as e:
        print(f"✗ {dataset_id}: Failed to walk git tree: {e}")
        return False

    # Step 3: List S3 files
    s3_files = await list_s3_files(dataset_id, s3_semaphore)
    if s3_files is None:
        return False

    # Step 4: Compute diff
    diff = compute_diff(dataset_id, tag, tag, git_files, s3_files)

    # Step 5: Write result
    dataset_dir = output_dir / "datasets" / dataset_id
    write_json(dataset_dir / "s3-diff.json", diff)

    if verbose:
        added_count = len(diff["added"])
        removed_count = len(diff["removed"])
        if added_count or removed_count:
            print(f"✗ {dataset_id}: +{added_count} -{removed_count} ({diff['status']})")
        else:
            print(f"✓ {dataset_id}: ok")

    return True


async def check_all_datasets(
    output_dir: Path,
    cache_dir: Path,
    git_concurrency: int = 10,
    s3_concurrency: int = 20,
    verbose: bool = False,
) -> None:
    """Check S3 files for all eligible datasets."""
    print("Checking S3 files...")

    cache_dir.mkdir(parents=True, exist_ok=True)

    # Load registry
    registry = load_json(output_dir / "datasets-registry.json")
    datasets = registry["latestSnapshots"]
    total = len(datasets)

    # Filter eligible datasets
    eligible = []
    for dataset_id in datasets:
        dataset_dir = output_dir / "datasets" / dataset_id
        s3_version = load_json_safe(dataset_dir / "s3-version.json")
        github = load_json_safe(dataset_dir / "github.json")
        existing_diff = load_json_safe(dataset_dir / "s3-diff.json")

        if is_eligible(dataset_id, s3_version, github, existing_diff):
            eligible.append((dataset_id, s3_version))

    print(f"Found {len(eligible)}/{total} eligible datasets")

    if not eligible:
        print("Nothing to do.")
        return

    git_semaphore = asyncio.Semaphore(git_concurrency)
    s3_semaphore = asyncio.Semaphore(s3_concurrency)

    success = 0
    failed = 0

    # Process datasets concurrently
    async def process_with_progress(dataset_id, s3_version_data, index):
        nonlocal success, failed
        ok = await process_dataset(
            dataset_id, s3_version_data, output_dir, cache_dir,
            git_semaphore, s3_semaphore, verbose,
        )
        if ok:
            success += 1
        else:
            failed += 1

        if not verbose and (index + 1) % 50 == 0:
            print(f"  Progress: {index + 1}/{len(eligible)}")

    tasks = [
        process_with_progress(dataset_id, s3_version, i)
        for i, (dataset_id, s3_version) in enumerate(eligible)
    ]
    await asyncio.gather(*tasks)

    print(f"\n✓ S3 file check complete")
    print(f"  Success: {success}/{len(eligible)}")
    print(f"  Failed: {failed}/{len(eligible)}")
    print(f"  Skipped: {total - len(eligible)}/{total}")


def main():
    parser = argparse.ArgumentParser(
        description="Check S3 files against git tree for OpenNeuro datasets"
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data"),
        help="Output directory (default: data)",
    )
    parser.add_argument(
        "--cache-dir", type=Path,
        default=Path.home() / ".cache" / "openneuro-dashboard" / "repos",
        help="Bare repo cache directory",
    )
    parser.add_argument(
        "--git-concurrency", type=int, default=10,
        help="Max concurrent git operations (default: 10)",
    )
    parser.add_argument(
        "--s3-concurrency", type=int, default=20,
        help="Max concurrent S3 requests (default: 20)",
    )
    parser.add_argument(
        "--validate", action="store_true",
        help="Validate results after checking",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    asyncio.run(
        check_all_datasets(
            args.output_dir, args.cache_dir,
            args.git_concurrency, args.s3_concurrency,
            args.verbose,
        )
    )


if __name__ == "__main__":
    main()
