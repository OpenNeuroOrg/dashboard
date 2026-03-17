#!/usr/bin/env python3
"""
Stage 5: Simulate S3 file diff.

Reads:
- data/datasets-registry.json
- data/datasets/{id}/s3-version.json
- data/datasets/{id}/snapshots/{tag}/files.json
- data/datasets/{id}/snapshots/{tag}/metadata.json

Writes:
- data/datasets/{id}/s3-diff.json (only if S3 version matches latest)
"""

import argparse
import random
from pathlib import Path

from utils import SCHEMA_VERSION, random_datetime, write_json, load_json


def generate_s3_diff(
    version: str, git_sha: str, git_files: list[str], scenario: str
) -> dict:
    """Generate s3-diff.json based on git files and scenario."""

    if scenario == "healthy":
        in_git_only = []
        in_s3_only = []
        total_in_s3 = len(git_files)
    elif scenario == "error":
        # S3 is missing some files and has extra files
        num_missing = random.randint(1, min(5, len(git_files) // 10))
        in_git_only = random.sample(git_files, num_missing)
        in_s3_only = [".DS_Store", "._sub-01_T1w.nii.gz", "Thumbs.db"][
            : random.randint(1, 3)
        ]
        total_in_s3 = len(git_files) - len(in_git_only) + len(in_s3_only)
    else:  # warning
        # S3 is missing one file
        in_git_only = [random.choice(git_files)]
        in_s3_only = []
        total_in_s3 = len(git_files) - 1

    in_both = len(git_files) - len(in_git_only)

    return {
        "schemaVersion": SCHEMA_VERSION,
        "lastChecked": random_datetime(days_ago=1),
        "s3Version": version,
        "gitHexsha": git_sha,
        "summary": {
            "totalInGit": len(git_files),
            "totalInS3": total_in_s3,
            "inBoth": in_both,
            "inGitOnly": len(in_git_only),
            "inS3Only": len(in_s3_only),
        },
        "inGitOnly": in_git_only,
        "inS3Only": in_s3_only,
    }


def generate_s3_diffs(output_dir: Path, seed: int = None):
    """Generate S3 diff data for all datasets where version matches."""
    if seed is not None:
        random.seed(seed)

    print("Generating S3 file diff data...")

    # Load registry
    registry = load_json(output_dir / "datasets-registry.json")
    datasets = registry["latestSnapshots"]

    generated = 0
    skipped = 0

    for i, (dataset_id, latest_snapshot) in enumerate(datasets.items(), 1):
        dataset_dir = output_dir / "datasets" / dataset_id

        # Load S3 version
        s3_version_path = dataset_dir / "s3-version.json"
        if not s3_version_path.exists():
            print(f"⚠ {dataset_id}: s3-version.json not found, skipping")
            skipped += 1
            continue

        s3_version = load_json(s3_version_path)

        # Only generate diff if versions match
        if s3_version["extractedVersion"] != latest_snapshot:
            skipped += 1
            continue

        # Load git files
        files_path = dataset_dir / "snapshots" / latest_snapshot / "files.json"
        if not files_path.exists():
            print(f"⚠ {dataset_id}: files.json not found, skipping")
            skipped += 1
            continue

        files_data = load_json(files_path)

        # Load metadata for git SHA
        metadata_path = dataset_dir / "snapshots" / latest_snapshot / "metadata.json"
        metadata = load_json(metadata_path)

        # Determine scenario
        scenario = random.choices(["healthy", "warning", "error"], weights=[85, 10, 5])[
            0
        ]

        # Generate and write s3-diff.json
        s3_diff = generate_s3_diff(
            latest_snapshot, metadata["hexsha"], files_data["files"], scenario
        )
        write_json(dataset_dir / "s3-diff.json", s3_diff)
        generated += 1

        if i % 100 == 0:
            print(f"  Processed {i}/{len(datasets)}")

    print("✓ S3 diff generation complete")
    print(f"  Generated: {generated} diffs")
    print(f"  Skipped: {skipped} (version mismatch or missing data)")


def main():
    parser = argparse.ArgumentParser(description="Generate simulated S3 diff data")
    parser.add_argument("--output-dir", type=Path, default=Path("data"))
    parser.add_argument("--seed", type=int, help="Random seed for reproducibility")
    args = parser.parse_args()

    generate_s3_diffs(args.output_dir, args.seed)


if __name__ == "__main__":
    main()
