#!/usr/bin/env python3
"""
Stage 5: Simulate S3 file diff.

Reads:
- data/datasets-registry.json
- data/datasets/{id}/s3-version.json
- data/datasets/{id}/snapshots/{tag}/files.json

Writes:
- data/datasets/{id}/s3-diff.json (only if S3 version matches latest)
"""

import argparse
import random
from pathlib import Path

from utils import SCHEMA_VERSION, random_datetime, write_json, load_json


def compute_context(sorted_files: list[str], changed: set[str], radius: int = 3) -> list[str]:
    """Compute context files within `radius` positions of any changed file."""
    context = set()
    for i, f in enumerate(sorted_files):
        if f in changed:
            for j in range(max(0, i - radius), min(len(sorted_files), i + radius + 1)):
                neighbor = sorted_files[j]
                if neighbor not in changed:
                    context.add(neighbor)
    return sorted(context)


def generate_s3_diff(
    dataset_id: str, version: str, git_files: list[str], scenario: str
) -> dict:
    """Generate s3-diff.json in v1.1.0 format."""
    if scenario == "healthy":
        added = []
        removed = []
        total_s3 = len(git_files)
    elif scenario == "error":
        num_missing = random.randint(1, min(5, max(1, len(git_files) // 10)))
        added = sorted(random.sample(git_files, num_missing))
        removed = sorted([".DS_Store", "._sub-01_T1w.nii.gz", "Thumbs.db"][
            : random.randint(1, 3)
        ])
        total_s3 = len(git_files) - len(added) + len(removed)
    else:  # warning — treat as error in new schema (any diff = error)
        added = sorted([random.choice(git_files)])
        removed = []
        total_s3 = len(git_files) - 1

    changed = set(added) | set(removed)
    context = compute_context(git_files, changed) if changed else []

    return {
        "schemaVersion": SCHEMA_VERSION,
        "datasetId": dataset_id,
        "snapshotTag": version,
        "s3Version": version,
        "checkedAt": random_datetime(days_ago=1),
        "status": "ok" if not changed else "error",
        "exportMissing": False,
        "totalS3Files": total_s3,
        "totalGitFiles": len(git_files),
        "added": added,
        "removed": removed,
        "context": context,
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

        # Determine scenario
        scenario = random.choices(["healthy", "warning", "error"], weights=[85, 10, 5])[
            0
        ]

        # Generate and write s3-diff.json
        s3_diff = generate_s3_diff(dataset_id, latest_snapshot, files_data["files"], scenario)
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
