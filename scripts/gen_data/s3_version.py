#!/usr/bin/env python3
"""
Stage 3: Simulate S3 version checks.

Reads:
- data/datasets-registry.json

Writes:
- data/datasets/{id}/s3-version.json
"""

import argparse
import random
from pathlib import Path

from utils import SCHEMA_VERSION, random_datetime, write_json, load_json


def generate_s3_version_check(
    dataset_id: str, latest_snapshot: str, scenario: str
) -> dict:
    """Generate s3-version.json for a dataset."""
    actual_version = latest_snapshot

    # For version-mismatch scenario, use an older version
    if scenario == "version-mismatch":
        parts = latest_snapshot.split(".")
        if len(parts) == 3 and parts[2].isdigit() and int(parts[2]) > 0:
            parts[2] = str(int(parts[2]) - 1)
            actual_version = ".".join(parts)

    doi = f"10.18112/openneuro.{dataset_id}.v{actual_version}"

    return {
        "schemaVersion": SCHEMA_VERSION,
        "lastChecked": random_datetime(days_ago=1),
        "datasetDescriptionDOI": doi,
        "extractedVersion": actual_version,
    }


def generate_s3_version_checks(output_dir: Path, seed: int = None):
    """Generate S3 version check data for all datasets."""
    if seed is not None:
        random.seed(seed)

    print("Generating S3 version check data...")

    # Load registry
    registry = load_json(output_dir / "datasets-registry.json")
    datasets = registry["latestSnapshots"]

    for i, (dataset_id, latest_snapshot) in enumerate(datasets.items(), 1):
        dataset_dir = output_dir / "datasets" / dataset_id

        # Determine scenario
        scenario = random.choices(["healthy", "version-mismatch"], weights=[95, 5])[0]

        # Generate and write s3-version.json
        s3_version_data = generate_s3_version_check(
            dataset_id, latest_snapshot, scenario
        )
        write_json(dataset_dir / "s3-version.json", s3_version_data)

        if i % 100 == 0:
            print(f"  Processed {i}/{len(datasets)}")

    print(f"✓ S3 version check generation complete ({len(datasets)} datasets)")


def main():
    parser = argparse.ArgumentParser(
        description="Generate simulated S3 version check data"
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data"))
    parser.add_argument("--seed", type=int, help="Random seed for reproducibility")
    args = parser.parse_args()

    generate_s3_version_checks(args.output_dir, args.seed)


if __name__ == "__main__":
    main()
