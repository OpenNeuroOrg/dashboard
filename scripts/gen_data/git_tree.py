#!/usr/bin/env python3
"""
Stage 4: Simulate git tree fetching.

Reads:
- data/datasets-registry.json
- data/datasets/{id}/snapshots/{tag}/metadata.json

Writes:
- data/datasets/{id}/snapshots/{tag}/files.json
"""

import argparse
import random
from pathlib import Path

from utils import SCHEMA_VERSION, generate_file_paths, write_json, load_json


def generate_git_files(output_dir: Path, dataset_size: str, seed: int = None):
    """Generate git file listings for all datasets."""
    if seed is not None:
        random.seed(seed)

    print(f"Generating git file listings ({dataset_size} size)...")

    # Load registry
    registry = load_json(output_dir / "datasets-registry.json")
    datasets = registry["latestSnapshots"]

    for i, (dataset_id, latest_snapshot) in enumerate(datasets.items(), 1):
        dataset_dir = output_dir / "datasets" / dataset_id

        # Only generate for latest snapshot to save space
        latest_dir = dataset_dir / "snapshots" / latest_snapshot
        metadata = load_json(latest_dir / "metadata.json")

        # Generate file list
        files = generate_file_paths(dataset_size)
        file_list = {
            "schemaVersion": SCHEMA_VERSION,
            "count": len(files),
            "files": files,
        }

        write_json(latest_dir / "files.json", file_list)

        if i % 100 == 0:
            print(f"  Processed {i}/{len(datasets)}")

    print(f"✓ Git file listing generation complete ({len(datasets)} datasets)")


def main():
    parser = argparse.ArgumentParser(description="Generate simulated git file listings")
    parser.add_argument("--output-dir", type=Path, default=Path("data"))
    parser.add_argument(
        "--dataset-size",
        choices=["small", "medium", "large", "xlarge"],
        default="medium",
    )
    parser.add_argument("--seed", type=int, help="Random seed for reproducibility")
    args = parser.parse_args()

    generate_git_files(args.output_dir, args.dataset_size, args.seed)


if __name__ == "__main__":
    main()
