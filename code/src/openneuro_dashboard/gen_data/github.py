"""Stage 2: Generate simulated GitHub mirror check data.

Reads:
- data/datasets-registry.json
- data/datasets/{id}/snapshots.json
- data/datasets/{id}/snapshots/{tag}/metadata.json

Writes:
- data/datasets/{id}/github.json
"""

import random
from pathlib import Path

from ..utils import SCHEMA_VERSION, load_json, write_json
from .utils import random_datetime, random_sha


def _generate_github_check(
    dataset_id: str, tags: list[str], snapshot_metadata: dict[str, dict], scenario: str
) -> dict:
    """Generate github.json for a dataset."""
    latest_tag = tags[-1]
    latest_sha = snapshot_metadata[latest_tag]["hexsha"]

    # Generate tag mapping using actual SHAs
    tag_mapping = {tag: snapshot_metadata[tag]["hexsha"] for tag in tags}

    # For error scenario, remove a tag
    if scenario == "error" and len(tag_mapping) > 1:
        del tag_mapping[latest_tag]

    # Determine HEAD
    head_branch = random.choice(["master", "main"])

    # For healthy scenario, HEAD points to latest
    if scenario == "healthy":
        head_sha = latest_sha
    else:  # warning or error - HEAD is stale
        if len(tags) > 1:
            old_tag = tags[-2]
            head_sha = snapshot_metadata[old_tag]["hexsha"]
        else:
            head_sha = random_sha()

    branches = {
        "git-annex": random_sha(),
        "main": head_sha if head_branch == "main" else random_sha(),
        "master": head_sha if head_branch == "master" else random_sha(),
    }

    return {
        "schemaVersion": SCHEMA_VERSION,
        "lastChecked": random_datetime(days_ago=1),
        "head": head_branch,
        "branches": branches,
        "tags": tag_mapping,
    }


def generate(output_dir: Path, seed: int = None):
    """Generate GitHub check data for all datasets."""
    if seed is not None:
        random.seed(seed)

    print("Generating GitHub check data...")

    # Load registry
    registry = load_json(output_dir / "datasets-registry.json")
    datasets = registry["latestSnapshots"]

    for i, (dataset_id, latest_snapshot) in enumerate(datasets.items(), 1):
        dataset_dir = output_dir / "datasets" / dataset_id

        # Load snapshots
        snapshots_data = load_json(dataset_dir / "snapshots.json")
        tags = snapshots_data["tags"]

        # Load metadata for all snapshots
        snapshot_metadata = {}
        for tag in tags:
            metadata_path = dataset_dir / "snapshots" / tag / "metadata.json"
            snapshot_metadata[tag] = load_json(metadata_path)

        # Determine scenario
        scenario = random.choices(["healthy", "warning", "error"], weights=[85, 10, 5])[
            0
        ]

        # Generate and write github.json
        github_data = _generate_github_check(
            dataset_id, tags, snapshot_metadata, scenario
        )
        write_json(dataset_dir / "github.json", github_data)

        if i % 100 == 0:
            print(f"  Processed {i}/{len(datasets)}")

    print(f"GitHub check generation complete ({len(datasets)} datasets)")
