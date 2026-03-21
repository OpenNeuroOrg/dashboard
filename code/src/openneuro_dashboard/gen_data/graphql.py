"""Stage 1: Generate simulated GraphQL fetch data.

Generates:
- data/datasets-registry.json
- data/datasets/{id}/snapshots.json
- data/datasets/{id}/snapshots/{tag}/metadata.json
"""

import random
from pathlib import Path

from ..converter import dump_typed
from ..models import DatasetsRegistry, SnapshotIndex, SnapshotMetadata
from .utils import (
    generate_dataset_id,
    generate_snapshots,
    get_latest_snapshot,
    random_datetime,
    random_sha,
)


def generate(output_dir: Path, num_datasets: int = 50, seed: int = None):
    """Generate simulated GraphQL data."""
    if seed is not None:
        random.seed(seed)

    print(f"Generating GraphQL data for {num_datasets} datasets...")

    latest_snapshots = {}
    timestamp = random_datetime(days_ago=1)

    for i in range(1, num_datasets + 1):
        dataset_id = generate_dataset_id(i)
        tags = generate_snapshots()
        latest = get_latest_snapshot(tags)

        latest_snapshots[dataset_id] = latest

        # Write snapshots.json
        dataset_dir = output_dir / "datasets" / dataset_id
        snapshots_index = SnapshotIndex(tags=tags)
        dump_typed(dataset_dir / "snapshots.json", snapshots_index)

        # Write metadata.json for each snapshot
        for idx, tag in enumerate(tags):
            # Older snapshots have older creation dates
            days_ago = (len(tags) - idx) * 180 + random.randint(0, 100)

            snapshot_dir = dataset_dir / "snapshots" / tag
            metadata = SnapshotMetadata(
                hexsha=random_sha(),
                created=random_datetime(days_ago=days_ago),
            )
            dump_typed(snapshot_dir / "metadata.json", metadata)

        if i % 100 == 0:
            print(f"  Processed {i}/{num_datasets}")

    # Write registry
    registry = DatasetsRegistry(
        lastChecked=timestamp,
        totalCount=num_datasets,
        latestSnapshots=latest_snapshots,
    )
    dump_typed(output_dir / "datasets-registry.json", registry)

    print(f"GraphQL data generation complete")
    print(f"  Registry: datasets-registry.json ({num_datasets} datasets)")
    print(f"  Per-dataset files: {num_datasets} snapshots.json files")
