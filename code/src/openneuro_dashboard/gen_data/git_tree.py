"""Stage 4: Generate simulated git tree file listings.

Reads:
- data/datasets-registry.json
- data/datasets/{id}/snapshots/{tag}/metadata.json

Writes:
- data/datasets/{id}/snapshots/{tag}/files.json
"""

import random
from pathlib import Path

from ..converter import dump_typed, load_typed
from ..models import DatasetsRegistry, FileList, SnapshotMetadata
from .utils import generate_file_paths


def generate(output_dir: Path, dataset_size: str = "medium", seed: int = None):
    """Generate git file listings for all datasets."""
    if seed is not None:
        random.seed(seed)

    print(f"Generating git file listings ({dataset_size} size)...")

    # Load registry
    registry = load_typed(output_dir / "datasets-registry.json", DatasetsRegistry)
    datasets = registry.latestSnapshots

    for i, (dataset_id, latest_snapshot) in enumerate(datasets.items(), 1):
        dataset_dir = output_dir / "datasets" / dataset_id

        # Only generate for latest snapshot to save space
        latest_dir = dataset_dir / "snapshots" / latest_snapshot
        load_typed(latest_dir / "metadata.json", SnapshotMetadata)

        # Generate file list
        files = generate_file_paths(dataset_size)
        file_list = FileList(count=len(files), files=files)

        dump_typed(latest_dir / "files.json", file_list)

        if i % 100 == 0:
            print(f"  Processed {i}/{len(datasets)}")

    print(f"Git file listing generation complete ({len(datasets)} datasets)")
