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

from ..converter import dump_typed, load_typed
from ..models import DatasetsRegistry, GitHubStatus, SnapshotIndex, SnapshotMetadata
from .utils import random_datetime, random_sha


def _generate_github_check(
    dataset_id: str,
    tags: list[str],
    snapshot_metadata: dict[str, SnapshotMetadata],
    scenario: str,
) -> GitHubStatus:
    """Generate github.json for a dataset."""
    latest_tag = tags[-1]
    latest_sha = snapshot_metadata[latest_tag].hexsha

    # Generate tag mapping using actual SHAs
    tag_mapping = {tag: snapshot_metadata[tag].hexsha for tag in tags}

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
            head_sha = snapshot_metadata[old_tag].hexsha
        else:
            head_sha = random_sha()

    branches = {
        "git-annex": random_sha(),
        "main": head_sha if head_branch == "main" else random_sha(),
        "master": head_sha if head_branch == "master" else random_sha(),
    }

    return GitHubStatus(
        lastChecked=random_datetime(days_ago=1),
        head=head_branch,
        branches=branches,
        tags=tag_mapping,
    )


def generate(output_dir: Path, seed: int = None):
    """Generate GitHub check data for all datasets."""
    if seed is not None:
        random.seed(seed)

    print("Generating GitHub check data...")

    # Load registry
    registry = load_typed(output_dir / "datasets-registry.json", DatasetsRegistry)
    datasets = registry.latestSnapshots

    for i, (dataset_id, latest_snapshot) in enumerate(datasets.items(), 1):
        dataset_dir = output_dir / "datasets" / dataset_id

        # Load snapshots
        snapshots_data = load_typed(dataset_dir / "snapshots.json", SnapshotIndex)
        tags = snapshots_data.tags

        # Load metadata for all snapshots
        snapshot_metadata: dict[str, SnapshotMetadata] = {}
        for tag in tags:
            metadata_path = dataset_dir / "snapshots" / tag / "metadata.json"
            snapshot_metadata[tag] = load_typed(metadata_path, SnapshotMetadata)

        # Determine scenario
        scenario = random.choices(
            ["healthy", "warning", "error", "repo_not_found", "repo_empty"],
            weights=[81, 10, 5, 2, 2],
        )[0]

        # Generate and write github.json
        if scenario in ("repo_not_found", "repo_empty"):
            error_value = "repo-not-found" if scenario == "repo_not_found" else "repo-empty"
            github_data = GitHubStatus(
                lastChecked=random_datetime(days_ago=1),
                head=None,
                branches={},
                tags={},
                error=error_value,
            )
        else:
            github_data = _generate_github_check(
                dataset_id, tags, snapshot_metadata, scenario
            )
        dump_typed(dataset_dir / "github.json", github_data)

        if i % 100 == 0:
            print(f"  Processed {i}/{len(datasets)}")

    print(f"GitHub check generation complete ({len(datasets)} datasets)")
