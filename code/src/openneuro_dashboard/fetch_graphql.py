"""Fetch dataset information from OpenNeuro GraphQL API.

Writes:
- data/datasets-registry.json
- data/datasets/{id}/snapshots.json
- data/datasets/{id}/snapshots/{tag}/metadata.json
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from ondiagnostics.graphql import (
    GraphQLResponse,
    PageInfo,
    create_client,
    get_page,
)

from .converter import dump_typed, load_typed
from .models import DatasetsRegistry, SnapshotIndex, SnapshotMetadata
from .utils import format_timestamp


async def _fetch_pages(
    client,
    queue: asyncio.Queue,
    page_size: int,
    verbose: bool,
    max_datasets: int | None = None,
) -> None:
    """Fetch pages and put them in queue for processing.

    Parameters
    ----------
    client
        GQL client session.
    queue
        Queue to put pages into.
    page_size
        Number of datasets per page.
    verbose
        Enable verbose logging.
    max_datasets
        Stop after fetching this many datasets (approximate).
    """
    page_info = PageInfo()
    page_num = 0
    total_fetched = 0

    try:
        while page_info.hasNextPage:
            result: GraphQLResponse = await get_page(
                client, page_size, page_info.endCursor, include_snapshots=True
            )

            page_info = result.datasets.pageInfo
            page_num += 1
            total_fetched += len(result.datasets.edges)

            if verbose:
                print(
                    f"  Fetched page {page_num}: "
                    f"{len(result.datasets.edges)} datasets, "
                    f"hasNext={page_info.hasNextPage}"
                )

            await queue.put(result.datasets.edges)

            if max_datasets is not None and total_fetched >= max_datasets:
                break
    finally:
        # Signal completion
        await queue.put(None)
    if verbose:
        print(f"  Page fetching complete: {page_num} pages")


def _write_dataset_json(
    dataset_dir: Path,
    snapshots_data: list,
    latest_snapshot_tag: str,
    dry_run: bool,
) -> None:
    """Write snapshots.json and per-snapshot metadata.json for one dataset."""
    snapshots_index = SnapshotIndex(
        tags=[snap.tag for snap in snapshots_data],
    )
    if dry_run:
        print(f"  [dry-run] Would write {dataset_dir / 'snapshots.json'}")
    else:
        dump_typed(dataset_dir / "snapshots.json", snapshots_index)

    for snapshot in snapshots_data:
        snapshot_dir = dataset_dir / "snapshots" / snapshot.tag
        metadata = SnapshotMetadata(
            hexsha=snapshot.hexsha,
            created=snapshot.created,
        )
        if dry_run:
            print(f"  [dry-run] Would write {snapshot_dir / 'metadata.json'}")
        else:
            dump_typed(snapshot_dir / "metadata.json", metadata)


async def fetch_and_write(
    output_dir: Path,
    page_size: int = 100,
    prefetch: int = 2,
    dry_run: bool = False,
    verbose: bool = False,
    max_datasets: int | None = None,
) -> None:
    """Fetch from GraphQL and write files.

    Parameters
    ----------
    output_dir
        Directory to write data files.
    page_size
        Number of datasets per GraphQL page.
    prefetch
        Number of pages to prefetch.
    dry_run
        If True, don't write files.
    verbose
        If True, enable verbose logging.
    max_datasets
        Maximum number of datasets to fetch.
    """
    client = create_client()

    # Get total count
    first_page = await get_page(client, 0, None)
    total_count = first_page.datasets.pageInfo.count
    print(f"Starting fetch: {total_count} total datasets")

    # Setup queue and background fetcher
    queue: asyncio.Queue = asyncio.Queue(maxsize=prefetch)
    fetch_task = asyncio.create_task(
        _fetch_pages(client, queue, page_size, verbose, max_datasets)
    )

    # Track progress
    latest_snapshots: dict[str, str] = {}
    processed = 0
    timestamp = format_timestamp()

    try:
        # Process datasets as they arrive
        while True:
            edges = await queue.get()

            # None signals end of pages
            if edges is None:
                break

            for edge in edges:
                if edge is None:
                    continue
                if edge.node is None:
                    print("Warning: Null node in edge")
                    continue

                dataset = edge.node

                # Validate snapshots exist
                if not dataset.snapshots:
                    print(
                        f"Warning: Dataset {dataset.id} has no snapshots, "
                        f"skipping"
                    )
                    continue

                processed += 1

                if max_datasets is not None and processed > max_datasets:
                    break

                # Track latest snapshot for registry
                latest_snapshots[dataset.id] = dataset.latestSnapshot.tag

                # Write per-dataset files
                dataset_dir = output_dir / "datasets" / dataset.id
                _write_dataset_json(
                    dataset_dir,
                    dataset.snapshots,
                    dataset.latestSnapshot.tag,
                    dry_run,
                )

                # Progress logging every 100 datasets
                if processed % 100 == 0:
                    percent = 100 * processed / total_count
                    print(
                        f"Progress: {processed}/{total_count} ({percent:.1f}%)"
                    )

            if max_datasets is not None and processed >= max_datasets:
                break
    finally:
        # Ensure fetch task completes
        await fetch_task

    # Write registry
    registry = DatasetsRegistry(
        lastChecked=timestamp,
        totalCount=len(latest_snapshots),
        latestSnapshots=latest_snapshots,
    )

    registry_path = output_dir / "datasets-registry.json"
    if dry_run:
        print(f"  [dry-run] Would write {registry_path}")
    else:
        dump_typed(registry_path, registry)

    print(f"\nFetch complete: {processed} datasets processed")
    if not dry_run:
        print(f"  Registry written to: {registry_path}")


def validate_output(output_dir: Path) -> None:
    """Validate the output data for consistency."""
    print("\nValidating output...")

    registry_path = output_dir / "datasets-registry.json"
    if not registry_path.exists():
        print("  FAIL: datasets-registry.json not found")
        return

    registry = load_typed(registry_path, DatasetsRegistry)

    issues = []
    for dataset_id, latest_snapshot in registry.latestSnapshots.items():
        dataset_dir = output_dir / "datasets" / dataset_id

        snapshots_path = dataset_dir / "snapshots.json"
        if not snapshots_path.exists():
            issues.append(f"{dataset_id}: snapshots.json missing")
            continue

        snapshots = load_typed(snapshots_path, SnapshotIndex)

        if latest_snapshot not in snapshots.tags:
            issues.append(f"{dataset_id}: latest {latest_snapshot} not in tags")

        for tag in snapshots.tags:
            metadata_path = dataset_dir / "snapshots" / tag / "metadata.json"
            if not metadata_path.exists():
                issues.append(f"{dataset_id}: metadata.json missing for {tag}")

    if issues:
        print(f"\n  Issues found ({len(issues)}):")
        for issue in issues[:20]:
            print(f"    {issue}")
        if len(issues) > 20:
            print(f"    ... and {len(issues) - 20} more")
    else:
        print("  OK: No issues found")
