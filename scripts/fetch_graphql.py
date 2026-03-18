#!/usr/bin/env python3
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "cattrs>=26.1.0",
#     "gql[httpx]>=4.0.0",
#     "httpx>=0.28.1",
#     "stamina>=25.2.0",
# ]
# ///
"""
Fetch dataset information from OpenNeuro GraphQL API.

Writes:
- data/datasets-registry.json
- data/datasets/{id}/snapshots.json
- data/datasets/{id}/snapshots/{tag}/metadata.json
"""

import argparse
import asyncio
import json
from pathlib import Path
from datetime import datetime, UTC
from dataclasses import dataclass

import cattrs
import httpx
import stamina
import gql
from gql.transport.httpx import HTTPXAsyncTransport
from gql.transport.exceptions import TransportQueryError

from utils import SCHEMA_VERSION, format_timestamp

ENDPOINT = "https://openneuro.org/crn/graphql"

converter = cattrs.Converter()


# GraphQL data structures
@dataclass
class Snapshot:
    """Snapshot metadata from GraphQL."""

    tag: str
    hexsha: str
    created: str


@dataclass
class DatasetNode:
    """Dataset node from GraphQL response."""

    id: str
    latestSnapshot: Snapshot
    snapshots: list[Snapshot]


@dataclass
class DatasetEdge:
    """Edge wrapper for dataset node."""

    node: DatasetNode | None


@dataclass
class PageInfo:
    """Pagination information."""

    hasNextPage: bool = True
    endCursor: str | None = None
    count: int = 0


@dataclass
class DatasetsResponse:
    """Response containing datasets and pagination info."""

    edges: list[DatasetEdge | None]
    pageInfo: PageInfo


@dataclass
class GraphQLResponse:
    """Top-level GraphQL response."""

    datasets: DatasetsResponse


GET_DATASETS = gql.gql("""
query DatasetsWithSnapshots($count: Int, $after: String) {
  datasets(
    first: $count,
    after: $after,
    orderBy: {created: ascending}
    filterBy: {public: true}
  ) {
    edges {
      node {
        id
        latestSnapshot {
          tag
          hexsha
          created
        }
        snapshots {
          tag
          hexsha
          created
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
      count
    }
  }
}
""")


@stamina.retry(on=httpx.HTTPError)
async def get_page(
    client: gql.Client, count: int, after: str | None
) -> GraphQLResponse:
    """Fetch a page of datasets from the GraphQL API."""
    try:
        result = await client.execute(
            GET_DATASETS, variable_values={"count": count, "after": after}
        )
    except TransportQueryError as e:
        if e.data is not None:
            result = e.data
        else:
            raise e
    return converter.structure(result, GraphQLResponse)


async def _fetch_pages(
    client: gql.Client, queue: asyncio.Queue, page_size: int, verbose: bool
) -> None:
    """
    Fetch pages and put them in queue for processing.

    Args:
        client: GraphQL client
        queue: Queue to put pages into
        page_size: Number of datasets per page
        verbose: Enable verbose logging
    """
    page_info = PageInfo()
    page_num = 0

    try:
        while page_info.hasNextPage:
            result = await get_page(client, page_size, page_info.endCursor)

            page_info = result.datasets.pageInfo
            page_num += 1

            if verbose:
                print(
                    f"  Fetched page {page_num}: {len(result.datasets.edges)} datasets, "
                    f"hasNext={page_info.hasNextPage}"
                )

            await queue.put(result.datasets.edges)
    finally:
        # Signal completion
        await queue.put(None)
    if verbose:
        print(f"  Page fetching complete: {page_num} pages")


def write_json(path: Path, data: dict, dry_run: bool) -> None:
    """Write JSON to file with pretty formatting."""
    if dry_run:
        print(f"  [dry-run] Would write {path}")
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


async def fetch_and_write(
    output_dir: Path, page_size: int, prefetch: int, dry_run: bool, verbose: bool
) -> None:
    """
    Fetch from GraphQL and write files.

    Args:
        output_dir: Directory to write data files
        page_size: Number of datasets per GraphQL page
        prefetch: Number of pages to prefetch
        dry_run: If True, don't write files
        verbose: If True, enable verbose logging
    """
    transport = HTTPXAsyncTransport(url=ENDPOINT)
    async with gql.Client(transport=transport) as client:
        # Get total count
        first_page = await get_page(client, 0, None)
        total_count = first_page.datasets.pageInfo.count
        print(f"Starting fetch: {total_count} total datasets")

        # Setup queue and background fetcher
        queue: asyncio.Queue = asyncio.Queue(maxsize=prefetch)
        fetch_task = asyncio.create_task(
            _fetch_pages(client, queue, page_size, verbose)
        )

        # Track progress
        latest_snapshots = {}
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
                        print("⚠ Null node in edge")
                        continue

                    dataset = edge.node

                    # Validate snapshots exist
                    if not dataset.snapshots:
                        print(f"⚠ Dataset {dataset.id} has no snapshots, skipping")
                        continue

                    processed += 1

                    # Track latest snapshot for registry
                    latest_snapshots[dataset.id] = dataset.latestSnapshot.tag

                    # Write snapshots.json
                    dataset_dir = output_dir / "datasets" / dataset.id
                    snapshots_index = {
                        "schemaVersion": SCHEMA_VERSION,
                        "tags": [snap.tag for snap in dataset.snapshots],
                    }
                    write_json(dataset_dir / "snapshots.json", snapshots_index, dry_run)

                    # Write metadata.json for each snapshot
                    for snapshot in dataset.snapshots:
                        snapshot_dir = dataset_dir / "snapshots" / snapshot.tag
                        metadata = {
                            "schemaVersion": SCHEMA_VERSION,
                            "hexsha": snapshot.hexsha,
                            "created": snapshot.created,
                        }
                        write_json(snapshot_dir / "metadata.json", metadata, dry_run)

                    # Progress logging every 100 datasets
                    if processed % 100 == 0:
                        percent = 100 * processed / total_count
                        print(f"Progress: {processed}/{total_count} ({percent:.1f}%)")

        finally:
            # Ensure fetch task completes
            await fetch_task

        # Write registry
        registry = {
            "schemaVersion": SCHEMA_VERSION,
            "lastChecked": timestamp,
            "totalCount": len(latest_snapshots),
            "latestSnapshots": latest_snapshots,
        }

        registry_path = output_dir / "datasets-registry.json"
        write_json(registry_path, registry, dry_run)

        print(f"\n✓ Fetch complete: {processed} datasets processed")
        if not dry_run:
            print(f"  Registry written to: {registry_path}")


def validate_output(output_dir: Path) -> None:
    """Validate the output data for consistency."""
    print("\nValidating output...")

    registry_path = output_dir / "datasets-registry.json"
    if not registry_path.exists():
        print("  ✗ datasets-registry.json not found")
        return

    import json
    with open(registry_path) as f:
        registry = json.load(f)

    issues = []
    for dataset_id, latest_snapshot in registry["latestSnapshots"].items():
        dataset_dir = output_dir / "datasets" / dataset_id

        snapshots_path = dataset_dir / "snapshots.json"
        if not snapshots_path.exists():
            issues.append(f"{dataset_id}: snapshots.json missing")
            continue

        with open(snapshots_path) as f:
            snapshots = json.load(f)

        if latest_snapshot not in snapshots["tags"]:
            issues.append(f"{dataset_id}: latest {latest_snapshot} not in tags")

        for tag in snapshots["tags"]:
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
        print("  ✓ No issues found")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch dataset info from OpenNeuro GraphQL API"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data"),
        help="Output directory for data files (default: data)",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Number of datasets per GraphQL page (default: 100)",
    )
    parser.add_argument(
        "--prefetch",
        type=int,
        default=4,
        help="Number of pages to prefetch (default: 2)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write files, just log what would be done",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument(
        "--validate", action="store_true",
        help="Validate output data after fetching",
    )

    args = parser.parse_args()

    asyncio.run(
        fetch_and_write(
            args.output_dir, args.page_size, args.prefetch, args.dry_run, args.verbose
        )
    )

    if args.validate:
        validate_output(args.output_dir)


if __name__ == "__main__":
    main()
