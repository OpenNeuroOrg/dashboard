"""Check S3 version from dataset_description.json for all datasets.

Reads:
- data/datasets-registry.json

Writes:
- data/datasets/{id}/s3-version.json
"""

from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path

import httpx

from .converter import dump_typed, load_typed
from .models import DatasetsRegistry, S3Version, SnapshotIndex, VersionSource
from .timestamps import S3_VERSION_MANIFEST, load_timestamp_manifest, save_timestamp_manifest
from .utils import SCHEMA_VERSION, format_timestamp

S3_BASE_URL = "https://s3.amazonaws.com/openneuro.org"
DOI_PATTERN = re.compile(r"10\.18112/openneuro\.([^.]+)\.v(.+)")


async def fetch_dataset_description(
    client: httpx.AsyncClient,
    dataset_id: str,
    latest_snapshot: str,
    verbose: bool = False,
) -> S3Version:
    """Fetch dataset_description.json from S3 for a dataset.

    Parameters
    ----------
    client
        HTTP client.
    dataset_id
        Dataset ID.
    latest_snapshot
        Latest snapshot tag from GraphQL.
    verbose
        Enable verbose logging.

    Returns
    -------
    S3Version
        S3 version data (always returns an instance).
    """
    url = f"{S3_BASE_URL}/{dataset_id}/dataset_description.json"

    try:
        response = await client.get(url, timeout=30.0)
        response.raise_for_status()

        dataset_desc = response.json()

        # Extract DOI
        doi = dataset_desc.get("DatasetDOI", "")

        if not doi:
            # Case 2: No DOI field - assume latest snapshot
            if verbose:
                print(
                    f"  {dataset_id}: No DatasetDOI field, "
                    f"assuming latest ({latest_snapshot})"
                )
            return S3Version(
                schemaVersion=SCHEMA_VERSION,
                accessible=True,
                extractedVersion=latest_snapshot,
                versionSource=VersionSource.assumed_latest,
            )

        # Try to extract version from DOI
        match = DOI_PATTERN.search(doi)
        if not match:
            # Case 2: Custom DOI - assume latest snapshot
            if verbose:
                print(
                    f"  {dataset_id}: Custom DOI ({doi}), "
                    f"assuming latest ({latest_snapshot})"
                )
            return S3Version(
                schemaVersion=SCHEMA_VERSION,
                accessible=True,
                datasetDescriptionDOI=doi,
                extractedVersion=latest_snapshot,
                versionSource=VersionSource.assumed_latest,
            )

        doi_dataset_id = match.group(1)
        version = match.group(2)

        # Check for DOI dataset ID mismatch
        if doi_dataset_id != dataset_id:
            if verbose:
                print(
                    f"  {dataset_id}: DOI has wrong ID ({doi_dataset_id}), "
                    f"using version {version}"
                )
            return S3Version(
                schemaVersion=SCHEMA_VERSION,
                accessible=True,
                datasetDescriptionDOI=doi,
                extractedVersion=version,
                versionSource=VersionSource.doi,
                doiIdMismatch=True,
                doiDatasetId=doi_dataset_id,
            )

        # Case 1: Success - version from DOI
        if verbose:
            print(f"  {dataset_id}: version {version}")

        return S3Version(
            schemaVersion=SCHEMA_VERSION,
            accessible=True,
            datasetDescriptionDOI=doi,
            extractedVersion=version,
            versionSource=VersionSource.doi,
        )

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 403:
            # Case 3: BLOCKED - Access denied
            print(f"  {dataset_id}: Access denied (403)")
            return S3Version(
                schemaVersion=SCHEMA_VERSION,
                accessible=False,
                httpStatus=403,
            )
        elif e.response.status_code == 404:
            # Case 4: Missing dataset_description.json - assume latest
            print(
                f"  {dataset_id}: dataset_description.json not found (404), "
                f"assuming latest ({latest_snapshot})"
            )
            return S3Version(
                schemaVersion=SCHEMA_VERSION,
                accessible=True,
                extractedVersion=latest_snapshot,
                versionSource=VersionSource.assumed_latest,
                datasetDescriptionMissing=True,
            )
        else:
            # Other HTTP errors - assume latest
            print(
                f"  {dataset_id}: HTTP {e.response.status_code}, "
                f"assuming latest ({latest_snapshot})"
            )
            return S3Version(
                schemaVersion=SCHEMA_VERSION,
                accessible=True,
                extractedVersion=latest_snapshot,
                versionSource=VersionSource.assumed_latest,
                httpError=e.response.status_code,
            )

    except httpx.RequestError as e:
        # Network error - assume latest
        print(
            f"  {dataset_id}: Request error, assuming latest ({latest_snapshot})"
        )
        return S3Version(
            schemaVersion=SCHEMA_VERSION,
            accessible=True,
            extractedVersion=latest_snapshot,
            versionSource=VersionSource.assumed_latest,
            requestError=str(e),
        )

    except json.JSONDecodeError:
        # Invalid JSON - assume latest
        print(
            f"  {dataset_id}: Invalid JSON, assuming latest ({latest_snapshot})"
        )
        return S3Version(
            schemaVersion=SCHEMA_VERSION,
            accessible=True,
            extractedVersion=latest_snapshot,
            versionSource=VersionSource.assumed_latest,
            invalidJson=True,
        )

    except Exception as e:
        # Unexpected error - assume latest
        print(
            f"  {dataset_id}: Unexpected error ({e}), "
            f"assuming latest ({latest_snapshot})"
        )
        return S3Version(
            schemaVersion=SCHEMA_VERSION,
            accessible=True,
            extractedVersion=latest_snapshot,
            versionSource=VersionSource.assumed_latest,
            unexpectedError=str(e),
        )


async def check_all_datasets(
    output_dir: Path,
    concurrency: int = 20,
    verbose: bool = False,
) -> None:
    """Check S3 versions for all datasets with concurrency control.

    Parameters
    ----------
    output_dir
        Base output directory.
    concurrency
        Maximum number of concurrent HTTP requests.
    verbose
        Enable verbose logging.
    """
    print("Checking S3 versions...")

    # Load registry
    registry = load_typed(output_dir / "datasets-registry.json", DatasetsRegistry)
    datasets = registry.latestSnapshots
    total = len(datasets)

    print(f"Found {total} datasets to check")

    # Create HTTP client with connection pooling
    limits = httpx.Limits(
        max_keepalive_connections=concurrency, max_connections=concurrency
    )

    async with httpx.AsyncClient(limits=limits) as client:
        # Semaphore to limit concurrency
        semaphore = asyncio.Semaphore(concurrency)

        async def check_with_semaphore(
            dataset_id: str, latest_snapshot: str, index: int
        ) -> tuple[str, S3Version]:
            """Check a dataset with semaphore for rate limiting."""
            async with semaphore:
                result = await fetch_dataset_description(
                    client, dataset_id, latest_snapshot, verbose
                )

                if not verbose and (index + 1) % 100 == 0:
                    print(f"  Progress: {index + 1}/{total}")

                return dataset_id, result

        # Launch all checks concurrently (but semaphore limits actual parallelism)
        tasks = [
            check_with_semaphore(dataset_id, latest_snapshot, i)
            for i, (dataset_id, latest_snapshot) in enumerate(datasets.items())
        ]

        results = await asyncio.gather(*tasks)

    # Write results and collect statistics
    stats = {
        "case1_doi": 0,  # DOI with version
        "case2_assumed": 0,  # Missing/custom DOI
        "case3_blocked": 0,  # 403 access denied
        "case4_not_found": 0,  # 404 not found
        "doi_mismatch": 0,  # Wrong dataset ID in DOI
    }

    # Load existing manifest
    manifest = load_timestamp_manifest(output_dir / S3_VERSION_MANIFEST)
    now = format_timestamp()

    for dataset_id, s3_version_data in results:
        dataset_dir = output_dir / "datasets" / dataset_id
        dump_typed(dataset_dir / "s3-version.json", s3_version_data)
        manifest[dataset_id] = now

        # Categorize
        if not s3_version_data.accessible:
            stats["case3_blocked"] += 1
        elif s3_version_data.versionSource == VersionSource.doi:
            stats["case1_doi"] += 1
            if s3_version_data.doiIdMismatch:
                stats["doi_mismatch"] += 1
        elif s3_version_data.datasetDescriptionMissing:
            stats["case4_not_found"] += 1
        else:
            stats["case2_assumed"] += 1

    save_timestamp_manifest(output_dir / S3_VERSION_MANIFEST, manifest)

    print(f"\nS3 version check complete ({total} datasets)")
    print("\nBreakdown:")
    print(f"  Case 1 - DOI with version: {stats['case1_doi']}")
    print(f"  Case 2 - Assumed latest: {stats['case2_assumed']}")
    print(f"  Case 3 - Blocked (403): {stats['case3_blocked']}")
    print(f"  Case 4 - Not found (404): {stats['case4_not_found']}")
    if stats["doi_mismatch"] > 0:
        print(f"\n  DOI ID mismatches: {stats['doi_mismatch']}")


async def validate_s3_versions(
    output_dir: Path,
    verbose: bool = False,
) -> None:
    """Validate S3 versions against expected latest snapshots.

    Parameters
    ----------
    output_dir
        Base output directory.
    verbose
        Enable verbose logging.
    """
    print("\nValidating S3 versions...")

    registry = load_typed(output_dir / "datasets-registry.json", DatasetsRegistry)

    issues: dict[str, list[str]] = {
        "version_mismatch": [],
        "unknown_version": [],
        "blocked": [],
    }

    for dataset_id, latest_snapshot in registry.latestSnapshots.items():
        dataset_dir = output_dir / "datasets" / dataset_id

        # Load S3 version
        s3_version_path = dataset_dir / "s3-version.json"
        if not s3_version_path.exists():
            continue

        s3_version_data = load_typed(s3_version_path, S3Version)

        # Track blocked datasets
        if not s3_version_data.accessible:
            issues["blocked"].append(dataset_id)
            continue

        extracted_version = s3_version_data.extractedVersion

        if not extracted_version:
            continue

        # Load snapshots to check if version is valid
        snapshots_data = load_typed(dataset_dir / "snapshots.json", SnapshotIndex)
        valid_tags = snapshots_data.tags

        # Check if version matches latest (only flag if from DOI)
        if (
            s3_version_data.versionSource == "doi"
            and extracted_version != latest_snapshot
        ):
            issues["version_mismatch"].append(
                f"{dataset_id}: S3 has {extracted_version}, "
                f"latest is {latest_snapshot}"
            )

        # Check if version is a known snapshot
        if extracted_version not in valid_tags:
            issues["unknown_version"].append(
                f"{dataset_id}: S3 version {extracted_version} "
                f"not in known snapshots"
            )

    # Print validation results
    print("\nValidation results:")

    if issues["blocked"]:
        print(f"\n  Blocked datasets (403) [{len(issues['blocked'])}]:")
        for dataset_id in issues["blocked"][:10]:
            print(f"    {dataset_id}")
        if len(issues["blocked"]) > 10:
            print(f"    ... and {len(issues['blocked']) - 10} more")

    if issues["version_mismatch"]:
        print(
            f"\n  Version mismatches (DOI != latest) "
            f"[{len(issues['version_mismatch'])}]:"
        )
        for issue in issues["version_mismatch"][:10]:
            print(f"    {issue}")
        if len(issues["version_mismatch"]) > 10:
            print(
                f"    ... and {len(issues['version_mismatch']) - 10} more"
            )

    if issues["unknown_version"]:
        print(f"\n  Unknown versions [{len(issues['unknown_version'])}]:")
        for issue in issues["unknown_version"][:10]:
            print(f"    {issue}")
        if len(issues["unknown_version"]) > 10:
            print(
                f"    ... and {len(issues['unknown_version']) - 10} more"
            )

    if not any(len(v) > 0 for v in issues.values()):
        print("  No issues found!")
