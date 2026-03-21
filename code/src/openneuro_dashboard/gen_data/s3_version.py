"""Stage 3: Generate simulated S3 version check data.

Reads:
- data/datasets-registry.json

Writes:
- data/datasets/{id}/s3-version.json
"""

import random
from pathlib import Path

from ..converter import dump_typed, load_typed
from ..models import DatasetsRegistry, S3Version, VersionSource
from ..utils import SCHEMA_VERSION
from .utils import random_datetime


def _generate_s3_version_check(
    dataset_id: str, latest_snapshot: str, scenario: str
) -> S3Version:
    """Generate s3-version.json for a dataset."""
    # Case 3: Blocked (403)
    if scenario == "blocked":
        return S3Version(
            schemaVersion=SCHEMA_VERSION,
            lastChecked=random_datetime(days_ago=1),
            accessible=False,
            httpStatus=403,
            datasetDescriptionDOI=None,
            extractedVersion=None,
        )

    # Case 4: Not found (404)
    if scenario == "not_found":
        return S3Version(
            schemaVersion=SCHEMA_VERSION,
            lastChecked=random_datetime(days_ago=1),
            accessible=True,
            datasetDescriptionDOI=None,
            extractedVersion=latest_snapshot,
            versionSource=VersionSource.assumed_latest,
            datasetDescriptionMissing=True,
        )

    # Case 2: Missing or custom DOI
    if scenario == "custom_doi":
        doi_type = random.choice(["custom", "missing"])
        if doi_type == "custom":
            custom_doi = f"10.5281/zenodo.{random.randint(1000000, 9999999)}"
            return S3Version(
                schemaVersion=SCHEMA_VERSION,
                lastChecked=random_datetime(days_ago=1),
                accessible=True,
                datasetDescriptionDOI=custom_doi,
                extractedVersion=latest_snapshot,
                versionSource=VersionSource.assumed_latest,
            )
        else:
            return S3Version(
                schemaVersion=SCHEMA_VERSION,
                lastChecked=random_datetime(days_ago=1),
                accessible=True,
                datasetDescriptionDOI=None,
                extractedVersion=latest_snapshot,
                versionSource=VersionSource.assumed_latest,
            )

    # Case 1: Normal DOI or version mismatch
    actual_version = latest_snapshot

    # For version-mismatch scenario, use an older version
    if scenario == "version-mismatch":
        parts = latest_snapshot.split(".")
        if len(parts) == 3 and parts[2].isdigit() and int(parts[2]) > 0:
            parts[2] = str(int(parts[2]) - 1)
            actual_version = ".".join(parts)

    doi = f"10.18112/openneuro.{dataset_id}.v{actual_version}"

    # Occasionally add DOI ID mismatch
    if random.random() < 0.01:  # 1% chance
        wrong_id = f"ds{random.randint(0, 999999):06d}"
        doi = f"10.18112/openneuro.{wrong_id}.v{actual_version}"
        return S3Version(
            schemaVersion=SCHEMA_VERSION,
            lastChecked=random_datetime(days_ago=1),
            accessible=True,
            datasetDescriptionDOI=doi,
            extractedVersion=actual_version,
            versionSource=VersionSource.doi,
            doiIdMismatch=True,
            doiDatasetId=wrong_id,
        )

    return S3Version(
        schemaVersion=SCHEMA_VERSION,
        lastChecked=random_datetime(days_ago=1),
        accessible=True,
        datasetDescriptionDOI=doi,
        extractedVersion=actual_version,
        versionSource=VersionSource.doi,
    )


def generate(output_dir: Path, seed: int = None):
    """Generate S3 version check data for all datasets."""
    if seed is not None:
        random.seed(seed)

    print("Generating S3 version check data...")

    # Load registry
    registry = load_typed(output_dir / "datasets-registry.json", DatasetsRegistry)
    datasets = registry.latestSnapshots

    for i, (dataset_id, latest_snapshot) in enumerate(datasets.items(), 1):
        dataset_dir = output_dir / "datasets" / dataset_id

        # Determine scenario with realistic weights
        scenario = random.choices(
            ["healthy", "version-mismatch", "custom_doi", "not_found", "blocked"],
            weights=[75, 10, 8, 5, 2],
        )[0]

        # Generate s3-version.json
        s3_version_data = _generate_s3_version_check(
            dataset_id, latest_snapshot, scenario
        )
        dump_typed(dataset_dir / "s3-version.json", s3_version_data)

        if i % 100 == 0:
            print(f"  Processed {i}/{len(datasets)}")

    print(f"S3 version check generation complete ({len(datasets)} datasets)")
