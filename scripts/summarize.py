#!/usr/bin/env python3
"""
Stage 6: Summarize check results.

Reads:
- data/datasets-registry.json
- data/datasets/{id}/github.json
- data/datasets/{id}/s3-version.json
- data/datasets/{id}/s3-diff.json

Writes:
- data/all-datasets.json
"""

import argparse
import json
from pathlib import Path
from datetime import datetime

SCHEMA_VERSION = "1.0"


def load_json_safe(path: Path) -> dict | None:
    """Load JSON file if it exists, return None otherwise."""
    if path.exists():
        return json.loads(path.read_text())
    return None


def summarize_dataset(dataset_id: str, latest_snapshot: str, dataset_dir: Path) -> dict:
    """Summarize check results for a single dataset."""
    checks = {}
    last_checked = {}

    # GitHub check
    github = load_json_safe(dataset_dir / "github.json")
    if not github:
        checks["github"] = "pending"
    elif latest_snapshot not in github.get("tags", {}):
        checks["github"] = "error"
    elif github["branches"].get(github["head"]) != github["tags"].get(latest_snapshot):
        checks["github"] = "warning"
    else:
        checks["github"] = "ok"

    if github:
        last_checked["github"] = github["lastChecked"]

    # S3 version check
    s3_version = load_json_safe(dataset_dir / "s3-version.json")
    if not s3_version:
        checks["s3Version"] = "pending"
    elif s3_version["extractedVersion"] != latest_snapshot:
        checks["s3Version"] = "version-mismatch"
    else:
        checks["s3Version"] = "ok"

    if s3_version:
        last_checked["s3Version"] = s3_version["lastChecked"]

    # S3 files check
    if not s3_version:
        checks["s3Files"] = "pending"
    elif s3_version["extractedVersion"] != latest_snapshot:
        checks["s3Files"] = "version-mismatch"
    else:
        s3_diff = load_json_safe(dataset_dir / "s3-diff.json")
        if not s3_diff:
            checks["s3Files"] = "pending"
        elif s3_diff["summary"]["inGitOnly"] > 0 or s3_diff["summary"]["inS3Only"] > 0:
            checks["s3Files"] = "error"
        else:
            checks["s3Files"] = "ok"

        if s3_diff:
            last_checked["s3Files"] = s3_diff["lastChecked"]

    # Overall status (worst of all checks)
    status_priority = {
        "ok": 0,
        "warning": 1,
        "version-mismatch": 2,
        "error": 3,
        "pending": 4,
    }
    overall_status = max(checks.values(), key=lambda s: status_priority[s])

    result = {"id": dataset_id, "status": overall_status, "checks": checks}

    if last_checked:
        result["lastChecked"] = last_checked

    return result


def generate_summary(output_dir: Path):
    """Generate summary from all check files."""
    print("Generating summary...")

    # Load registry
    registry = load_json_safe(output_dir / "datasets-registry.json")
    datasets_dict = registry["latestSnapshots"]

    # Summarize each dataset
    datasets = []
    for dataset_id, latest_snapshot in datasets_dict.items():
        dataset_dir = output_dir / "datasets" / dataset_id
        summary = summarize_dataset(dataset_id, latest_snapshot, dataset_dir)
        datasets.append(summary)

    # Create summary document
    summary_doc = {
        "schemaVersion": SCHEMA_VERSION,
        "lastUpdated": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "datasets": datasets,
    }

    Path.write_text(output_dir / "all-datasets.json", json.dumps(summary_doc))

    # Print statistics
    status_counts = {}
    for ds in datasets:
        status = ds["status"]
        status_counts[status] = status_counts.get(status, 0) + 1

    print(f"\n✓ Summary generation complete ({len(datasets)} datasets)")
    print("\nDataset status breakdown:")
    for status in ["ok", "warning", "version-mismatch", "error", "pending"]:
        count = status_counts.get(status, 0)
        if count > 0:
            print(f"  {status}: {count}")


def main():
    parser = argparse.ArgumentParser(description="Generate summary from check results")
    parser.add_argument("--output-dir", type=Path, default=Path("data"))
    args = parser.parse_args()

    generate_summary(args.output_dir)


if __name__ == "__main__":
    main()
