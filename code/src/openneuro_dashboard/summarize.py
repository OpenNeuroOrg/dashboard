"""Stage 6: Summarize check results."""

from pathlib import Path

from .utils import SCHEMA_VERSION, format_timestamp, load_json, load_json_safe, write_json


def summarize_dataset(dataset_id, latest_snapshot, dataset_dir) -> dict:
    """Summarize check results for a single dataset."""
    checks = {}
    last_checked = {}

    # GitHub check
    github = load_json_safe(dataset_dir / "github.json")
    if not github:
        checks["github"] = "pending"
    elif latest_snapshot not in github.get("tags", {}):
        checks["github"] = "error"
    elif github["branches"].get(github["head"]) != github["tags"].get(
        latest_snapshot
    ):
        checks["github"] = "warning"
    else:
        checks["github"] = "ok"
    if github:
        last_checked["github"] = github["lastChecked"]

    # S3 version check
    s3_version = load_json_safe(dataset_dir / "s3-version.json")
    s3_blocked = False
    if not s3_version:
        checks["s3Version"] = "pending"
        checks["s3Files"] = "pending"
    elif not s3_version.get("accessible", True):
        checks["s3Version"] = "error"
        checks["s3Files"] = "error"
        s3_blocked = True
        last_checked["s3Version"] = s3_version["lastChecked"]
    else:
        if s3_version.get("versionSource") == "doi":
            if s3_version["extractedVersion"] == latest_snapshot:
                checks["s3Version"] = "ok"
            else:
                checks["s3Version"] = "version-mismatch"
        else:
            checks["s3Version"] = "warning"
        last_checked["s3Version"] = s3_version["lastChecked"]

        s3_diff = load_json_safe(dataset_dir / "s3-diff.json")
        if not s3_diff:
            checks["s3Files"] = "pending"
        elif s3_diff.get("exportMissing"):
            checks["s3Files"] = "error"
            last_checked["s3Files"] = s3_diff["checkedAt"]
        else:
            checks["s3Files"] = s3_diff.get("status", "pending")
            last_checked["s3Files"] = s3_diff["checkedAt"]

    status_priority = {
        "ok": 0,
        "warning": 1,
        "version-mismatch": 2,
        "error": 3,
        "pending": 4,
    }
    overall_status = max(checks.values(), key=lambda s: status_priority[s])

    result = {"id": dataset_id, "status": overall_status, "checks": checks}
    if s3_blocked:
        result["s3Blocked"] = True
    if last_checked:
        result["lastChecked"] = last_checked
    return result


def generate_summary(output_dir: Path):
    """Generate summary from all check files."""
    print("Generating summary...")
    registry = load_json(output_dir / "datasets-registry.json")
    datasets_dict = registry["latestSnapshots"]

    datasets = []
    for dataset_id, latest_snapshot in datasets_dict.items():
        dataset_dir = output_dir / "datasets" / dataset_id
        summary = summarize_dataset(dataset_id, latest_snapshot, dataset_dir)
        datasets.append(summary)

    summary_doc = {
        "schemaVersion": SCHEMA_VERSION,
        "lastUpdated": format_timestamp(),
        "datasets": datasets,
    }
    write_json(output_dir / "all-datasets.json", summary_doc)

    # Print statistics
    status_counts = {}
    s3_blocked_count = 0
    for ds in datasets:
        status = ds["status"]
        status_counts[status] = status_counts.get(status, 0) + 1
        if ds.get("s3Blocked"):
            s3_blocked_count += 1

    print(f"\nSummary generation complete ({len(datasets)} datasets)")
    print("\nDataset status breakdown:")
    for status in ["ok", "warning", "version-mismatch", "error", "pending"]:
        count = status_counts.get(status, 0)
        if count > 0:
            print(f"  {status}: {count}")
    if s3_blocked_count > 0:
        print(f"\nS3 blocked (403): {s3_blocked_count} datasets")
