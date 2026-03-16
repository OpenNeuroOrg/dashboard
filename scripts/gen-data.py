#!/usr/bin/env python3
"""
Generate test data for OpenNeuro Dashboard.

Usage:
    python generate_test_data.py --output-dir data/ --num-datasets 10
"""

import argparse
import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

SCHEMA_VERSION = "1.0.0"

def random_sha() -> str:
    """Generate a random git SHA."""
    return ''.join(random.choices('0123456789abcdef', k=40))

def random_datetime(days_ago: int = 30) -> str:
    """Generate a random datetime within the last N days."""
    dt = datetime.now() - timedelta(days=random.randint(0, days_ago))
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def generate_dataset_id(num: int) -> str:
    """Generate a dataset ID."""
    return f"ds{num:06d}"

def generate_version_tag(major: int = 1, minor: int = 0) -> str:
    """Generate a semantic version tag."""
    return f"{major}.{minor}.{random.randint(0, 5)}"

def generate_old_style_tag() -> str:
    """Generate an old-style numeric tag."""
    return f"{random.randint(10000, 99999):05d}"

def generate_snapshots(num_snapshots: int = None) -> List[str]:
    """Generate a list of snapshot tags in chronological order."""
    if num_snapshots is None:
        num_snapshots = random.randint(1, 7)
    
    tags = []
    # Sometimes include old-style tags (always start with 0)
    if random.random() < 0.3:
        for _ in range(random.randint(1, 3)):
            tags.append(f"0{random.randint(1000, 9999):04d}")
    
    # Add semantic version tags
    for i in range(num_snapshots):
        tags.append(generate_version_tag(minor=i))
    
    # Sort: old-style tags first (by value), then semantic versions
    old_style = sorted([t for t in tags if t[0] == '0' and '.' not in t])
    semantic = sorted([t for t in tags if '.' in t], 
                     key=lambda x: tuple(map(int, x.split('.'))))
    
    return old_style + semantic

def get_latest_snapshot(tags: List[str]) -> str:
    """Get the latest snapshot from a list of tags."""
    # max() will work because:
    # - Old-style tags start with '0' (e.g., "00001")
    # - Semantic version tags start with digits >= 1 (e.g., "1.0.0")
    # - String comparison: "1.0.0" > "00001"
    return max(tags)

def generate_datasets_registry(num_datasets: int) -> tuple[Dict[str, Any], Dict[str, List[str]]]:
    """
    Generate datasets-registry.json.
    Returns: (registry_dict, dataset_tags_dict)
    """
    latest_snapshots = {}
    all_tags = {}
    
    for i in range(1, num_datasets + 1):
        dataset_id = generate_dataset_id(i)
        tags = generate_snapshots()
        all_tags[dataset_id] = tags
        latest_snapshots[dataset_id] = get_latest_snapshot(tags)
    
    registry = {
        "schemaVersion": SCHEMA_VERSION,
        "lastChecked": random_datetime(days_ago=1),
        "totalCount": num_datasets,
        "latestSnapshots": latest_snapshots
    }
    
    return registry, all_tags

def generate_file_paths(size: str = "medium") -> List[str]:
    """Generate a realistic list of file paths."""
    sizes = {
        "small": 50,
        "medium": 500,
        "large": 5000,
        "xlarge": 20000
    }
    
    num_files = sizes.get(size, 500)
    files = ["dataset_description.json", "participants.tsv", "participants.json", "README", "CHANGES"]
    
    num_subjects = random.randint(5, 50)
    for sub in range(1, num_subjects + 1):
        sub_id = f"sub-{sub:02d}"
        
        # Anatomical
        if random.random() < 0.9:
            files.append(f"{sub_id}/anat/{sub_id}_T1w.nii.gz")
            files.append(f"{sub_id}/anat/{sub_id}_T1w.json")
        
        # Functional
        num_runs = random.randint(1, 4)
        for run in range(1, num_runs + 1):
            if random.random() < 0.8:
                files.append(f"{sub_id}/func/{sub_id}_task-rest_run-{run:02d}_bold.nii.gz")
                files.append(f"{sub_id}/func/{sub_id}_task-rest_run-{run:02d}_bold.json")
    
    # Pad to approximate size
    while len(files) < num_files:
        sub_id = f"sub-{random.randint(1, num_subjects):02d}"
        session = random.randint(1, 3)
        files.append(f"{sub_id}/ses-{session:02d}/anat/{sub_id}_ses-{session:02d}_T2w.nii.gz")
    
    return sorted(set(files))[:num_files]

def generate_snapshots_index(tags: List[str]) -> Dict[str, Any]:
    """Generate snapshots.json."""
    return {
        "schemaVersion": SCHEMA_VERSION,
        "tags": tags
    }

def generate_snapshot_metadata(tag: str, days_ago: int) -> Dict[str, Any]:
    """Generate metadata.json for a snapshot."""
    created = random_datetime(days_ago=days_ago)
    
    return {
        "schemaVersion": SCHEMA_VERSION,
        "hexsha": random_sha(),
        "created": created
    }

def generate_github_status(dataset_id: str, tags: List[str], 
                          snapshot_metadata: Dict[str, Dict[str, Any]],
                          scenario: str = "healthy") -> Dict[str, Any]:
    """Generate github.json."""
    latest_tag = tags[-1]
    latest_sha = snapshot_metadata[latest_tag]["hexsha"]
    
    # Generate tag mapping - use actual SHAs from metadata
    tag_mapping = {}
    for tag in tags:
        if tag in snapshot_metadata:
            tag_mapping[tag] = snapshot_metadata[tag]["hexsha"]
    
    # For error scenario, some tags might be missing
    if scenario == "error":
        # Remove a random tag
        if len(tag_mapping) > 1:
            missing_tag = random.choice(list(tag_mapping.keys())[:-1])
            del tag_mapping[missing_tag]
    
    # Determine HEAD
    head_branch = random.choice(["master", "main"])
    
    # For healthy scenario, HEAD points to latest
    if scenario == "healthy":
        head_sha = latest_sha
    else:  # warning or error - HEAD is stale
        if len(tags) > 1:
            old_tag = tags[-2]
            head_sha = snapshot_metadata.get(old_tag, {}).get("hexsha", random_sha())
        else:
            head_sha = random_sha()
    
    branches = {
        "git-annex": random_sha(),
        "main": head_sha if head_branch == "main" else random_sha(),
        "master": head_sha if head_branch == "master" else random_sha()
    }
    
    return {
        "schemaVersion": SCHEMA_VERSION,
        "lastChecked": random_datetime(days_ago=1),
        "head": head_branch,
        "branches": branches,
        "tags": tag_mapping
    }

def generate_s3_version(dataset_id: str, version: str, scenario: str = "healthy") -> Dict[str, Any]:
    """Generate s3-version.json."""
    actual_version = version
    
    # For version-mismatch scenario, use an older version
    if scenario == "version-mismatch":
        parts = version.split('.')
        if len(parts) == 3 and parts[2].isdigit() and int(parts[2]) > 0:
            parts[2] = str(int(parts[2]) - 1)
            actual_version = '.'.join(parts)
    
    doi = f"10.18112/openneuro.{dataset_id}.v{actual_version}"
    
    return {
        "schemaVersion": SCHEMA_VERSION,
        "lastChecked": random_datetime(days_ago=1),
        "datasetDescriptionDOI": doi,
        "extractedVersion": actual_version
    }

def generate_s3_diff(version: str, git_sha: str, git_files: List[str], 
                    scenario: str = "healthy") -> Dict[str, Any]:
    """Generate s3-diff.json based on actual git files."""
    
    if scenario == "healthy":
        in_git_only = []
        in_s3_only = []
        total_in_s3 = len(git_files)
    elif scenario == "error":
        # S3 is missing some files and has extra files
        num_missing = random.randint(1, min(5, len(git_files) // 10))
        in_git_only = random.sample(git_files, num_missing)
        in_s3_only = [".DS_Store", "._sub-01_T1w.nii.gz", "Thumbs.db"][:random.randint(1, 3)]
        total_in_s3 = len(git_files) - len(in_git_only) + len(in_s3_only)
    else:  # warning
        # S3 is missing one file
        in_git_only = [random.choice(git_files)]
        in_s3_only = []
        total_in_s3 = len(git_files) - 1
    
    in_both = len(git_files) - len(in_git_only)
    
    return {
        "schemaVersion": SCHEMA_VERSION,
        "lastChecked": random_datetime(days_ago=1),
        "s3Version": version,
        "gitHexsha": git_sha,
        "summary": {
            "totalInGit": len(git_files),
            "totalInS3": total_in_s3,
            "inBoth": in_both,
            "inGitOnly": len(in_git_only),
            "inS3Only": len(in_s3_only)
        },
        "inGitOnly": in_git_only,
        "inS3Only": in_s3_only
    }

def generate_file_list(size: str, git_sha: str) -> Dict[str, Any]:
    """Generate files.json."""
    files = generate_file_paths(size)
    
    return {
        "schemaVersion": SCHEMA_VERSION,
        "count": len(files),
        "files": files
    }

def load_json_file(path: Path) -> Optional[Dict[str, Any]]:
    """Load a JSON file if it exists."""
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return None

def summarize_dataset(dataset_id: str, latest_snapshot: str, 
                     datasets_dir: Path) -> Dict[str, Any]:
    """
    Summarize check results for a dataset.
    This mimics the actual SUMMARIZE process.
    """
    dataset_dir = datasets_dir / dataset_id
    
    checks = {}
    last_checked = {}
    
    # GitHub check
    github = load_json_file(dataset_dir / "github.json")
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
    s3_version = load_json_file(dataset_dir / "s3-version.json")
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
        s3_diff = load_json_file(dataset_dir / "s3-diff.json")
        if not s3_diff:
            checks["s3Files"] = "pending"
        elif s3_diff["summary"]["inGitOnly"] > 0 or s3_diff["summary"]["inS3Only"] > 0:
            checks["s3Files"] = "error"
        else:
            checks["s3Files"] = "ok"
        
        if s3_diff:
            last_checked["s3Files"] = s3_diff["lastChecked"]
    
    # Overall status
    status_priority = {"ok": 0, "warning": 1, "version-mismatch": 2, "error": 3, "pending": 4}
    overall_status = max(checks.values(), key=lambda s: status_priority[s])
    
    result = {
        "id": dataset_id,
        "status": overall_status,
        "checks": checks
    }
    
    if last_checked:
        result["lastChecked"] = last_checked
    
    return result

def generate_all_datasets_summary(registry: Dict[str, Any], 
                                  datasets_dir: Path) -> Dict[str, Any]:
    """
    Generate all-datasets.json by summarizing actual check files.
    This is the real SUMMARIZE process.
    """
    datasets = []
    
    for dataset_id, latest_snapshot in registry["latestSnapshots"].items():
        dataset_summary = summarize_dataset(dataset_id, latest_snapshot, datasets_dir)
        datasets.append(dataset_summary)
    
    return {
        "schemaVersion": SCHEMA_VERSION,
        "lastUpdated": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "datasets": datasets
    }

def main():
    parser = argparse.ArgumentParser(description="Generate test data for OpenNeuro Dashboard")
    parser.add_argument("--output-dir", type=Path, default=Path("data"),
                       help="Output directory for generated data")
    parser.add_argument("--num-datasets", type=int, default=10,
                       help="Number of datasets to generate")
    parser.add_argument("--dataset-size", choices=["small", "medium", "large", "xlarge"],
                       default="medium", help="Size of file listings")
    parser.add_argument("--seed", type=int, help="Random seed for reproducibility")
    
    args = parser.parse_args()
    
    if args.seed:
        random.seed(args.seed)
    
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Generating test data for {args.num_datasets} datasets...")
    
    # Generate registry (FETCH-GRAPHQL stage)
    print("Generating datasets-registry.json...")
    registry, all_dataset_tags = generate_datasets_registry(args.num_datasets)
    with open(output_dir / "datasets-registry.json", "w") as f:
        json.dump(registry, f, indent=2)
    
    # Generate per-dataset files
    datasets_dir = output_dir / "datasets"
    datasets_dir.mkdir(exist_ok=True)
    
    for dataset_id, latest_snapshot in registry["latestSnapshots"].items():
        print(f"Generating data for {dataset_id}...")
        
        dataset_dir = datasets_dir / dataset_id
        dataset_dir.mkdir(exist_ok=True)
        
        # Get pre-generated tags for this dataset
        tags = all_dataset_tags[dataset_id]
        
        # Verify latest is correct
        assert latest_snapshot == get_latest_snapshot(tags), \
            f"Latest snapshot mismatch for {dataset_id}"
        
        # snapshots.json
        snapshots_index = generate_snapshots_index(tags)
        with open(dataset_dir / "snapshots.json", "w") as f:
            json.dump(snapshots_index, f, indent=2)
        
        # Generate snapshot metadata for all tags (FETCH-GRAPHQL stage)
        snapshots_dir = dataset_dir / "snapshots"
        snapshots_dir.mkdir(exist_ok=True)
        
        snapshot_metadata = {}
        for i, tag in enumerate(tags):
            # Older snapshots have older creation dates
            days_ago = (len(tags) - i) * 180 + random.randint(0, 100)
            
            tag_dir = snapshots_dir / tag
            tag_dir.mkdir(exist_ok=True)
            
            # metadata.json
            metadata = generate_snapshot_metadata(tag, days_ago)
            snapshot_metadata[tag] = metadata
            with open(tag_dir / "metadata.json", "w") as f:
                json.dump(metadata, f, indent=2)
        
        # Determine scenario for this dataset
        scenario = random.choices(
            ["healthy", "warning", "error", "version-mismatch"],
            weights=[70, 15, 10, 5]
        )[0]
        
        # CHECK-GITHUB stage
        github_status = generate_github_status(
            dataset_id, 
            tags, 
            snapshot_metadata,
            scenario if scenario in ["healthy", "warning", "error"] else "healthy"
        )
        with open(dataset_dir / "github.json", "w") as f:
            json.dump(github_status, f, indent=2)
        
        # CHECK-S3-VERSION stage
        s3_version = generate_s3_version(dataset_id, latest_snapshot, scenario)
        with open(dataset_dir / "s3-version.json", "w") as f:
            json.dump(s3_version, f, indent=2)
        
        # FETCH-GIT-TREES stage (only for latest snapshot in test data)
        latest_tag_dir = snapshots_dir / latest_snapshot
        latest_metadata = snapshot_metadata[latest_snapshot]
        file_list = generate_file_list(args.dataset_size, latest_metadata["hexsha"])
        with open(latest_tag_dir / "files.json", "w") as f:
            json.dump(file_list, f, indent=2)
        
        # CHECK-S3-FILES stage (only if versions match)
        if s3_version["extractedVersion"] == latest_snapshot:
            s3_diff = generate_s3_diff(
                latest_snapshot,
                latest_metadata["hexsha"],
                file_list["files"],
                scenario if scenario in ["healthy", "warning", "error"] else "healthy"
            )
            with open(dataset_dir / "s3-diff.json", "w") as f:
                json.dump(s3_diff, f, indent=2)
        
        # If S3 version doesn't match latest but we still want to generate some test cases,
        # also create files.json for the S3 version
        if s3_version["extractedVersion"] != latest_snapshot and s3_version["extractedVersion"] in tags:
            s3_tag_dir = snapshots_dir / s3_version["extractedVersion"]
            s3_tag_dir.mkdir(exist_ok=True)
            s3_metadata = snapshot_metadata[s3_version["extractedVersion"]]
            s3_file_list = generate_file_list(args.dataset_size, s3_metadata["hexsha"])
            with open(s3_tag_dir / "files.json", "w") as f:
                json.dump(s3_file_list, f, indent=2)
    
    # SUMMARIZE stage - generate summary from actual check files
    print("Generating all-datasets.json (summarizing check results)...")
    summary = generate_all_datasets_summary(registry, datasets_dir)
    with open(output_dir / "all-datasets.json", "w") as f:
        json.dump(summary, f, indent=2)
    
    print(f"\nTest data generated successfully in {output_dir}")
    print(f"\nSummary:")
    print(f"  Total datasets: {args.num_datasets}")
    print(f"  Registry: datasets-registry.json")
    print(f"  Summary: all-datasets.json")
    print(f"  Per-dataset files in: datasets/")
    
    # Print status breakdown
    status_counts = {}
    for dataset in summary["datasets"]:
        status = dataset["status"]
        status_counts[status] = status_counts.get(status, 0) + 1
    
    print(f"\nDataset status breakdown:")
    for status, count in sorted(status_counts.items()):
        print(f"  {status}: {count}")
    
    # Print check-specific breakdowns
    check_names = ["github", "s3Version", "s3Files"]
    for check_name in check_names:
        check_counts = {}
        for dataset in summary["datasets"]:
            status = dataset["checks"][check_name]
            check_counts[status] = check_counts.get(status, 0) + 1
        print(f"\n{check_name} check breakdown:")
        for status, count in sorted(check_counts.items()):
            print(f"  {status}: {count}")

if __name__ == "__main__":
    main()
