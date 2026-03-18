"""Shared utilities for test data generation."""

import json
import random
from pathlib import Path
from datetime import datetime, timedelta, UTC

SCHEMA_VERSION = "1.1.0"


def format_timestamp() -> str:
    """Format current UTC time in the standard dashboard timestamp format."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def random_sha() -> str:
    """Generate a random git SHA."""
    return "".join(random.choices("0123456789abcdef", k=40))


def random_datetime(days_ago: int = 30) -> str:
    """Generate a random datetime within the last N days."""
    dt = datetime.now(UTC) - timedelta(days=random.randint(0, days_ago))
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def generate_dataset_id(num: int) -> str:
    """Generate a dataset ID."""
    return f"ds{num:06d}"


def generate_version_tag(major: int = 1, minor: int = 0) -> str:
    """Generate a semantic version tag."""
    return f"{major}.{minor}.{random.randint(0, 5)}"


def generate_old_style_tag() -> str:
    """Generate an old-style numeric tag starting with 0."""
    return f"0{random.randint(1000, 9999):04d}"


def get_latest_snapshot(tags: list[str]) -> str:
    """Get the latest snapshot from a list of tags."""
    return max(tags)


def generate_snapshots(num_snapshots: int = None) -> list[str]:
    """Generate a list of snapshot tags in chronological order."""
    if num_snapshots is None:
        num_snapshots = random.randint(1, 7)

    tags = []
    # Sometimes include old-style tags
    if random.random() < 0.3:
        for _ in range(random.randint(1, 3)):
            tags.append(generate_old_style_tag())

    # Add semantic version tags
    for i in range(num_snapshots):
        tags.append(generate_version_tag(minor=i))

    # Sort: old-style tags first, then semantic versions
    old_style = sorted([t for t in tags if t[0] == "0" and "." not in t])
    semantic = sorted(
        [t for t in tags if "." in t], key=lambda x: tuple(map(int, x.split(".")))
    )

    return old_style + semantic


def generate_file_paths(size: str = "medium") -> list[str]:
    """Generate a realistic list of file paths."""
    sizes = {"small": 50, "medium": 500, "large": 5000, "xlarge": 20000}

    num_files = sizes.get(size, 500)
    files = [
        "dataset_description.json",
        "participants.tsv",
        "participants.json",
        "README",
        "CHANGES",
    ]

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
                files.append(
                    f"{sub_id}/func/{sub_id}_task-rest_run-{run:02d}_bold.nii.gz"
                )
                files.append(
                    f"{sub_id}/func/{sub_id}_task-rest_run-{run:02d}_bold.json"
                )

    # Pad to approximate size
    while len(files) < num_files:
        sub_id = f"sub-{random.randint(1, num_subjects):02d}"
        session = random.randint(1, 3)
        files.append(
            f"{sub_id}/ses-{session:02d}/anat/{sub_id}_ses-{session:02d}_T2w.nii.gz"
        )

    return sorted(set(files))[:num_files]


def write_json(path: Path, data: dict) -> None:
    """Write JSON to file with pretty formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def load_json(path: Path) -> dict:
    """Load JSON from file."""
    with open(path) as f:
        return json.load(f)
