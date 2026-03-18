# Dashboard Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the OpenNeuro Dashboard with the S3 file diff pipeline stage, code quality fixes, frontend error boundaries, and CI/CD scheduling.

**Architecture:** A new `check_s3_files.py` pipeline stage clones bare git repos on cache miss and compares file trees against S3 listings using `pygit2` and `aioboto3`. The existing frontend diff viewer is updated to consume a new `added`/`removed`/`context` data shape. Schema bumps to 1.1.0. A GitHub Actions workflow runs the full pipeline daily.

**Tech Stack:** Python 3.13+, `uv` (inline PEP 723 deps), `pygit2`, `aioboto3`, asyncio, vanilla JS

**Spec:** `docs/superpowers/specs/2026-03-17-dashboard-completion-design.md`

---

## File Structure

### New files
- `scripts/check_s3_files.py` — Stage 4: S3 file diff with integrated git cache
- `scripts/gen_data/git_tree.py` — Already exists, generates test file trees (used by updated s3_diff.py)
- `.github/workflows/update-data.yml` — Daily CI/CD pipeline

### Modified files
- `scripts/utils.py` — Add `format_timestamp()`, bump `SCHEMA_VERSION` to `"1.1.0"`
- `scripts/summarize.py` — Consume new `s3-diff.json` format
- `scripts/check_github.py` — Use `format_timestamp()` instead of `.isoformat()`
- `scripts/check_s3_version.py` — Use `format_timestamp()` instead of inline `.strftime()`
- `scripts/fetch_graphql.py` — Use `format_timestamp()`, add `--validate` flag
- `scripts/gen_data/s3_diff.py` — Generate new format (`added`/`removed`/`context`)
- `schema/openneuro-dashboard.yaml` — Update `S3FileDiff` class for v1.1.0
- `js/dataset.js` — Update `renderS3FilesCheck`, `renderDiffViewer`, `getS3FilesStatus` for new data shape
- `dataset.html` — Replace diff summary/file list HTML with unified diff viewer
- `CLAUDE.md` — Update pipeline description (5 stages, not 6)

---

### Task 1: Timestamp standardization in `scripts/utils.py`

**Files:**
- Modify: `scripts/utils.py`

- [ ] **Step 1: Add `format_timestamp` to `scripts/utils.py`**

Add after the existing imports at the top of the file:

```python
from datetime import datetime, UTC

def format_timestamp() -> str:
    """Format current UTC time in the standard dashboard timestamp format."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")
```

Note: `datetime` and `timedelta` are already imported. Add `UTC` to the existing import.

- [ ] **Step 2: Bump `SCHEMA_VERSION`**

Change `SCHEMA_VERSION = "1.0.0"` to `SCHEMA_VERSION = "1.1.0"` in `scripts/utils.py`.

- [ ] **Step 3: Fix `random_datetime` timezone**

Change `datetime.now()` to `datetime.now(UTC)` on line 18 of `scripts/utils.py`.

- [ ] **Step 4: Update `check_github.py` to use `format_timestamp`**

In `scripts/check_github.py`:
- Add `format_timestamp` to the import from `utils`: `from utils import SCHEMA_VERSION, write_json, load_json, format_timestamp`
- Remove `from datetime import datetime, UTC`
- Replace line 118 (`"lastChecked": datetime.now(UTC).isoformat(),`) with `"lastChecked": format_timestamp(),`

- [ ] **Step 5: Update `check_s3_version.py` to use `format_timestamp`**

In `scripts/check_s3_version.py`:
- Add `format_timestamp` to the import from `utils`: `from utils import SCHEMA_VERSION, write_json, load_json, format_timestamp`
- Remove `from datetime import datetime, UTC`
- Replace all occurrences of `datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")` with `format_timestamp()` (there are 10 occurrences in this file)

- [ ] **Step 6: Update `fetch_graphql.py` to use `format_timestamp`**

In `scripts/fetch_graphql.py`:
- Import `format_timestamp` from utils: add `from utils import format_timestamp` after the existing imports (note: this file imports `SCHEMA_VERSION` as a local constant, not from utils — that's fine, leave it)
- Actually, `fetch_graphql.py` defines its own `SCHEMA_VERSION` and `write_json`. Just add: `from utils import format_timestamp`
- Replace line 217 (`timestamp = datetime.now(UTC).strftime(...)`) with `timestamp = format_timestamp()`
- The `from datetime import datetime, UTC` import is still needed for the dataclasses, so keep it.

- [ ] **Step 7: Update `summarize.py` to use `format_timestamp`**

In `scripts/summarize.py`:
- Add `format_timestamp` to the import: `from utils import SCHEMA_VERSION, write_json, load_json, format_timestamp`
- Remove `from datetime import datetime, UTC`
- Replace line 133 (`datetime.now(UTC).strftime(...)`) with `format_timestamp()`

- [ ] **Step 8: Verify all scripts still run**

```bash
cd scripts && uv run python -c "from utils import format_timestamp, SCHEMA_VERSION; print(format_timestamp(), SCHEMA_VERSION)"
```

Expected: Prints a timestamp and `1.1.0`.

- [ ] **Step 9: Commit**

```bash
git add scripts/utils.py scripts/check_github.py scripts/check_s3_version.py scripts/fetch_graphql.py scripts/summarize.py
git commit -m "refactor: Standardize timestamps via shared format_timestamp()"
```

---

### Task 2: Schema update for S3FileDiff v1.1.0

**Files:**
- Modify: `schema/openneuro-dashboard.yaml`

- [ ] **Step 1: Replace `S3FileDiff` and `DiffSummary` classes**

In `schema/openneuro-dashboard.yaml`, replace the `S3FileDiff` class (lines 233-280) and `DiffSummary` class (lines 282-299) with:

```yaml
  S3FileDiff:
    description: >-
      Comparison between S3 file listing and git tree.
      Only created when S3 is accessible (not 403).
      Diff semantics: "what needs to happen to bring S3 into sync with git."
    attributes:
      schemaVersion:
        range: string
        required: true
        description: Version of the schema used for this file
        pattern: "^[0-9]+\\.[0-9]+\\.[0-9]+$"
      datasetId:
        range: string
        required: true
        pattern: "^ds[0-9]{6}$"
      snapshotTag:
        range: string
        required: true
        description: Git tag compared against
      s3Version:
        range: string
        required: true
        description: Version tag that S3 was compared against
      checkedAt:
        range: datetime
        required: true
        description: When this comparison was performed
      status:
        range: CheckStatus
        required: true
        description: "ok if no differences, error if any"
      exportMissing:
        range: boolean
        required: false
        description: >-
          True if S3 has zero files (entire export is missing).
          When true, added, removed, and context will be empty arrays.
      totalS3Files:
        range: integer
        required: true
        description: Total number of files found in S3
      totalGitFiles:
        range: integer
        required: true
        description: Total number of files in the git tree
      added:
        range: string
        required: true
        multivalued: true
        inlined_as_list: true
        description: >-
          Files in git but not S3 (S3 needs these).
          Sorted for deterministic output.
      removed:
        range: string
        required: true
        multivalued: true
        inlined_as_list: true
        description: >-
          Files in S3 but not git (orphaned in S3).
          Sorted for deterministic output.
      context:
        range: string
        required: true
        multivalued: true
        inlined_as_list: true
        description: >-
          Neighboring files from the git tree within 3 sorted positions
          of any added/removed file. Used to reconstruct a unified diff
          view with context. Sorted for deterministic output.
```

Delete the `DiffSummary` class entirely (it's no longer needed).

- [ ] **Step 2: Commit**

```bash
git add schema/openneuro-dashboard.yaml
git commit -m "schema: Update S3FileDiff for v1.1.0 (added/removed/context)"
```

---

### Task 3: Update test data generator for new s3-diff format

**Files:**
- Modify: `scripts/gen_data/s3_diff.py`

- [ ] **Step 1: Rewrite `generate_s3_diff` function**

Replace the `generate_s3_diff` function in `scripts/gen_data/s3_diff.py` with:

```python
def compute_context(sorted_files: list[str], changed: set[str], radius: int = 3) -> list[str]:
    """Compute context files within `radius` positions of any changed file."""
    context = set()
    for i, f in enumerate(sorted_files):
        if f in changed:
            for j in range(max(0, i - radius), min(len(sorted_files), i + radius + 1)):
                neighbor = sorted_files[j]
                if neighbor not in changed:
                    context.add(neighbor)
    return sorted(context)


def generate_s3_diff(
    dataset_id: str, version: str, git_files: list[str], scenario: str
) -> dict:
    """Generate s3-diff.json in v1.1.0 format."""
    if scenario == "healthy":
        added = []
        removed = []
        total_s3 = len(git_files)
    elif scenario == "error":
        num_missing = random.randint(1, min(5, max(1, len(git_files) // 10)))
        added = sorted(random.sample(git_files, num_missing))
        removed = sorted([".DS_Store", "._sub-01_T1w.nii.gz", "Thumbs.db"][
            : random.randint(1, 3)
        ])
        total_s3 = len(git_files) - len(added) + len(removed)
    else:  # warning — treat as error in new schema (any diff = error)
        added = sorted([random.choice(git_files)])
        removed = []
        total_s3 = len(git_files) - 1

    changed = set(added) | set(removed)
    context = compute_context(git_files, changed) if changed else []

    return {
        "schemaVersion": SCHEMA_VERSION,
        "datasetId": dataset_id,
        "snapshotTag": version,
        "s3Version": version,
        "checkedAt": random_datetime(days_ago=1),
        "status": "ok" if not changed else "error",
        "exportMissing": False,
        "totalS3Files": total_s3,
        "totalGitFiles": len(git_files),
        "added": added,
        "removed": removed,
        "context": context,
    }
```

- [ ] **Step 2: Update `generate_s3_diffs` function call**

In the `generate_s3_diffs` function, update the call to `generate_s3_diff` (around line 114) to pass `dataset_id`:

```python
        s3_diff = generate_s3_diff(
            dataset_id, latest_snapshot, files_data["files"], scenario
        )
```

Remove the `metadata` loading (lines 105-106) since `gitHexsha` is no longer in the output. Also remove the `metadata["hexsha"]` argument.

- [ ] **Step 3: Test the generator**

```bash
cd scripts && uv run gen_data/graphql.py --num-datasets 5 && uv run gen_data/github.py && uv run gen_data/s3_version.py && uv run gen_data/git_tree.py && uv run gen_data/s3_diff.py && cat ../data/datasets/ds000001/s3-diff.json
```

Expected: JSON output with `added`, `removed`, `context` arrays and `schemaVersion: "1.1.0"`.

- [ ] **Step 4: Commit**

```bash
git add scripts/gen_data/s3_diff.py
git commit -m "feat: Update s3_diff test data generator for v1.1.0 format"
```

---

### Task 4: Update summarizer for new s3-diff format

**Files:**
- Modify: `scripts/summarize.py`

- [ ] **Step 1: Update s3-diff consumption logic**

In `scripts/summarize.py`, replace the s3 files check block (lines 80-93) with:

```python
        # S3 files check
        s3_diff = load_json_safe(dataset_dir / "s3-diff.json")
        if not s3_diff:
            checks["s3Files"] = "pending"
        elif s3_diff.get("exportMissing"):
            checks["s3Files"] = "error"
            last_checked["s3Files"] = s3_diff["checkedAt"]
        else:
            checks["s3Files"] = s3_diff.get("status", "pending")
            last_checked["s3Files"] = s3_diff["checkedAt"]
```

- [ ] **Step 2: Run the summarizer against test data**

```bash
cd scripts && uv run summarize.py && cat ../data/all-datasets.json | python -m json.tool | head -30
```

Expected: Summary JSON with `schemaVersion: "1.1.0"`, datasets with s3Files status from the new format.

- [ ] **Step 3: Commit**

```bash
git add scripts/summarize.py
git commit -m "feat: Update summarizer for s3-diff v1.1.0 format"
```

---

### Task 5: Implement `check_s3_files.py` — Stage 4

**Files:**
- Create: `scripts/check_s3_files.py`

This is the largest task. The script:
1. Loads registry, s3-version, and github data for each dataset
2. Applies preconditions to filter eligible datasets
3. Manages bare repo cache (clone on miss, fetch on tag miss)
4. Walks git tree with pygit2 to build file set
5. Lists S3 objects with unsigned aioboto3 to build file set
6. Computes diff (added/removed/context)
7. Writes s3-diff.json

- [ ] **Step 1: Create `scripts/check_s3_files.py` with inline deps and imports**

```python
#!/usr/bin/env python3
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "aioboto3>=13.4.0",
#     "pygit2>=1.17.0",
# ]
# ///
"""
Stage 4: Compare S3 file listing against git tree.

Manages a bare repo cache for git trees. Clones on cache miss,
fetches missing tags from existing repos.

Reads:
- data/datasets-registry.json
- data/datasets/{id}/s3-version.json
- data/datasets/{id}/github.json

Writes:
- data/datasets/{id}/s3-diff.json
"""

import argparse
import asyncio
from asyncio.subprocess import PIPE
from dataclasses import dataclass, field
from datetime import datetime, UTC, timedelta
from pathlib import Path

import aioboto3
import pygit2
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig

from utils import SCHEMA_VERSION, write_json, load_json, format_timestamp


S3_BUCKET = "openneuro.org"
S3_REGION = "us-east-1"
GITHUB_BASE = "https://github.com/OpenNeuroDatasets"
STALE_DAYS = 7
```

- [ ] **Step 2: Add subprocess helper and precondition logic**

Append to `check_s3_files.py`:

```python
@dataclass
class SubprocessResult:
    args: tuple[str, ...]
    returncode: int
    stdout: bytes = field(repr=False)
    stderr: bytes = field(repr=False)


async def git(*args: str, cwd: Path | None = None) -> SubprocessResult:
    """Run a git command and return the result."""
    args_tuple = ("git", *args)
    proc = await asyncio.create_subprocess_exec(
        *args_tuple, stdout=PIPE, stderr=PIPE, cwd=cwd
    )
    stdout, stderr = await proc.communicate()
    assert proc.returncode is not None
    return SubprocessResult(
        args=args_tuple, returncode=proc.returncode, stdout=stdout, stderr=stderr
    )


def load_json_safe(path: Path) -> dict | None:
    """Load JSON file if it exists, return None otherwise."""
    if path.exists():
        return load_json(path)
    return None


def is_eligible(
    dataset_id: str,
    s3_version: dict | None,
    github: dict | None,
    existing_diff: dict | None,
) -> bool:
    """Check preconditions for running the S3 file diff."""
    if not s3_version:
        return False

    # Skip if S3 is blocked (403) or has no extracted version
    if not s3_version.get("accessible", True):
        return False
    extracted = s3_version.get("extractedVersion")
    if not extracted:
        return False

    # Skip if tag not available on GitHub
    if not github:
        return False
    if extracted not in github.get("tags", {}):
        return False

    # Incremental skip logic
    if existing_diff and existing_diff.get("s3Version") == extracted:
        checked_at = existing_diff.get("checkedAt", "")
        try:
            checked_time = datetime.fromisoformat(checked_at.replace("Z", "+00:00"))
            age = datetime.now(UTC) - checked_time
            if age < timedelta(days=STALE_DAYS):
                return False
            # Older than STALE_DAYS: re-run regardless
        except (ValueError, TypeError):
            pass  # Can't parse timestamp — re-run

    return True
```

- [ ] **Step 3: Add git cache management functions**

Append to `check_s3_files.py`:

```python
async def ensure_tag_cached(
    dataset_id: str, tag: str, cache_dir: Path, semaphore: asyncio.Semaphore
) -> bool:
    """Ensure the bare repo cache has the needed tag. Returns True on success."""
    repo_path = cache_dir / f"{dataset_id}.git"

    async with semaphore:
        if not repo_path.exists():
            # Clone bare repo
            result = await git(
                "clone", "--bare", "--filter=blob:none", "--depth=1",
                "--branch", tag,
                f"{GITHUB_BASE}/{dataset_id}.git",
                str(repo_path),
            )
            if result.returncode != 0:
                print(f"✗ {dataset_id}: git clone failed: {result.stderr.decode().strip()}")
                return False
            return True

        # Repo exists — check if tag is present
        try:
            repo = await asyncio.to_thread(pygit2.Repository, str(repo_path))
            ref_name = f"refs/tags/{tag}"
            has_tag = await asyncio.to_thread(lambda: ref_name in repo.references)
            if has_tag:
                return True
        except Exception as e:
            print(f"⚠ {dataset_id}: Error checking repo: {e}")

        # Fetch the missing tag
        result = await git(
            "fetch", "--refetch", "--filter=blob:none", "--depth=1",
            "origin", "tag", tag,
            cwd=repo_path,
        )
        if result.returncode != 0:
            print(f"✗ {dataset_id}: git fetch failed: {result.stderr.decode().strip()}")
            return False

        return True
```

- [ ] **Step 4: Add git tree walking function**

Append to `check_s3_files.py`:

```python
def walk_git_tree(repo_path: Path, tag: str) -> set[str]:
    """Walk the git tree at the given tag and return all file paths as a set."""
    repo = pygit2.Repository(str(repo_path))
    ref = repo.references.get(f"refs/tags/{tag}")
    if ref is None:
        raise ValueError(f"Tag {tag} not found in {repo_path}")

    commit = ref.peel(pygit2.Commit)
    tree = commit.tree

    files = set()

    def _walk(tree_obj, prefix=""):
        for entry in tree_obj:
            path = entry.name if not prefix else f"{prefix}/{entry.name}"
            if entry.type == pygit2.GIT_OBJECT_TREE:
                _walk(repo.get(entry.id), path)
            else:
                files.add(path)

    _walk(tree)
    return files
```

- [ ] **Step 5: Add S3 listing function**

Append to `check_s3_files.py`:

```python
async def list_s3_files(
    dataset_id: str, semaphore: asyncio.Semaphore
) -> set[str] | None:
    """List all S3 objects under the dataset prefix. Returns file paths as a set."""
    prefix = f"{dataset_id}/"

    async with semaphore:
        session = aioboto3.Session()
        async with session.client(
            "s3",
            region_name=S3_REGION,
            config=BotoConfig(signature_version=UNSIGNED),
        ) as s3:
            files = set()
            paginator = s3.get_paginator("list_objects_v2")
            try:
                async for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
                    for obj in page.get("Contents", []):
                        key = obj["Key"]
                        # Strip the dataset prefix
                        rel_path = key[len(prefix):]
                        if rel_path:
                            files.add(rel_path)
            except Exception as e:
                print(f"✗ {dataset_id}: S3 listing failed: {e}")
                return None

            return files
```

- [ ] **Step 6: Add diff computation and context functions**

Append to `check_s3_files.py`:

```python
def compute_context(sorted_files: list[str], changed: set[str], radius: int = 3) -> list[str]:
    """Compute context files within `radius` sorted positions of any changed file."""
    context = set()
    for i, f in enumerate(sorted_files):
        if f in changed:
            for j in range(max(0, i - radius), min(len(sorted_files), i + radius + 1)):
                neighbor = sorted_files[j]
                if neighbor not in changed:
                    context.add(neighbor)
    return sorted(context)


def compute_diff(
    dataset_id: str, tag: str, s3_version: str,
    git_files: set[str], s3_files: set[str]
) -> dict:
    """Compute the S3 file diff and return the s3-diff.json data."""
    added = sorted(git_files - s3_files)
    removed = sorted(s3_files - git_files)

    changed = set(added) | set(removed)
    sorted_git = sorted(git_files)
    context = compute_context(sorted_git, changed) if changed else []

    export_missing = len(s3_files) == 0
    status = "ok" if not added and not removed else "error"

    return {
        "schemaVersion": SCHEMA_VERSION,
        "datasetId": dataset_id,
        "snapshotTag": tag,
        "s3Version": s3_version,
        "checkedAt": format_timestamp(),
        "status": status,
        "exportMissing": export_missing,
        "totalS3Files": len(s3_files),
        "totalGitFiles": len(git_files),
        "added": added,
        "removed": removed,
        "context": context,
    }
```

- [ ] **Step 7: Add per-dataset processing function**

Append to `check_s3_files.py`:

```python
async def process_dataset(
    dataset_id: str,
    s3_version_data: dict,
    output_dir: Path,
    cache_dir: Path,
    git_semaphore: asyncio.Semaphore,
    s3_semaphore: asyncio.Semaphore,
    verbose: bool = False,
) -> bool:
    """Process a single dataset: ensure cache, get git tree, list S3, compute diff."""
    tag = s3_version_data["extractedVersion"]

    # Step 1: Ensure git cache
    if not await ensure_tag_cached(dataset_id, tag, cache_dir, git_semaphore):
        return False

    # Step 2: Get git file tree (blocking I/O in thread)
    repo_path = cache_dir / f"{dataset_id}.git"
    try:
        git_files = await asyncio.to_thread(walk_git_tree, repo_path, tag)
    except Exception as e:
        print(f"✗ {dataset_id}: Failed to walk git tree: {e}")
        return False

    # Step 3: List S3 files
    s3_files = await list_s3_files(dataset_id, s3_semaphore)
    if s3_files is None:
        return False

    # Step 4: Compute diff
    diff = compute_diff(dataset_id, tag, tag, git_files, s3_files)

    # Step 5: Write result
    dataset_dir = output_dir / "datasets" / dataset_id
    write_json(dataset_dir / "s3-diff.json", diff)

    if verbose:
        added_count = len(diff["added"])
        removed_count = len(diff["removed"])
        if added_count or removed_count:
            print(f"✗ {dataset_id}: +{added_count} -{removed_count} ({diff['status']})")
        else:
            print(f"✓ {dataset_id}: ok")

    return True
```

- [ ] **Step 8: Add main orchestration and CLI**

Append to `check_s3_files.py`:

```python
async def check_all_datasets(
    output_dir: Path,
    cache_dir: Path,
    git_concurrency: int = 10,
    s3_concurrency: int = 20,
    verbose: bool = False,
) -> None:
    """Check S3 files for all eligible datasets."""
    print("Checking S3 files...")

    cache_dir.mkdir(parents=True, exist_ok=True)

    # Load registry
    registry = load_json(output_dir / "datasets-registry.json")
    datasets = registry["latestSnapshots"]
    total = len(datasets)

    # Filter eligible datasets
    eligible = []
    for dataset_id in datasets:
        dataset_dir = output_dir / "datasets" / dataset_id
        s3_version = load_json_safe(dataset_dir / "s3-version.json")
        github = load_json_safe(dataset_dir / "github.json")
        existing_diff = load_json_safe(dataset_dir / "s3-diff.json")

        if is_eligible(dataset_id, s3_version, github, existing_diff):
            eligible.append((dataset_id, s3_version))

    print(f"Found {len(eligible)}/{total} eligible datasets")

    if not eligible:
        print("Nothing to do.")
        return

    git_semaphore = asyncio.Semaphore(git_concurrency)
    s3_semaphore = asyncio.Semaphore(s3_concurrency)

    success = 0
    failed = 0

    # Process datasets concurrently
    async def process_with_progress(dataset_id, s3_version_data, index):
        nonlocal success, failed
        ok = await process_dataset(
            dataset_id, s3_version_data, output_dir, cache_dir,
            git_semaphore, s3_semaphore, verbose,
        )
        if ok:
            success += 1
        else:
            failed += 1

        if not verbose and (index + 1) % 50 == 0:
            print(f"  Progress: {index + 1}/{len(eligible)}")

    tasks = [
        process_with_progress(dataset_id, s3_version, i)
        for i, (dataset_id, s3_version) in enumerate(eligible)
    ]
    await asyncio.gather(*tasks)

    print(f"\n✓ S3 file check complete")
    print(f"  Success: {success}/{len(eligible)}")
    print(f"  Failed: {failed}/{len(eligible)}")
    print(f"  Skipped: {total - len(eligible)}/{total}")


def main():
    parser = argparse.ArgumentParser(
        description="Check S3 files against git tree for OpenNeuro datasets"
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data"),
        help="Output directory (default: data)",
    )
    parser.add_argument(
        "--cache-dir", type=Path,
        default=Path.home() / ".cache" / "openneuro-dashboard" / "repos",
        help="Bare repo cache directory",
    )
    parser.add_argument(
        "--git-concurrency", type=int, default=10,
        help="Max concurrent git operations (default: 10)",
    )
    parser.add_argument(
        "--s3-concurrency", type=int, default=20,
        help="Max concurrent S3 requests (default: 20)",
    )
    parser.add_argument(
        "--validate", action="store_true",
        help="Validate results after checking",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    asyncio.run(
        check_all_datasets(
            args.output_dir, args.cache_dir,
            args.git_concurrency, args.s3_concurrency,
            args.verbose,
        )
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 9: Test with generated data**

First generate test data, then run the check to verify structure:

```bash
cd scripts && uv run gen_data/graphql.py --num-datasets 3 && uv run gen_data/github.py && uv run gen_data/s3_version.py
```

Then verify the script loads and parses arguments:

```bash
cd scripts && uv run check_s3_files.py --output-dir ../data --verbose --help
```

Expected: Help text showing all CLI flags.

- [ ] **Step 10: Commit**

```bash
git add scripts/check_s3_files.py
git commit -m "feat: Add check_s3_files.py — S3 file diff with git cache"
```

---

### Task 6: Update frontend diff viewer for new data shape

**Files:**
- Modify: `dataset.html`
- Modify: `js/dataset.js`

- [ ] **Step 1: Replace diff detail HTML in `dataset.html`**

Replace the `s3-diff-container` div (lines 149-200 of `dataset.html`) with:

```html
                    <div id="s3-diff-container" style="display: none;">
                        <details id="s3-diff-details">
                            <summary>Show file differences</summary>
                            <div class="details-content">
                                <div class="diff-summary">
                                    <div class="diff-stat">
                                        <strong>Git Files:</strong>
                                        <span id="diff-total-git">...</span>
                                    </div>
                                    <div class="diff-stat">
                                        <strong>S3 Files:</strong>
                                        <span id="diff-total-s3">...</span>
                                    </div>
                                    <div class="diff-stat error-stat">
                                        <strong>Missing from S3:</strong>
                                        <span id="diff-added">...</span>
                                    </div>
                                    <div class="diff-stat error-stat">
                                        <strong>Orphaned in S3:</strong>
                                        <span id="diff-removed">...</span>
                                    </div>
                                </div>

                                <div class="diff-viewer-container">
                                    <div class="diff-legend">
                                        <div class="diff-legend-item">
                                            <span class="diff-legend-marker added"></span>
                                            Need to add to S3
                                        </div>
                                        <div class="diff-legend-item">
                                            <span class="diff-legend-marker removed"></span>
                                            Remove from S3
                                        </div>
                                        <div class="diff-legend-item">
                                            <span class="diff-legend-marker unchanged"></span>
                                            Context
                                        </div>
                                    </div>
                                    <div class="diff-viewer" id="diff-viewer"></div>
                                </div>

                                <div class="detail-row">
                                    <strong>Snapshot Tag:</strong>
                                    <code id="diff-snapshot-tag">...</code>
                                </div>
                                <div class="detail-row">
                                    <strong>S3 Version:</strong>
                                    <code id="diff-s3-version">...</code>
                                </div>
                                <div class="detail-row">
                                    <strong>Last Checked:</strong>
                                    <span id="s3-files-last-checked">...</span>
                                </div>
                            </div>
                        </details>
                    </div>
```

- [ ] **Step 2: Update `getS3FilesStatus` in `js/dataset.js`**

Replace the `getS3FilesStatus` function (lines 397-407) with:

```javascript
function getS3FilesStatus() {
    if (!s3Version) return 'pending';
    if (s3Version.extractedVersion !== latestSnapshot) return 'version-mismatch';
    if (!s3Diff) return 'pending';
    return s3Diff.status || 'pending';
}
```

- [ ] **Step 3: Update `renderS3FilesCheck` in `js/dataset.js`**

Replace the `renderS3FilesCheck` function (lines 313-392) with:

```javascript
function renderS3FilesCheck() {
    const status = getS3FilesStatus();
    const icon = document.getElementById('s3-files-icon');

    // Check if blocked by 403
    if (s3Version && !s3Version.accessible) {
        icon.textContent = '🔒';
        icon.className = 'check-icon-large error';
        document.getElementById('s3-files-summary').textContent =
            'File check blocked - S3 access denied (see S3 Version Status above)';
        document.getElementById('s3-diff-container').style.display = 'none';
        return;
    }

    icon.textContent = status === 'ok' ? '✓' : status === 'error' ? '✗' : status === 'version-mismatch' ? '≠' : '⏳';
    icon.className = `check-icon-large ${status}`;

    if (!s3Version) {
        document.getElementById('s3-files-summary').textContent = 'S3 version check not yet run';
        return;
    }

    if (!s3Diff) {
        document.getElementById('s3-files-summary').textContent = 'File comparison not yet run';
        return;
    }

    // Check for export missing
    if (s3Diff.exportMissing) {
        const summary = `No files found on S3 (0/${s3Diff.totalGitFiles}). ` +
                       `The dataset export appears to be completely missing and needs to be regenerated.`;
        document.getElementById('s3-files-summary').textContent = summary;
        document.getElementById('s3-diff-container').style.display = 'none';
        return;
    }

    // Normal case: show file comparison
    const addedCount = s3Diff.added.length;
    const removedCount = s3Diff.removed.length;

    let summary = '';
    if (status === 'ok') {
        summary = `✓ All ${s3Diff.totalGitFiles} files match between Git and S3.`;
    } else {
        summary = `✗ File mismatch: ${addedCount} file${addedCount !== 1 ? 's' : ''} missing from S3, ` +
                 `${removedCount} extra file${removedCount !== 1 ? 's' : ''} in S3.`;
    }
    document.getElementById('s3-files-summary').textContent = summary;

    // Show diff details
    document.getElementById('s3-diff-container').style.display = 'block';

    // Populate stats
    document.getElementById('diff-total-git').textContent = s3Diff.totalGitFiles;
    document.getElementById('diff-total-s3').textContent = s3Diff.totalS3Files;
    document.getElementById('diff-added').textContent = addedCount;
    document.getElementById('diff-removed').textContent = removedCount;

    // Render unified diff viewer
    if (addedCount > 0 || removedCount > 0) {
        renderDiffViewer(s3Diff);
    }

    // Other details
    document.getElementById('diff-snapshot-tag').textContent = s3Diff.snapshotTag;
    document.getElementById('diff-s3-version').textContent = s3Diff.s3Version;
    document.getElementById('s3-files-last-checked').textContent = formatDate(s3Diff.checkedAt);
}
```

- [ ] **Step 4: Rewrite `renderDiffViewer` in `js/dataset.js`**

Replace the `renderDiffViewer` function (lines 412-538) with:

```javascript
/**
 * Render unified diff viewer from added/removed/context lists
 */
function renderDiffViewer(diff) {
    const viewer = document.getElementById('diff-viewer');

    // Build unified diff: sort by filename, prefixed with +/-/space
    const unified = [
        ...diff.added.map(f => ({ file: f, type: 'added' })),
        ...diff.removed.map(f => ({ file: f, type: 'removed' })),
        ...diff.context.map(f => ({ file: f, type: 'context' })),
    ].sort((a, b) => a.file.localeCompare(b.file));

    // Group into sections with collapsible context gaps
    const CONTEXT_SIZE = 3;
    const lines = [];
    let contextBuffer = [];

    function flushContext() {
        if (contextBuffer.length === 0) return;

        if (contextBuffer.length > CONTEXT_SIZE * 2) {
            // Large gap — show first/last CONTEXT_SIZE, collapse middle
            lines.push(...contextBuffer.slice(0, CONTEXT_SIZE));
            const hidden = contextBuffer.slice(CONTEXT_SIZE, -CONTEXT_SIZE);
            if (hidden.length > 0) {
                lines.push({ type: 'collapse', count: hidden.length, files: hidden });
            }
            lines.push(...contextBuffer.slice(-CONTEXT_SIZE));
        } else {
            lines.push(...contextBuffer);
        }
        contextBuffer = [];
    }

    for (const entry of unified) {
        if (entry.type === 'context') {
            contextBuffer.push(entry);
        } else {
            flushContext();
            lines.push(entry);
        }
    }
    flushContext();

    // Render HTML
    let html = '';
    let collapseId = 0;

    for (const line of lines) {
        if (line.type === 'collapse') {
            const id = `collapse-${collapseId++}`;
            html += `
                <button class="diff-collapse" data-target="${id}">
                    ${line.count} unchanged file${line.count !== 1 ? 's' : ''}
                </button>
                <div class="diff-section" id="${id}">
                    ${line.files.map(f =>
                        `<div class="diff-line context">  ${escapeHtml(f.file)}</div>`
                    ).join('')}
                </div>
            `;
        } else {
            const prefix = line.type === 'added' ? '+ ' : line.type === 'removed' ? '- ' : '  ';
            html += `<div class="diff-line ${line.type}">${prefix}${escapeHtml(line.file)}</div>`;
        }
    }

    viewer.innerHTML = html;

    // Setup collapse/expand handlers
    viewer.querySelectorAll('.diff-collapse').forEach(button => {
        button.addEventListener('click', () => {
            const targetId = button.dataset.target;
            const section = document.getElementById(targetId);
            section.classList.toggle('expanded');
            button.classList.toggle('expanded');
        });
    });
}
```

- [ ] **Step 5: Test with generated data**

```bash
cd scripts && uv run gen_data/graphql.py --num-datasets 5 && uv run gen_data/github.py && uv run gen_data/s3_version.py && uv run gen_data/git_tree.py && uv run gen_data/s3_diff.py --seed 42 && uv run summarize.py
```

Then serve and visually verify:

```bash
python -m http.server 8000
```

Open `http://localhost:8000/dataset.html?id=ds000001` and check that:
- The S3 Files Status section renders
- If there's a diff, the unified diff viewer shows with +/- lines and context
- Collapsible sections work

- [ ] **Step 6: Commit**

```bash
git add dataset.html js/dataset.js
git commit -m "feat: Update diff viewer for unified added/removed/context format"
```

---

### Task 7: Frontend error boundaries

**Files:**
- Modify: `js/main.js`
- Modify: `js/dataset.js`

- [ ] **Step 1: Verify `index.html` error handling**

`index.html` already has an error div (`id="error"`) and `main.js` already catches errors in `init()` and shows the error div (lines 60-65). This is already sufficient — if `all-datasets.json` fails to load, the error message is shown. No changes needed for `index.html` or `main.js`.

- [ ] **Step 2: Improve `dataset.html` error handling for missing `snapshots.json`**

In `js/dataset.js`, the `init()` function already catches errors and calls `showError()`. However, if `snapshots.json` fails to load (line 52 doesn't have `.catch()`), it will throw and show a generic error. This is acceptable — snapshots are required data.

The `.catch(() => null)` on lines 53-55 for github, s3Version, and s3Diff already handles missing optional data gracefully.

No changes needed — the existing error handling is sufficient.

- [ ] **Step 3: Commit (if any changes were made)**

If no changes were needed, skip this commit. The error boundaries are already in place.

---

### Task 8: Add `--validate` flag to `fetch_graphql.py`

**Files:**
- Modify: `scripts/fetch_graphql.py`

- [ ] **Step 1: Add `--validate` argument to the parser**

In `scripts/fetch_graphql.py`, add after line 317:

```python
    parser.add_argument(
        "--validate", action="store_true",
        help="Validate output data after fetching",
    )
```

- [ ] **Step 2: Add validation function**

Add before `main()`:

```python
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

        # Check snapshots.json exists
        snapshots_path = dataset_dir / "snapshots.json"
        if not snapshots_path.exists():
            issues.append(f"{dataset_id}: snapshots.json missing")
            continue

        with open(snapshots_path) as f:
            snapshots = json.load(f)

        # Check latest snapshot is in tags list
        if latest_snapshot not in snapshots["tags"]:
            issues.append(f"{dataset_id}: latest {latest_snapshot} not in tags")

        # Check metadata exists for each tag
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
```

- [ ] **Step 3: Wire up validation in `main()`**

Add after the `asyncio.run(...)` call in `main()`:

```python
    if args.validate:
        validate_output(args.output_dir)
```

- [ ] **Step 4: Commit**

```bash
git add scripts/fetch_graphql.py
git commit -m "feat: Add --validate flag to fetch_graphql.py"
```

---

### Task 9: GitHub Actions CI/CD workflow

**Files:**
- Create: `.github/workflows/update-data.yml`

- [ ] **Step 1: Create the workflow file**

```yaml
name: Update Dashboard Data

on:
  schedule:
    - cron: '0 6 * * *'  # Daily at 6am UTC
  workflow_dispatch:

permissions:
  contents: write

concurrency:
  group: update-data
  cancel-in-progress: false

jobs:
  update:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v6

      - name: Set up Python
        run: uv python install 3.13

      - name: Restore bare repo cache
        uses: actions/cache@v4
        with:
          path: ~/.cache/openneuro-dashboard/repos
          key: git-repos-${{ github.run_id }}
          restore-keys: git-repos-

      - name: Fetch GraphQL data
        run: uv run scripts/fetch_graphql.py --output-dir data

      - name: Check GitHub mirrors
        run: uv run scripts/check_github.py --output-dir data

      - name: Check S3 versions
        run: uv run scripts/check_s3_version.py --output-dir data

      - name: Check S3 files
        run: uv run scripts/check_s3_files.py --output-dir data --cache-dir ~/.cache/openneuro-dashboard/repos

      - name: Generate summary
        run: uv run scripts/summarize.py --output-dir data

      - name: Commit and push if changed
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add data/
          if git diff --staged --quiet; then
            echo "No changes to commit"
          else
            git commit -m "chore: Update dashboard data $(date -u +%Y-%m-%d)"
            git push
          fi
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/update-data.yml
git commit -m "ci: Add daily data update workflow"
```

---

### Task 10: Update CLAUDE.md

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Update pipeline description**

In `CLAUDE.md`, update the pipeline description to reflect 5 stages and the new script:

Change the pipeline diagram from:
```
fetch_graphql.py → check_github.py → check_s3_version.py → [git trees] → [s3 diff] → summarize.py
```

To:
```
fetch_graphql.py → check_github.py → check_s3_version.py → check_s3_files.py → summarize.py
```

Update the stage count from "6-stage ETL, stages 4-5 not yet implemented" to "5-stage ETL".

Remove references to "test data generators substitute for unimplemented stages" since stage 4 is now implemented.

Add `check_s3_files.py` to the "Running Scripts" section:
```bash
uv run scripts/check_s3_files.py --cache-dir ~/.cache/openneuro-dashboard/repos
```

- [ ] **Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: Update CLAUDE.md for 5-stage pipeline"
```
