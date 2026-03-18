# Dashboard Completion Design

Complete the OpenNeuro Dashboard with the S3 file diff pipeline stage, code quality fixes, and CI/CD scheduling.

## Context

OpenNeuro hosts git/git-annex datasets. The lifecycle of a dataset is:

* User uploads data in embargoed (non-public) mode, and may make edits to the dataset.
* When the user creates a new version (or snapshot):
  * A persistent URL (DOI) is minted of the form doi:10.18112/openneuro.{id}.v{version}
  * The DOI is embedded in dataset_description.json, in the `DatasetDOI` field
  * A git tag is created with the given version number
  * The tag is exported to S3, producing a mirror of the dataset, both annexed and unannexed data as versioned keys.
    These keys are also tagged private, causing unauthenticated fetches to return a 403 Forbidden error.
* When the dataset is published (taken out of embargo)
  * A GitHub mirror is created: https://github.com/OpenNeuroDatasets/{id}
  * The S3 keys are untagged, allowing them to be downloaded without authentication.
  * The latest tag is pushed as `main`, and the `git-annex` branch and all other tags are also synced
  * Future S3 exports disable tagging exported keys as private.
  * Future snapshots add a GitHub synchronization step after the S3 export, updating `main` and tags.

The OpenNeuro GraphQL API can return all snapshots of all public datasets.
If all snapshots are published to GitHub, then we can use the GitHub dataset as a cryptographically
verified source of truth.
We have identified several failure modes:

* Unsynced GitHub mirror. May indicate large objects in datasets that cannot be pushed, or a failure in an earlier export stage.
* Additional keys in S3 listing, indicating a failure of the export process to mark removed files as deleted.
* 403 errors on public keys, indicating a failure to untag keys during publication.

Because OpenNeuro's processes have evolved over time, datasets created or updated in different periods experience different issues. The oldest datasets have long hexadecimal tags, the next oldest 5-digit serial number tags, and finally `<major>.<minor>.<patch>` tags. Some datasets do not have DOIs embedded.

This dashboard aims to summarize and monitor the synchronization of OpenNeuro datasets across its GraphQL API, GitHub mirrors, and S3 exports. The pipeline has 5 stages: stages 1-3 (GraphQL fetch, GitHub check, S3 version check) and stage 5 (summarize) are implemented. Stage 4 (S3 file diff with integrated git cache) exists only as a test data generator. The frontend is complete but expects data from the unimplemented stage and lacks error boundaries.

Reference implementation: [OpenNeuroOrg/ondiagnostics](https://github.com/OpenNeuroOrg/ondiagnostics) — an async pipeline that clones bare repos and compares S3 files for automated cleanup. This dashboard differs in that it is read-only: it presents diagnostic information for human review rather than performing destructive actions.

## Stage 4: `check_s3_files.py`

Compare S3 file listing against git tree at the resolved tag. Manages its own bare repo cache — clones on miss, fetches missing tags, skips when present.

### Inputs

- `datasets-registry.json` — list of all datasets
- `datasets/{id}/s3-version.json` — resolved S3 version and accessibility status
- `datasets/{id}/github.json` — GitHub mirror status, including available tags

### Preconditions (per dataset)

Skip the dataset if any of these are true:
- S3 is blocked (case 3, HTTP 403) or not found (case 4) — no file diff is meaningful without S3 access.
- The resolved tag (from `extractedVersion` in `s3-version.json`) is not present in `github.json` tags — the tag can't be cloned if GitHub doesn't have it.
- An existing `s3-diff.json` can be skipped if:
  - It has the same `s3Version` as the current `s3-version.json`, AND
  - The diff is empty (`added` and `removed` are both empty), OR
  - The `checkedAt` timestamp is less than 7 days old.
  - Empty diffs older than 7 days are re-run to ensure new errors do not go undetected.

### Logic

For each eligible dataset:

1. **Ensure bare repo cache has the needed tag:**
   - Resolve `{tag}` from the `extractedVersion` field in `s3-version.json`.
   - **No cached repo:** `git clone --bare --filter=blob:none --depth=1 --branch {tag} https://github.com/OpenNeuroDatasets/{id}.git {cache_dir}/{id}.git`
   - **Repo exists, tag missing:** `git fetch --refetch --filter=blob:none --depth=1 origin tag {tag}`
   - **Tag already present:** proceed to diff.

2. **Build git file set:** Open bare repo with `pygit2`, resolve tag, walk tree to build a set of file paths.

3. **Build S3 file set:** List S3 objects under `{id}/` prefix using unsigned `aioboto3` requests. Strip prefix. Build a set.

4. **Compute diff:**
   - `added` = git files not in S3 (S3 needs these)
   - `removed` = S3 files not in git (orphaned in S3)

5. **Compute context:** Sort all git files. For each file in `added` or `removed`, find its position in the sorted list and collect up to 3 files on each side. Context = union of all neighbors minus the added/removed files themselves.

6. **Write `s3-diff.json`.**

### Diff semantics

The diff represents "what needs to happen to bring S3 into sync with git":
- `+` (added): file exists in git but not S3 — S3 is missing it
- `-` (removed): file exists in S3 but not git — S3 has an orphan

### Data structures

- Use sets for git and S3 file lists — O(1) membership testing for diff computation.
- Sort only when computing context neighbors (positional lookup in sorted list).
- Output lists are sorted for deterministic output.

### Output

`datasets/{id}/s3-diff.json`:

```json
{
  "schemaVersion": "1.1.0",
  "datasetId": "ds000001",
  "snapshotTag": "1.0.0",
  "s3Version": "1.0.0",
  "checkedAt": "2026-03-17T00:00:00.000Z",
  "status": "ok",
  "exportMissing": false,
  "totalS3Files": 142,
  "totalGitFiles": 140,
  "added": [],
  "removed": [],
  "context": []
}
```

Status: `ok` if no differences, `error` if any.

### CLI

- `--cache-dir` — bare repo cache location (default `~/.cache/openneuro-dashboard/repos`)
- `--output-dir` — data directory (default `data/`)
- `--validate` — verify output consistency

### Concurrency

- Asyncio semaphore (default 10) for git clone/fetch operations
- Asyncio semaphore (default 20) for S3 list requests
- `pygit2` tree walks in thread pool via `asyncio.to_thread` (same pattern as ondiagnostics)

## Frontend changes

### Diff viewer

Update `renderS3FilesCheck` and `renderDiffViewer` in `js/dataset.js` to consume the new `s3-diff.json` shape (`added`/`removed`/`context` lists instead of `inGitOnly`/`inS3Only` with `summary` object).

The display reconstructs the unified diff by merging the three lists:

```javascript
const unified = sorted(
  [...added.map(f => '+' + f), ...removed.map(f => '-' + f), ...context.map(f => ' ' + f)],
  (a, b) => a.slice(1).localeCompare(b.slice(1))
);
```

Render as a single sorted list with:
- Green highlight + left border for `+` lines (need to add to S3)
- Red highlight + left border + strikethrough for `-` lines (remove from S3)
- Muted text for context lines

Collapse gaps of unchanged context between distant changes into expandable sections (existing `diff-collapse` / `diff-section` CSS classes support this).

### Error boundaries

- `index.html`: if `all-datasets.json` fails to load, show an error message in the table area instead of leaving the UI in a loading state.
- `dataset.html`: show "data not available" states for missing per-dataset files instead of silent failures.
- Handle partial data gracefully — `s3-diff.json` not yet generated shows "not yet checked."

## Summarizer update

Update `summarize.py` to consume the new `s3-diff.json` format:
- Read `added`/`removed` list lengths instead of `summary.inGitOnly`/`summary.inS3Only` counts.
- Use `checkedAt` instead of `lastChecked`.
- Use `status` field directly instead of recomputing from summary counts.

## Code quality fixes

### Timestamp standardization

Add a shared timestamp formatting function to `scripts/utils.py`:

```python
def format_timestamp() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")
```

Replace inconsistent formatting across scripts:
- `fetch_graphql.py`: uses `.strftime(...)` (correct format, but inline)
- `check_github.py`: uses `.isoformat()` (includes microseconds — wrong)
- `check_s3_version.py`: uses `.strftime(...)` (correct format, but inline)

### Validation

Add `--validate` flag to `fetch_graphql.py` (currently missing; other stages already have it).

## Schema update

Bump `schemaVersion` from `1.0.0` to `1.1.0`.

Changes to `S3FileDiff` class in `schema/openneuro-dashboard.yaml`:
- Remove: `summary` object, `inGitOnly`, `inS3Only`, `gitHexsha`, `lastChecked`
- Add: `added` (list of strings), `removed` (list of strings), `context` (list of strings), `totalS3Files` (int), `totalGitFiles` (int), `snapshotTag` (string), `checkedAt` (string), `status` (enum)

Update test data generators (`scripts/gen_data/s3_diff.py`) to produce the new format.

Update `scripts/utils.py:SCHEMA_VERSION` to `"1.1.0"`.

## CI/CD

### GitHub Actions workflow

File: `.github/workflows/update-data.yml`

**Triggers:**
- `schedule: cron: '0 6 * * *'` (daily at 6am UTC)
- `workflow_dispatch` (manual)

**Steps:**
1. Checkout repository
2. Set up Python 3.13+ and `uv`
3. Restore actions/cache for `~/.cache/openneuro-dashboard/repos`
4. Run pipeline stages sequentially:
   - `uv run scripts/fetch_graphql.py`
   - `uv run scripts/check_github.py`
   - `uv run scripts/check_s3_version.py`
   - `uv run scripts/check_s3_files.py --cache-dir ~/.cache/openneuro-dashboard/repos`
   - `uv run scripts/summarize.py`
5. Save actions/cache (bare repos)
6. Commit and push updated `data/` (only if changes exist)

**Secrets:** None required — all sources are public (unsigned S3, public GraphQL API, public GitHub repos).
