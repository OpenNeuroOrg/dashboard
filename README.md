# OpenNeuro Dataset Monitor

A dashboard for tracking synchronization status of OpenNeuro datasets across GraphQL API, GitHub mirrors, and S3 exports.

## Architecture

### Data Pipeline

The monitoring system uses a multi-stage pipeline that generates static JSON files consumed by a client-side dashboard:

```
GraphQL → GitHub Check → S3 Version → Git Trees → S3 Diff → Summarize → Dashboard
```

Each stage reads from previous stages and writes new check files, allowing incremental updates and independent execution.

### Data Model

All data files (aspirationally) follow a versioned schema defined in `schema/openneuro-dashboard.yaml` (LinkML format).
This is not yet validated.

#### Registry Files

- **`data/datasets-registry.json`**: Master registry from GraphQL
  - Maps dataset IDs to latest snapshot tags
  - Source of truth for what datasets exist

- **`data/all-datasets.json`**: Pre-computed summary for dashboard
  - Aggregates all check results
  - Includes per-dataset status and timestamps

#### Per-Dataset Files

```
data/datasets/{id}/
├── snapshots.json              # List of all snapshot tags
├── github.json                 # GitHub mirror status (branches, tags, HEAD)
├── s3-version.json             # Version from S3 dataset_description.json
├── s3-diff.json                # File differences (only if S3 accessible)
└── snapshots/{tag}/
    ├── metadata.json           # Snapshot metadata (SHA, creation date)
    └── files.json              # Complete file list from git tree
```

### Check Logic

#### GitHub Check
- Uses `git ls-remote --symref` to fetch all refs
- Validates:
  - All snapshot tags exist on GitHub
  - HEAD points to latest snapshot
  - Commit SHAs match GraphQL data

#### S3 Version Check
- Fetches `dataset_description.json` from S3
- Extracts version from `DatasetDOI` field
- **Edge cases**:
  1. **Normal**: DOI with correct dataset ID and version
  2. **Assumed latest**: Missing/custom DOI → use latest snapshot for comparison
  3. **Blocked (403)**: Access denied → no file comparison possible
  4. **Not found (404)**: Missing file → use latest snapshot for comparison

- Only 403 errors block further validation
- All other cases allow file comparison with assumed version

#### S3 File Diff
- Compares S3 file listing against git tree
- Uses version from `s3-version.json` (either from DOI or assumed latest)
- Skipped if S3 is blocked (403)
- Special case: `exportMissing: true` if S3 has zero files

### Status Values

**Per-check statuses**:
- `ok`: Check passed
- `warning`: Minor issues (e.g., assumed version, HEAD mismatch)
- `error`: Check failed or blocked
- `version-mismatch`: S3 DOI version ≠ latest snapshot
- `pending`: Check not yet run

**Special flags**:
- `s3Blocked: true` in summary indicates 403 error (shows lock icon 🔒)

## Running the Pipeline

Some scripts declare dependencies in their headers.
The simplest way to run these scripts is `uv run`.

### Stage 1: Fetch GraphQL Data

```bash
uv run scripts/fetch_graphql.py --output-dir data
```

Queries OpenNeuro GraphQL API for all public datasets and their snapshots. Creates:
- `datasets-registry.json`
- Per-dataset `snapshots.json` and `snapshots/{tag}/metadata.json`

**Options**:
- `--page-size N`: Datasets per GraphQL page (default: 100)
- `--prefetch N`: Pages to buffer (default: 2)
- `--verbose`: Detailed logging

### Stage 2: Check GitHub Mirrors

```bash
uv run scripts/check_github.py --output-dir data
```

Validates GitHub mirror status for all datasets.

**Options**:
- `--concurrency N`: Parallel git operations (default: 10)
- `--validate`: Run post-check validation
- `--verbose`: Detailed logging

### Stage 3: Check S3 Versions

```bash
uv run scripts/check_s3_version.py --output-dir data
```

Fetches `dataset_description.json` from S3 and extracts versions.

**Options**:
- `--concurrency N`: Parallel HTTP requests (default: 20)
- `--validate`: Run post-check validation

### Stage 4: Fetch Git File Trees

(Not yet implemented - currently using generated test data)

Should fetch file listings from git for each snapshot tag:
```bash
git clone --bare --depth=1 --filter=blob:none --branch {tag} {repo}
git ls-files --with-tree {tag}
```

### Stage 5: Generate S3 File Diffs

(Not yet implemented - currently using generated test data)

Should compare S3 file listings against git trees and create `s3-diff.json`.

### Stage 6: Summarize

```bash
uv run scripts/summarize.py --output-dir data
```

Reads all check files and generates `all-datasets.json` with aggregated results.

## Dashboard

Static HTML/CSS/JS dashboard served from the repository root.

### Files

- **`index.html`**: Main dashboard (dataset list)
- **`dataset.html`**: Detail view for individual datasets
- **`js/main.js`**: Dashboard logic
- **`js/dataset.js`**: Detail view logic
- **`js/utils.js`**: Shared utilities

### Serving

```bash
# Python
python -m http.server 8000
```

Navigate to `http://localhost:8000`

### Features

**Main view**:
- Sortable/filterable dataset table
- Summary statistics by status
- Search by dataset ID
- Color-coded status badges
- Lock icons (🔒) for blocked S3 datasets

**Detail view**:
- Snapshot history
- Detailed check results with expandable sections
- File diff viewer (when mismatches exist)
- Lazy-loaded file listings

## Test Data Generation

Located in `scripts/gen_data/`, these scripts simulate pipeline stages for development:

```bash
python scripts/gen_data/graphql.py
python scripts/gen_data/github.py
python scripts/gen_data/s3_version.py
python scripts/gen_data/s3_version.py
```

## Development Workflow

1. **Add real pipeline stage**: Implement stage script (e.g., `fetch_git_trees.py`)
2. **Update test generator**: Modify corresponding `gen_data/*.py` to match
3. **Test incrementally**: Run new stage, then existing summarize + dashboard
4. **Validate**: Use `--validate` flags to check data consistency

## Data Immutability

**Immutable** (never changes once created):
- `snapshots/{tag}/metadata.json`
- `snapshots/{tag}/files.json`

**Mutable** (updated on each check run):
- `datasets-registry.json`
- `github.json`
- `s3-version.json`
- `s3-diff.json`
- `all-datasets.json`

This allows caching of snapshot data while keeping check results fresh.

## Future Enhancements

- [ ] Implement git tree fetching (stage 4)
- [ ] Implement S3 file diff (stage 5)
- [ ] Scripts to auto-fix issues based on outputs
- [ ] Schedule data updates in CI
- [ ] Track historical trends
- [ ] Integration with GitHub issues to track known problems

## Schema Evolution

The LinkML schema (`schema/openneuro-dashboard.yaml`) includes a `schemaVersion` field in all data files. When making breaking changes:

1. Increment schema version
2. Update all pipeline scripts to write new version
3. Add migration logic if needed
4. Update dashboard to handle both versions during transition
