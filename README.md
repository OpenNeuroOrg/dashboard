# OpenNeuro Dataset Monitor

A dashboard for tracking synchronization status of OpenNeuro datasets across GraphQL API, GitHub mirrors, and S3 exports.

## Architecture

### Data Pipeline

The monitoring system uses a multi-stage pipeline that generates static JSON files consumed by a client-side dashboard:

```
fetch-graphql → check-github → check-s3-version → check-s3-files → summarize
```

Each stage reads from previous stages and writes new check files, allowing incremental updates and independent execution.

The pipeline is implemented as an installable Python package under `code/`, exposing an `openneuro-dashboard` CLI.

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

- `s3Blocked: true` in summary indicates 403 error (shows lock icon)

## Setup

```bash
cd code
uv sync
```

Requires Python 3.14+.

## Running the Pipeline

### Full Pipeline

```bash
cd code
uv run openneuro-dashboard run-all --output-dir ../data
```

### Individual Stages

```bash
cd code

# Stage 1: Fetch GraphQL data
uv run openneuro-dashboard fetch-graphql --output-dir ../data

# Stage 2: Check GitHub mirrors
uv run openneuro-dashboard check-github --output-dir ../data

# Stage 3: Check S3 versions
uv run openneuro-dashboard check-s3-version --output-dir ../data

# Stage 4: Check S3 files
uv run openneuro-dashboard check-s3-files --output-dir ../data --cache-dir ~/.cache/openneuro-dashboard/repos

# Stage 5: Summarize
uv run openneuro-dashboard summarize --output-dir ../data
```

Common options:

- `--verbose` / `-v`: Enable verbose output
- `--max-datasets N`: Limit number of datasets (for `fetch-graphql` and `run-all`)

### Generating Test Data

```bash
cd code
uv run openneuro-dashboard gen-data --output-dir ../data --num-datasets 50 --seed 42
```

## Running Tests

```bash
cd code
uv run --group test pytest -v
```

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
python -m http.server 8000
```

Navigate to `http://localhost:8000`

### Features

**Main view**:

- Sortable/filterable dataset table
- Summary statistics by status
- Search by dataset ID
- Color-coded status badges
- Lock icons for blocked S3 datasets

**Detail view**:

- Snapshot history
- Detailed check results with expandable sections
- File diff viewer (when mismatches exist)
- Lazy-loaded file listings

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

## Schema Evolution

The LinkML schema (`schema/openneuro-dashboard.yaml`) includes a `schemaVersion` field in all data files. When making breaking changes:

1. Increment schema version
2. Update all pipeline scripts to write new version
3. Add migration logic if needed
4. Update dashboard to handle both versions during transition
