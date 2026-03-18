# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OpenNeuro Dataset Monitor — a static HTML/JS dashboard with a Python data pipeline that tracks synchronization status of OpenNeuro datasets across three sources: GraphQL API, GitHub mirrors, and S3 exports.

## Architecture

**Data Pipeline** (5-stage ETL):

```
fetch_graphql.py → check_github.py → check_s3_version.py → check_s3_files.py → summarize.py
```

Each stage reads outputs from previous stages and can be run independently.

**Frontend**: Two static HTML pages (`index.html` for list view, `dataset.html?id=dsXXXXXX` for detail view) with vanilla JS modules in `js/`.

**Data**: Pipeline outputs go to `data/` as JSON. The dashboard loads these via fetch. Schema defined in `schema/openneuro-dashboard.yaml` (LinkML, version 1.0.0).

## Running Scripts

All Python scripts use **`uv`** with inline PEP 723 dependency declarations (no requirements.txt or pyproject.toml). Run with:

```bash
uv run scripts/fetch_graphql.py
uv run scripts/check_github.py
uv run scripts/check_s3_version.py
uv run scripts/check_s3_files.py --cache-dir ~/.cache/openneuro-dashboard/repos
```

Test data generators:
```bash
uv run scripts/gen_data/graphql.py
uv run scripts/gen_data/github.py
uv run scripts/gen_data/s3_version.py
```

After running either the full or test scripts, aggregate summary data with:

```bash
uv run scripts/summarize.py
```

## Serving the Dashboard

```bash
python -m http.server 8000
# Navigate to http://localhost:8000
```

## Key Conventions

- Dataset IDs match pattern `^ds[0-9]{6}$`
- All output JSON files include `schemaVersion: "1.1.0"` (from `scripts/utils.py:SCHEMA_VERSION`)
- Snapshot metadata and file listings are immutable; registry, check results, and summary are mutable
- Scripts use async I/O (asyncio) and include `--validate` flags for data consistency checking
- Python requires >=3.13
