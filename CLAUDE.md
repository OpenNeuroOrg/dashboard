# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OpenNeuro Dataset Monitor — a static HTML/JS dashboard with a Python data pipeline that tracks synchronization status of OpenNeuro datasets across three sources: GraphQL API, GitHub mirrors, and S3 exports.

## Architecture

**Data Pipeline** (5-stage ETL, implemented in `code/src/openneuro_dashboard/`):

```
fetch-graphql → check-github → check-s3-version → check-s3-files → summarize
```

Each stage reads outputs from previous stages and can be run independently.

**Frontend**: Two static HTML pages (`index.html` for list view, `dataset.html?id=dsXXXXXX` for detail view) with vanilla JS modules in `js/`.

**Data**: Pipeline outputs go to `data/` as JSON. The dashboard loads these via fetch. Schema defined in `schema/openneuro-dashboard.yaml` (LinkML, version 1.0.0).

## Running the Pipeline

The pipeline is an installable Python package with a `openneuro-dashboard` CLI:

```bash
cd code
uv sync
uv run openneuro-dashboard run-all --output-dir ../data
```

Individual stages:

```bash
cd code
uv run openneuro-dashboard fetch-graphql --output-dir ../data
uv run openneuro-dashboard check-github --output-dir ../data
uv run openneuro-dashboard check-s3-version --output-dir ../data
uv run openneuro-dashboard check-s3-files --output-dir ../data --cache-dir ~/.cache/openneuro-dashboard/repos
uv run openneuro-dashboard summarize --output-dir ../data
```

Test data generation:

```bash
cd code
uv run openneuro-dashboard gen-data --output-dir ../data --seed 42
```

## Running Tests

```bash
cd code
uv run --group test pytest -v
```

## Serving the Dashboard

```bash
python -m http.server 8000
# Navigate to http://localhost:8000
```

## Key Conventions

- Dataset IDs match pattern `^ds[0-9]{6}$`
- All output JSON files include `schemaVersion: "1.1.0"` (from `code/src/openneuro_dashboard/utils.py:SCHEMA_VERSION`)
- Snapshot metadata and file listings are immutable; registry, check results, and summary are mutable
- Pipeline modules use async I/O (asyncio) and include `--validate` flags for data consistency checking
- Python requires >=3.14
