"""OpenNeuro Dashboard data-population tools.

ondiagnostics modules used
--------------------------
- ``ondiagnostics.graphql``: GraphQLResponse, PageInfo, create_client, get_page
  (pagination over the OpenNeuro GraphQL API in fetch_graphql.py)
- ``ondiagnostics.subprocs``: git
  (async subprocess wrapper for bare-clone / fetch in check_s3_files.py)
- ``ondiagnostics.tasks.git``: list_refs
  (remote ref listing for GitHub mirror checks in check_github.py)

Dashboard-specific logic
------------------------
- fetch_graphql: writes datasets-registry.json and per-snapshot metadata
- check_github:  compares registry against GitHub mirror refs
- check_s3_version: extracts S3 export version from dataset_description.json
- check_s3_files: diffs git tree against S3 object listing
- summarize: aggregates per-dataset check results into all-datasets.json
- utils: shared JSON I/O helpers and schema version constant
"""
