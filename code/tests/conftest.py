"""Shared test fixtures for the OpenNeuro Dashboard test suite."""

from unittest.mock import AsyncMock, patch

import pytest

# ---------------------------------------------------------------------------
# Canned dataset metadata (3 datasets with different health states)
# ---------------------------------------------------------------------------

HEALTHY_DATASET = {
    "id": "ds000001",
    "name": "Healthy Dataset",
    "public": True,
    "created": "2020-01-15T00:00:00.000Z",
    "uploader": {"name": "Test User"},
    "latestSnapshot": {
        "tag": "1.0.2",
        "created": "2024-06-01T12:00:00.000Z",
        "description": {
            "Name": "Healthy Dataset",
            "BIDSVersion": "1.9.0",
        },
        "size": 1024000,
        "summary": {"subjects": 20, "sessions": 1, "tasks": ["rest"]},
    },
}

BLOCKED_DATASET = {
    "id": "ds000002",
    "name": "Blocked Dataset",
    "public": True,
    "created": "2021-03-10T00:00:00.000Z",
    "uploader": {"name": "Test User 2"},
    "latestSnapshot": None,
}

VERSION_MISMATCH_DATASET = {
    "id": "ds000003",
    "name": "Version Mismatch Dataset",
    "public": True,
    "created": "2022-07-20T00:00:00.000Z",
    "uploader": {"name": "Test User 3"},
    "latestSnapshot": {
        "tag": "1.1.0",
        "created": "2025-01-10T08:30:00.000Z",
        "description": {
            "Name": "Version Mismatch Dataset",
            "BIDSVersion": "1.8.0",
        },
        "size": 5120000,
        "summary": {"subjects": 5, "sessions": 2, "tasks": ["nback", "rest"]},
    },
}

ALL_DATASETS = [HEALTHY_DATASET, BLOCKED_DATASET, VERSION_MISMATCH_DATASET]


# ---------------------------------------------------------------------------
# GraphQL fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def graphql_datasets():
    """Return canned GraphQL dataset responses."""
    return ALL_DATASETS


@pytest.fixture()
def mock_graphql_response():
    """Return a mock HTTP response for a GraphQL datasets query."""
    return {
        "data": {
            "datasets": {
                "edges": [{"node": ds} for ds in ALL_DATASETS],
                "pageInfo": {"hasNextPage": False, "endCursor": "cursor_end"},
            }
        }
    }


# ---------------------------------------------------------------------------
# Git fixtures
# ---------------------------------------------------------------------------

GIT_REFS = {
    "ds000001": {
        "refs/heads/main": "aaa1111111111111111111111111111111111111",
        "refs/tags/1.0.2": "aaa2222222222222222222222222222222222222",
    },
    "ds000002": {
        "refs/heads/main": "bbb1111111111111111111111111111111111111",
    },
    "ds000003": {
        "refs/heads/main": "ccc1111111111111111111111111111111111111",
        "refs/tags/1.1.0": "ccc2222222222222222222222222222222222222",
        "refs/tags/1.0.0": "ccc3333333333333333333333333333333333333",
    },
}


@pytest.fixture()
def git_refs():
    """Return canned git ls-remote ref maps keyed by dataset id."""
    return GIT_REFS


@pytest.fixture()
def mock_git_ls_remote():
    """Patch subprocess to return canned ls-remote output for any dataset."""
    outputs = {}
    for ds_id, refs in GIT_REFS.items():
        lines = "\n".join(f"{sha}\t{ref}" for ref, sha in refs.items())
        outputs[ds_id] = lines

    async def _fake_ls_remote(url: str, *_args, **_kwargs):
        for ds_id in GIT_REFS:
            if ds_id in url:
                return outputs[ds_id]
        return ""

    with patch(
        "openneuro_dashboard._git_ls_remote",
        new_callable=lambda: lambda: _fake_ls_remote,
        create=True,
    ) as mock:
        yield mock


# ---------------------------------------------------------------------------
# S3 / aioboto3 fixtures
# ---------------------------------------------------------------------------

S3_FILE_LISTINGS = {
    "ds000001": [
        {"Key": "dataset_description.json", "Size": 256},
        {"Key": "participants.tsv", "Size": 1024},
        {"Key": "sub-01/anat/sub-01_T1w.nii.gz", "Size": 50000},
    ],
    "ds000002": [],
    "ds000003": [
        {"Key": "dataset_description.json", "Size": 300},
        {"Key": "participants.tsv", "Size": 512},
    ],
}

S3_DATASET_DESCRIPTIONS = {
    "ds000001": '{"Name": "Healthy Dataset", "BIDSVersion": "1.9.0"}',
    "ds000003": '{"Name": "Version Mismatch Dataset", "BIDSVersion": "1.8.0"}',
}


@pytest.fixture()
def s3_file_listings():
    """Return canned S3 file listings keyed by dataset id."""
    return S3_FILE_LISTINGS


@pytest.fixture()
def s3_dataset_descriptions():
    """Return canned dataset_description.json content keyed by dataset id."""
    return S3_DATASET_DESCRIPTIONS


@pytest.fixture()
def mock_s3_client():
    """Provide a mock aioboto3 S3 client with canned responses."""
    client = AsyncMock()

    async def _list_objects(Bucket, Prefix="", **_kwargs):  # noqa: N803
        ds_id = Prefix.rstrip("/")
        contents = S3_FILE_LISTINGS.get(ds_id, [])
        return {"Contents": contents} if contents else {}

    async def _get_object(Bucket, Key, **_kwargs):  # noqa: N803
        ds_id = Key.split("/")[0] if "/" in Key else ""
        body_text = S3_DATASET_DESCRIPTIONS.get(ds_id, "{}")
        body = AsyncMock()
        body.read = AsyncMock(return_value=body_text.encode())
        return {"Body": body}

    client.list_objects_v2 = AsyncMock(side_effect=_list_objects)
    client.get_object = AsyncMock(side_effect=_get_object)

    return client
