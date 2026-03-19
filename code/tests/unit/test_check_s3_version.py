"""Unit tests for openneuro_dashboard.check_s3_version (DOI parsing)."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from openneuro_dashboard.check_s3_version import fetch_dataset_description


def _mock_response(status_code: int, json_data: dict | None = None, text: str = ""):
    """Build a mock httpx.Response with the given status and body."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code

    if json_data is not None:
        resp.json.return_value = json_data
    elif text:
        resp.json.side_effect = json.JSONDecodeError("bad", text, 0)
    else:
        resp.json.return_value = {}

    if status_code >= 400:
        exc = httpx.HTTPStatusError(
            message=f"HTTP {status_code}",
            request=MagicMock(),
            response=resp,
        )
        resp.raise_for_status.side_effect = exc
    else:
        resp.raise_for_status.return_value = None

    return resp


def _mock_client(response):
    """Return an AsyncMock client whose .get() resolves to *response*."""
    client = AsyncMock(spec=httpx.AsyncClient)
    client.get.return_value = response
    return client


# ------------------------------------------------------------------
# Test cases
# ------------------------------------------------------------------


@patch("openneuro_dashboard.check_s3_version.format_timestamp", return_value="2026-01-01T00:00:00.000Z")
async def test_normal_doi(_ts):
    """DOI matching the standard pattern yields version from DOI."""
    body = {"DatasetDOI": "10.18112/openneuro.ds000001.v1.0.2"}
    client = _mock_client(_mock_response(200, json_data=body))

    result = await fetch_dataset_description(client, "ds000001", "1.0.2")

    assert result["extractedVersion"] == "1.0.2"
    assert result["versionSource"] == "doi"
    assert result["accessible"] is True
    assert result["datasetDescriptionDOI"] == body["DatasetDOI"]


@patch("openneuro_dashboard.check_s3_version.format_timestamp", return_value="2026-01-01T00:00:00.000Z")
async def test_missing_doi(_ts):
    """No DatasetDOI field falls back to assumed_latest."""
    body = {"Name": "Some Dataset"}
    client = _mock_client(_mock_response(200, json_data=body))

    result = await fetch_dataset_description(client, "ds000001", "1.0.2")

    assert result["extractedVersion"] == "1.0.2"
    assert result["versionSource"] == "assumed_latest"
    assert result["datasetDescriptionDOI"] is None


@patch("openneuro_dashboard.check_s3_version.format_timestamp", return_value="2026-01-01T00:00:00.000Z")
async def test_custom_doi(_ts):
    """Non-standard DOI that doesn't match the OpenNeuro pattern."""
    body = {"DatasetDOI": "10.5281/zenodo.1234567"}
    client = _mock_client(_mock_response(200, json_data=body))

    result = await fetch_dataset_description(client, "ds000001", "1.0.2")

    assert result["extractedVersion"] == "1.0.2"
    assert result["versionSource"] == "assumed_latest"
    assert result["datasetDescriptionDOI"] == body["DatasetDOI"]


@patch("openneuro_dashboard.check_s3_version.format_timestamp", return_value="2026-01-01T00:00:00.000Z")
async def test_doi_id_mismatch(_ts):
    """DOI references a different dataset ID."""
    body = {"DatasetDOI": "10.18112/openneuro.ds999999.v2.0.0"}
    client = _mock_client(_mock_response(200, json_data=body))

    result = await fetch_dataset_description(client, "ds000001", "1.0.2")

    assert result["doiIdMismatch"] is True
    assert result["extractedVersion"] == "2.0.0"
    assert result["versionSource"] == "doi"
    assert result["doiDatasetId"] == "ds999999"


@patch("openneuro_dashboard.check_s3_version.format_timestamp", return_value="2026-01-01T00:00:00.000Z")
async def test_403_blocked(_ts):
    """HTTP 403 marks the dataset as inaccessible."""
    client = _mock_client(_mock_response(403))

    result = await fetch_dataset_description(client, "ds000001", "1.0.2")

    assert result["accessible"] is False
    assert result["httpStatus"] == 403


@patch("openneuro_dashboard.check_s3_version.format_timestamp", return_value="2026-01-01T00:00:00.000Z")
async def test_404_not_found(_ts):
    """HTTP 404 sets datasetDescriptionMissing and falls back to latest."""
    client = _mock_client(_mock_response(404))

    result = await fetch_dataset_description(client, "ds000001", "1.0.2")

    assert result["datasetDescriptionMissing"] is True
    assert result["extractedVersion"] == "1.0.2"
    assert result["versionSource"] == "assumed_latest"
    assert result["accessible"] is True


@patch("openneuro_dashboard.check_s3_version.format_timestamp", return_value="2026-01-01T00:00:00.000Z")
async def test_malformed_json(_ts):
    """Invalid JSON body sets invalidJson flag and falls back to latest."""
    resp = _mock_response(200, text="not json")
    # Override: make raise_for_status succeed, but json() raise
    resp.raise_for_status.side_effect = None
    resp.json.side_effect = json.JSONDecodeError("bad json", "", 0)
    client = _mock_client(resp)

    result = await fetch_dataset_description(client, "ds000001", "1.0.2")

    assert result["invalidJson"] is True
    assert result["extractedVersion"] == "1.0.2"
    assert result["versionSource"] == "assumed_latest"
