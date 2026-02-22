"""Unit tests for the DataverseSource — search, metadata, download."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from harvester.sources.dataverse import DataverseSource, _clean_html, _field_val, _name_from_headers


@pytest.fixture
def dv():
    return DataverseSource("https://example.dataverse.org", "test-dv")


# ── Search ──────────────────────────────────────────────────


def _mock_search_response(items, total=None):
    """Build a fake httpx response for the search endpoint."""
    if total is None:
        total = len(items)
    mock = MagicMock()
    mock.raise_for_status = MagicMock()
    mock.json.return_value = {
        "data": {"items": items, "total_count": total}
    }
    return mock


def test_find_basic(dv):
    items = [
        {
            "name": "Test Dataset",
            "url": "https://example.dataverse.org/dataset/1",
            "global_id": "doi:10.5072/FK2/TEST",
            "description": "A qualitative study",
            "authors": ["Alice", "Bob"],
            "published_at": "2024-01-01",
            "subjects": ["Social Sciences"],
        }
    ]
    with patch("httpx.get", return_value=_mock_search_response(items)):
        hits = dv.find("qualitative")

    assert len(hits) == 1
    assert hits[0].title == "Test Dataset"
    assert "persistentId=doi:10.5072/FK2/TEST" in hits[0].source_url


def test_find_empty(dv):
    with patch("httpx.get", return_value=_mock_search_response([])):
        hits = dv.find("nonexistent")
    assert hits == []


def test_find_pagination(dv):
    page1 = [{"name": f"DS {i}", "url": f"u/{i}", "global_id": f"doi:{i}"} for i in range(100)]
    page2 = [{"name": f"DS {i}", "url": f"u/{i}", "global_id": f"doi:{i}"} for i in range(100, 150)]

    responses = [
        _mock_search_response(page1, total=150),
        _mock_search_response(page2, total=150),
    ]
    with patch("httpx.get", side_effect=responses):
        hits = dv.find("interview")
    assert len(hits) == 150


def test_find_respects_cap(dv):
    huge = [{"name": f"DS{i}", "url": f"u/{i}", "global_id": f"doi:{i}"} for i in range(600)]
    with patch("httpx.get", return_value=_mock_search_response(huge, total=600)):
        hits = dv.find("big")
    assert len(hits) <= 500


# ── Metadata ────────────────────────────────────────────────


def _build_metadata_response(fields_dict, files=None, license_info=None):
    """Construct a mock metadata JSON response."""
    fields = []
    for type_name, value in fields_dict.items():
        fields.append({"typeName": type_name, "value": value})

    version = {
        "metadataBlocks": {"citation": {"fields": fields}},
        "files": files or [],
        "license": license_info or {},
        "releaseTime": "2024-06-15",
    }
    mock = MagicMock()
    mock.raise_for_status = MagicMock()
    mock.json.return_value = {"data": {"latestVersion": version}}
    return mock


def test_fetch_metadata_basic(dv):
    resp = _build_metadata_response(
        {
            "title": "Interview Transcripts 2024",
            "dsDescription": [
                {"dsDescriptionValue": {"value": "Qualitative study transcripts."}}
            ],
            "author": [{"authorName": {"value": "Dr. Smith"}}],
            "subject": ["Social Sciences"],
        },
        files=[{
            "dataFile": {
                "id": 42,
                "filename": "transcript.pdf",
                "filesize": 1024,
                "contentType": "application/pdf",
                "friendlyType": "PDF",
                "checksum": {"type": "SHA-256", "value": "abc"},
            },
            "restricted": False,
        }],
        license_info={"name": "CC BY 4.0", "uri": "https://creativecommons.org/licenses/by/4.0/"},
    )

    url = "https://example.dataverse.org/dataset.xhtml?persistentId=doi:10.5072/FK2/A"
    with patch("httpx.get", return_value=resp):
        meta = dv.fetch_metadata(url)

    assert meta.title == "Interview Transcripts 2024"
    assert meta.authors == "Dr. Smith"
    assert meta.license_type == "CC BY 4.0"
    assert len(meta.files) == 1
    assert meta.files[0]["name"] == "transcript.pdf"


def test_fetch_metadata_html_stripped(dv):
    resp = _build_metadata_response({
        "title": "HTML Test",
        "dsDescription": [
            {"dsDescriptionValue": {"value": "<p>Some <b>bold</b> text</p>"}}
        ],
    })
    with patch("httpx.get", return_value=resp):
        meta = dv.fetch_metadata("https://x.org/d?persistentId=doi:1")
    assert "<" not in meta.description
    assert "bold" in meta.description


def test_fetch_metadata_extended_fields(dv):
    resp = _build_metadata_response({
        "title": "Extended",
        "keyword": [{"keywordValue": {"value": "interviews"}}],
        "kindOfData": ["Qualitative"],
        "language": ["English", "French"],
        "software": [{"softwareName": {"value": "NVivo"}}],
        "geographicCoverage": [{"country": {"value": "Canada"}}],
        "depositor": "Jane Doe",
        "producer": [{"producerName": {"value": "ACME Lab"}}],
    })
    with patch("httpx.get", return_value=resp):
        meta = dv.fetch_metadata("https://x.org/d?persistentId=doi:2")

    assert "interviews" in meta.keywords
    assert "English" in meta.language
    assert "NVivo" in meta.software
    assert "Canada" in meta.geographic_coverage
    assert meta.depositor == "Jane Doe"


def test_fetch_metadata_terms_of_access_fallback(dv):
    """When license block is empty, termsOfAccess should be used."""
    fields = [{"typeName": "title", "value": "TOA Test"}]
    version = {
        "metadataBlocks": {"citation": {"fields": fields}},
        "files": [],
        "license": {},
        "termsOfAccess": "Standard Access",
        "releaseTime": "2024-01-01",
    }
    mock = MagicMock()
    mock.raise_for_status = MagicMock()
    mock.json.return_value = {"data": {"latestVersion": version}}

    with patch("httpx.get", return_value=mock):
        meta = dv.fetch_metadata("https://x.org/d?persistentId=doi:3")
    assert meta.license_type == "Standard Access"


def test_fetch_metadata_terms_of_use_fallback(dv):
    """Borealis uses termsOfUse on older datasets instead of termsOfAccess."""
    fields = [{"typeName": "title", "value": "TOU Test"}]
    version = {
        "metadataBlocks": {"citation": {"fields": fields}},
        "files": [],
        "license": {},
        "termsOfUse": "CC BY 4.0",
        "releaseTime": "2024-01-01",
    }
    mock = MagicMock()
    mock.raise_for_status = MagicMock()
    mock.json.return_value = {"data": {"latestVersion": version}}

    with patch("httpx.get", return_value=mock):
        meta = dv.fetch_metadata("https://x.org/d?persistentId=doi:tou")
    assert meta.license_type == "CC BY 4.0"


def test_fetch_metadata_md5_fallback(dv):
    """AUSSDA provides md5 directly on dataFile instead of a checksum wrapper."""
    resp = _build_metadata_response(
        {"title": "MD5 Test"},
        files=[{
            "dataFile": {
                "id": 99,
                "filename": "data.tab",
                "filesize": 512,
                "contentType": "text/tab-separated-values",
                "friendlyType": "Tab-Delimited",
                "md5": "d41d8cd98f00b204e9800998ecf8427e",
            },
            "restricted": False,
        }],
    )
    with patch("httpx.get", return_value=resp):
        meta = dv.fetch_metadata("https://x.org/d?persistentId=doi:md5")
    assert meta.files[0]["api_checksum"] == "MD5:d41d8cd98f00b204e9800998ecf8427e"


# ── Download ────────────────────────────────────────────────


def test_pull_file(dv, tmp_path):
    content = b"fake file content"

    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.headers = {"content-disposition": 'attachment; filename="data.csv"'}
    mock_resp.iter_bytes = MagicMock(return_value=iter([content]))
    mock_resp.__enter__ = MagicMock(return_value=mock_resp)
    mock_resp.__exit__ = MagicMock(return_value=False)

    with patch("httpx.stream", return_value=mock_resp):
        path = dv.pull_file("https://example.org/file/1", str(tmp_path))

    assert Path(path).exists()
    assert Path(path).read_bytes() == content


def test_pull_file_explicit_name(dv, tmp_path):
    content = b"named file"
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.headers = {}
    mock_resp.iter_bytes = MagicMock(return_value=iter([content]))
    mock_resp.__enter__ = MagicMock(return_value=mock_resp)
    mock_resp.__exit__ = MagicMock(return_value=False)

    with patch("httpx.stream", return_value=mock_resp):
        path = dv.pull_file("https://example.org/file/2", str(tmp_path), filename="custom.txt")

    assert Path(path).name == "custom.txt"


# ── Utility functions ───────────────────────────────────────


def test_clean_html():
    assert _clean_html("<p>Hello <b>world</b></p>") == "Hello world"
    assert _clean_html("no tags") == "no tags"


def test_field_val():
    fields = {"title": {"value": "Test"}}
    assert _field_val(fields, "title", "") == "Test"
    assert _field_val(fields, "missing", "default") == "default"


def test_name_from_headers():
    from httpx import Headers

    h = Headers({"content-disposition": 'attachment; filename="report.pdf"'})
    assert _name_from_headers(h) == "report.pdf"

    h2 = Headers({"content-type": "text/plain"})
    assert _name_from_headers(h2) is None


def test_parse_persistent_id(dv):
    url = "https://example.org/dataset.xhtml?persistentId=doi:10.5072/FK2/ABC"
    assert dv._parse_persistent_id(url) == "doi:10.5072/FK2/ABC"

    assert dv._parse_persistent_id("doi:10.5072/FK2/XYZ") == "doi:10.5072/FK2/XYZ"
    assert dv._parse_persistent_id("https://example.org/dataset/123") is None
