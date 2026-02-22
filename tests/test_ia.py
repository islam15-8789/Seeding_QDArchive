"""Unit tests for the IASource — Internet Archive search, metadata, download."""

from unittest.mock import MagicMock, patch

import pytest

from harvester.sources.base import BaseSource
from harvester.sources.ia import (
    IASource,
    _extract_identifier,
    _ensure_str,
    _license_name_from_url,
)


@pytest.fixture
def ia():
    src = IASource()
    src._last_request_time = 0.0
    return src


# ── Interface compliance ───────────────────────────────────


def test_implements_base_source(ia):
    assert isinstance(ia, BaseSource)


def test_label(ia):
    assert ia.label == "ia"


# ── Search ─────────────────────────────────────────────────

SEARCH_RESPONSE = {
    "response": {
        "numFound": 2,
        "start": 0,
        "docs": [
            {
                "identifier": "oral-history-001",
                "title": "Interview with Kenneth Fisher",
                "description": "An oral history interview about Vietnam War experiences.",
                "date": "1985-01-01T00:00:00Z",
                "creator": "Museum of History",
                "licenseurl": "https://creativecommons.org/licenses/by/4.0/",
                "subject": ["oral history", "Vietnam War"],
                "mediatype": "texts",
            },
            {
                "identifier": "oral-history-002",
                "title": "Focus Group Discussion on Education",
                "description": "Group discussion transcripts.",
                "date": "2020-05-15T00:00:00Z",
                "creator": "Education Research Center",
                "licenseurl": "",
                "subject": "education; focus groups",
                "mediatype": "texts",
            },
        ],
    },
}


def test_find_basic(ia):
    with patch("httpx.get") as mock_get:
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = SEARCH_RESPONSE
        mock_get.return_value = resp

        hits = ia.find("oral history interview")

    assert len(hits) == 2
    assert hits[0].source_name == "ia"
    assert hits[0].title == "Interview with Kenneth Fisher"
    assert "oral-history-001" in hits[0].source_url
    assert hits[1].title == "Focus Group Discussion on Education"


def test_find_handles_string_subject(ia):
    """Subject can be a semicolon-separated string instead of a list."""
    with patch("httpx.get") as mock_get:
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = SEARCH_RESPONSE
        mock_get.return_value = resp

        hits = ia.find("education")

    # Second hit has string subject
    assert isinstance(hits[1].tags, list)
    assert "education" in hits[1].tags


def test_find_pagination(ia):
    page1 = {
        "response": {
            "numFound": 70,
            "start": 0,
            "docs": [
                {"identifier": f"item-{i}", "title": f"Item {i}",
                 "description": "", "date": "2023-01-01", "creator": "",
                 "subject": [], "mediatype": "texts"}
                for i in range(50)
            ],
        },
    }
    page2 = {
        "response": {
            "numFound": 70,
            "start": 50,
            "docs": [
                {"identifier": f"item-{50 + i}", "title": f"Item {50 + i}",
                 "description": "", "date": "2023-01-01", "creator": "",
                 "subject": [], "mediatype": "texts"}
                for i in range(20)
            ],
        },
    }

    call_count = 0

    def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = page1 if call_count == 1 else page2
        return resp

    with patch("httpx.get", side_effect=side_effect):
        hits = ia.find("test")

    assert len(hits) == 70
    assert call_count == 2


def test_find_empty(ia):
    empty_resp = {"response": {"numFound": 0, "start": 0, "docs": []}}

    with patch("httpx.get") as mock_get:
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = empty_resp
        mock_get.return_value = resp

        hits = ia.find("nonexistent_xyz")

    assert hits == []


# ── Full metadata ──────────────────────────────────────────

METADATA_RESPONSE = {
    "metadata": {
        "identifier": "oral-history-001",
        "title": "Interview with Kenneth Fisher",
        "description": "<p>An oral history <b>interview</b> about Vietnam.</p>",
        "date": "1985-01-01",
        "creator": "Museum of History",
        "licenseurl": "https://creativecommons.org/licenses/by/4.0/",
        "subject": ["oral history", "Vietnam War"],
        "language": "eng",
        "publicdate": "2019-02-25 19:43:37",
    },
    "files": [
        {
            "name": "interview.pdf",
            "source": "original",
            "format": "Text PDF",
            "size": "197264",
            "md5": "abc123def456",
            "sha1": "deadbeef",
        },
        {
            "name": "interview_djvu.txt",
            "source": "derivative",
            "format": "DjVuTXT",
            "size": "50000",
            "md5": "111222333",
        },
        {
            "name": "oral-history-001_meta.xml",
            "source": "original",
            "format": "Metadata",
            "size": "1000",
            "md5": "metahash",
        },
    ],
}


def test_fetch_metadata_basic(ia):
    with patch.object(ia, "_get_json", return_value=METADATA_RESPONSE):
        meta = ia.fetch_metadata("https://archive.org/details/oral-history-001")

    assert meta.source_name == "ia"
    assert meta.title == "Interview with Kenneth Fisher"
    assert "oral history" in meta.description.lower()
    assert "<b>" not in meta.description
    assert meta.authors == "Museum of History"
    assert meta.license_type == "CC BY 4.0"
    assert "creativecommons.org" in meta.license_url
    assert meta.language == ["eng"]
    assert "oral history" in meta.tags

    # Only original non-metadata files
    assert len(meta.files) == 1
    f0 = meta.files[0]
    assert f0["name"] == "interview.pdf"
    assert f0["size"] == 197264
    assert f0["content_type"] == "application/pdf"
    assert f0["api_checksum"] == "MD5:abc123def456"
    assert "download" in f0["download_url"]


def test_fetch_metadata_no_license(ia):
    data = {
        "metadata": {
            "identifier": "item-no-lic",
            "title": "No License Item",
            "description": "",
            "date": "2023-01-01",
            "creator": "Anonymous",
            "subject": [],
        },
        "files": [],
    }

    with patch.object(ia, "_get_json", return_value=data):
        meta = ia.fetch_metadata("https://archive.org/details/item-no-lic")

    assert meta.license_type == ""
    assert meta.license_url == ""


def test_fetch_metadata_list_description(ia):
    """Description can be a list of strings instead of a single string."""
    data = {
        "metadata": {
            "identifier": "item-list-desc",
            "title": "List Description Item",
            "description": ["Part 1 of description.", "Part 2."],
            "creator": "Someone",
            "subject": [],
        },
        "files": [],
    }

    with patch.object(ia, "_get_json", return_value=data):
        meta = ia.fetch_metadata("item-list-desc")

    assert "Part 1" in meta.description


def test_fetch_metadata_no_files(ia):
    data = {
        "metadata": {
            "identifier": "empty-item",
            "title": "Empty Item",
            "description": "",
            "creator": "",
            "subject": [],
        },
        "files": [],
    }

    with patch.object(ia, "_get_json", return_value=data):
        meta = ia.fetch_metadata("empty-item")

    assert meta.files == []


# ── Download ───────────────────────────────────────────────


def test_pull_file(ia, tmp_path):
    content = b"fake ia file content"

    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.iter_bytes = MagicMock(return_value=iter([content]))
    mock_resp.headers = {}
    mock_resp.__enter__ = MagicMock(return_value=mock_resp)
    mock_resp.__exit__ = MagicMock(return_value=False)

    with patch("httpx.stream", return_value=mock_resp):
        path = ia.pull_file(
            "https://archive.org/download/oral-history-001/interview.pdf",
            str(tmp_path),
            filename="interview.pdf",
        )

    assert path == str(tmp_path / "interview.pdf")
    assert (tmp_path / "interview.pdf").read_bytes() == content


# ── Utility functions ──────────────────────────────────────


def test_extract_identifier_details():
    assert _extract_identifier("https://archive.org/details/my-item") == "my-item"


def test_extract_identifier_metadata():
    assert _extract_identifier("https://archive.org/metadata/my-item") == "my-item"


def test_extract_identifier_download():
    assert _extract_identifier(
        "https://archive.org/download/my-item/file.pdf"
    ) == "my-item"


def test_extract_identifier_bare():
    assert _extract_identifier("my-item") == "my-item"


def test_ensure_str_string():
    assert _ensure_str("hello") == "hello"


def test_ensure_str_list():
    assert _ensure_str(["a", "b"]) == "a; b"


def test_ensure_str_none():
    assert _ensure_str(None) == ""


def test_license_name_cc_by():
    assert _license_name_from_url(
        "https://creativecommons.org/licenses/by/4.0/"
    ) == "CC BY 4.0"


def test_license_name_cc_by_nc_sa():
    assert _license_name_from_url(
        "https://creativecommons.org/licenses/by-nc-sa/4.0/"
    ) == "CC BY-NC-SA 4.0"


def test_license_name_public_domain():
    assert _license_name_from_url(
        "https://creativecommons.org/publicdomain/mark/1.0/"
    ) == "Public Domain Mark 1.0"


def test_license_name_empty():
    assert _license_name_from_url("") == ""


# ── Registry ──────────────────────────────────────────────


def test_source_registry():
    from harvester.sources import SOURCES

    assert "ia" in SOURCES
    assert isinstance(SOURCES["ia"], IASource)
