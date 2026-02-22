"""Unit tests for the LOCSource — Library of Congress search, metadata, download."""

from unittest.mock import MagicMock, patch

import pytest

from harvester.sources.base import BaseSource
from harvester.sources.loc import (
    LOCSource,
    _extract_item_id,
    _normalize_item_url,
)


@pytest.fixture
def loc():
    src = LOCSource()
    src._last_request_time = 0.0
    return src


# ── Interface compliance ───────────────────────────────────


def test_implements_base_source(loc):
    assert isinstance(loc, BaseSource)


def test_label(loc):
    assert loc.label == "loc"


# ── Search ─────────────────────────────────────────────────

SEARCH_RESPONSE = {
    "results": [
        {
            "title": "Oral history interview of Chamba.",
            "url": "https://www.loc.gov/item/2020706022/",
            "id": "http://www.loc.gov/item/2020706022/",
            "date": "1990-01-01",
            "description": ["An oral history interview conducted in Tibet."],
            "contributor": ["Goldstein, Melvyn C."],
            "subject": ["biography", "interviews", "tibetans"],
            "language": ["tibetan"],
            "digitized": True,
        },
        {
            "title": "Civil Rights Interview",
            "url": "https://www.loc.gov/item/2020706023/",
            "id": "http://www.loc.gov/item/2020706023/",
            "date": "2005-03-15",
            "description": ["An interview about civil rights movement."],
            "contributor": ["Smith, John"],
            "subject": ["civil rights", "interviews"],
            "language": ["english"],
            "digitized": True,
        },
        {
            "title": "Some Collection Page",
            "url": "https://www.loc.gov/collections/some-collection/",
            "id": "http://www.loc.gov/collections/some-collection/",
            "date": "",
            "description": ["A collection, not an item."],
        },
    ],
    "pagination": {
        "current": 1,
        "next": None,
        "of": 2,
        "total": 1,
    },
}


def test_find_basic(loc):
    with patch.object(loc, "_get_json", return_value=SEARCH_RESPONSE):
        hits = loc.find("oral history interview")

    # Should skip the collection result (no /item/ in URL)
    assert len(hits) == 2
    assert hits[0].source_name == "loc"
    assert hits[0].title == "Oral history interview of Chamba."
    assert "2020706022" in hits[0].source_url
    assert hits[0].authors == "Goldstein, Melvyn C."
    assert "tibetan" in hits[0].language


def test_find_skips_non_items(loc):
    """Results without /item/ in URL should be skipped."""
    with patch.object(loc, "_get_json", return_value=SEARCH_RESPONSE):
        hits = loc.find("test")

    titles = [h.title for h in hits]
    assert "Some Collection Page" not in titles


def test_find_pagination(loc):
    page1 = {
        "results": [
            {"title": f"Item {i}", "url": f"https://www.loc.gov/item/{i}/",
             "id": f"http://www.loc.gov/item/{i}/", "date": "2023-01-01",
             "description": [], "contributor": [], "subject": [], "language": []}
            for i in range(150)
        ],
        "pagination": {
            "current": 1,
            "next": "https://www.loc.gov/search/?q=test&fo=json&sp=2",
            "of": 200,
            "total": 2,
        },
    }
    page2 = {
        "results": [
            {"title": f"Item {150 + i}", "url": f"https://www.loc.gov/item/{150 + i}/",
             "id": f"http://www.loc.gov/item/{150 + i}/", "date": "2023-01-01",
             "description": [], "contributor": [], "subject": [], "language": []}
            for i in range(50)
        ],
        "pagination": {
            "current": 2,
            "next": None,
            "of": 200,
            "total": 2,
        },
    }

    call_count = 0

    def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return page1 if call_count == 1 else page2

    with patch.object(loc, "_get_json", side_effect=side_effect):
        hits = loc.find("test")

    assert len(hits) == 200
    assert call_count == 2


def test_find_empty(loc):
    empty = {
        "results": [],
        "pagination": {"current": 1, "next": None, "of": 0, "total": 0},
    }

    with patch.object(loc, "_get_json", return_value=empty):
        hits = loc.find("nonexistent_xyz")

    assert hits == []


# ── Full metadata ──────────────────────────────────────────

ITEM_RESPONSE = {
    "item": {
        "title": "Oral history interview of Chamba.",
        "description": ["An oral history interview conducted in Tibet about daily life."],
        "contributor_names": ["Goldstein, Melvyn C.", "Tibet Oral History Project"],
        "subject_headings": ["Tibetans--Interviews", "Tibetans--Biography"],
        "date": "1990-01-01",
        "language": ["tibetan"],
        "rights": [
            '<p>The Library of Congress is not aware of any copyright. '
            'See <a href="https://creativecommons.org/publicdomain/mark/1.0/">'
            'Public Domain</a>.</p>'
        ],
        "access_restricted": False,
        "genre": ["Oral histories"],
        "original_format": ["sound recording"],
        "created_published": ["Cleveland, Ohio : Case Western Reserve, 1990"],
        "repository": ["Library of Congress Asian Division"],
    },
    "resources": [
        {
            "audio": "https://tile.loc.gov/storage-services/service/asian/recording.mp3",
            "download_restricted": False,
            "files": [[
                {
                    "mimetype": "audio/mp3",
                    "url": "https://tile.loc.gov/storage-services/service/asian/recording.mp3",
                    "size": 5000000,
                },
            ]],
        },
    ],
}


def test_fetch_metadata_basic(loc):
    with patch.object(loc, "_get_json", return_value=ITEM_RESPONSE):
        meta = loc.fetch_metadata("https://www.loc.gov/item/2020706022/")

    assert meta.source_name == "loc"
    assert meta.title == "Oral history interview of Chamba."
    assert "oral history" in meta.description.lower()
    assert meta.authors == "Goldstein, Melvyn C.; Tibet Oral History Project"
    assert "Tibetans--Interviews" in meta.tags
    assert "tibetan" in meta.language
    assert meta.kind_of_data == ["Oral histories"]
    assert "creativecommons.org" in meta.license_url

    # Files — audio shortcut
    assert len(meta.files) == 1
    f0 = meta.files[0]
    assert "recording.mp3" in f0["name"]
    assert f0["content_type"] == "audio/mpeg"
    assert f0["restricted"] is False


def test_fetch_metadata_with_pdf(loc):
    data = {
        "item": {
            "title": "Interview Transcript",
            "description": ["A transcript PDF."],
            "contributor_names": ["Interviewer, A."],
            "subject_headings": ["Interviews"],
            "date": "2005-01-01",
            "language": ["english"],
            "rights": [],
            "access_restricted": False,
            "genre": [],
        },
        "resources": [
            {
                "pdf": "https://tile.loc.gov/storage-services/test/transcript.pdf",
                "download_restricted": False,
                "files": [],
            },
        ],
    }

    with patch.object(loc, "_get_json", return_value=data):
        meta = loc.fetch_metadata("https://www.loc.gov/item/12345/")

    assert len(meta.files) == 1
    assert meta.files[0]["content_type"] == "application/pdf"
    assert "transcript.pdf" in meta.files[0]["name"]


def test_fetch_metadata_no_resources(loc):
    data = {
        "item": {
            "title": "No Resources Item",
            "description": [],
            "contributor_names": [],
            "subject_headings": [],
            "date": "",
            "language": [],
            "rights": [],
            "access_restricted": False,
            "genre": [],
        },
        "resources": [],
    }

    with patch.object(loc, "_get_json", return_value=data):
        meta = loc.fetch_metadata("https://www.loc.gov/item/99999/")

    assert meta.files == []
    assert meta.title == "No Resources Item"


def test_fetch_metadata_fallback_files_array(loc):
    """When no shortcut keys, parse the files array."""
    data = {
        "item": {
            "title": "Fallback Files",
            "description": [],
            "contributor_names": [],
            "subject_headings": [],
            "date": "",
            "language": [],
            "rights": [],
            "access_restricted": False,
            "genre": [],
        },
        "resources": [
            {
                "download_restricted": False,
                "files": [[
                    {"mimetype": "application/pdf", "url": "https://tile.loc.gov/test.pdf", "size": 1000},
                    {"mimetype": "text/xml", "url": "https://tile.loc.gov/test.xml", "size": 500},
                ]],
            },
        ],
    }

    with patch.object(loc, "_get_json", return_value=data):
        meta = loc.fetch_metadata("https://www.loc.gov/item/88888/")

    assert len(meta.files) == 2
    assert meta.files[0]["content_type"] == "application/pdf"
    assert meta.files[1]["content_type"] == "text/xml"


# ── Download ───────────────────────────────────────────────


def test_pull_file(loc, tmp_path):
    content = b"fake loc file content"

    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.iter_bytes = MagicMock(return_value=iter([content]))
    mock_resp.headers = {}
    mock_resp.__enter__ = MagicMock(return_value=mock_resp)
    mock_resp.__exit__ = MagicMock(return_value=False)

    with patch("httpx.stream", return_value=mock_resp):
        path = loc.pull_file(
            "https://tile.loc.gov/storage-services/test/recording.mp3",
            str(tmp_path),
            filename="recording.mp3",
        )

    assert path == str(tmp_path / "recording.mp3")
    assert (tmp_path / "recording.mp3").read_bytes() == content


# ── Utility functions ──────────────────────────────────────


def test_extract_item_id_https():
    assert _extract_item_id("https://www.loc.gov/item/2020706022/") == "2020706022"


def test_extract_item_id_http():
    assert _extract_item_id("http://www.loc.gov/item/2020706022/") == "2020706022"


def test_extract_item_id_bare():
    assert _extract_item_id("2020706022") == "2020706022"


def test_normalize_item_url():
    assert _normalize_item_url("2020706022") == "https://www.loc.gov/item/2020706022/"


def test_normalize_item_url_existing():
    result = _normalize_item_url("https://www.loc.gov/item/2020706022/")
    assert result == "https://www.loc.gov/item/2020706022/"


# ── Registry ──────────────────────────────────────────────


def test_source_registry():
    from harvester.sources import SOURCES

    assert "loc" in SOURCES
    assert isinstance(SOURCES["loc"], LOCSource)
