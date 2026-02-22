"""Unit tests for the FigshareSource — search, metadata, download."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from harvester.sources.base import BaseSource
from harvester.sources.figshare import (
    FigshareSource,
    _clean_title,
    _extract_article_id,
)


@pytest.fixture
def fs():
    src = FigshareSource()
    src._last_request_time = 0.0  # disable throttle in tests
    return src


# ── Interface compliance ───────────────────────────────────


def test_implements_base_source(fs):
    assert isinstance(fs, BaseSource)


def test_label(fs):
    assert fs.label == "figshare"


# ── Search ─────────────────────────────────────────────────

SEARCH_ITEMS = [
    {
        "id": 12345,
        "title": "<p>Qualitative Interview Transcripts</p>",
        "defined_type_name": "dataset",
        "url_public_html": "https://figshare.com/articles/dataset/Qualitative_Interview_Transcripts/12345",
        "published_date": "2023-06-15T00:00:00Z",
    },
    {
        "id": 67890,
        "title": "Focus Group Data\nwith newlines",
        "defined_type_name": "dataset",
        "url_public_html": "https://figshare.com/articles/dataset/Focus_Group_Data/67890",
        "published_date": "2024-01-20T00:00:00Z",
    },
]


def test_find_basic(fs):
    resp1 = MagicMock()
    resp1.json.return_value = SEARCH_ITEMS
    resp1.raise_for_status = MagicMock()

    resp_empty = MagicMock()
    resp_empty.json.return_value = []
    resp_empty.raise_for_status = MagicMock()

    with patch("httpx.post", side_effect=[resp1, resp_empty]):
        hits = fs.find("qualitative interview")

    assert len(hits) == 2
    assert hits[0].source_name == "figshare"
    assert hits[0].title == "Qualitative Interview Transcripts"  # HTML stripped
    assert "12345" in hits[0].source_url
    assert hits[1].title == "Focus Group Data with newlines"  # newlines collapsed


def test_find_skips_figures(fs):
    items = [
        {
            "id": 111, "title": "A Figure", "defined_type_name": "figure",
            "url_public_html": "https://figshare.com/articles/figure/A_Figure/111",
            "published_date": "2023-01-01",
        },
        {
            "id": 222, "title": "Interview Data", "defined_type_name": "dataset",
            "url_public_html": "https://figshare.com/articles/dataset/Interview_Data/222",
            "published_date": "2023-01-01",
        },
    ]
    resp = MagicMock()
    resp.json.return_value = items
    resp.raise_for_status = MagicMock()

    resp_empty = MagicMock()
    resp_empty.json.return_value = []
    resp_empty.raise_for_status = MagicMock()

    with patch("httpx.post", side_effect=[resp, resp_empty]):
        hits = fs.find("test")

    assert len(hits) == 1
    assert hits[0].title == "Interview Data"


def test_find_skips_all_non_data_types(fs):
    items = [
        {"id": i, "title": f"Item {t}", "defined_type_name": t,
         "url_public_html": f"https://figshare.com/articles/{t}/x/{i}",
         "published_date": "2023-01-01"}
        for i, t in enumerate(["figure", "media", "code", "poster", "presentation"], 1)
    ]
    resp = MagicMock()
    resp.json.return_value = items
    resp.raise_for_status = MagicMock()

    resp_empty = MagicMock()
    resp_empty.json.return_value = []
    resp_empty.raise_for_status = MagicMock()

    with patch("httpx.post", side_effect=[resp, resp_empty]):
        hits = fs.find("test")

    assert len(hits) == 0


def test_find_pagination(fs):
    page1 = [
        {"id": i, "title": f"Item {i}", "defined_type_name": "dataset",
         "url_public_html": f"https://figshare.com/articles/dataset/x/{i}",
         "published_date": "2023-01-01"}
        for i in range(50)  # full page → keeps going
    ]
    page2 = [
        {"id": 50 + i, "title": f"Item {50 + i}", "defined_type_name": "dataset",
         "url_public_html": f"https://figshare.com/articles/dataset/x/{50 + i}",
         "published_date": "2023-01-01"}
        for i in range(10)  # partial → stops
    ]

    r1 = MagicMock()
    r1.json.return_value = page1
    r1.raise_for_status = MagicMock()

    r2 = MagicMock()
    r2.json.return_value = page2
    r2.raise_for_status = MagicMock()

    with patch("httpx.post", side_effect=[r1, r2]) as mock_post:
        hits = fs.find("test")

    assert len(hits) == 60
    assert mock_post.call_count == 2


def test_find_empty(fs):
    resp = MagicMock()
    resp.json.return_value = []
    resp.raise_for_status = MagicMock()

    with patch("httpx.post", return_value=resp):
        hits = fs.find("nonexistent")

    assert hits == []


# ── Full metadata ──────────────────────────────────────────

ARTICLE_RESPONSE = {
    "id": 12345,
    "title": "<p>Qualitative <i>Interview</i> Transcripts</p>",
    "description": "<p>A set of semi-structured <b>interviews</b> about health.</p>",
    "defined_type_name": "dataset",
    "published_date": "2023-06-15T00:00:00Z",
    "url_public_html": "https://figshare.com/articles/dataset/x/12345",
    "is_confidential": False,
    "is_metadata_record": False,
    "authors": [
        {"full_name": "Smith, Jane"},
        {"full_name": "Doe, Adam"},
    ],
    "license": {
        "name": "CC BY 4.0",
        "url": "https://creativecommons.org/licenses/by/4.0/",
    },
    "tags": ["qualitative research", "interviews"],
    "categories": [
        {"title": "Social Sciences"},
        {"title": "Health Sciences"},
    ],
    "references": ["https://doi.org/10.1234/test"],
    "files": [
        {
            "id": 111,
            "name": "transcripts.pdf",
            "size": 204800,
            "download_url": "https://ndownloader.figshare.com/files/111",
            "mimetype": "application/pdf",
            "computed_md5": "abc123def456",
            "supplied_md5": "",
            "is_link_only": False,
        },
        {
            "id": 222,
            "name": "codebook.docx",
            "size": 51200,
            "download_url": "https://ndownloader.figshare.com/files/222",
            "mimetype": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "computed_md5": "deadbeef",
            "supplied_md5": "",
            "is_link_only": False,
        },
    ],
}


def test_fetch_metadata_basic(fs):
    resp = MagicMock()
    resp.json.return_value = ARTICLE_RESPONSE
    resp.raise_for_status = MagicMock()

    url = "https://figshare.com/articles/dataset/x/12345"
    with patch("httpx.get", return_value=resp) as mock_get:
        meta = fs.fetch_metadata(url)

    assert meta.source_name == "figshare"
    assert meta.source_url == url
    assert meta.title == "Qualitative Interview Transcripts"
    assert meta.authors == "Smith, Jane; Doe, Adam"
    assert meta.license_type == "CC BY 4.0"
    assert meta.license_url == "https://creativecommons.org/licenses/by/4.0/"
    assert meta.date_published == "2023-06-15T00:00:00Z"
    assert meta.keywords == ["qualitative research", "interviews"]
    assert meta.tags == ["Social Sciences", "Health Sciences"]
    assert meta.kind_of_data == ["dataset"]
    assert meta.publication == ["https://doi.org/10.1234/test"]
    assert meta.uploader_name == "Smith, Jane"

    # Files
    assert len(meta.files) == 2
    f0 = meta.files[0]
    assert f0["name"] == "transcripts.pdf"
    assert f0["id"] == 111
    assert f0["size"] == 204800
    assert f0["download_url"] == "https://ndownloader.figshare.com/files/111"
    assert f0["content_type"] == "application/pdf"
    assert f0["restricted"] is False
    assert f0["api_checksum"] == "MD5:abc123def456"
    assert meta.files[1]["name"] == "codebook.docx"

    # Correct API endpoint
    assert "/v2/articles/12345" in mock_get.call_args[0][0]


def test_fetch_metadata_html_stripped(fs):
    resp = MagicMock()
    resp.json.return_value = ARTICLE_RESPONSE
    resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=resp):
        meta = fs.fetch_metadata("https://figshare.com/articles/dataset/x/12345")

    assert "<p>" not in meta.title
    assert "<i>" not in meta.title
    assert meta.title == "Qualitative Interview Transcripts"
    assert "<p>" not in meta.description
    assert "<b>" not in meta.description
    assert "semi-structured" in meta.description


def test_fetch_metadata_skips_link_only(fs):
    data = {**ARTICLE_RESPONSE}
    data["files"] = [
        {
            "id": 111, "name": "transcripts.pdf", "size": 204800,
            "download_url": "https://ndownloader.figshare.com/files/111",
            "mimetype": "application/pdf", "computed_md5": "abc123",
            "supplied_md5": "", "is_link_only": False,
        },
        {
            "id": 333, "name": "external_link", "size": 0,
            "download_url": "https://example.com/external",
            "mimetype": "undefined", "computed_md5": "",
            "supplied_md5": "", "is_link_only": True,
        },
    ]

    resp = MagicMock()
    resp.json.return_value = data
    resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=resp):
        meta = fs.fetch_metadata("https://figshare.com/articles/dataset/x/12345")

    assert len(meta.files) == 1
    assert meta.files[0]["name"] == "transcripts.pdf"


def test_fetch_metadata_confidential(fs):
    data = {
        "id": 99999,
        "title": "Secret Data",
        "is_confidential": True,
        "is_metadata_record": False,
    }
    resp = MagicMock()
    resp.json.return_value = data
    resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=resp):
        meta = fs.fetch_metadata("https://figshare.com/articles/dataset/x/99999")

    assert meta.files == []
    assert meta.title == "Secret Data"


def test_fetch_metadata_undefined_mime(fs):
    data = {**ARTICLE_RESPONSE}
    data["files"] = [{
        "id": 444, "name": "data.xyz", "size": 100,
        "download_url": "https://ndownloader.figshare.com/files/444",
        "mimetype": "undefined", "computed_md5": "aaa",
        "supplied_md5": "", "is_link_only": False,
    }]

    resp = MagicMock()
    resp.json.return_value = data
    resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=resp):
        meta = fs.fetch_metadata("https://figshare.com/articles/dataset/x/12345")

    assert meta.files[0]["content_type"] == ""


# ── Download ───────────────────────────────────────────────


def test_pull_file(fs, tmp_path):
    content = b"fake figshare file content"

    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.iter_bytes = MagicMock(return_value=iter([content]))
    mock_resp.headers = {}
    mock_resp.__enter__ = MagicMock(return_value=mock_resp)
    mock_resp.__exit__ = MagicMock(return_value=False)

    with patch("httpx.stream", return_value=mock_resp):
        path = fs.pull_file(
            "https://ndownloader.figshare.com/files/111",
            str(tmp_path),
            filename="transcripts.pdf",
        )

    assert path == str(tmp_path / "transcripts.pdf")
    assert (tmp_path / "transcripts.pdf").read_bytes() == content


def test_pull_file_infers_filename(fs, tmp_path):
    content = b"data"

    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.iter_bytes = MagicMock(return_value=iter([content]))
    mock_resp.headers = {}
    mock_resp.__enter__ = MagicMock(return_value=mock_resp)
    mock_resp.__exit__ = MagicMock(return_value=False)

    with patch("httpx.stream", return_value=mock_resp):
        path = fs.pull_file(
            "https://ndownloader.figshare.com/files/interview.txt",
            str(tmp_path),
        )

    assert Path(path).name == "interview.txt"


# ── Utility functions ──────────────────────────────────────


def test_parse_article_id_standard():
    assert _extract_article_id(
        "https://figshare.com/articles/dataset/My_Title/12345"
    ) == "12345"


def test_parse_article_id_versioned():
    assert _extract_article_id(
        "https://figshare.com/articles/dataset/My_Title/12345/2"
    ) == "12345"


def test_parse_article_id_institution():
    assert _extract_article_id(
        "https://monash.figshare.com/articles/dataset/Something/99999"
    ) == "99999"


def test_parse_article_id_bare_number():
    assert _extract_article_id("12345") == "12345"


def test_clean_title_strips_html():
    assert _clean_title("<p>Hello <i>world</i></p>") == "Hello world"


def test_clean_title_collapses_newlines():
    assert _clean_title("Line one\nLine two") == "Line one Line two"


def test_clean_title_empty():
    assert _clean_title("") == ""


# ── Registry ──────────────────────────────────────────────


def test_source_registry():
    from harvester.sources import SOURCES

    assert "figshare" in SOURCES
    assert isinstance(SOURCES["figshare"], FigshareSource)
