"""Unit tests for the OSFSource — search, metadata, download."""

from unittest.mock import MagicMock, patch

import pytest

from harvester.sources.base import BaseSource
from harvester.sources.osf import OSFSource, _extract_node_id


@pytest.fixture
def osf():
    src = OSFSource()
    src._last_request_time = 0.0  # disable throttle in tests
    return src


# ── Helpers to build JSON:API responses ────────────────────

def _make_node(node_id, title, **overrides):
    """Build a minimal OSF node resource."""
    attrs = {
        "title": title,
        "description": "",
        "date_created": "2023-06-15T00:00:00Z",
        "public": True,
        "registration": False,
        "preprint": False,
        "fork": False,
        "collection": False,
        "category": "project",
        "tags": [],
        "subjects": [],
    }
    attrs.update(overrides)
    return {"id": node_id, "attributes": attrs}


def _wrap_page(nodes, next_link=None):
    """Wrap node list in a JSON:API envelope."""
    return {
        "data": nodes,
        "links": {"next": next_link},
        "meta": {"total": len(nodes)},
    }


# ── Interface compliance ───────────────────────────────────


def test_implements_base_source(osf):
    assert isinstance(osf, BaseSource)


def test_label(osf):
    assert osf.label == "osf"


# ── Search ─────────────────────────────────────────────────


def test_find_basic(osf):
    nodes = [
        _make_node("abc12", "Qualitative Interview Data"),
        _make_node("def34", "Focus Group Transcripts"),
    ]
    page1 = _wrap_page(nodes)

    with patch("httpx.get") as mock_get:
        resp = MagicMock()
        resp.json.return_value = page1
        resp.raise_for_status = MagicMock()
        resp.status_code = 200
        mock_get.return_value = resp

        hits = osf.find("qualitative interview")

    assert len(hits) == 2
    assert hits[0].source_name == "osf"
    assert hits[0].title == "Qualitative Interview Data"
    assert "abc12" in hits[0].source_url
    assert hits[1].title == "Focus Group Transcripts"


def test_find_skips_non_public(osf):
    nodes = [
        _make_node("aaa11", "Registration Node", registration=True),
        _make_node("bbb22", "Preprint Node", preprint=True),
        _make_node("ccc33", "Forked Node", fork=True),
        _make_node("ddd44", "Collection Node", category="collection"),
        _make_node("eee55", "Valid Public Project"),
    ]
    page = _wrap_page(nodes)

    with patch("httpx.get") as mock_get:
        resp = MagicMock()
        resp.json.return_value = page
        resp.raise_for_status = MagicMock()
        resp.status_code = 200
        mock_get.return_value = resp

        hits = osf.find("test")

    assert len(hits) == 1
    assert hits[0].title == "Valid Public Project"


def test_find_pagination(osf):
    page1_nodes = [_make_node(f"n{i:03d}", f"Item {i}") for i in range(50)]
    page2_nodes = [_make_node(f"n{50 + i:03d}", f"Item {50 + i}") for i in range(10)]

    page1 = _wrap_page(page1_nodes, next_link="https://api.osf.io/v2/nodes/?page=2")
    page2 = _wrap_page(page2_nodes)

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
        hits = osf.find("test")

    assert len(hits) == 60
    assert call_count == 2


def test_find_empty(osf):
    page = _wrap_page([])

    with patch("httpx.get") as mock_get:
        resp = MagicMock()
        resp.json.return_value = page
        resp.raise_for_status = MagicMock()
        resp.status_code = 200
        mock_get.return_value = resp

        hits = osf.find("nonexistent")

    assert hits == []


# ── Full metadata ──────────────────────────────────────────

NODE_RESPONSE = {
    "data": {
        "id": "4vtu3",
        "attributes": {
            "title": "Semi-structured Interviews on Health",
            "description": "<p>A collection of <b>qualitative</b> interviews.</p>",
            "date_created": "2023-06-15T00:00:00Z",
            "tags": ["qualitative", "health"],
            "subjects": [
                [{"text": "Social and Behavioral Sciences"}, {"text": "Sociology"}],
            ],
            "node_license": {"year": "2023", "copyright_holders": ["Smith"]},
        },
        "relationships": {
            "license": {
                "links": {
                    "related": {
                        "href": "https://api.osf.io/v2/licenses/abc123/",
                    },
                },
            },
        },
    },
}

CONTRIBUTORS_RESPONSE = {
    "data": [
        {
            "embeds": {
                "users": {
                    "data": {
                        "attributes": {"full_name": "Jane Smith"},
                    },
                },
            },
        },
        {
            "embeds": {
                "users": {
                    "data": {
                        "attributes": {"full_name": "Adam Doe"},
                    },
                },
            },
        },
    ],
    "links": {"next": None},
}

FILES_RESPONSE = {
    "data": [
        {
            "id": "file001",
            "attributes": {
                "name": "transcripts.pdf",
                "kind": "file",
                "size": 204800,
                "content_type": "application/pdf",
                "extra": {
                    "hashes": {
                        "md5": "abc123def456",
                        "sha256": "deadbeefcafe1234567890",
                    },
                },
                "links": {
                    "download": "https://files.osf.io/v1/resources/4vtu3/providers/osfstorage/file001?action=download",
                },
            },
        },
        {
            "id": "file002",
            "attributes": {
                "name": "codebook.docx",
                "kind": "file",
                "size": 51200,
                "content_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "extra": {
                    "hashes": {"md5": "deadbeef", "sha256": "aabbccddee"},
                },
                "links": {
                    "download": "https://files.osf.io/v1/resources/4vtu3/providers/osfstorage/file002?action=download",
                },
            },
        },
    ],
    "links": {"next": None},
}

LICENSE_RESPONSE = {
    "data": {
        "attributes": {
            "name": "CC-By Attribution 4.0 International",
            "url": "https://creativecommons.org/licenses/by/4.0/legalcode",
        },
    },
}


def _osf_side_effect(call_map):
    """Build a side_effect that matches longest key first."""
    # Sort keys longest-first so /contributors/ matches before /nodes/id/
    sorted_keys = sorted(call_map.keys(), key=len, reverse=True)

    def side_effect(url, **kwargs):
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        for key in sorted_keys:
            if key in url:
                resp.json.return_value = call_map[key]
                return resp
        resp.json.return_value = {"data": {}}
        return resp

    return side_effect


def test_fetch_metadata_basic(osf):
    call_map = {
        "/v2/nodes/4vtu3/": NODE_RESPONSE,
        "/v2/nodes/4vtu3/contributors/": CONTRIBUTORS_RESPONSE,
        "/v2/nodes/4vtu3/files/osfstorage/": FILES_RESPONSE,
        "/v2/licenses/abc123/": LICENSE_RESPONSE,
    }

    url = "https://osf.io/4vtu3/"
    with patch("httpx.get", side_effect=_osf_side_effect(call_map)):
        meta = osf.fetch_metadata(url)

    assert meta.source_name == "osf"
    assert meta.source_url == url
    assert meta.title == "Semi-structured Interviews on Health"
    assert "qualitative" in meta.description
    assert "<b>" not in meta.description
    assert meta.authors == "Jane Smith; Adam Doe"
    assert meta.uploader_name == "Jane Smith"
    assert meta.license_type == "CC-By Attribution 4.0 International"
    assert "creativecommons.org" in meta.license_url
    assert meta.keywords == ["qualitative", "health"]
    assert "Social and Behavioral Sciences" in meta.tags
    assert "Sociology" in meta.tags

    # Files
    assert len(meta.files) == 2
    f0 = meta.files[0]
    assert f0["name"] == "transcripts.pdf"
    assert f0["id"] == "file001"
    assert f0["size"] == 204800
    assert "download" in f0["download_url"]
    assert f0["content_type"] == "application/pdf"
    assert f0["restricted"] is False
    assert f0["api_checksum"] == "SHA-256:deadbeefcafe1234567890"
    assert meta.files[1]["name"] == "codebook.docx"


def test_fetch_metadata_no_license(osf):
    node_no_lic = {
        "data": {
            "id": "xyz99",
            "attributes": {
                "title": "No License Node",
                "description": "",
                "date_created": "2024-01-01",
                "tags": [],
                "subjects": [],
                "node_license": None,
            },
            "relationships": {
                "license": {
                    "links": {"related": {"href": ""}},
                },
            },
        },
    }

    call_map = {
        "/v2/nodes/xyz99/": node_no_lic,
        "/v2/nodes/xyz99/contributors/": {"data": [], "links": {"next": None}},
        "/v2/nodes/xyz99/files/osfstorage/": {"data": [], "links": {"next": None}},
    }

    with patch("httpx.get", side_effect=_osf_side_effect(call_map)):
        meta = osf.fetch_metadata("https://osf.io/xyz99/")

    assert meta.license_type == ""
    assert meta.license_url == ""
    assert meta.title == "No License Node"


def test_fetch_metadata_no_files(osf):
    call_map = {
        "/v2/nodes/4vtu3/": NODE_RESPONSE,
        "/v2/nodes/4vtu3/contributors/": CONTRIBUTORS_RESPONSE,
        "/v2/nodes/4vtu3/files/osfstorage/": {"data": [], "links": {"next": None}},
        "/v2/licenses/abc123/": LICENSE_RESPONSE,
    }

    with patch("httpx.get", side_effect=_osf_side_effect(call_map)):
        meta = osf.fetch_metadata("https://osf.io/4vtu3/")

    assert meta.files == []
    assert meta.authors == "Jane Smith; Adam Doe"


# ── Download ───────────────────────────────────────────────


def test_pull_file(osf, tmp_path):
    content = b"fake osf file content"

    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.iter_bytes = MagicMock(return_value=iter([content]))
    mock_resp.headers = {}
    mock_resp.__enter__ = MagicMock(return_value=mock_resp)
    mock_resp.__exit__ = MagicMock(return_value=False)

    with patch("httpx.stream", return_value=mock_resp):
        path = osf.pull_file(
            "https://files.osf.io/v1/resources/4vtu3/providers/osfstorage/file001",
            str(tmp_path),
            filename="transcripts.pdf",
        )

    assert path == str(tmp_path / "transcripts.pdf")
    assert (tmp_path / "transcripts.pdf").read_bytes() == content


# ── Utility functions ──────────────────────────────────────


def test_extract_node_id_web_url():
    assert _extract_node_id("https://osf.io/4vtu3/") == "4vtu3"


def test_extract_node_id_api_url():
    assert _extract_node_id("https://api.osf.io/v2/nodes/4vtu3/") == "4vtu3"


def test_extract_node_id_api_url_with_path():
    assert _extract_node_id("https://api.osf.io/v2/nodes/abc12/files/") == "abc12"


def test_extract_node_id_bare():
    assert _extract_node_id("4vtu3") == "4vtu3"


# ── Registry ──────────────────────────────────────────────


def test_source_registry():
    from harvester.sources import SOURCES

    assert "osf" in SOURCES
    assert isinstance(SOURCES["osf"], OSFSource)
