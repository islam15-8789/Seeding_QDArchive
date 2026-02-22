"""Unit tests for the FSDSource — OAI-PMH search, metadata, download."""

from unittest.mock import MagicMock, patch
from xml.etree.ElementTree import Element, SubElement, tostring

import pytest

from harvester.sources.base import BaseSource
from harvester.sources.fsd import (
    FSDSource,
    _extract_fsd_id,
    _to_oai_identifier,
    _NS,
)


@pytest.fixture
def fsd():
    src = FSDSource()
    src._last_request_time = 0.0
    return src


# ── Helpers to build OAI-PMH XML responses ─────────────────

def _oai_envelope() -> Element:
    root = Element("{http://www.openarchives.org/OAI/2.0/}OAI-PMH")
    return root


def _make_dc_record(
    oai_id: str,
    title_en: str,
    description_en: str = "",
    subjects: list[str] | None = None,
    set_specs: list[str] | None = None,
):
    """Build one OAI-PMH record with Dublin Core metadata."""
    record = Element("{http://www.openarchives.org/OAI/2.0/}record")
    header = SubElement(record, "{http://www.openarchives.org/OAI/2.0/}header")
    ident = SubElement(header, "{http://www.openarchives.org/OAI/2.0/}identifier")
    ident.text = oai_id
    ds = SubElement(header, "{http://www.openarchives.org/OAI/2.0/}datestamp")
    ds.text = "2024-01-01T00:00:00Z"

    for spec in (set_specs or []):
        ss = SubElement(header, "{http://www.openarchives.org/OAI/2.0/}setSpec")
        ss.text = spec

    md = SubElement(record, "{http://www.openarchives.org/OAI/2.0/}metadata")
    dc = SubElement(md, "{http://www.openarchives.org/OAI/2.0/oai_dc/}dc")

    t = SubElement(dc, "{http://purl.org/dc/elements/1.1/}title")
    t.set("{http://www.w3.org/XML/1998/namespace}lang", "en")
    t.text = title_en

    if description_en:
        d = SubElement(dc, "{http://purl.org/dc/elements/1.1/}description")
        d.set("{http://www.w3.org/XML/1998/namespace}lang", "en")
        d.text = description_en

    for subj in (subjects or []):
        s = SubElement(dc, "{http://purl.org/dc/elements/1.1/}subject")
        s.text = subj

    u = SubElement(dc, "{http://purl.org/dc/elements/1.1/}identifier")
    u.text = f"https://urn.fi/urn:nbn:fi:fsd:T-{oai_id.split(':')[-1]}"

    return record


def _wrap_list_records(records, token=None):
    """Wrap records in an OAI-PMH ListRecords envelope."""
    root = _oai_envelope()
    lr = SubElement(root, "{http://www.openarchives.org/OAI/2.0/}ListRecords")
    for rec in records:
        lr.append(rec)
    if token:
        tok = SubElement(lr, "{http://www.openarchives.org/OAI/2.0/}resumptionToken")
        tok.text = token
    else:
        tok = SubElement(lr, "{http://www.openarchives.org/OAI/2.0/}resumptionToken")
    return root


# ── Interface compliance ───────────────────────────────────


def test_implements_base_source(fsd):
    assert isinstance(fsd, BaseSource)


def test_label(fsd):
    assert fsd.label == "fsd"


# ── Search ─────────────────────────────────────────────────


def test_find_basic(fsd):
    records = [
        _make_dc_record(
            "oai:fsd.uta.fi:FSD0001",
            "Qualitative Interview Data on Health",
            "Semi-structured interviews about health care.",
            subjects=["health", "qualitative research"],
        ),
        _make_dc_record(
            "oai:fsd.uta.fi:FSD0002",
            "Focus Group Transcripts",
            "Focus group discussion transcripts.",
            subjects=["focus groups"],
        ),
    ]
    page = _wrap_list_records(records)

    with patch("httpx.get") as mock_get:
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.content = tostring(page, encoding="unicode").encode()
        mock_get.return_value = resp

        hits = fsd.find("health")

    assert len(hits) == 1  # only "health" matches
    assert hits[0].source_name == "fsd"
    assert "Qualitative Interview" in hits[0].title
    assert "urn.fi" in hits[0].source_url


def test_find_empty(fsd):
    page = _wrap_list_records([])

    with patch("httpx.get") as mock_get:
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.content = tostring(page, encoding="unicode").encode()
        mock_get.return_value = resp

        hits = fsd.find("nonexistent_term_xyz")

    assert hits == []


def test_find_pagination(fsd):
    rec1 = _make_dc_record(
        "oai:fsd.uta.fi:FSD0001",
        "Interview Data Part 1",
        "Interview transcripts.",
        subjects=["interview"],
    )
    rec2 = _make_dc_record(
        "oai:fsd.uta.fi:FSD0002",
        "Interview Data Part 2",
        "More interview transcripts.",
        subjects=["interview"],
    )
    page1 = _wrap_list_records([rec1], token="resume_token_123")
    page2 = _wrap_list_records([rec2])

    call_count = 0

    def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.content = tostring(page1 if call_count == 1 else page2, encoding="unicode").encode()
        return resp

    with patch("httpx.get", side_effect=side_effect):
        hits = fsd.find("interview")

    assert len(hits) == 2
    assert call_count == 2


def test_find_skips_deleted(fsd):
    rec = _make_dc_record(
        "oai:fsd.uta.fi:FSD0001",
        "Deleted Record Test",
        "This should be skipped.",
    )
    # Mark as deleted
    header = rec.find("{http://www.openarchives.org/OAI/2.0/}header")
    header.set("status", "deleted")

    page = _wrap_list_records([rec])

    with patch("httpx.get") as mock_get:
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.content = tostring(page, encoding="unicode").encode()
        mock_get.return_value = resp

        hits = fsd.find("deleted")

    assert hits == []


# ── Full metadata (DDI) ───────────────────────────────────

def _make_ddi_response(
    fsd_id: str = "FSD4012",
    title_en: str = "Child Barometer 2024",
    abstract_en: str = "Survey about children's experiences.",
    authors: list[str] | None = None,
    keywords: list[str] | None = None,
    restrctn: str = "(A) openly available for all users without registration (CC BY 4.0)",
    files: list[str] | None = None,
):
    """Build a GetRecord DDI 2.5 XML response."""
    if files is None:
        files = ["daF4012_eng.sav"]
    root = _oai_envelope()
    gr = SubElement(root, "{http://www.openarchives.org/OAI/2.0/}GetRecord")
    record = SubElement(gr, "{http://www.openarchives.org/OAI/2.0/}record")
    header = SubElement(record, "{http://www.openarchives.org/OAI/2.0/}header")
    ident = SubElement(header, "{http://www.openarchives.org/OAI/2.0/}identifier")
    ident.text = f"oai:fsd.uta.fi:{fsd_id}"
    ds = SubElement(header, "{http://www.openarchives.org/OAI/2.0/}datestamp")
    ds.text = "2024-01-01T00:00:00Z"
    ss = SubElement(header, "{http://www.openarchives.org/OAI/2.0/}setSpec")
    ss.text = "language:en"

    md = SubElement(record, "{http://www.openarchives.org/OAI/2.0/}metadata")
    cb = SubElement(md, "{ddi:codebook:2_5}codeBook")
    stdy = SubElement(cb, "{ddi:codebook:2_5}stdyDscr")

    # Citation
    citation = SubElement(stdy, "{ddi:codebook:2_5}citation")
    titl_stmt = SubElement(citation, "{ddi:codebook:2_5}titlStmt")
    titl = SubElement(titl_stmt, "{ddi:codebook:2_5}titl")
    titl.set("{http://www.w3.org/XML/1998/namespace}lang", "en")
    titl.text = title_en

    rsp_stmt = SubElement(citation, "{ddi:codebook:2_5}rspStmt")
    for a in (authors or ["Smith, Jane", "Doe, Adam"]):
        auth = SubElement(rsp_stmt, "{ddi:codebook:2_5}AuthEnty")
        auth.text = a

    dist_stmt = SubElement(citation, "{ddi:codebook:2_5}distStmt")
    dist_date = SubElement(dist_stmt, "{ddi:codebook:2_5}distDate")
    dist_date.text = "2024-06-15"

    # StudyInfo
    stdy_info = SubElement(stdy, "{ddi:codebook:2_5}stdyInfo")
    abstract = SubElement(stdy_info, "{ddi:codebook:2_5}abstract")
    abstract.set("{http://www.w3.org/XML/1998/namespace}lang", "en")
    abstract.text = abstract_en

    subject = SubElement(stdy_info, "{ddi:codebook:2_5}subject")
    for kw in (keywords or ["children", "barometer"]):
        kw_el = SubElement(subject, "{ddi:codebook:2_5}keyword")
        kw_el.text = kw

    sum_dscr = SubElement(stdy_info, "{ddi:codebook:2_5}sumDscr")
    nation = SubElement(sum_dscr, "{ddi:codebook:2_5}nation")
    nation.text = "Finland"
    dk = SubElement(sum_dscr, "{ddi:codebook:2_5}dataKind")
    dk.text = "Quantitative"

    # Access
    data_accs = SubElement(stdy, "{ddi:codebook:2_5}dataAccs")
    use_stmt = SubElement(data_accs, "{ddi:codebook:2_5}useStmt")
    restr = SubElement(use_stmt, "{ddi:codebook:2_5}restrctn")
    restr.text = restrctn

    # Files
    for fname in files:
        file_dscr = SubElement(cb, "{ddi:codebook:2_5}fileDscr")
        file_dscr.set("ID", fname.split(".")[0])
        file_txt = SubElement(file_dscr, "{ddi:codebook:2_5}fileTxt")
        fn_el = SubElement(file_txt, "{ddi:codebook:2_5}fileName")
        fn_el.text = fname

    return root


def test_fetch_metadata_basic(fsd):
    ddi_resp = _make_ddi_response()

    with patch("httpx.get") as mock_get:
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.content = tostring(ddi_resp, encoding="unicode").encode()
        mock_get.return_value = resp

        meta = fsd.fetch_metadata("FSD4012")

    assert meta.source_name == "fsd"
    assert meta.title == "Child Barometer 2024"
    assert meta.authors == "Smith, Jane; Doe, Adam"
    assert meta.date_published == "2024-06-15"
    assert "children" in meta.keywords
    assert "Finland" in meta.geographic_coverage
    assert "(A)" in meta.license_type
    assert "creativecommons.org" in meta.license_url
    assert len(meta.files) == 1
    assert meta.files[0]["name"] == "daF4012_eng.sav"
    assert meta.files[0]["restricted"] is False


def test_fetch_metadata_restricted(fsd):
    ddi_resp = _make_ddi_response(
        restrctn="(B) available for research, teaching and study",
    )

    with patch("httpx.get") as mock_get:
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.content = tostring(ddi_resp, encoding="unicode").encode()
        mock_get.return_value = resp

        meta = fsd.fetch_metadata("FSD4012")

    assert "(B)" in meta.license_type
    assert meta.license_url == ""
    assert meta.files[0]["restricted"] is True


def test_fetch_metadata_no_files(fsd):
    ddi_resp = _make_ddi_response(files=[])

    with patch("httpx.get") as mock_get:
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.content = tostring(ddi_resp, encoding="unicode").encode()
        mock_get.return_value = resp

        meta = fsd.fetch_metadata("FSD4012")

    assert meta.files == []
    assert meta.title == "Child Barometer 2024"


# ── Download ───────────────────────────────────────────────


def test_pull_file(fsd, tmp_path):
    content = b"fake fsd file content"

    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.iter_bytes = MagicMock(return_value=iter([content]))
    mock_resp.headers = {}
    mock_resp.__enter__ = MagicMock(return_value=mock_resp)
    mock_resp.__exit__ = MagicMock(return_value=False)

    with patch("httpx.stream", return_value=mock_resp):
        path = fsd.pull_file(
            "https://services.fsd.tuni.fi/catalogue/download/FSD4012",
            str(tmp_path),
            filename="daF4012_eng.sav",
        )

    assert path == str(tmp_path / "daF4012_eng.sav")
    assert (tmp_path / "daF4012_eng.sav").read_bytes() == content


# ── Utility functions ──────────────────────────────────────


def test_extract_fsd_id_bare():
    assert _extract_fsd_id("FSD4012") == "FSD4012"


def test_extract_fsd_id_oai():
    assert _extract_fsd_id("oai:fsd.uta.fi:FSD4012") == "FSD4012"


def test_extract_fsd_id_urn():
    assert _extract_fsd_id("https://urn.fi/urn:nbn:fi:fsd:T-FSD4012") == "FSD4012"


def test_to_oai_identifier():
    assert _to_oai_identifier("FSD4012") == "oai:fsd.uta.fi:FSD4012"


def test_to_oai_identifier_already_oai():
    assert _to_oai_identifier("oai:fsd.uta.fi:FSD4012") == "oai:fsd.uta.fi:FSD4012"


# ── Registry ──────────────────────────────────────────────


def test_source_registry():
    from harvester.sources import SOURCES

    assert "fsd" in SOURCES
    assert isinstance(SOURCES["fsd"], FSDSource)
