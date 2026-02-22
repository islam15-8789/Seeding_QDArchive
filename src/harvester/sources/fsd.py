"""FSD Finland (Finnish Social Science Data Archive) OAI-PMH source."""

import logging
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import httpx

from harvester.sources.base import BaseSource, DatasetHit
from harvester.sources.dataverse import _clean_html, _name_from_headers

log = logging.getLogger("harvester")

_OAI_BASE = "https://services.fsd.tuni.fi/v0/oai"
_API_TIMEOUT = 60.0
_DOWNLOAD_TIMEOUT = 120.0
_RETRY_LIMIT = 3
_INITIAL_BACKOFF = 2.0
_SEARCH_CAP = 500
_THROTTLE = 1.0

# OAI-PMH / Dublin Core namespaces
_NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "dc": "http://purl.org/dc/elements/1.1/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
    "ddi": "ddi:codebook:2_5",
}

# Access level prefixes in DDI restrctn field
_OPEN_ACCESS_MARKER = "(A)"


class FSDSource(BaseSource):
    """Source for the Finnish Social Science Data Archive.

    Uses the OAI-PMH v2 endpoint to harvest metadata in Dublin Core
    and DDI 2.5 formats.  Only category (A) datasets (CC BY 4.0) are
    freely downloadable without authentication.
    """

    def __init__(self) -> None:
        self._last_request_time = 0.0

    @property
    def label(self) -> str:
        return "fsd"

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < _THROTTLE:
            time.sleep(_THROTTLE - elapsed)
        self._last_request_time = time.monotonic()

    def _oai_get(self, params: dict) -> ET.Element:
        """Send an OAI-PMH request and return the parsed XML root."""
        for attempt in range(1, _RETRY_LIMIT + 1):
            self._throttle()
            r = httpx.get(_OAI_BASE, params=params, timeout=_API_TIMEOUT)
            if r.status_code == 429:
                wait = _INITIAL_BACKOFF * (2 ** (attempt - 1))
                log.warning("[fsd] 429 rate-limited — retrying in %.0fs", wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return ET.fromstring(r.content)
        r.raise_for_status()
        return ET.fromstring(r.content)

    # ── Search ──────────────────────────────────────────────

    def find(self, query: str, file_type: str | None = None) -> list[DatasetHit]:
        """Harvest OAI-PMH records and filter locally by query keywords.

        Uses ``ListRecords`` with ``oai_dc`` (lightweight).  The OAI-PMH
        protocol has no search verb, so we harvest all records and match
        titles/descriptions/subjects against the query.
        """
        query_terms = [t.lower() for t in query.split() if t]
        hits: list[DatasetHit] = []
        params: dict[str, str] = {
            "verb": "ListRecords",
            "metadataPrefix": "oai_dc",
        }

        while True:
            root = self._oai_get(params)

            list_records = root.find("oai:ListRecords", _NS)
            if list_records is None:
                # Check for OAI error (e.g. expired token)
                error = root.find("oai:error", _NS)
                if error is not None:
                    log.warning("[fsd] OAI error: %s", error.text)
                break

            for record in list_records.findall("oai:record", _NS):
                header = record.find("oai:header", _NS)
                if header is None:
                    continue
                # Skip deleted records
                if header.get("status") == "deleted":
                    continue

                metadata = record.find("oai:metadata/oai_dc:dc", _NS)
                if metadata is None:
                    continue

                hit = self._dc_to_hit(header, metadata)
                if hit is None:
                    continue

                # Local keyword matching
                searchable = f"{hit.title} {hit.description} {' '.join(hit.tags)}".lower()
                if query_terms and not all(t in searchable for t in query_terms):
                    continue

                hits.append(hit)

                if len(hits) >= _SEARCH_CAP:
                    log.info("[fsd] '%s': capped at %d results", query, _SEARCH_CAP)
                    return hits[:_SEARCH_CAP]

            # Pagination via resumptionToken
            token_el = list_records.find("oai:resumptionToken", _NS)
            if token_el is None or not token_el.text:
                break

            params = {"verb": "ListRecords", "resumptionToken": token_el.text}

        log.info("[fsd] '%s': %d record(s)", query, len(hits))
        return hits

    # ── Full metadata ───────────────────────────────────────

    def fetch_metadata(self, url: str) -> DatasetHit:
        """Fetch rich DDI 2.5 metadata for one record.

        Accepts FSD identifiers like ``FSD4012``, OAI identifiers
        like ``oai:fsd.uta.fi:FSD4012``, or URN URLs.
        """
        oai_id = _to_oai_identifier(url)
        fsd_id = _extract_fsd_id(url)

        # Try DDI 2.5 first for richer metadata
        root = self._oai_get({
            "verb": "GetRecord",
            "metadataPrefix": "oai_ddi25",
            "identifier": oai_id,
        })

        record = root.find(".//oai:record", _NS)
        if record is None:
            # Fallback to Dublin Core
            return self._fetch_dc_metadata(oai_id, url)

        md = record.find("oai:metadata", _NS)
        if md is None:
            return self._fetch_dc_metadata(oai_id, url)

        cb = md.find("ddi:codeBook", _NS)
        if cb is None:
            return self._fetch_dc_metadata(oai_id, url)

        stdy = cb.find("ddi:stdyDscr", _NS)
        if stdy is None:
            return DatasetHit(source_name="fsd", source_url=url, title="")

        # Title (prefer English)
        title = _ddi_text(stdy, "ddi:citation/ddi:titlStmt/ddi:titl", lang="en")

        # Description
        description = _clean_html(
            _ddi_text(stdy, "ddi:stdyInfo/ddi:abstract", lang="en")
        )

        # Authors
        authors = []
        for auth in stdy.findall("ddi:citation/ddi:rspStmt/ddi:AuthEnty", _NS):
            name = (auth.text or "").strip()
            if name:
                authors.append(name)

        # Keywords
        keywords = []
        for kw in stdy.findall("ddi:stdyInfo/ddi:subject/ddi:keyword", _NS):
            text = (kw.text or "").strip()
            if text:
                keywords.append(text)

        # Topic classifications → tags
        tags = []
        for tc in stdy.findall("ddi:stdyInfo/ddi:subject/ddi:topcClas", _NS):
            text = (tc.text or "").strip()
            if text:
                tags.append(text)

        # Date
        dist_date = _ddi_text(stdy, "ddi:citation/ddi:distStmt/ddi:distDate")

        # Geographic coverage
        geo = []
        for n in stdy.findall("ddi:stdyInfo/ddi:sumDscr/ddi:nation", _NS):
            text = (n.text or "").strip()
            if text:
                geo.append(text)
        for g in stdy.findall("ddi:stdyInfo/ddi:sumDscr/ddi:geogCover", _NS):
            text = (g.text or "").strip()
            if text and text not in geo:
                geo.append(text)

        # Language
        language = []
        # Look in the header setSpecs for language info
        header = record.find("oai:header", _NS)
        if header is not None:
            for ss in header.findall("oai:setSpec", _NS):
                text = (ss.text or "")
                if text.startswith("language:"):
                    language.append(text.split(":", 1)[1])

        # Collection dates
        date_of_collection = ""
        coll_dates = stdy.findall("ddi:stdyInfo/ddi:sumDscr/ddi:collDate", _NS)
        starts = [d.get("date", "") for d in coll_dates if d.get("event") == "start"]
        ends = [d.get("date", "") for d in coll_dates if d.get("event") == "end"]
        if starts or ends:
            s = starts[0] if starts else ""
            e = ends[0] if ends else ""
            date_of_collection = f"{s} – {e}" if s and e else (s or e)

        # Time period
        time_period_covered = ""
        time_els = stdy.findall("ddi:stdyInfo/ddi:sumDscr/ddi:timePrd", _NS)
        tp_starts = [t.get("date", "") for t in time_els if t.get("event") == "start"]
        tp_ends = [t.get("date", "") for t in time_els if t.get("event") == "end"]
        if tp_starts or tp_ends:
            s = tp_starts[0] if tp_starts else ""
            e = tp_ends[0] if tp_ends else ""
            time_period_covered = f"{s} – {e}" if s and e else (s or e)

        # Kind of data
        kind_of_data = []
        for dk in stdy.findall("ddi:stdyInfo/ddi:sumDscr/ddi:dataKind", _NS):
            text = (dk.text or "").strip()
            if text:
                kind_of_data.append(text)

        # Access / license
        license_type = ""
        restrctn = stdy.find("ddi:dataAccs/ddi:useStmt/ddi:restrctn", _NS)
        if restrctn is not None and restrctn.text:
            license_type = restrctn.text.strip()

        license_url = ""
        if _OPEN_ACCESS_MARKER in license_type:
            license_url = "https://creativecommons.org/licenses/by/4.0/"

        # Producer
        producers = []
        for p in stdy.findall("ddi:citation/ddi:prodStmt/ddi:producer", _NS):
            text = (p.text or "").strip()
            if text:
                producers.append(text)

        # Files (from fileDscr)
        file_list = []
        for fd in cb.findall("ddi:fileDscr", _NS):
            ft = fd.find("ddi:fileTxt", _NS)
            if ft is None:
                continue
            fn_el = ft.find("ddi:fileName", _NS)
            fname = (fn_el.text or "").strip() if fn_el is not None else ""
            if fname:
                file_list.append({
                    "id": fd.get("ID", ""),
                    "name": fname,
                    "size": 0,
                    "download_url": "",
                    "content_type": "",
                    "restricted": _OPEN_ACCESS_MARKER not in license_type,
                    "api_checksum": "",
                })

        source_url = url
        if fsd_id and not url.startswith("http"):
            source_url = f"https://urn.fi/urn:nbn:fi:fsd:T-{fsd_id}"

        return DatasetHit(
            source_name="fsd",
            source_url=source_url,
            title=title,
            description=description,
            authors="; ".join(authors),
            license_type=license_type,
            license_url=license_url,
            date_published=dist_date,
            keywords=keywords,
            tags=tags,
            kind_of_data=kind_of_data,
            language=language,
            geographic_coverage=geo,
            depositor="",
            producer=producers,
            publication=[],
            date_of_collection=date_of_collection,
            time_period_covered=time_period_covered,
            uploader_name=authors[0] if authors else "",
            uploader_email="",
            files=file_list,
            software=[],
        )

    def _fetch_dc_metadata(self, oai_id: str, url: str) -> DatasetHit:
        """Fallback: fetch metadata via Dublin Core."""
        root = self._oai_get({
            "verb": "GetRecord",
            "metadataPrefix": "oai_dc",
            "identifier": oai_id,
        })
        record = root.find(".//oai:record", _NS)
        if record is None:
            return DatasetHit(source_name="fsd", source_url=url, title="")

        header = record.find("oai:header", _NS)
        metadata = record.find("oai:metadata/oai_dc:dc", _NS)
        if header is None or metadata is None:
            return DatasetHit(source_name="fsd", source_url=url, title="")

        hit = self._dc_to_hit(header, metadata)
        return hit or DatasetHit(source_name="fsd", source_url=url, title="")

    # ── File download ───────────────────────────────────────

    def pull_file(self, url: str, dest_dir: str, filename: str | None = None) -> str:
        """Stream a file from FSD with retry."""
        target = Path(dest_dir)
        target.mkdir(parents=True, exist_ok=True)

        for attempt in range(1, _RETRY_LIMIT + 1):
            try:
                with httpx.stream(
                    "GET", url, timeout=_DOWNLOAD_TIMEOUT, follow_redirects=True,
                ) as resp:
                    resp.raise_for_status()

                    if not filename:
                        filename = _name_from_headers(resp.headers)
                    if not filename:
                        filename = url.rstrip("/").split("/")[-1]

                    out = target / filename
                    with open(out, "wb") as fh:
                        for chunk in resp.iter_bytes(chunk_size=8192):
                            fh.write(chunk)

                log.info("Saved %s → %s", url, out)
                return str(out)

            except (httpx.ConnectError, httpx.ReadError, ConnectionError) as exc:
                if attempt < _RETRY_LIMIT:
                    wait = _INITIAL_BACKOFF * (2 ** (attempt - 1))
                    log.warning(
                        "Attempt %d/%d failed for %s (%s) — retrying in %.0fs",
                        attempt, _RETRY_LIMIT, url, exc, wait,
                    )
                    time.sleep(wait)
                else:
                    raise

    # ── Internal helpers ────────────────────────────────────

    def _dc_to_hit(
        self, header: ET.Element, dc: ET.Element,
    ) -> DatasetHit | None:
        """Convert a Dublin Core record to a DatasetHit."""
        # Prefer English title
        title = _dc_text(dc, "dc:title", lang="en")
        if not title:
            return None

        description = _clean_html(_dc_text(dc, "dc:description", lang="en"))

        # Source URL — prefer the https://urn.fi/ identifier
        source_url = ""
        for ident in dc.findall("dc:identifier", _NS):
            text = (ident.text or "").strip()
            if text.startswith("https://urn.fi/"):
                source_url = text
                break
        if not source_url:
            oai_id = ""
            id_el = header.find("oai:identifier", _NS)
            if id_el is not None:
                oai_id = (id_el.text or "").strip()
            fsd_id = oai_id.split(":")[-1] if oai_id else ""
            if fsd_id:
                source_url = f"https://urn.fi/urn:nbn:fi:fsd:T-{fsd_id}"

        # Authors
        creators = []
        for c in dc.findall("dc:creator", _NS):
            text = (c.text or "").strip()
            if text:
                creators.append(text)

        # Subjects → tags
        tags = []
        for s in dc.findall("dc:subject", _NS):
            text = (s.text or "").strip()
            if text:
                tags.append(text)

        # Date
        date_published = _dc_text(dc, "dc:date")

        # Language
        language = []
        for lang in dc.findall("dc:language", _NS):
            text = (lang.text or "").strip()
            if text:
                language.append(text)

        # Geographic coverage
        geo = []
        for cov in dc.findall("dc:coverage", _NS):
            text = (cov.text or "").strip()
            if text:
                geo.append(text)

        # Kind of data from setSpecs
        kind_of_data = []
        for ss in header.findall("oai:setSpec", _NS):
            text = (ss.text or "")
            if text.startswith("data_kind:"):
                kind_of_data.append(text.split(":", 1)[1])

        return DatasetHit(
            source_name="fsd",
            source_url=source_url,
            title=title,
            description=description,
            authors="; ".join(creators),
            date_published=date_published,
            tags=tags,
            language=language,
            geographic_coverage=geo,
            kind_of_data=kind_of_data,
        )


# ── Module-level utilities ──────────────────────────────────


def _extract_fsd_id(url: str) -> str:
    """Extract the FSD identifier (e.g. 'FSD4012') from various formats.

    Handles:
    - FSD4012
    - oai:fsd.uta.fi:FSD4012
    - https://urn.fi/urn:nbn:fi:fsd:T-FSD4012
    - https://services.fsd.tuni.fi/catalogue/FSD4012
    """
    match = re.search(r"(FSD\d+)", url, re.IGNORECASE)
    if match:
        return match.group(1).upper()
    return url.strip().rstrip("/").split("/")[-1]


def _to_oai_identifier(url: str) -> str:
    """Convert any FSD reference to an OAI identifier."""
    fsd_id = _extract_fsd_id(url)
    if fsd_id.startswith("oai:"):
        return fsd_id
    return f"oai:fsd.uta.fi:{fsd_id}"


def _dc_text(element: ET.Element, tag: str, lang: str | None = None) -> str:
    """Get text from a Dublin Core element, preferring a given language."""
    candidates = element.findall(tag, _NS)
    if not candidates:
        return ""

    if lang:
        for c in candidates:
            el_lang = c.get("{http://www.w3.org/XML/1998/namespace}lang", "")
            if el_lang == lang:
                return (c.text or "").strip()

    # Fall back to first element
    return (candidates[0].text or "").strip()


def _ddi_text(
    element: ET.Element, path: str, lang: str | None = None,
) -> str:
    """Get text from a DDI element path, preferring a given language."""
    candidates = element.findall(path, _NS)
    if not candidates:
        return ""

    if lang:
        for c in candidates:
            el_lang = c.get("{http://www.w3.org/XML/1998/namespace}lang", "")
            if el_lang == lang:
                return (c.text or "").strip()

    return (candidates[0].text or "").strip()
