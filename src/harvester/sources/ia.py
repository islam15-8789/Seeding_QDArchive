"""Internet Archive Advanced Search API source for oral history content."""

import logging
import re
import time
from pathlib import Path
from urllib.parse import quote

import httpx

from harvester.sources.base import BaseSource, DatasetHit
from harvester.sources.dataverse import _clean_html, _name_from_headers

log = logging.getLogger("harvester")

_SEARCH_BASE = "https://archive.org/advancedsearch.php"
_METADATA_BASE = "https://archive.org/metadata"
_DOWNLOAD_BASE = "https://archive.org/download"
_API_TIMEOUT = 30.0
_DOWNLOAD_TIMEOUT = 120.0
_RETRY_LIMIT = 3
_INITIAL_BACKOFF = 2.0
_SEARCH_CAP = 500
_PAGE_SIZE = 50
_THROTTLE = 1.0  # conservative for unauthenticated access

# Fields to request from the search API
_SEARCH_FIELDS = [
    "identifier", "title", "description", "date", "creator",
    "licenseurl", "subject", "mediatype", "language", "publicdate",
]

# File sources to keep (skip derivatives and metadata)
_KEEP_SOURCES = {"original"}


class IASource(BaseSource):
    """Source for the Internet Archive (oral history and text collections).

    Uses the Advanced Search API (https://archive.org/advancedsearch.php)
    and the Metadata API (https://archive.org/metadata/<id>).
    No authentication required.
    """

    def __init__(self) -> None:
        self._last_request_time = 0.0

    @property
    def label(self) -> str:
        return "ia"

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < _THROTTLE:
            time.sleep(_THROTTLE - elapsed)
        self._last_request_time = time.monotonic()

    def _get_json(self, url: str, params: dict | None = None) -> dict:
        """GET with throttle and 429 backoff."""
        for attempt in range(1, _RETRY_LIMIT + 1):
            self._throttle()
            r = httpx.get(url, params=params, timeout=_API_TIMEOUT)
            if r.status_code == 429:
                wait = _INITIAL_BACKOFF * (2 ** (attempt - 1))
                log.warning(
                    "[ia] 429 rate-limited — retrying in %.0fs (attempt %d/%d)",
                    wait, attempt, _RETRY_LIMIT,
                )
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        r.raise_for_status()
        return r.json()

    # ── Search ──────────────────────────────────────────────

    def find(self, query: str, file_type: str | None = None) -> list[DatasetHit]:
        """Search Internet Archive items via Advanced Search API.

        Searches within ``subject:"oral history"`` and ``mediatype:texts``
        by default.  The user query is added as a free-text filter.
        """
        hits: list[DatasetHit] = []
        start = 0

        # Build Lucene query
        q_parts = [f"({query})"]
        q_parts.append('mediatype:(texts OR audio)')
        lucene_q = " AND ".join(q_parts)

        while True:
            params: dict[str, str | int] = {
                "q": lucene_q,
                "output": "json",
                "rows": _PAGE_SIZE,
                "start": start,
            }
            # Add field selectors
            for field in _SEARCH_FIELDS:
                params[f"fl[]"] = field  # httpx handles repeated keys via list
            # Use list for repeated fl[] params
            param_list = [
                ("q", lucene_q),
                ("output", "json"),
                ("rows", str(_PAGE_SIZE)),
                ("start", str(start)),
            ]
            for field in _SEARCH_FIELDS:
                param_list.append(("fl[]", field))

            self._throttle()
            for attempt in range(1, _RETRY_LIMIT + 1):
                r = httpx.get(
                    _SEARCH_BASE,
                    params=param_list,
                    timeout=_API_TIMEOUT,
                )
                if r.status_code == 429:
                    wait = _INITIAL_BACKOFF * (2 ** (attempt - 1))
                    log.warning("[ia] 429 rate-limited — retrying in %.0fs", wait)
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                break

            data = r.json()
            response = data.get("response", {})
            docs = response.get("docs", [])
            num_found = response.get("numFound", 0)

            if not docs:
                break

            for doc in docs:
                identifier = doc.get("identifier", "")
                title = doc.get("title", "")
                if not title:
                    continue

                description = _ensure_str(doc.get("description", ""))
                description = _clean_html(description)

                subject = doc.get("subject", [])
                if isinstance(subject, str):
                    subject = [s.strip() for s in subject.split(";")]

                hit = DatasetHit(
                    source_name="ia",
                    source_url=f"https://archive.org/details/{identifier}",
                    title=title,
                    description=description,
                    authors=_ensure_str(doc.get("creator", "")),
                    date_published=doc.get("date", "") or doc.get("publicdate", ""),
                    tags=subject,
                )
                hits.append(hit)

            if len(hits) >= _SEARCH_CAP:
                log.info("[ia] '%s': capped at %d results", query, _SEARCH_CAP)
                hits = hits[:_SEARCH_CAP]
                break

            start += _PAGE_SIZE
            if start >= num_found:
                break

        log.info("[ia] '%s': %d item(s)", query, len(hits))
        return hits

    # ── Full metadata ───────────────────────────────────────

    def fetch_metadata(self, url: str) -> DatasetHit:
        """Retrieve complete metadata for an Internet Archive item.

        Accepts URLs like ``https://archive.org/details/<identifier>``
        or bare identifiers.
        """
        identifier = _extract_identifier(url)

        data = self._get_json(f"{_METADATA_BASE}/{identifier}")
        md = data.get("metadata", {})

        title = _ensure_str(md.get("title", ""))
        description = _clean_html(_ensure_str(md.get("description", "")))
        creator = _ensure_str(md.get("creator", ""))
        date = md.get("date", "") or md.get("publicdate", "")
        license_url = md.get("licenseurl", "") or ""

        # Derive license type from URL
        license_type = _license_name_from_url(license_url)

        # Fallback: detect public domain from other metadata fields
        if not license_type:
            rights = _ensure_str(md.get("rights", ""))
            possible_info = _ensure_str(md.get("rights-info", ""))
            combined = f"{rights} {possible_info} {description}".lower()
            if "public domain" in combined or "no known copyright" in combined:
                license_type = "Public Domain"
            elif "united states government" in combined:
                license_type = "Public Domain (US Government)"

        # Subject / tags
        subject = md.get("subject", [])
        if isinstance(subject, str):
            subject = [s.strip() for s in subject.split(";")]

        # Language
        lang = md.get("language", "")
        language = [lang] if lang else []

        # Files — keep originals
        file_list = []
        for f in data.get("files", []):
            source = f.get("source", "")
            if source not in _KEEP_SOURCES:
                continue
            # Skip internal metadata files
            name = f.get("name", "")
            if name.endswith("_meta.xml") or name.endswith("_files.xml"):
                continue

            md5 = f.get("md5", "")
            api_checksum = f"MD5:{md5}" if md5 else ""
            size_str = f.get("size", "0")
            try:
                size = int(size_str)
            except (ValueError, TypeError):
                size = 0

            file_list.append({
                "id": name,
                "name": name,
                "size": size,
                "download_url": f"{_DOWNLOAD_BASE}/{identifier}/{quote(name)}",
                "content_type": _format_to_mime(f.get("format", "")),
                "restricted": f.get("private", "") == "true",
                "api_checksum": api_checksum,
            })

        return DatasetHit(
            source_name="ia",
            source_url=f"https://archive.org/details/{identifier}",
            title=title,
            description=description,
            authors=creator,
            license_type=license_type,
            license_url=license_url,
            date_published=_ensure_str(date),
            keywords=[],
            tags=subject if isinstance(subject, list) else [],
            kind_of_data=[],
            language=language,
            geographic_coverage=[],
            software=[],
            depositor="",
            producer=[],
            publication=[],
            uploader_name=creator,
            uploader_email="",
            files=file_list,
        )

    # ── File download ───────────────────────────────────────

    def pull_file(self, url: str, dest_dir: str, filename: str | None = None) -> str:
        """Stream a file from the Internet Archive with retry."""
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


# ── Module-level utilities ──────────────────────────────────


def _extract_identifier(url: str) -> str:
    """Extract the Internet Archive identifier from various URL formats.

    Handles:
    - https://archive.org/details/my-item
    - https://archive.org/metadata/my-item
    - https://archive.org/download/my-item/file.pdf
    - Bare identifiers like 'my-item'
    """
    match = re.search(r"archive\.org/(?:details|metadata|download)/([^/?]+)", url)
    if match:
        return match.group(1)
    return url.strip().rstrip("/").split("/")[-1]


def _ensure_str(value) -> str:
    """Coerce a value that may be a string or list to a single string."""
    if isinstance(value, list):
        return "; ".join(str(v) for v in value if v)
    return str(value) if value else ""


def _license_name_from_url(url: str) -> str:
    """Derive a human-readable license name from a Creative Commons URL."""
    if not url:
        return ""
    match = re.search(r"creativecommons\.org/(?:licenses|publicdomain)/([^/]+)/([^/]+)", url)
    if match:
        kind = match.group(1).upper()
        version = match.group(2)
        if "publicdomain" in url or kind == "MARK":
            return f"Public Domain Mark {version}"
        if kind == "ZERO":
            return f"CC0 {version}"
        return f"CC {kind} {version}"
    if "publicdomain" in url:
        return "Public Domain"
    return url


def _format_to_mime(fmt: str) -> str:
    """Map Internet Archive format strings to MIME types."""
    _map = {
        "Text PDF": "application/pdf",
        "DjVuTXT": "text/plain",
        "hOCR": "text/html",
        "Word Document": "application/msword",
        "MPEG4": "video/mp4",
        "VBR MP3": "audio/mpeg",
        "Ogg Vorbis": "audio/ogg",
        "WAVE": "audio/wav",
        "JPEG": "image/jpeg",
        "PNG": "image/png",
    }
    return _map.get(fmt, "")
