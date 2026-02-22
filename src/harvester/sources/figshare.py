"""Figshare REST API v2 source for public articles."""

import logging
import re
import time
from pathlib import Path

import httpx

from harvester.sources.base import BaseSource, DatasetHit
from harvester.sources.dataverse import _clean_html, _name_from_headers

log = logging.getLogger("harvester")

_API_BASE = "https://api.figshare.com/v2"
_API_TIMEOUT = 30.0
_DOWNLOAD_TIMEOUT = 120.0
_RETRY_LIMIT = 3
_INITIAL_BACKOFF = 2.0
_SEARCH_CAP = 500
_PAGE_SIZE = 50
_THROTTLE = 0.5

# Figshare content types that are not qualitative data
_SKIP_TYPES = {"figure", "media", "code", "poster", "presentation"}


class FigshareSource(BaseSource):
    """Source for the Figshare open-access repository.

    Uses the Figshare REST API v2 (https://docs.figshare.com/).
    No authentication required for public data.  Downloads go through
    S3 redirects, so ``follow_redirects=True`` is essential.
    """

    def __init__(self) -> None:
        self._last_request_time = 0.0

    @property
    def label(self) -> str:
        return "figshare"

    def _throttle(self) -> None:
        """Enforce minimum interval between API requests."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < _THROTTLE:
            time.sleep(_THROTTLE - elapsed)
        self._last_request_time = time.monotonic()

    # ── Search ──────────────────────────────────────────────

    def find(self, query: str, file_type: str | None = None) -> list[DatasetHit]:
        """Search Figshare articles via POST /v2/articles/search."""
        hits: list[DatasetHit] = []
        page = 1

        while True:
            self._throttle()
            body = {
                "search_for": query,
                "page": page,
                "page_size": _PAGE_SIZE,
            }
            r = httpx.post(
                f"{_API_BASE}/articles/search",
                json=body,
                timeout=_API_TIMEOUT,
            )
            r.raise_for_status()
            items = r.json()

            if not items:
                break

            for item in items:
                dtype = item.get("defined_type_name", "")
                if dtype.lower() in _SKIP_TYPES:
                    continue

                title = _clean_title(item.get("title", ""))
                hit = DatasetHit(
                    source_name="figshare",
                    source_url=item.get("url_public_html", ""),
                    title=title,
                    date_published=item.get("published_date", ""),
                )
                hits.append(hit)

            if len(hits) >= _SEARCH_CAP:
                log.info("[figshare] '%s': capped at %d results", query, len(hits))
                hits = hits[:_SEARCH_CAP]
                break

            if len(items) < _PAGE_SIZE:
                break

            page += 1

        log.info("[figshare] '%s': %d article(s)", query, len(hits))
        return hits

    # ── Full metadata ───────────────────────────────────────

    def fetch_metadata(self, url: str) -> DatasetHit:
        """Retrieve complete metadata for a Figshare article.

        Accepts URLs like ``https://figshare.com/articles/dataset/Title/12345``
        or institution subdomains.
        """
        article_id = _extract_article_id(url)

        self._throttle()
        r = httpx.get(
            f"{_API_BASE}/articles/{article_id}",
            timeout=_API_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()

        # Skip confidential or metadata-only records
        if data.get("is_confidential") or data.get("is_metadata_record"):
            return DatasetHit(
                source_name="figshare",
                source_url=url,
                title=_clean_title(data.get("title", "")),
                files=[],
            )

        title = _clean_title(data.get("title", ""))
        description = _clean_html(data.get("description", ""))

        # Authors
        author_entries = data.get("authors", [])
        authors = "; ".join(
            a.get("full_name", "") for a in author_entries if a.get("full_name")
        )

        # License
        lic = data.get("license", {})
        license_type = lic.get("name", "") if isinstance(lic, dict) else ""
        license_url = lic.get("url", "") if isinstance(lic, dict) else ""

        # Tags → keywords (clean HTML entities / broken tags)
        raw_tags = data.get("tags", [])
        keywords = [_clean_html(t) for t in raw_tags if t]

        # Categories → tags
        categories = data.get("categories", [])
        tags = [c.get("title", "") for c in categories if c.get("title")]

        # Resource type → kind_of_data
        dtype = data.get("defined_type_name", "")
        kind_of_data = [dtype] if dtype else []

        # Related materials → publication
        refs = data.get("references", [])
        publication = [ref for ref in refs if ref]

        # Uploader = first author
        uploader_name = author_entries[0].get("full_name", "") if author_entries else ""

        # Files
        file_list = []
        for f in data.get("files", []):
            if f.get("is_link_only"):
                continue

            mime = f.get("mimetype", "")
            if mime == "undefined":
                mime = ""

            md5 = f.get("computed_md5", "") or f.get("supplied_md5", "")
            api_checksum = f"MD5:{md5}" if md5 else ""

            file_list.append({
                "id": f.get("id"),
                "name": f.get("name", ""),
                "size": f.get("size", 0),
                "download_url": f.get("download_url", ""),
                "content_type": mime,
                "friendly_type": "",
                "restricted": False,
                "api_checksum": api_checksum,
            })

        return DatasetHit(
            source_name="figshare",
            source_url=url,
            title=title,
            description=description,
            authors=authors,
            license_type=license_type,
            license_url=license_url,
            date_published=data.get("published_date", ""),
            keywords=keywords,
            tags=tags,
            kind_of_data=kind_of_data,
            language=[],
            geographic_coverage=[],
            software=[],
            depositor="",
            producer=[],
            publication=publication,
            uploader_name=uploader_name,
            uploader_email="",
            files=file_list,
        )

    # ── File download ───────────────────────────────────────

    def pull_file(self, url: str, dest_dir: str, filename: str | None = None) -> str:
        """Stream a file from Figshare (via S3 redirect) with retry."""
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


def _extract_article_id(url: str) -> str:
    """Extract the numeric article ID from a Figshare URL.

    Handles patterns like:
    - https://figshare.com/articles/dataset/Title/12345
    - https://figshare.com/articles/dataset/Title/12345/2  (versioned)
    - https://institution.figshare.com/articles/...
    - Bare numeric IDs
    """
    match = re.search(r"/articles/[^/]+/[^/]+/(\d+)", url)
    if match:
        return match.group(1)
    stripped = url.strip().rstrip("/")
    if stripped.isdigit():
        return stripped
    parts = stripped.split("/")
    for part in reversed(parts):
        if part.isdigit():
            return part
    return parts[-1]


def _clean_title(title: str) -> str:
    """Strip HTML tags and collapse whitespace/newlines."""
    clean = _clean_html(title)
    return re.sub(r"\s+", " ", clean).strip()
