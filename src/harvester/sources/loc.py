"""Library of Congress JSON API source for digital collections."""

import logging
import re
import time
from pathlib import Path
from urllib.parse import urlencode

import httpx

from harvester.sources.base import BaseSource, DatasetHit
from harvester.sources.dataverse import _clean_html, _name_from_headers

log = logging.getLogger("harvester")

_BASE_URL = "https://www.loc.gov"
_API_TIMEOUT = 30.0
_DOWNLOAD_TIMEOUT = 120.0
_RETRY_LIMIT = 3
_INITIAL_BACKOFF = 2.0
_SEARCH_CAP = 500
_PAGE_SIZE = 150  # max reliable page size for loc.gov
_THROTTLE = 3.5  # 20 req/min limit → ≥3s between requests


class LOCSource(BaseSource):
    """Source for the Library of Congress digital collections.

    Uses the loc.gov JSON API (https://www.loc.gov/apis/).
    No authentication required.  Rate limit is 20 requests/minute
    for the JSON API; exceeding it triggers a 1-hour IP block.
    """

    def __init__(self) -> None:
        self._last_request_time = 0.0

    @property
    def label(self) -> str:
        return "loc"

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < _THROTTLE:
            time.sleep(_THROTTLE - elapsed)
        self._last_request_time = time.monotonic()

    def _get_json(self, url: str, params: dict | None = None) -> dict:
        """GET with throttle and retry on 429."""
        for attempt in range(1, _RETRY_LIMIT + 1):
            self._throttle()
            r = httpx.get(url, params=params, timeout=_API_TIMEOUT,
                          follow_redirects=True)
            if r.status_code == 429:
                wait = _INITIAL_BACKOFF * (2 ** (attempt - 1))
                log.warning(
                    "[loc] 429 rate-limited — retrying in %.0fs (attempt %d/%d)",
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
        """Search LOC digital collections via the JSON API.

        Searches ``/search/`` with ``fo=json`` and filters for
        digitised items available online.
        """
        hits: list[DatasetHit] = []
        page = 1

        while True:
            params = {
                "q": query,
                "fo": "json",
                "c": _PAGE_SIZE,
                "sp": page,
                "fa": "digitized:true",
            }

            data = self._get_json(f"{_BASE_URL}/search/", params)
            results = data.get("results", [])
            pagination = data.get("pagination", {})

            if not results:
                break

            for item in results:
                title = item.get("title", "")
                if not title:
                    continue

                # Skip non-item results (collections, web pages, etc.)
                item_url = item.get("url", "") or item.get("id", "")
                if "/item/" not in item_url:
                    continue

                desc_list = item.get("description", [])
                description = ""
                if isinstance(desc_list, list) and desc_list:
                    description = _clean_html(desc_list[0])
                elif isinstance(desc_list, str):
                    description = _clean_html(desc_list)

                subjects = item.get("subject", [])
                if isinstance(subjects, str):
                    subjects = [subjects]

                contributors = item.get("contributor", [])
                if isinstance(contributors, str):
                    contributors = [contributors]
                authors = "; ".join(contributors)

                language = item.get("language", [])
                if isinstance(language, str):
                    language = [language]

                date = item.get("date", "")

                hit = DatasetHit(
                    source_name="loc",
                    source_url=item_url,
                    title=title,
                    description=description,
                    authors=authors,
                    date_published=date,
                    tags=subjects,
                    language=language,
                )
                hits.append(hit)

            if len(hits) >= _SEARCH_CAP:
                log.info("[loc] '%s': capped at %d results", query, _SEARCH_CAP)
                hits = hits[:_SEARCH_CAP]
                break

            # Pagination
            next_url = pagination.get("next")
            if not next_url:
                break
            page += 1

        log.info("[loc] '%s': %d item(s)", query, len(hits))
        return hits

    # ── Full metadata ───────────────────────────────────────

    def fetch_metadata(self, url: str) -> DatasetHit:
        """Retrieve complete metadata for a LOC item.

        Accepts URLs like ``https://www.loc.gov/item/2020706022/``
        or bare item IDs.
        """
        item_url = _normalize_item_url(url)

        data = self._get_json(item_url, params={"fo": "json"})

        item = data.get("item", {})
        resources = data.get("resources", [])

        title = item.get("title", "")
        description_list = item.get("description", [])
        description = ""
        if isinstance(description_list, list) and description_list:
            description = _clean_html(description_list[0])
        elif isinstance(description_list, str):
            description = _clean_html(description_list)

        # Contributors / authors
        contributor_names = item.get("contributor_names", [])
        if isinstance(contributor_names, str):
            contributor_names = [contributor_names]
        authors = "; ".join(contributor_names)

        # Subjects
        subject_headings = item.get("subject_headings", [])
        if isinstance(subject_headings, str):
            subject_headings = [subject_headings]

        # Date
        date = item.get("date", "")

        # Language
        language = item.get("language", [])
        if isinstance(language, str):
            language = [language]

        # Access restriction
        access_restricted = item.get("access_restricted", False)

        # Rights / license
        rights_list = item.get("rights", [])
        license_type = ""
        license_url = ""
        if isinstance(rights_list, list):
            for r in rights_list:
                r_clean = _clean_html(r) if isinstance(r, str) else ""
                if r_clean:
                    license_type = r_clean
                    # Try to extract a CC URL
                    cc_match = re.search(
                        r"https?://creativecommons\.org/[^\s\"'<>]+", r,
                    )
                    if cc_match:
                        license_url = cc_match.group(0)
                    break
        elif isinstance(rights_list, str):
            license_type = _clean_html(rights_list)

        # Detect public domain from rights text or access status
        if not license_type and not access_restricted:
            rights_advisory = item.get("rights_advisory", [])
            if isinstance(rights_advisory, list):
                for adv in rights_advisory:
                    if isinstance(adv, str):
                        license_type = _clean_html(adv)
                        break
            if not license_type:
                license_type = "No known restrictions"

        # Genre → kind_of_data
        genre = item.get("genre", [])
        if isinstance(genre, str):
            genre = [genre]

        # Original format
        original_format = item.get("original_format", [])
        if isinstance(original_format, str):
            original_format = [original_format]

        # Notes
        notes = item.get("notes", [])
        if isinstance(notes, str):
            notes = [notes]

        # Created/published info
        created_published = item.get("created_published", [])
        if isinstance(created_published, list) and created_published:
            producer = created_published
        else:
            producer = []

        # Repository
        repository = item.get("repository", [])
        if isinstance(repository, str):
            repository = [repository]

        # Files from resources
        file_list = []
        for resource in resources:
            download_restricted = resource.get("download_restricted", False)

            # Use shortcut keys first
            for key in ("pdf", "audio", "video", "fulltext"):
                res_url = resource.get(key)
                if res_url and isinstance(res_url, str):
                    name = res_url.rstrip("/").split("/")[-1]
                    file_list.append({
                        "id": name,
                        "name": name,
                        "size": 0,
                        "download_url": res_url,
                        "content_type": _key_to_mime(key),
                        "restricted": download_restricted or access_restricted,
                        "api_checksum": "",
                    })

            # If no shortcut, parse files array
            if not any(resource.get(k) for k in ("pdf", "audio", "video", "fulltext")):
                for file_group in resource.get("files", []):
                    if not isinstance(file_group, list):
                        continue
                    for f in file_group:
                        if not isinstance(f, dict):
                            continue
                        f_url = f.get("url") or f.get("download", "")
                        if not f_url:
                            continue
                        mime = f.get("mimetype", "")
                        name = f_url.rstrip("/").split("/")[-1]
                        file_list.append({
                            "id": name,
                            "name": name,
                            "size": f.get("size", 0) or 0,
                            "download_url": f_url,
                            "content_type": mime,
                            "restricted": download_restricted or access_restricted,
                            "api_checksum": "",
                        })

        return DatasetHit(
            source_name="loc",
            source_url=url,
            title=title,
            description=description,
            authors=authors,
            license_type=license_type,
            license_url=license_url,
            date_published=date,
            keywords=[],
            tags=subject_headings,
            kind_of_data=genre,
            language=language,
            geographic_coverage=[],
            software=[],
            depositor="",
            producer=producer,
            publication=[],
            uploader_name="",
            uploader_email="",
            files=file_list,
        )

    # ── File download ───────────────────────────────────────

    def pull_file(self, url: str, dest_dir: str, filename: str | None = None) -> str:
        """Stream a file from LOC with retry."""
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


def _extract_item_id(url: str) -> str:
    """Extract the LOC item ID from a URL.

    Handles:
    - https://www.loc.gov/item/2020706022/
    - http://www.loc.gov/item/2020706022/
    - Bare IDs like '2020706022'
    """
    match = re.search(r"/item/([^/?]+)", url)
    if match:
        return match.group(1).rstrip("/")
    return url.strip().rstrip("/").split("/")[-1]


def _normalize_item_url(url: str) -> str:
    """Ensure we have a full item URL."""
    if "/item/" in url:
        # Strip query params and ensure trailing slash
        base = url.split("?")[0].rstrip("/") + "/"
        return base
    item_id = _extract_item_id(url)
    return f"{_BASE_URL}/item/{item_id}/"


def _key_to_mime(key: str) -> str:
    """Map resource shortcut keys to MIME types."""
    return {
        "pdf": "application/pdf",
        "audio": "audio/mpeg",
        "video": "video/mp4",
        "fulltext": "application/xml",
    }.get(key, "")
