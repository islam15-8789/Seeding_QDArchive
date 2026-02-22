"""OSF (Open Science Framework) JSON:API v2 source for public nodes."""

import logging
import re
import time
from pathlib import Path

import httpx

from harvester.sources.base import BaseSource, DatasetHit
from harvester.sources.dataverse import _clean_html, _name_from_headers

log = logging.getLogger("harvester")

_API_BASE = "https://api.osf.io/v2"
_API_TIMEOUT = 30.0
_DOWNLOAD_TIMEOUT = 120.0
_RETRY_LIMIT = 3
_INITIAL_BACKOFF = 2.0
_SEARCH_CAP = 500
_PAGE_SIZE = 50
_THROTTLE = 1.0  # conservative — OSF allows 100 req/hr unauthenticated


class OSFSource(BaseSource):
    """Source for the Open Science Framework repository.

    Uses the OSF JSON:API v2 (https://developer.osf.io/).
    No authentication required for public data.
    """

    def __init__(self) -> None:
        self._last_request_time = 0.0

    @property
    def label(self) -> str:
        return "osf"

    def _throttle(self) -> None:
        """Enforce minimum interval between API requests."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < _THROTTLE:
            time.sleep(_THROTTLE - elapsed)
        self._last_request_time = time.monotonic()

    def _get(self, url: str, params: dict | None = None) -> dict:
        """GET with throttle and 429 backoff."""
        for attempt in range(1, _RETRY_LIMIT + 1):
            self._throttle()
            r = httpx.get(url, params=params, timeout=_API_TIMEOUT)
            if r.status_code == 429:
                wait = _INITIAL_BACKOFF * (2 ** (attempt - 1))
                log.warning(
                    "[osf] 429 rate-limited — retrying in %.0fs (attempt %d/%d)",
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
        """Search OSF nodes via GET /v2/nodes/?filter[title]=<query>."""
        hits: list[DatasetHit] = []
        url: str | None = f"{_API_BASE}/nodes/"
        params: dict[str, str | int] = {
            "filter[title]": query,
            "page[size]": _PAGE_SIZE,
        }

        while url:
            data = self._get(url, params)
            # After the first page, pagination URL includes params already
            params = None  # type: ignore[assignment]

            for node in data.get("data", []):
                attrs = node.get("attributes", {})

                # Skip non-public, registrations, preprints, forks, collections
                if attrs.get("public") is False:
                    continue
                if attrs.get("registration") or attrs.get("preprint"):
                    continue
                if attrs.get("fork"):
                    continue
                if attrs.get("collection"):
                    continue
                category = attrs.get("category", "")
                if category == "collection":
                    continue

                node_id = node.get("id", "")
                hit = DatasetHit(
                    source_name="osf",
                    source_url=f"https://osf.io/{node_id}/",
                    title=attrs.get("title", ""),
                    description=_clean_html(attrs.get("description", "") or ""),
                    date_published=attrs.get("date_created", ""),
                    tags=attrs.get("tags", []),
                )
                hits.append(hit)

            if len(hits) >= _SEARCH_CAP:
                log.info("[osf] '%s': capped at %d results", query, _SEARCH_CAP)
                hits = hits[:_SEARCH_CAP]
                break

            url = data.get("links", {}).get("next")

        log.info("[osf] '%s': %d node(s)", query, len(hits))
        return hits

    # ── Full metadata ───────────────────────────────────────

    def fetch_metadata(self, url: str) -> DatasetHit:
        """Retrieve complete metadata for an OSF node.

        Accepts URLs like ``https://osf.io/4vtu3/`` or API URLs.
        """
        node_id = _extract_node_id(url)

        # 1. Node details
        node_data = self._get(f"{_API_BASE}/nodes/{node_id}/")
        attrs = node_data.get("data", {}).get("attributes", {})

        title = attrs.get("title", "")
        description = _clean_html(attrs.get("description", "") or "")
        date_published = attrs.get("date_created", "")

        # Tags from node tags
        keywords = attrs.get("tags", [])

        # Subjects → tags (flattened category names)
        tags: list[str] = []
        for subject_list in attrs.get("subjects", []):
            if isinstance(subject_list, list):
                for subj in subject_list:
                    text = subj.get("text", "") if isinstance(subj, dict) else ""
                    if text:
                        tags.append(text)
            elif isinstance(subject_list, dict):
                text = subject_list.get("text", "")
                if text:
                    tags.append(text)

        # 2. Contributors
        authors_list: list[str] = []
        contributors_url: str | None = f"{_API_BASE}/nodes/{node_id}/contributors/?embed=users"
        while contributors_url:
            contrib_data = self._get(contributors_url)
            contributors_url = None
            for contrib in contrib_data.get("data", []):
                embeds = contrib.get("embeds", {})
                user_data = embeds.get("users", {}).get("data", {})
                user_attrs = user_data.get("attributes", {})
                full_name = user_attrs.get("full_name", "")
                if full_name:
                    authors_list.append(full_name)
            contributors_url = contrib_data.get("links", {}).get("next")

        authors = "; ".join(authors_list)
        uploader_name = authors_list[0] if authors_list else ""

        # 3. Files from osfstorage
        file_list: list[dict] = []
        files_url: str | None = f"{_API_BASE}/nodes/{node_id}/files/osfstorage/"
        while files_url:
            files_data = self._get(files_url)
            files_url = None
            for f in files_data.get("data", []):
                f_attrs = f.get("attributes", {})
                # Skip folders
                if f_attrs.get("kind") == "folder":
                    continue

                extra = f_attrs.get("extra", {})
                hashes = extra.get("hashes", {})
                sha256 = hashes.get("sha256", "")
                api_checksum = f"SHA-256:{sha256}" if sha256 else ""

                download_url = f_attrs.get("links", {}).get("download", "")
                if not download_url:
                    # Fallback: construct from file GUID
                    guid = f.get("id", "")
                    if guid:
                        download_url = f"https://osf.io/download/{guid}/"

                file_list.append({
                    "id": f.get("id", ""),
                    "name": f_attrs.get("name", ""),
                    "size": f_attrs.get("size", 0),
                    "download_url": download_url,
                    "content_type": f_attrs.get("content_type", "") or "",
                    "restricted": False,
                    "api_checksum": api_checksum,
                })
            files_url = files_data.get("links", {}).get("next")

        # 4. License
        license_type = ""
        license_url = ""
        node_license = attrs.get("node_license")
        rels = node_data.get("data", {}).get("relationships", {})
        license_rel = rels.get("license", {})
        license_link = license_rel.get("links", {}).get("related", {})
        license_href = license_link.get("href", "") if isinstance(license_link, dict) else license_link
        if license_href:
            try:
                lic_data = self._get(license_href)
                lic_attrs = lic_data.get("data", {}).get("attributes", {})
                license_type = lic_attrs.get("name", "")
                license_url = lic_attrs.get("url", "")
            except httpx.HTTPStatusError:
                log.debug("[osf] Could not fetch license for node %s", node_id)

        return DatasetHit(
            source_name="osf",
            source_url=url,
            title=title,
            description=description,
            authors=authors,
            license_type=license_type,
            license_url=license_url,
            date_published=date_published,
            keywords=keywords,
            tags=tags,
            kind_of_data=[],
            language=[],
            geographic_coverage=[],
            software=[],
            depositor="",
            producer=[],
            publication=[],
            uploader_name=uploader_name,
            uploader_email="",
            files=file_list,
        )

    # ── File download ───────────────────────────────────────

    def pull_file(self, url: str, dest_dir: str, filename: str | None = None) -> str:
        """Stream a file from OSF with retry."""
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


def _extract_node_id(url: str) -> str:
    """Extract the OSF node ID from various URL formats.

    Handles:
    - https://osf.io/4vtu3/
    - https://api.osf.io/v2/nodes/4vtu3/
    - Bare node IDs like '4vtu3'
    """
    # API URL: /v2/nodes/<id>/...
    match = re.search(r"/v2/nodes/([^/?]+)", url)
    if match:
        return match.group(1)
    # Web URL: https://osf.io/<id>/
    match = re.search(r"osf\.io/([a-z0-9]{3,10})", url, re.IGNORECASE)
    if match:
        return match.group(1)
    # Bare ID
    return url.strip().rstrip("/").split("/")[-1]
