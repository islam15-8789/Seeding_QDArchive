"""Generic Dataverse API source — instantiated once per installation."""

import logging
import re
import time
from pathlib import Path

import httpx

from harvester.sources.base import BaseSource, DatasetHit

log = logging.getLogger("harvester")

# Network tuning
_API_TIMEOUT = 30.0
_DOWNLOAD_TIMEOUT = 120.0
_RETRY_LIMIT = 3
_INITIAL_BACKOFF = 2.0       # doubled on each subsequent attempt

# Guard against enormous result sets on large installations
_SEARCH_CAP = 500

# Some Dataverse hosts (e.g. SciELO) block the default python-httpx User-Agent
# while others (e.g. Sciences Po) block browser-style UAs.  Allow per-instance
# overrides via the *headers* constructor parameter.
_BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


class DataverseSource(BaseSource):
    """Talks to any standard Dataverse installation.

    Harvested (remote-indexed) datasets are excluded automatically so we only
    hit datasets whose files are actually served by this host.
    """

    def __init__(
        self, host_url: str, key: str, *, headers: dict | None = None,
    ) -> None:
        self._host = host_url.rstrip("/")
        self._key = key
        self._headers = headers or {}

    @property
    def label(self) -> str:
        return self._key

    # ── Search ──────────────────────────────────────────────

    def find(self, query: str, file_type: str | None = None) -> list[DatasetHit]:
        """Paginate through the Dataverse search endpoint."""
        hits: list[DatasetHit] = []
        page_size = 100
        offset = 0
        reported_total = 0

        while True:
            params: dict[str, str | int] = {
                "q": query,
                "type": "dataset",
                "per_page": page_size,
                "start": offset,
                "fq": "-isHarvested:true",
            }

            r = httpx.get(
                f"{self._host}/api/search",
                params=params,
                headers=self._headers,
                timeout=_API_TIMEOUT,
            )
            r.raise_for_status()
            payload = r.json().get("data", {})

            items = payload.get("items", [])
            if not items:
                break

            for item in items:
                hit = DatasetHit(
                    source_name=self._key,
                    source_url=item.get("url", ""),
                    title=item.get("name", ""),
                    description=item.get("description", ""),
                    authors="; ".join(item.get("authors", [])),
                    date_published=item.get("published_at", ""),
                    tags=item.get("subjects", []),
                )
                pid = item.get("global_id", "")
                if pid:
                    hit.source_url = f"{self._host}/dataset.xhtml?persistentId={pid}"
                hits.append(hit)

            reported_total = payload.get("total_count", 0)
            offset += page_size
            if offset >= reported_total or len(hits) >= _SEARCH_CAP:
                break

        if len(hits) > _SEARCH_CAP:
            hits = hits[:_SEARCH_CAP]

        if len(hits) < reported_total:
            log.info(
                "[%s] '%s': showing %d of %d (capped at %d)",
                self._key, query, len(hits), reported_total, _SEARCH_CAP,
            )
        else:
            log.info("[%s] '%s': %d dataset(s)", self._key, query, len(hits))

        return hits

    # ── Full metadata ───────────────────────────────────────

    def fetch_metadata(self, url: str) -> DatasetHit:
        """Pull the full JSON metadata blob for a dataset."""
        pid = self._parse_persistent_id(url)

        if pid:
            r = httpx.get(
                f"{self._host}/api/datasets/:persistentId",
                params={"persistentId": pid},
                headers=self._headers,
                timeout=_API_TIMEOUT,
            )
        else:
            numeric_id = url.rstrip("/").split("/")[-1]
            r = httpx.get(
                f"{self._host}/api/datasets/{numeric_id}",
                headers=self._headers,
                timeout=_API_TIMEOUT,
            )

        r.raise_for_status()
        blob = r.json().get("data", {})
        latest = blob.get("latestVersion", {})
        blocks = latest.get("metadataBlocks", {})
        citation = blocks.get("citation", {})
        fld = {f["typeName"]: f for f in citation.get("fields", [])}

        title = _field_val(fld, "title", "")

        # Description (strip HTML)
        desc_entries = _field_val(fld, "dsDescription", [])
        description = ""
        if isinstance(desc_entries, list) and desc_entries:
            raw_html = desc_entries[0].get("dsDescriptionValue", {}).get("value", "")
            description = _clean_html(raw_html)

        # Authors
        author_entries = _field_val(fld, "author", [])
        author_names = []
        if isinstance(author_entries, list):
            for a in author_entries:
                n = a.get("authorName", {}).get("value", "")
                if n:
                    author_names.append(n)

        subject_vals = _field_val(fld, "subject", [])

        # Keywords
        kw_entries = _field_val(fld, "keyword", [])
        kw_list = []
        if isinstance(kw_entries, list):
            for entry in kw_entries:
                v = entry.get("keywordValue", {}).get("value", "")
                if v:
                    kw_list.append(v)

        kind_of_data = _field_val(fld, "kindOfData", [])
        if not isinstance(kind_of_data, list):
            kind_of_data = []

        lang_vals = _field_val(fld, "language", [])
        if not isinstance(lang_vals, list):
            lang_vals = []

        # Software
        sw_entries = _field_val(fld, "software", [])
        software_list = []
        if isinstance(sw_entries, list):
            for sw in sw_entries:
                v = sw.get("softwareName", {}).get("value", "")
                if v:
                    software_list.append(v)

        # Geographic coverage
        geo_entries = _field_val(fld, "geographicCoverage", [])
        geo_list = []
        if isinstance(geo_entries, list):
            for g in geo_entries:
                country = g.get("country", {}).get("value", "")
                if country:
                    geo_list.append(country)

        # Depositor
        depositor = _field_val(fld, "depositor", "")
        if not isinstance(depositor, str):
            depositor = ""

        # Producers
        prod_entries = _field_val(fld, "producer", [])
        producers = []
        if isinstance(prod_entries, list):
            for p in prod_entries:
                n = p.get("producerName", {}).get("value", "")
                if n:
                    producers.append(n)

        # Related publications
        pub_entries = _field_val(fld, "publication", [])
        pubs = []
        if isinstance(pub_entries, list):
            for pub in pub_entries:
                cite = _clean_html(pub.get("publicationCitation", {}).get("value", ""))
                link = pub.get("publicationURL", {}).get("value", "")
                pubs.append(cite or link) if (cite or link) else None

        # Collection dates
        coll_entries = _field_val(fld, "dateOfCollection", [])
        date_of_collection = ""
        if isinstance(coll_entries, list) and coll_entries:
            d_start = coll_entries[0].get("dateOfCollectionStart", {}).get("value", "")
            d_end = coll_entries[0].get("dateOfCollectionEnd", {}).get("value", "")
            if d_start or d_end:
                date_of_collection = (
                    f"{d_start} – {d_end}" if d_start and d_end else (d_start or d_end)
                )

        # Time period covered
        tp_entries = _field_val(fld, "timePeriodCovered", [])
        time_period_covered = ""
        if isinstance(tp_entries, list) and tp_entries:
            t_start = tp_entries[0].get("timePeriodCoveredStart", {}).get("value", "")
            t_end = tp_entries[0].get("timePeriodCoveredEnd", {}).get("value", "")
            if t_start or t_end:
                time_period_covered = (
                    f"{t_start} – {t_end}" if t_start and t_end else (t_start or t_end)
                )

        # Contact info
        contact_entries = _field_val(fld, "datasetContact", [])
        uploader_name = ""
        uploader_email = ""
        if isinstance(contact_entries, list) and contact_entries:
            uploader_name = contact_entries[0].get("datasetContactName", {}).get("value", "")
            uploader_email = contact_entries[0].get("datasetContactEmail", {}).get("value", "")

        # License — check license block first, then termsOfAccess, then termsOfUse
        lic_block = latest.get("license", {})
        license_type = lic_block.get("name", "") if isinstance(lic_block, dict) else ""
        license_url = lic_block.get("uri", "") if isinstance(lic_block, dict) else ""
        if not license_type:
            raw_terms = latest.get("termsOfAccess", "") or latest.get("termsOfUse", "")
            license_type = _clean_html(raw_terms) if raw_terms else ""

        # Files
        file_list = []
        for fe in latest.get("files", []):
            df = fe.get("dataFile", {})
            cs = df.get("checksum", {})
            if cs:
                remote_cs = f"{cs.get('type', '')}:{cs.get('value', '')}"
            elif df.get("md5"):
                remote_cs = f"MD5:{df['md5']}"
            else:
                remote_cs = ""
            file_list.append({
                "id": df.get("id"),
                "name": df.get("filename", ""),
                "size": df.get("filesize", 0),
                "content_type": df.get("contentType", ""),
                "friendly_type": df.get("friendlyType", ""),
                "download_url": f"{self._host}/api/access/datafile/{df.get('id')}",
                "restricted": fe.get("restricted", False),
                "api_checksum": remote_cs,
            })

        return DatasetHit(
            source_name=self._key,
            source_url=url,
            title=title,
            description=description,
            authors="; ".join(author_names),
            license_type=license_type,
            license_url=license_url,
            date_published=latest.get("releaseTime", ""),
            tags=subject_vals if isinstance(subject_vals, list) else [],
            keywords=kw_list,
            kind_of_data=kind_of_data,
            language=lang_vals,
            software=software_list,
            geographic_coverage=geo_list,
            depositor=depositor,
            producer=producers,
            publication=pubs,
            date_of_collection=date_of_collection,
            time_period_covered=time_period_covered,
            uploader_name=uploader_name,
            uploader_email=uploader_email,
            files=file_list,
        )

    # ── File download ───────────────────────────────────────

    def pull_file(self, url: str, dest_dir: str, filename: str | None = None) -> str:
        """Stream a file to disk with automatic retry on transient failures."""
        target = Path(dest_dir)
        target.mkdir(parents=True, exist_ok=True)

        for attempt in range(1, _RETRY_LIMIT + 1):
            try:
                with httpx.stream(
                    "GET", url, headers=self._headers,
                    timeout=_DOWNLOAD_TIMEOUT, follow_redirects=True,
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

    # ── Helpers ──────────────────────────────────────────────

    @staticmethod
    def _parse_persistent_id(url: str) -> str | None:
        """Pull the persistentId query parameter from a dataset URL."""
        if "persistentId=" in url:
            return url.split("persistentId=", 1)[1].split("&")[0]
        if url.startswith("doi:") or url.startswith("hdl:"):
            return url
        return None


# ── Module-level utilities ──────────────────────────────────


def _clean_html(text: str) -> str:
    """Strip HTML tags, decode entities, and normalise whitespace."""
    import html
    # Remove XML/HTML fragments (including broken ones like '</ p')
    stripped = re.sub(r"</?[^>]*>", " ", text)
    stripped = re.sub(r"</?\s*\w*$", " ", stripped)  # trailing broken tags
    # Fix malformed entities like '&# 8217;' → '&#8217;'
    stripped = re.sub(r"&#\s+(\d+);", r"&#\1;", stripped)
    # Remove stray XML attribute fragments like 'xlink ">'
    stripped = re.sub(r'\w+\s*">', " ", stripped)
    decoded = html.unescape(stripped)
    return re.sub(r"\s+", " ", decoded).strip()


def _field_val(fields: dict, key: str, fallback=None):
    """Safely extract the value entry from a Dataverse metadata field."""
    node = fields.get(key)
    if node is None:
        return fallback
    return node.get("value", fallback)


def _name_from_headers(headers: httpx.Headers) -> str | None:
    """Attempt to read a filename from Content-Disposition."""
    cd = headers.get("content-disposition", "")
    if "filename=" in cd:
        segment = cd.split("filename=", 1)[1]
        return segment.strip('"').strip("'").split(";")[0].strip()
    return None
