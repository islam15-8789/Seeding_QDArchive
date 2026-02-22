"""Utilities for on-disk file organisation and integrity checking."""

import hashlib
import re
import unicodedata
from pathlib import Path

from harvester.settings import DOWNLOAD_DIR


def to_slug(text: str, ceiling: int = 60) -> str:
    """Turn arbitrary text into a safe directory-name fragment.

    Unicode → ASCII → lowercase → replace non-alnum with dashes → truncate.
    """
    normalised = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    lowered = normalised.lower()
    dashed = re.sub(r"[^a-z0-9]+", "-", lowered)
    collapsed = re.sub(r"-{2,}", "-", dashed).strip("-")
    if len(collapsed) > ceiling:
        collapsed = collapsed[:ceiling].rsplit("-", 1)[0]
    return collapsed


def build_output_path(
    source_key: str, record_id: str, filename: str, title: str | None = None,
) -> Path:
    """Compute the download destination: downloads/<source>/<slug>-<id>/<file>.

    Falls back to just ``<id>/`` when no title is provided.
    """
    slug = to_slug(title) if title else ""
    folder = f"{slug}-{record_id}" if slug else record_id
    full = DOWNLOAD_DIR / source_key / folder
    full.mkdir(parents=True, exist_ok=True)
    return full / filename


def sha256_digest(path: Path) -> str:
    """Return the hex SHA-256 of a file, read in 128 KiB blocks."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            block = fh.read(131_072)
            if not block:
                break
            h.update(block)
    return h.hexdigest()
