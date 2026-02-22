"""Load project settings from config.yml and derive runtime paths."""

from pathlib import Path

import yaml

ROOT_DIR = Path(__file__).resolve().parent.parent.parent

_config_file = ROOT_DIR / "config.yml"
with open(_config_file, encoding="utf-8") as _f:
    _raw = yaml.safe_load(_f)

# ── Paths (resolved relative to project root) ──
DOWNLOAD_DIR = ROOT_DIR / _raw["paths"]["downloads"]
OUTPUT_DIR = ROOT_DIR / _raw["paths"]["output"]
DATABASE_PATH = ROOT_DIR / _raw["paths"]["database"]
LOG_PATH = ROOT_DIR / _raw["paths"]["log"]

DATABASE_URL = f"sqlite:///{DATABASE_PATH}"

# ── File type sets ──
QDA_FORMATS: set[str] = set(_raw.get("qda_formats", []))
QUALITATIVE_FORMATS: set[str] = set(_raw.get("qualitative_formats", []))

# ── Filtering rules ──
EXCLUDED_RESOURCE_TYPES: set[str] = set(_raw.get("excluded_resource_types", []))
RELEVANCE_KEYWORDS: set[str] = set(_raw.get("relevance_keywords", []))

# ── Source → folder mapping ──
FOLDER_NAMES: dict[str, str] = _raw.get("folder_names", {})


def prepare_directories() -> None:
    """Ensure that the download and output directories exist."""
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
