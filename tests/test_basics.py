"""Smoke tests for core components — settings, helpers, storage, DB, and CLI."""

import tempfile
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

# ── Settings ────────────────────────────────────────────────


def test_settings_loads():
    from harvester.settings import QDA_FORMATS, RELEVANCE_KEYWORDS, ROOT_DIR

    assert ROOT_DIR.is_dir()
    assert ".qdpx" in QDA_FORMATS
    assert "qualitative" in RELEVANCE_KEYWORDS


def test_prepare_directories(tmp_path):
    with patch("harvester.settings.DOWNLOAD_DIR", tmp_path / "dl"), \
         patch("harvester.settings.OUTPUT_DIR", tmp_path / "out"):
        from harvester.settings import prepare_directories
        prepare_directories()
        assert (tmp_path / "dl").is_dir()
        assert (tmp_path / "out").is_dir()


# ── Licensing ───────────────────────────────────────────────


def test_license_is_open_accepts_cc():
    from harvester.helpers.licensing import license_is_open

    assert license_is_open("CC BY 4.0") is True
    assert license_is_open("CC0 1.0") is True
    assert license_is_open("cc-by-sa") is True


def test_license_is_open_rejects_missing():
    from harvester.helpers.licensing import license_is_open

    assert license_is_open(None) is False
    assert license_is_open("") is False


def test_license_is_open_rejects_proprietary():
    from harvester.helpers.licensing import license_is_open

    assert license_is_open("All rights reserved") is False
    assert license_is_open("Proprietary") is False


def test_license_standard_access():
    from harvester.helpers.licensing import license_is_open

    assert license_is_open("Standard Access") is True


# ── Storage ─────────────────────────────────────────────────


def test_to_slug():
    from harvester.storage.files import to_slug

    assert to_slug("Hello World!") == "hello-world"
    assert to_slug("   ") == ""
    assert to_slug("Ünïcödé") == "unicode"


def test_to_slug_truncates():
    from harvester.storage.files import to_slug

    long = "a-" * 100
    result = to_slug(long, ceiling=20)
    assert len(result) <= 20


def test_build_output_path(tmp_path):
    with patch("harvester.storage.files.DOWNLOAD_DIR", tmp_path):
        from harvester.storage.files import build_output_path

        p = build_output_path("qdr", "abc123", "file.pdf", title="My Dataset")
        assert "qdr" in str(p)
        assert "abc123" in str(p)
        assert p.name == "file.pdf"


def test_build_output_path_no_title(tmp_path):
    with patch("harvester.storage.files.DOWNLOAD_DIR", tmp_path):
        from harvester.storage.files import build_output_path

        p = build_output_path("qdr", "xyz", "data.txt", title=None)
        assert "xyz" in str(p.parent.name)
        assert p.name == "data.txt"


def test_sha256_digest():
    from harvester.storage.files import sha256_digest

    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
        f.write(b"test content")
        f.flush()
        digest = sha256_digest(Path(f.name))
        assert len(digest) == 64
        assert digest.isalnum()
    Path(f.name).unlink()


# ── Database ────────────────────────────────────────────────


def test_record_model():
    from harvester.database.models import File

    r = File(
        source_name="test",
        source_url="https://example.com/ds/1",
        download_url="https://example.com/file/1",
        file_name="sample.pdf",
    )
    assert r.source_name == "test"
    assert not r.is_qda_file
    assert "File" in repr(r)


def test_setup_database(tmp_path):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from harvester.database.models import Base, File

    db = tmp_path / "test.db"
    eng = create_engine(f"sqlite:///{db}")
    Base.metadata.create_all(eng)

    Session = sessionmaker(bind=eng)
    s = Session()
    s.add(File(
        source_name="unit", source_url="u://1", download_url="u://f/1", file_name="a.txt",
    ))
    s.commit()
    assert s.query(File).count() == 1
    s.close()


def test_write_csv(tmp_path):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from harvester.database.models import Base, File

    db = tmp_path / "export_test.db"
    eng = create_engine(f"sqlite:///{db}")
    Base.metadata.create_all(eng)

    Session = sessionmaker(bind=eng)
    s = Session()
    s.add(File(
        source_name="csv_test", source_url="u://2", download_url="u://f/2", file_name="b.txt",
    ))
    s.commit()
    s.close()

    with patch("harvester.database.export.open_session", return_value=Session()):
        from harvester.database.export import write_csv

        out = tmp_path / "out.csv"
        count = write_csv(out)
        assert count == 1
        assert out.exists()
        lines = out.read_text().splitlines()
        assert len(lines) == 2  # header + 1 row


# ── Source registry ─────────────────────────────────────────


def test_source_registry():
    from harvester.sources import SOURCES

    assert "qdr" in SOURCES
    assert "borealis" in SOURCES
    assert "dataversenl" in SOURCES
    assert "rdg" in SOURCES
    assert "goettingen" in SOURCES
    assert "figshare" in SOURCES
    assert "odissei" not in SOURCES  # metadata-only aggregator, no files
    assert "aussda" not in SOURCES   # non-open license (AUSSDA SUF)
    assert "csda" not in SOURCES     # non-open license (ČSDA)
    assert "lida" not in SOURCES     # non-open license (LiDA)
    assert "issda" not in SOURCES    # non-open license (ISSDA EUL)
    assert len(SOURCES) >= 13


def test_source_has_label():
    from harvester.sources import SOURCES

    for key, src in SOURCES.items():
        assert src.label == key


# ── CLI smoke tests ─────────────────────────────────────────


def test_cli_help():
    from harvester.cli import app

    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "QDArchive" in result.output


def test_cli_sources():
    from harvester.cli import app

    runner = CliRunner()
    result = runner.invoke(app, ["sources"])
    assert result.exit_code == 0
    assert "qdr" in result.output


def test_cli_harvest_help():
    from harvester.cli import app

    runner = CliRunner()
    result = runner.invoke(app, ["harvest", "--help"])
    assert result.exit_code == 0
    assert "--limit" in result.output


def test_cli_collect_all_help():
    from harvester.cli import app

    runner = CliRunner()
    result = runner.invoke(app, ["collect-all", "--help"])
    assert result.exit_code == 0
    assert "--retries" in result.output


def test_cli_find_help():
    from harvester.cli import app

    runner = CliRunner()
    result = runner.invoke(app, ["find", "--help"])
    assert result.exit_code == 0
    assert "--query" in result.output


def test_cli_browse_help():
    from harvester.cli import app

    runner = CliRunner()
    result = runner.invoke(app, ["browse", "--help"])
    assert result.exit_code == 0
    assert "--qda-only" in result.output


def test_cli_dump_help():
    from harvester.cli import app

    runner = CliRunner()
    result = runner.invoke(app, ["dump", "--help"])
    assert result.exit_code == 0
    assert "--output" in result.output


def test_cli_detail_help():
    from harvester.cli import app

    runner = CliRunner()
    result = runner.invoke(app, ["detail", "--help"])
    assert result.exit_code == 0


def test_cli_overview():
    from harvester.cli import app

    runner = CliRunner()
    result = runner.invoke(app, ["overview"])
    assert result.exit_code == 0
    assert "Total records" in result.output
