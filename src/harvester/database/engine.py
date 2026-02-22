"""SQLAlchemy engine, session factory, and lightweight schema migration."""

import logging

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from harvester.database.models import Base, File
from harvester.settings import DATABASE_URL

log = logging.getLogger("harvester")

_engine = create_engine(DATABASE_URL, echo=False)
_SessionFactory = sessionmaker(bind=_engine)

# Columns added after the initial release.  Keys are column names; values are
# the SQL type used in the ALTER TABLE statement.
_EXTRA_COLUMNS: dict[str, str] = {
    "keywords": "TEXT",
    "kind_of_data": "TEXT",
    "language": "VARCHAR(100)",
    "content_type": "VARCHAR(200)",
    "friendly_type": "VARCHAR(200)",
    "software": "TEXT",
    "geographic_coverage": "TEXT",
    "restricted": "BOOLEAN",
    "api_checksum": "VARCHAR(150)",
    "depositor": "TEXT",
    "producer": "TEXT",
    "publication": "TEXT",
    "date_of_collection": "TEXT",
    "time_period_covered": "TEXT",
    "uploader_name": "TEXT",
    "uploader_email": "VARCHAR(200)",
    "local_directory": "TEXT",
}


def _apply_migrations() -> None:
    """Add any columns present in the model but missing in the table."""
    inspector = inspect(_engine)
    table = File.__tablename__
    if not inspector.has_table(table):
        return

    current_cols = {c["name"] for c in inspector.get_columns(table)}
    with _engine.begin() as conn:
        for col, col_type in _EXTRA_COLUMNS.items():
            if col not in current_cols:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}"))
                log.info("Migration: added column %r to %s", col, table)


def setup_database() -> None:
    """Create tables (if needed) and run pending migrations."""
    Base.metadata.create_all(_engine)
    _apply_migrations()


def open_session() -> Session:
    """Return a fresh database session."""
    return _SessionFactory()
