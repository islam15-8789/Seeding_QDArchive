"""Write the full records table to a CSV file."""

import csv
from pathlib import Path

from sqlalchemy import inspect

from harvester.database.engine import open_session
from harvester.database.models import File


def write_csv(destination: Path) -> int:
    """Dump every record to *destination* and return the row count."""
    session = open_session()
    try:
        rows = session.query(File).all()
        if not rows:
            return 0

        col_names = [attr.key for attr in inspect(File).mapper.column_attrs]

        with open(destination, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(col_names)
            for row in rows:
                writer.writerow([getattr(row, c) for c in col_names])

        return len(rows)
    finally:
        session.close()
