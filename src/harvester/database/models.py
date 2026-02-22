"""SQLAlchemy models for the pipeline metadata database."""

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class File(Base):
    """Metadata record for a downloaded file."""

    __tablename__ = "files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Source info
    source_name: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    download_url: Mapped[str] = mapped_column(Text, nullable=False)

    # File info
    file_name: Mapped[str] = mapped_column(String(500), nullable=False)
    file_type: Mapped[str | None] = mapped_column(String(50))
    file_hash: Mapped[str | None] = mapped_column(String(64), index=True)
    file_size_bytes: Mapped[int | None] = mapped_column(Integer)
    local_path: Mapped[str | None] = mapped_column(Text)

    # License
    license_type: Mapped[str | None] = mapped_column(String(100))
    license_url: Mapped[str | None] = mapped_column(Text)

    # Metadata
    title: Mapped[str | None] = mapped_column(Text)
    description: Mapped[str | None] = mapped_column(Text)
    authors: Mapped[str | None] = mapped_column(Text)
    date_published: Mapped[str | None] = mapped_column(String(50))
    tags: Mapped[str | None] = mapped_column(Text)

    # Extended metadata (from API)
    keywords: Mapped[str | None] = mapped_column(Text)
    kind_of_data: Mapped[str | None] = mapped_column(Text)
    language: Mapped[str | None] = mapped_column(String(100))
    content_type: Mapped[str | None] = mapped_column(String(200))
    friendly_type: Mapped[str | None] = mapped_column(String(200))
    software: Mapped[str | None] = mapped_column(Text)
    geographic_coverage: Mapped[str | None] = mapped_column(Text)
    restricted: Mapped[bool | None] = mapped_column(Boolean)
    api_checksum: Mapped[str | None] = mapped_column(String(150))

    # Provenance
    depositor: Mapped[str | None] = mapped_column(Text)
    producer: Mapped[str | None] = mapped_column(Text)
    publication: Mapped[str | None] = mapped_column(Text)
    date_of_collection: Mapped[str | None] = mapped_column(Text)
    time_period_covered: Mapped[str | None] = mapped_column(Text)

    # Uploader info
    uploader_name: Mapped[str | None] = mapped_column(Text)
    uploader_email: Mapped[str | None] = mapped_column(String(200))

    # Local storage
    local_directory: Mapped[str | None] = mapped_column(Text)

    # Classification
    is_qda_file: Mapped[bool] = mapped_column(Boolean, default=False)

    # Timestamps
    downloaded_at: Mapped[datetime | None] = mapped_column(DateTime)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Notes
    notes: Mapped[str | None] = mapped_column(Text)

    def __repr__(self) -> str:
        return f"<File(id={self.id}, source={self.source_name}, name={self.file_name})>"
