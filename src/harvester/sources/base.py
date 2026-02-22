"""Interface that every data source must satisfy."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class DatasetHit:
    """Represents one dataset returned by a source search or metadata lookup."""

    source_name: str
    source_url: str
    title: str
    description: str = ""
    authors: str = ""
    license_type: str = ""
    license_url: str = ""
    date_published: str = ""
    tags: list[str] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    kind_of_data: list[str] = field(default_factory=list)
    language: list[str] = field(default_factory=list)
    software: list[str] = field(default_factory=list)
    geographic_coverage: list[str] = field(default_factory=list)
    depositor: str = ""
    producer: list[str] = field(default_factory=list)
    publication: list[str] = field(default_factory=list)
    date_of_collection: str = ""
    time_period_covered: str = ""
    uploader_name: str = ""
    uploader_email: str = ""
    files: list[dict] = field(default_factory=list)


class BaseSource(ABC):
    """Contract for pluggable data sources."""

    @property
    @abstractmethod
    def label(self) -> str:
        """Short human-readable identifier shown in output."""

    @abstractmethod
    def find(self, query: str, file_type: str | None = None) -> list[DatasetHit]:
        """Run a keyword search and return lightweight hit objects."""

    @abstractmethod
    def fetch_metadata(self, url: str) -> DatasetHit:
        """Retrieve the complete metadata for a single dataset."""

    @abstractmethod
    def pull_file(self, url: str, dest_dir: str, filename: str | None = None) -> str:
        """Download one file and return its local path."""
