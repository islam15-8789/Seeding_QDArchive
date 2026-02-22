"""Source registry — maps short keys to ready-to-use source instances."""

from harvester.sources.base import BaseSource, DatasetHit
from harvester.sources.dataverse import DataverseSource, _BROWSER_UA
from harvester.sources.figshare import FigshareSource
from harvester.sources.fsd import FSDSource
from harvester.sources.ia import IASource
from harvester.sources.loc import LOCSource
from harvester.sources.osf import OSFSource

SOURCES: dict[str, BaseSource] = {
    "qdr": DataverseSource("https://data.qdr.syr.edu", "qdr"),
    "borealis": DataverseSource("https://borealisdata.ca", "borealis"),
    "dataversenl": DataverseSource("https://dataverse.nl", "dataversenl"),
    "sciencespo": DataverseSource("https://data.sciencespo.fr", "sciencespo"),
    "rdg": DataverseSource("https://entrepot.recherche.data.gouv.fr", "rdg"),
    "abacus": DataverseSource("https://abacus.library.ubc.ca", "abacus"),
    "jhu": DataverseSource("https://archive.data.jhu.edu", "jhu"),
    "cora": DataverseSource("https://dataverse.csuc.cat", "cora"),
    "ucla": DataverseSource("https://dataverse.ucla.edu", "ucla"),
    "drntu": DataverseSource("https://researchdata.ntu.edu.sg", "drntu"),
    "goettingen": DataverseSource("https://data.goettingen-research-online.de", "goettingen"),
    "nie": DataverseSource("https://researchdata.nie.edu.sg", "nie"),
    "figshare": FigshareSource(),
    "osf": OSFSource(),
    "fsd": FSDSource(),
    "ia": IASource(),
    "loc": LOCSource(),
    "eciencia": DataverseSource("https://edatos.consorciomadrono.es", "eciencia"),
    "scielo": DataverseSource(
        "https://data.scielo.org", "scielo",
        headers={"User-Agent": _BROWSER_UA},
    ),
}

__all__ = [
    "SOURCES",
    "BaseSource",
    "DatasetHit",
    "DataverseSource",
    "FigshareSource",
    "FSDSource",
    "IASource",
    "LOCSource",
    "OSFSource",
]
