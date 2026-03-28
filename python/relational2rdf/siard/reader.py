"""
SiardFileReader – entry point that opens a .siard archive and returns a
SiardDataSource.  Mirrors Relational2Rdf.DataSources.Siard.SiardFileReader.
"""
from __future__ import annotations

from pathlib import Path

from relational2rdf.siard.data_source import SiardDataSource


class SiardFileReader:
    def read(self, path: str | Path) -> SiardDataSource:
        return SiardDataSource(path)
