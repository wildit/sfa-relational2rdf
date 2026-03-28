"""
SiardDataSource – wraps a parsed SIARD archive and fulfils the
IRelationalDataSource protocol used by the converter layer.

Version detection and metadata parsing are delegated to siard.v1 / siard.v2.
"""
from __future__ import annotations

import zipfile
from pathlib import Path
from typing import Optional, Union

from relational2rdf.models import IAttribute, IRelationalDataSource, ISchema, ITable, IType
from relational2rdf.siard.v1 import SiardArchiveV1, SchemaV1, parse_v1
from relational2rdf.siard.v2 import (
    AttributeV2,
    ColumnV2,
    SchemaV2,
    SiardArchiveV2,
    TypeV2,
    parse_v2,
)

SiardArchive = Union[SiardArchiveV1, SiardArchiveV2]


def _detect_version(zf: zipfile.ZipFile) -> int:
    """Return the major SIARD version integer (1 or 2)."""
    names = zf.namelist()
    version_entries = [n for n in names if n.startswith("header/siardversion/") and n != "header/siardversion/"]
    if not version_entries:
        return 1
    version_str = version_entries[0].rstrip("/").split("/")[-1]
    dot = version_str.find(".")
    if dot > 0:
        version_str = version_str[:dot]
    try:
        return int(version_str)
    except ValueError:
        return 1


class SiardDataSource:
    """Concrete IRelationalDataSource backed by a .siard ZIP file."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        with zipfile.ZipFile(self._path, "r") as zf:
            version = _detect_version(zf)
            metadata_bytes = zf.read("header/metadata.xml")

        if version == 1:
            self._archive: SiardArchive = parse_v1(metadata_bytes)
        else:
            self._archive = parse_v2(metadata_bytes)

        # Wire back-references so columns/attributes can resolve UDTs
        self._wire_data_source_refs()

    # ------------------------------------------------------------------
    # Wire back-references
    # ------------------------------------------------------------------

    def _wire_data_source_refs(self) -> None:
        """Set _data_source on every ColumnV2/AttributeV2 that needs it."""
        if isinstance(self._archive, SiardArchiveV1):
            return
        for schema in self._archive.schemas:
            for table in schema.tables:
                for col in table.columns:
                    col._data_source = self
            for type_obj in schema.types:
                for attr in type_obj.attributes:
                    attr._data_source = self

    # ------------------------------------------------------------------
    # IRelationalDataSource protocol
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        return self._archive.name

    @property
    def schemas(self) -> list:
        return self._archive.schemas

    @property
    def producer_application(self) -> Optional[str]:
        return self._archive.producer_application

    @property
    def data_owner(self) -> Optional[str]:
        return self._archive.data_owner

    @property
    def lob_folder(self) -> str:
        return self._archive.lob_folder if hasattr(self._archive, "lob_folder") else ""

    def find_schema(self, name: str):
        for s in self._archive.schemas:
            if s.name == name:
                return s
        return None

    def find_table(self, schema_name: str, table_name: str):
        schema = self.find_schema(schema_name)
        if schema is None:
            return None
        return schema.find_table(table_name)

    def find_type(self, schema_name: Optional[str], type_name: Optional[str]):
        if not schema_name or not type_name:
            return None
        schema = self.find_schema(schema_name)
        if schema is None:
            return None
        if hasattr(schema, "find_type"):
            return schema.find_type(type_name)
        return None

    def get_all_attributes(self, type_obj) -> list[IAttribute]:
        """Walk the supertype chain and collect all attributes."""
        result: list[IAttribute] = []
        current = type_obj
        while current is not None:
            result.extend(current.attributes)
            if not current.has_super_type:
                break
            if isinstance(self._archive, SiardArchiveV2) and hasattr(current, "under_schema"):
                current = self.find_type(current.under_schema, current.under_type)
            else:
                break
        return result

    def open_zip(self) -> zipfile.ZipFile:
        """Open a fresh ZipFile handle (caller is responsible for closing)."""
        return zipfile.ZipFile(self._path, "r")
