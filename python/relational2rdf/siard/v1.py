"""
SIARD V1 metadata parser.

SIARD 1.x archives store their schema metadata in `header/metadata.xml`
using the namespace http://www.bar.admin.ch/xmlns/siard/1.0/metadata.xsd
The table data XML files live at content/{schemaFolder}/{tableFolder}/{tableFolder}.xml.

No user-defined types in V1 – all columns are simple SQL types.
No explicit LOB folder in V1 archives.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional
from xml.etree.ElementTree import Element

from relational2rdf.models import (
    AttributeType,
    CommonType,
    IAttribute,
    IColumn,
    IColumnReference,
    IField,
    IForeignKey,
    ISchema,
    ITable,
    IType,
    TypeType,
)
from relational2rdf.siard.sql_types import get_common_type

NS_V1 = "http://www.bar.admin.ch/xmlns/siard/1.0/metadata.xsd"


def _tag(local: str) -> str:
    return f"{{{NS_V1}}}{local}"


# ---------------------------------------------------------------------------
# Column
# ---------------------------------------------------------------------------

@dataclass
class ColumnV1:
    name: str
    source_type: str
    original_source_type: Optional[str]
    description: Optional[str]

    # IAttribute / IColumn protocol
    udt_type: Optional[str] = None
    udt_schema: Optional[str] = None
    cardinality: Optional[int] = 1
    fields: list[IField] = field(default_factory=list)

    # Use identity-based hashing so instances can be used as dict keys
    # (mirrors C# reference-type semantics).
    __hash__ = object.__hash__

    @property
    def attribute_type(self) -> AttributeType:
        return AttributeType.Value

    @property
    def common_type(self) -> CommonType:
        return get_common_type(self.source_type)


# ---------------------------------------------------------------------------
# Foreign key reference
# ---------------------------------------------------------------------------

@dataclass
class ColumnReferenceV1:
    source_column: str
    target_column: str


# ---------------------------------------------------------------------------
# Foreign key
# ---------------------------------------------------------------------------

@dataclass
class ForeignKeyV1:
    name: str
    from_schema: str
    from_table: str
    referenced_schema: str
    referenced_table: str
    references: list[ColumnReferenceV1]


# ---------------------------------------------------------------------------
# Table
# ---------------------------------------------------------------------------

@dataclass
class TableV1:
    name: str
    folder: str
    row_count: int
    columns: list[ColumnV1]
    foreign_keys: list[ForeignKeyV1]
    primary_key_columns: list[str]

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def key_columns(self) -> list[IColumn]:
        pk = set(self.primary_key_columns)
        return [c for c in self.columns if c.name in pk]

    def find_column(self, name: str) -> Optional[ColumnV1]:
        for c in self.columns:
            if c.name == name:
                return c
        return None


# ---------------------------------------------------------------------------
# Schema (V1 has no UDTs)
# ---------------------------------------------------------------------------

@dataclass
class SchemaV1:
    name: str
    folder: str
    tables: list[TableV1]
    types: list[IType] = field(default_factory=list)

    def find_table(self, name: str) -> Optional[TableV1]:
        for t in self.tables:
            if t.name == name:
                return t
        return None

    def find_type(self, name: str) -> Optional[IType]:
        return None


# ---------------------------------------------------------------------------
# Archive
# ---------------------------------------------------------------------------

@dataclass
class SiardArchiveV1:
    name: str
    producer_application: Optional[str]
    data_owner: Optional[str]
    lob_folder: str = ""
    schemas: list[SchemaV1] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

def _text(el: Optional[Element]) -> Optional[str]:
    return el.text if el is not None else None


def _parse_column(el: Element) -> ColumnV1:
    return ColumnV1(
        name=_text(el.find(_tag("name"))) or "",
        source_type=_text(el.find(_tag("type"))) or "",
        original_source_type=_text(el.find(_tag("typeOriginal"))),
        description=_text(el.find(_tag("description"))),
    )


def _parse_reference(el: Element) -> ColumnReferenceV1:
    return ColumnReferenceV1(
        source_column=_text(el.find(_tag("column"))) or "",
        target_column=_text(el.find(_tag("referenced"))) or "",
    )


def _parse_foreign_key(el: Element, from_schema: str, from_table: str) -> ForeignKeyV1:
    refs = [_parse_reference(r) for r in el.findall(_tag("reference"))]
    return ForeignKeyV1(
        name=_text(el.find(_tag("name"))) or "",
        from_schema=from_schema,
        from_table=from_table,
        referenced_schema=_text(el.find(_tag("referencedSchema"))) or from_schema,
        referenced_table=_text(el.find(_tag("referencedTable"))) or "",
        references=refs,
    )


def _parse_table(el: Element, schema_name: str) -> TableV1:
    table_name = _text(el.find(_tag("name"))) or ""
    folder = _text(el.find(_tag("folder"))) or table_name
    rows_text = _text(el.find(_tag("rows"))) or "0"

    columns_el = el.find(_tag("columns"))
    columns = [_parse_column(c) for c in (columns_el.findall(_tag("column")) if columns_el is not None else [])]

    pk_el = el.find(_tag("primaryKey"))
    pk_cols: list[str] = []
    if pk_el is not None:
        pk_cols = [c.text for c in pk_el.findall(_tag("column")) if c.text]

    fk_els = el.findall(_tag("foreignKeys"))
    fkeys: list[ForeignKeyV1] = []
    for fk_wrap in fk_els:
        for fk_el in fk_wrap.findall(_tag("foreignKey")):
            fkeys.append(_parse_foreign_key(fk_el, schema_name, table_name))

    # Also handle direct <foreignKey> children (some variants)
    for fk_el in el.findall(_tag("foreignKey")):
        fkeys.append(_parse_foreign_key(fk_el, schema_name, table_name))

    return TableV1(
        name=table_name,
        folder=folder,
        row_count=int(rows_text),
        columns=columns,
        foreign_keys=fkeys,
        primary_key_columns=pk_cols,
    )


def _parse_schema(el: Element) -> SchemaV1:
    schema_name = _text(el.find(_tag("name"))) or ""
    folder = _text(el.find(_tag("folder"))) or schema_name

    tables_el = el.find(_tag("tables"))
    tables = [_parse_table(t, schema_name) for t in (tables_el.findall(_tag("table")) if tables_el is not None else [])]

    return SchemaV1(name=schema_name, folder=folder, tables=tables)


def parse_v1(metadata_bytes: bytes) -> SiardArchiveV1:
    """Parse a SIARD V1 metadata.xml byte string and return an archive object."""
    import xml.etree.ElementTree as ET

    root = ET.fromstring(metadata_bytes)

    # Root element may be <siardArchive> or have namespace prefix
    schemas_el = root.find(_tag("schemas"))
    schemas: list[SchemaV1] = []
    if schemas_el is not None:
        for s in schemas_el.findall(_tag("schema")):
            schemas.append(_parse_schema(s))

    return SiardArchiveV1(
        name=_text(root.find(_tag("dbname"))) or "",
        producer_application=_text(root.find(_tag("producerApplication"))),
        data_owner=_text(root.find(_tag("dataOwner"))),
        lob_folder="",
        schemas=schemas,
    )
