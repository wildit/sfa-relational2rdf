"""
SIARD V2 / V2.1 metadata parser.

Namespace: http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd
Both SIARD 2.0 and 2.1 use the same major namespace; the version folder inside
the ZIP archive distinguishes them (header/siardversion/2/ or 2.1/).

V2 supports:
  - User-defined types (UDTs) with inheritance (underType / underSchema)
  - Array cardinality on columns/attributes
  - LOB references with mimeType
  - An archive-level lobFolder
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

NS_V2 = "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"


def _tag(local: str) -> str:
    return f"{{{NS_V2}}}{local}"


def _text(el: Optional[Element]) -> Optional[str]:
    return el.text if el is not None else None


# ---------------------------------------------------------------------------
# Field (nested UDT field)
# ---------------------------------------------------------------------------

@dataclass
class FieldV2:
    name: str
    lob_folder: str = ""
    mime_type: Optional[str] = None
    fields: list["FieldV2"] = field(default_factory=list)


def _parse_field(el: Element) -> FieldV2:
    name = _text(el.find(_tag("name"))) or ""
    lob_folder = _text(el.find(_tag("lobFolder"))) or ""
    mime_type = _text(el.find(_tag("mimeType")))
    nested = [_parse_field(f) for f in el.findall(_tag("field"))]
    return FieldV2(name=name, lob_folder=lob_folder, mime_type=mime_type, fields=nested)


# ---------------------------------------------------------------------------
# Column
# ---------------------------------------------------------------------------

@dataclass
class ColumnV2:
    name: str
    # Exactly one of these will be set (simple type or UDT reference)
    source_type: Optional[str]
    udt_schema: Optional[str]
    udt_type: Optional[str]
    original_source_type: Optional[str]
    cardinality: Optional[int]
    lob_folder: str
    mime_type: Optional[str]
    fields: list[FieldV2]
    # Back-reference to the data source (set after construction)
    _data_source: object = field(default=None, repr=False, compare=False)

    # Identity-based hashing so instances can be used as dict keys.
    __hash__ = object.__hash__

    @property
    def attribute_type(self) -> AttributeType:
        return _compute_attr_type(
            is_udt=self.udt_type is not None,
            is_array=(self.cardinality or 0) > 1,
            data_source=self._data_source,
            udt_schema=self.udt_schema,
            udt_type=self.udt_type,
        )

    @property
    def common_type(self) -> CommonType:
        return _compute_common_type(
            source_type=self.source_type,
            udt_schema=self.udt_schema,
            udt_type=self.udt_type,
            data_source=self._data_source,
        )


# ---------------------------------------------------------------------------
# Attribute (inside a UDT type)
# ---------------------------------------------------------------------------

@dataclass
class AttributeV2:
    name: str
    source_type: Optional[str]
    udt_schema: Optional[str]
    udt_type: Optional[str]
    original_source_type: Optional[str]
    cardinality: Optional[int]
    fields: list[FieldV2]
    _data_source: object = field(default=None, repr=False, compare=False)

    # Identity-based hashing so instances can be used as dict keys.
    __hash__ = object.__hash__

    @property
    def attribute_type(self) -> AttributeType:
        return _compute_attr_type(
            is_udt=self.udt_type is not None,
            is_array=(self.cardinality or 0) > 1,
            data_source=self._data_source,
            udt_schema=self.udt_schema,
            udt_type=self.udt_type,
        )

    @property
    def common_type(self) -> CommonType:
        return _compute_common_type(
            source_type=self.source_type,
            udt_schema=self.udt_schema,
            udt_type=self.udt_type,
            data_source=self._data_source,
        )

    # IAttribute protocol alias
    lob_folder: str = ""
    mime_type: Optional[str] = None


# ---------------------------------------------------------------------------
# Type (UDT)
# ---------------------------------------------------------------------------

@dataclass
class TypeV2:
    name: str
    category: str  # "udt" or "distinct"
    under_type: Optional[str]
    under_schema: Optional[str]
    base: Optional[str]
    attributes: list[AttributeV2] = field(default_factory=list)

    @property
    def type(self) -> TypeType:
        return TypeType.UserDefined if self.category == "udt" else TypeType.Distinct

    @property
    def base_type(self) -> CommonType:
        if self.type == TypeType.Distinct:
            return get_common_type(self.base)
        return CommonType.Unknown

    @property
    def has_super_type(self) -> bool:
        return bool(self.under_type)


# ---------------------------------------------------------------------------
# Foreign key
# ---------------------------------------------------------------------------

@dataclass
class ColumnReferenceV2:
    source_column: str
    target_column: str


@dataclass
class ForeignKeyV2:
    name: str
    from_schema: str
    from_table: str
    referenced_schema: str
    referenced_table: str
    references: list[ColumnReferenceV2]


# ---------------------------------------------------------------------------
# Table
# ---------------------------------------------------------------------------

@dataclass
class TableV2:
    name: str
    folder: str
    row_count: int
    columns: list[ColumnV2]
    foreign_keys: list[ForeignKeyV2]
    primary_key_columns: list[str]

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def key_columns(self) -> list[ColumnV2]:
        pk = set(self.primary_key_columns)
        return [c for c in self.columns if c.name in pk]

    def find_column(self, name: str) -> Optional[ColumnV2]:
        for c in self.columns:
            if c.name == name:
                return c
        return None


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

@dataclass
class SchemaV2:
    name: str
    folder: str
    tables: list[TableV2]
    types: list[TypeV2] = field(default_factory=list)

    def find_table(self, name: str) -> Optional[TableV2]:
        for t in self.tables:
            if t.name == name:
                return t
        return None

    def find_type(self, name: str) -> Optional[TypeV2]:
        for t in self.types:
            if t.name == name:
                return t
        return None


# ---------------------------------------------------------------------------
# Archive
# ---------------------------------------------------------------------------

@dataclass
class SiardArchiveV2:
    name: str
    producer_application: Optional[str]
    data_owner: Optional[str]
    lob_folder: str
    schemas: list[SchemaV2] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers for attribute/common type computation
# ---------------------------------------------------------------------------

def _compute_attr_type(
    is_udt: bool,
    is_array: bool,
    data_source: object,
    udt_schema: Optional[str],
    udt_type: Optional[str],
) -> AttributeType:
    if is_udt and data_source is not None:
        type_obj = data_source.find_type(udt_schema, udt_type)
        if type_obj is not None and type_obj.type == TypeType.Distinct:
            return AttributeType.Array if is_array else AttributeType.Value
        return AttributeType.UdtArray if is_array else AttributeType.Udt
    elif is_udt:
        return AttributeType.UdtArray if is_array else AttributeType.Udt
    elif is_array:
        return AttributeType.Array
    return AttributeType.Value


def _compute_common_type(
    source_type: Optional[str],
    udt_schema: Optional[str],
    udt_type: Optional[str],
    data_source: object,
) -> CommonType:
    if udt_type is not None and data_source is not None:
        type_obj = data_source.find_type(udt_schema, udt_type)
        if type_obj is not None and type_obj.type == TypeType.Distinct:
            return type_obj.base_type
        return CommonType.Unknown
    return get_common_type(source_type)


# ---------------------------------------------------------------------------
# Parser helpers
# ---------------------------------------------------------------------------

def _parse_attribute(el: Element) -> AttributeV2:
    name = _text(el.find(_tag("name"))) or ""
    source_type = _text(el.find(_tag("type")))
    udt_schema = _text(el.find(_tag("typeSchema")))
    udt_type = _text(el.find(_tag("typeName")))
    original = _text(el.find(_tag("typeOriginal")))
    card_text = _text(el.find(_tag("cardinality")))
    cardinality = int(card_text) if card_text is not None else None
    fields = [_parse_field(f) for f in el.findall(_tag("field"))]
    return AttributeV2(
        name=name,
        source_type=source_type,
        udt_schema=udt_schema,
        udt_type=udt_type,
        original_source_type=original,
        cardinality=cardinality,
        fields=fields,
    )


def _parse_type(el: Element) -> TypeV2:
    name = _text(el.find(_tag("name"))) or ""
    category = _text(el.find(_tag("category"))) or "distinct"
    under_type = _text(el.find(_tag("underType")))
    under_schema = _text(el.find(_tag("underSchema")))
    base = _text(el.find(_tag("base")))
    attributes = [_parse_attribute(a) for a in el.findall(_tag("attribute"))]
    return TypeV2(
        name=name,
        category=category,
        under_type=under_type,
        under_schema=under_schema,
        base=base,
        attributes=attributes,
    )


def _parse_column_v2(el: Element) -> ColumnV2:
    name = _text(el.find(_tag("name"))) or ""
    source_type = _text(el.find(_tag("type")))
    udt_schema = _text(el.find(_tag("typeSchema")))
    udt_type = _text(el.find(_tag("typeName")))
    original = _text(el.find(_tag("typeOriginal")))
    card_text = _text(el.find(_tag("cardinality")))
    cardinality = int(card_text) if card_text is not None else None
    lob_folder = _text(el.find(_tag("lobFolder"))) or ""
    mime_type = _text(el.find(_tag("mimeType")))
    fields = [_parse_field(f) for f in el.findall(_tag("field"))]
    return ColumnV2(
        name=name,
        source_type=source_type,
        udt_schema=udt_schema,
        udt_type=udt_type,
        original_source_type=original,
        cardinality=cardinality,
        lob_folder=lob_folder,
        mime_type=mime_type,
        fields=fields,
    )


def _parse_reference_v2(el: Element) -> ColumnReferenceV2:
    return ColumnReferenceV2(
        source_column=_text(el.find(_tag("column"))) or "",
        target_column=_text(el.find(_tag("referenced"))) or "",
    )


def _parse_foreign_key_v2(el: Element, schema_name: str, table_name: str) -> ForeignKeyV2:
    refs = [_parse_reference_v2(r) for r in el.findall(_tag("reference"))]
    return ForeignKeyV2(
        name=_text(el.find(_tag("name"))) or "",
        from_schema=schema_name,
        from_table=table_name,
        referenced_schema=_text(el.find(_tag("referencedSchema"))) or schema_name,
        referenced_table=_text(el.find(_tag("referencedTable"))) or "",
        references=refs,
    )


def _parse_table_v2(el: Element, schema_name: str) -> TableV2:
    table_name = _text(el.find(_tag("name"))) or ""
    folder = _text(el.find(_tag("folder"))) or table_name
    rows_text = _text(el.find(_tag("rows"))) or "0"

    cols_el = el.find(_tag("columns"))
    columns = [_parse_column_v2(c) for c in (cols_el.findall(_tag("column")) if cols_el is not None else [])]

    pk_el = el.find(_tag("primaryKey"))
    pk_cols: list[str] = []
    if pk_el is not None:
        pk_cols = [c.text for c in pk_el.findall(_tag("column")) if c.text]

    fkeys: list[ForeignKeyV2] = []
    fkeys_el = el.find(_tag("foreignKeys"))
    if fkeys_el is not None:
        for fk_el in fkeys_el.findall(_tag("foreignKey")):
            fkeys.append(_parse_foreign_key_v2(fk_el, schema_name, table_name))
    for fk_el in el.findall(_tag("foreignKey")):
        fkeys.append(_parse_foreign_key_v2(fk_el, schema_name, table_name))

    return TableV2(
        name=table_name,
        folder=folder,
        row_count=int(rows_text),
        columns=columns,
        foreign_keys=fkeys,
        primary_key_columns=pk_cols,
    )


def _parse_schema_v2(el: Element) -> SchemaV2:
    schema_name = _text(el.find(_tag("name"))) or ""
    folder = _text(el.find(_tag("folder"))) or schema_name

    types_el = el.find(_tag("types"))
    types = [_parse_type(t) for t in (types_el.findall(_tag("type")) if types_el is not None else [])]

    tables_el = el.find(_tag("tables"))
    tables = [_parse_table_v2(t, schema_name) for t in (tables_el.findall(_tag("table")) if tables_el is not None else [])]

    return SchemaV2(name=schema_name, folder=folder, tables=tables, types=types)


def parse_v2(metadata_bytes: bytes) -> SiardArchiveV2:
    """Parse a SIARD V2/V2.1 metadata.xml and return an archive object."""
    import xml.etree.ElementTree as ET

    root = ET.fromstring(metadata_bytes)

    schemas_el = root.find(_tag("schemas"))
    schemas: list[SchemaV2] = []
    if schemas_el is not None:
        for s in schemas_el.findall(_tag("schema")):
            schemas.append(_parse_schema_v2(s))

    lob_folder = _text(root.find(_tag("lobFolder"))) or ""

    return SiardArchiveV2(
        name=_text(root.find(_tag("dbname"))) or "",
        producer_application=_text(root.find(_tag("producerApplication"))),
        data_owner=_text(root.find(_tag("dataOwner"))),
        lob_folder=lob_folder,
        schemas=schemas,
    )
