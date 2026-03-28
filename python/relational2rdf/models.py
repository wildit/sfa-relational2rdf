"""
Shared data model abstractions that mirror the C# interface hierarchy in
Relational2Rdf.Common.Abstractions.  All concrete SIARD objects implement
these protocols so that the converter layer stays data-source agnostic.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import IO, Iterator, Optional, Protocol, runtime_checkable


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class CommonType(Enum):
    Unknown = auto()
    String = auto()
    Boolean = auto()
    Integer = auto()
    Decimal = auto()
    DateTime = auto()
    Date = auto()
    Time = auto()
    TimeSpan = auto()
    Blob = auto()

    def can_write_raw(self) -> bool:
        """Types that can be embedded as unquoted Turtle literals."""
        return self in (
            CommonType.Boolean,
            CommonType.Integer,
            CommonType.Decimal,
        )


class AttributeType(Enum):
    Value = auto()
    Array = auto()
    Udt = auto()
    UdtArray = auto()


class TypeType(Enum):
    UserDefined = auto()
    Distinct = auto()


# ---------------------------------------------------------------------------
# Blob (large objects stored in separate files inside the ZIP)
# ---------------------------------------------------------------------------

@runtime_checkable
class IBlob(Protocol):
    identifier: str
    length: int
    mime_type: Optional[str]

    def get_stream(self) -> IO[bytes]: ...


# ---------------------------------------------------------------------------
# Column reference (part of a foreign key)
# ---------------------------------------------------------------------------

@runtime_checkable
class IColumnReference(Protocol):
    source_column: str
    target_column: str


# ---------------------------------------------------------------------------
# Foreign key
# ---------------------------------------------------------------------------

@runtime_checkable
class IForeignKey(Protocol):
    name: str
    from_schema: str
    from_table: str
    referenced_table: str
    referenced_schema: str
    references: list[IColumnReference]


# ---------------------------------------------------------------------------
# Field  (nested inside UDT columns)
# ---------------------------------------------------------------------------

@runtime_checkable
class IField(Protocol):
    name: str
    fields: list["IField"]


# ---------------------------------------------------------------------------
# IAttribute  (both columns and UDT attributes share this)
# ---------------------------------------------------------------------------

@runtime_checkable
class IAttribute(Protocol):
    name: str
    source_type: str
    original_source_type: Optional[str]
    udt_type: Optional[str]
    udt_schema: Optional[str]
    attribute_type: AttributeType
    common_type: CommonType
    cardinality: Optional[int]
    fields: list[IField]


# ---------------------------------------------------------------------------
# IColumn  (extends IAttribute)
# ---------------------------------------------------------------------------

@runtime_checkable
class IColumn(IAttribute, Protocol):
    pass


# ---------------------------------------------------------------------------
# IType  (user-defined type)
# ---------------------------------------------------------------------------

@runtime_checkable
class IType(Protocol):
    name: str
    type: TypeType
    base_type: CommonType
    has_super_type: bool
    attributes: list[IAttribute]


# ---------------------------------------------------------------------------
# ITable
# ---------------------------------------------------------------------------

@runtime_checkable
class ITable(Protocol):
    name: str
    folder: str
    row_count: int
    columns: list[IColumn]
    column_names: list[str]
    key_columns: list[IColumn]
    foreign_keys: list[IForeignKey]


# ---------------------------------------------------------------------------
# ISchema
# ---------------------------------------------------------------------------

@runtime_checkable
class ISchema(Protocol):
    name: str
    folder: str
    tables: list[ITable]
    types: list[IType]

    def find_table(self, name: str) -> Optional[ITable]: ...
    def find_type(self, name: str) -> Optional[IType]: ...


# ---------------------------------------------------------------------------
# IRelationalDataSource
# ---------------------------------------------------------------------------

@runtime_checkable
class IRelationalDataSource(Protocol):
    name: str
    schemas: list[ISchema]
    producer_application: Optional[str]
    data_owner: Optional[str]

    def find_schema(self, name: str) -> Optional[ISchema]: ...
    def find_table(self, schema_name: str, table_name: str) -> Optional[ITable]: ...
    def find_type(self, schema_name: str, type_name: str) -> Optional[IType]: ...
    def get_all_attributes(self, type_obj: IType) -> list[IAttribute]: ...


# ---------------------------------------------------------------------------
# IRow  (one data row read from a table)
# ---------------------------------------------------------------------------

@runtime_checkable
class IRow(Protocol):
    attributes: list[IAttribute]

    def get_item(self, column: str) -> object: ...
    def enumerate(self) -> Iterator[tuple[IAttribute, object]]: ...


# ---------------------------------------------------------------------------
# ITableReader
# ---------------------------------------------------------------------------

@runtime_checkable
class ITableReader(Protocol):
    schema: ISchema
    table: ITable

    def read_next(self) -> Optional[IRow]: ...
    def close(self) -> None: ...
