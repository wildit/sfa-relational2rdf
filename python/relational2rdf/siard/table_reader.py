"""
SiardTableReader – streams rows from the per-table XML file inside a SIARD ZIP.

The table XML format (both V1 and V2):

    <table>
      <row>
        <c1>value</c1>
        <c2 file="lob1.bin" length="42"/>  <!-- external LOB -->
        <c3>                               <!-- UDT -->
          <u1>field1</u1>
          <u2>field2</u2>
        </c3>
        <c4>                               <!-- array -->
          <a1>item</a1>
          <a2>item</a2>
        </c4>
      </row>
      ...
    </table>

Columns are numbered c1, c2, … (1-based), UDT fields u1, u2, … (1-based).

LOBs that are too large may have a configurable placeholder.
"""
from __future__ import annotations

import base64
import gzip
import zipfile
from dataclasses import dataclass, field
from io import BytesIO, RawIOBase
from typing import IO, Iterator, Optional

from relational2rdf.models import (
    AttributeType,
    CommonType,
    IAttribute,
    IBlob,
    IRow,
    ISchema,
    ITable,
)


# ---------------------------------------------------------------------------
# Blob implementation
# ---------------------------------------------------------------------------

@dataclass
class ZipBlob:
    """Lazy blob backed by a ZipFile entry."""
    identifier: str
    length: int
    mime_type: Optional[str]
    _zip: zipfile.ZipFile
    _entry_path: str

    def get_stream(self) -> IO[bytes]:
        return self._zip.open(self._entry_path)


# ---------------------------------------------------------------------------
# Row implementation
# ---------------------------------------------------------------------------

class Row:
    """Mutable row that is reused across reads within one table scan."""

    def __init__(self, attributes: list[IAttribute]) -> None:
        self.attributes = attributes
        self._index: dict[str, int] = {}
        self._values: list[object] = [None] * len(attributes)

        for i, attr in enumerate(attributes):
            self._index[attr.name] = i
            # SIARD XML encoding: columns c1..cN, UDT fields u1..uN
            xml_key = f"c{i + 1}"
            self._index[xml_key] = i

    def clear(self) -> None:
        for i in range(len(self._values)):
            self._values[i] = None

    def set(self, key: str, value: object) -> None:
        idx = self._index.get(key)
        if idx is not None:
            self._values[idx] = value

    def get_item(self, column: str) -> object:
        idx = self._index.get(column)
        if idx is None:
            return None
        return self._values[idx]

    def enumerate(self) -> Iterator[tuple[IAttribute, object]]:
        for i, attr in enumerate(self.attributes):
            yield attr, self._values[i]

    def __getitem__(self, column: str) -> object:
        return self.get_item(column)


class UdtRow:
    """Nested row for UDT columns/fields (u1, u2, … indexing)."""

    def __init__(self, attributes: list[IAttribute]) -> None:
        self.attributes = attributes
        self._index: dict[str, int] = {}
        self._values: list[object] = [None] * len(attributes)

        for i, attr in enumerate(attributes):
            self._index[attr.name] = i
            xml_key = f"u{i + 1}"
            self._index[xml_key] = i

    def clear(self) -> None:
        for i in range(len(self._values)):
            self._values[i] = None

    def set(self, key: str, value: object) -> None:
        idx = self._index.get(key)
        if idx is not None:
            self._values[idx] = value

    def get_item(self, column: str) -> object:
        idx = self._index.get(column)
        if idx is None:
            return None
        return self._values[idx]

    def enumerate(self) -> Iterator[tuple[IAttribute, object]]:
        for i, attr in enumerate(self.attributes):
            yield attr, self._values[i]


# ---------------------------------------------------------------------------
# Table reader
# ---------------------------------------------------------------------------

class SiardTableReader:
    """
    Streams rows from content/{schemaFolder}/{tableFolder}/{tableFolder}.xml.
    """

    def __init__(
        self,
        data_source,
        schema,
        table,
        max_blob_length: Optional[int] = 128 * 1024 * 1024,
        max_blob_before_compression: Optional[int] = 8192,
        blob_too_large_value: str = "Error Blob was too large during conversion",
    ) -> None:
        self._data_source = data_source
        self.schema = schema
        self.table = table
        self._max_blob = max_blob_length
        self._max_before_gz = max_blob_before_compression
        self._blob_error = blob_too_large_value

        self._zip: zipfile.ZipFile = data_source.open_zip()
        schema_folder = schema.folder
        table_folder = table.folder
        self._path_prefix = f"content/{schema_folder}/{table_folder}/"
        xml_path = f"{self._path_prefix}{table_folder}.xml"
        self._xml_data = self._zip.read(xml_path)
        self._iter = self._parse_rows()

    # ------------------------------------------------------------------
    # ITableReader protocol
    # ------------------------------------------------------------------

    def read_next(self) -> Optional[IRow]:
        try:
            return next(self._iter)
        except StopIteration:
            return None

    def close(self) -> None:
        self._zip.close()

    # ------------------------------------------------------------------
    # XML streaming parser
    # ------------------------------------------------------------------

    def _parse_rows(self) -> Iterator[IRow]:
        """
        Full-document iterparse that collects complete <row> elements before
        processing, avoiding partial-element issues with streaming SAX parsers.
        """
        import xml.etree.ElementTree as ET

        columns = list(self.table.columns)

        col_map: dict[str, tuple[int, IAttribute]] = {}
        for i, attr in enumerate(columns):
            col_map[attr.name] = (i, attr)
            col_map[f"c{i + 1}"] = (i, attr)

        row = Row(columns)

        context = ET.iterparse(BytesIO(self._xml_data), events=("end",))
        for event, el in context:
            local = el.tag.split("}")[-1] if "}" in el.tag else el.tag
            if local == "row":
                row.clear()
                for child in el:
                    child_local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                    entry = col_map.get(child_local)
                    if entry is None:
                        continue
                    idx, attr = entry
                    row._values[idx] = self._read_cell(child, attr)
                yield row
                el.clear()

    def _read_row_el(self, row_el, row_obj, attributes: list[IAttribute], is_udt: bool) -> None:
        """Populate *row_obj* from the child elements of *row_el* (used for UDTs)."""
        col_map: dict[str, tuple[int, IAttribute]] = {}
        for i, attr in enumerate(attributes):
            col_map[attr.name] = (i, attr)
            xml_key = f"u{i + 1}" if is_udt else f"c{i + 1}"
            col_map[xml_key] = (i, attr)

        for child in row_el:
            local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            entry = col_map.get(local)
            if entry is None:
                continue
            idx, attr = entry
            value = self._read_cell(child, attr)
            row_obj._values[idx] = value

    def _read_cell(self, el, attr: IAttribute) -> object:
        """Read a single cell element and return the appropriate Python value."""
        attr_type = attr.attribute_type

        if attr_type == AttributeType.Value:
            return self._read_value(el, attr)

        if attr_type == AttributeType.Array:
            items = []
            for child in el:
                items.append(self._read_value(child, attr))
            return items

        if attr_type == AttributeType.Udt:
            return self._read_udt(el, attr)

        if attr_type == AttributeType.UdtArray:
            rows = []
            for child in el:
                rows.append(self._read_udt(child, attr))
            return rows

        return None

    def _read_value(self, el, attr: IAttribute) -> object:
        """Read a primitive (or LOB) cell value."""
        lob_file = el.get("file")
        if lob_file is not None:
            return self._read_lob(el, attr, lob_file)
        if el.text is None:
            return None
        return el.text

    def _read_lob(self, el, attr: IAttribute, lob_file: str) -> Optional[ZipBlob]:
        """Resolve an external LOB reference to a ZipBlob."""
        length_str = el.get("length", "0")
        try:
            length = int(length_str)
        except ValueError:
            length = 0

        # LOB folder: archive lob_folder + column lob_folder + filename
        archive_lob = self._data_source.lob_folder
        col_lob = getattr(attr, "lob_folder", "") or ""
        mime = getattr(attr, "mime_type", None)
        entry_path = f"{archive_lob}{col_lob}{lob_file}"

        return ZipBlob(
            identifier=lob_file,
            length=length,
            mime_type=mime,
            _zip=self._zip,
            _entry_path=entry_path,
        )

    def _read_udt(self, el, attr: IAttribute):
        """Read a UDT cell, returning a UdtRow."""
        if self._data_source is None:
            return None
        type_obj = self._data_source.find_type(attr.udt_schema, attr.udt_type)
        if type_obj is None:
            return None
        udt_attrs = self._data_source.get_all_attributes(type_obj)
        udt_row = UdtRow(udt_attrs)
        self._read_row_el(el, udt_row, udt_attrs, is_udt=True)
        return udt_row
