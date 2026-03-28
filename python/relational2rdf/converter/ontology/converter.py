"""
Ontology converter – maps the relational structure to the fixed SIARD ontology.

Mirrors Relational2Rdf.Converter.Ontology.* in the C# project.

The ontology models a relational database as:
  - siard:Table  → has siard:Row(s)
  - siard:Row    → has siard:Cell(s), typed as siard:Row
  - siard:Cell   → siard:hasColumn, siard:value, siard:hasReference

Relationships are represented as siard:Reference objects rather than
direct predicates, preserving the original structure.
"""
from __future__ import annotations

import base64
import gzip
import logging
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import quote

from rdflib import Literal, Namespace, URIRef
from rdflib.namespace import RDF

from relational2rdf.converter.settings import OntologySettings, TableConversionSettings
from relational2rdf.models import AttributeType, CommonType, IBlob, IRow, ISchema, ITable
from relational2rdf.rdf.writer import TurtleWriter

log = logging.getLogger(__name__)

_COUNTER: dict[str, int] = {}


def _inc(key: str) -> int:
    _COUNTER[key] = _COUNTER.get(key, 0) + 1
    return _COUNTER[key]


def _uri(*parts: str) -> URIRef:
    result = parts[0].rstrip("/")
    for p in parts[1:]:
        result = result.rstrip("/") + "/" + quote(str(p), safe="")
    return URIRef(result)


# ---------------------------------------------------------------------------
# Conversion context
# ---------------------------------------------------------------------------

class OntologyConversionContext:
    def __init__(self, data_source, settings: OntologySettings) -> None:
        self._ds = data_source
        self._settings = settings
        self._base = settings.base_iri.rstrip("/")
        ns = Namespace(settings.siard_iri)

        self.siard_iri = settings.siard_iri.rstrip("/")
        self.has_table_pred = URIRef(f"{self.siard_iri}hasTable")
        self.has_row_pred = URIRef(f"{self.siard_iri}hasRow")
        self.has_cell_pred = URIRef(f"{self.siard_iri}hasCell")
        self.has_column_pred = URIRef(f"{self.siard_iri}hasColumn")
        self.value_pred = URIRef(f"{self.siard_iri}value")
        self.has_reference_pred = URIRef(f"{self.siard_iri}hasReference")
        self.is_referenced_by_pred = URIRef(f"{self.siard_iri}isReferencedBy")
        self.referenced_row_pred = URIRef(f"{self.siard_iri}referencedRow")
        self.is_of_key_pred = URIRef(f"{self.siard_iri}isOfKey")

        self.row_type = URIRef(f"{self.siard_iri}Row")
        self.cell_type = URIRef(f"{self.siard_iri}Cell")
        self.reference_type = URIRef(f"{self.siard_iri}Reference")

    @property
    def data_source(self):
        return self._ds

    @property
    def table_settings(self) -> TableConversionSettings:
        return self._settings.table_settings

    def get_table_iri(self, schema, table) -> URIRef:
        return URIRef(f"{self._base}/{quote(schema.name, safe='')}/{quote(table.name, safe='')}")

    def get_type_iri(self, schema_name_or_schema, type_name_or_type) -> URIRef:
        if isinstance(schema_name_or_schema, str):
            sname = schema_name_or_schema
        else:
            sname = schema_name_or_schema.name
        if isinstance(type_name_or_type, str):
            tname = type_name_or_type
        else:
            tname = type_name_or_type.name
        return URIRef(f"{self._base}/{quote(sname, safe='')}/{quote(tname, safe='')}")

    def get_column_iri(self, schema, table, column) -> URIRef:
        return _uri(self.get_table_iri(schema, table), column.name)

    def get_attribute_iri(self, schema, type_obj, attr) -> URIRef:
        return _uri(self.get_type_iri(schema.name, type_obj.name), attr.name)

    def get_foreign_key_iri(self, schema, table, fk) -> URIRef:
        return _uri(self.get_table_iri(schema, table), "fk", fk.name)

    def get_counter(self, key: str) -> str:
        return key


# ---------------------------------------------------------------------------
# Conversion meta
# ---------------------------------------------------------------------------

@dataclass
class AttributeItemInfo:
    attribute: object
    attribute_iri: URIRef
    cell_name: str  # "c1", "c2", …


@dataclass
class SingleKeyMeta:
    type_name: str
    type_iri: URIRef
    key_column: str
    row_base_iri: URIRef
    attribute_item_infos: dict  # IAttribute -> AttributeItemInfo
    attributes: list
    nested_metas: dict
    references: list
    needs_escaping: bool = True

    def get_key(self, row: IRow) -> str:
        val = row.get_item(self.key_column)
        return str(val) if val is not None else str(_inc(f"singlekey_{self.type_iri}"))


@dataclass
class MultiKeyMeta:
    type_name: str
    type_iri: URIRef
    key_columns: list[str]
    row_base_iri: URIRef
    attribute_item_infos: dict
    attributes: list
    nested_metas: dict
    references: list

    def get_key(self, row: IRow) -> str:
        return "_".join(str(row.get_item(c) or "") for c in self.key_columns)


@dataclass
class NoKeyMeta:
    type_name: str
    type_iri: URIRef
    row_base_iri: URIRef
    attribute_item_infos: dict
    attributes: list
    nested_metas: dict
    references: list
    _counter_key: str = ""

    def get_key(self, row: IRow) -> str:
        return str(_inc(self._counter_key or str(self.type_iri)))


def _build_meta_for_type(ctx: OntologyConversionContext, schema, type_obj) -> NoKeyMeta:
    type_iri = ctx.get_type_iri(schema.name, type_obj.name)
    attrs = ctx.data_source.get_all_attributes(type_obj)
    item_infos = {}
    for i, attr in enumerate(type_obj.attributes, 1):
        item_infos[attr] = AttributeItemInfo(
            attribute=attr,
            attribute_iri=ctx.get_attribute_iri(schema, type_obj, attr),
            cell_name=f"c{i}",
        )
    udt_attrs = [a for a in attrs if a.attribute_type in (AttributeType.Udt, AttributeType.UdtArray)]
    nested = {}
    for attr in udt_attrs:
        t = ctx.data_source.find_type(attr.udt_schema, attr.udt_type)
        s = ctx.data_source.find_schema(attr.udt_schema)
        if t and s:
            nested[attr] = _build_meta_for_type(ctx, s, t)
    return NoKeyMeta(
        type_name=type_obj.name,
        type_iri=type_iri,
        row_base_iri=_uri(str(type_iri), "row"),
        attribute_item_infos=item_infos,
        attributes=list(attrs),
        nested_metas=nested,
        references=[],
        _counter_key=f"{schema.name}.{type_obj.name}",
    )


@dataclass
class SingleKeyRefMeta:
    foreign_key: object
    fk_column: str
    target_row_iri: URIRef
    source_attributes: list
    foreign_key_iri: URIRef

    def get_target_key(self, row: IRow) -> Optional[str]:
        val = row.get_item(self.fk_column)
        return str(val) if val is not None else None


@dataclass
class MultiKeyRefMeta:
    foreign_key: object
    fk_columns: list[str]
    target_row_iri: URIRef
    source_attributes: list
    foreign_key_iri: URIRef

    def get_target_key(self, row: IRow) -> Optional[str]:
        parts = [str(row.get_item(c) or "") for c in self.fk_columns]
        if all(p == "" for p in parts):
            return None
        return "_".join(parts)


def _build_reference_metas(ctx: OntologyConversionContext, schema, table) -> list:
    metas = []
    for fk in table.foreign_keys:
        target_row_iri = _uri(
            str(ctx.get_type_iri(fk.referenced_schema, fk.referenced_table)), "row"
        )
        fk_iri = ctx.get_foreign_key_iri(schema, table, fk)
        src_attrs = [next((c for c in table.columns if c.name == r.source_column), None) for r in fk.references]
        src_attrs = [a for a in src_attrs if a is not None]
        if len(fk.references) == 1:
            metas.append(SingleKeyRefMeta(
                foreign_key=fk,
                fk_column=fk.references[0].source_column,
                target_row_iri=target_row_iri,
                source_attributes=src_attrs,
                foreign_key_iri=fk_iri,
            ))
        else:
            metas.append(MultiKeyRefMeta(
                foreign_key=fk,
                fk_columns=[r.source_column for r in fk.references],
                target_row_iri=target_row_iri,
                source_attributes=src_attrs,
                foreign_key_iri=fk_iri,
            ))
    return metas


def build_conversion_meta(ctx: OntologyConversionContext, schema, table):
    table_iri = ctx.get_table_iri(schema, table)
    ref_col_names = {r.source_column for fk in table.foreign_keys for r in fk.references}

    item_infos = {}
    for i, col in enumerate(table.columns, 1):
        item_infos[col] = AttributeItemInfo(
            attribute=col,
            attribute_iri=ctx.get_column_iri(schema, table, col),
            cell_name=f"c{i}",
        )

    udt_cols = [c for c in table.columns if c.attribute_type in (AttributeType.Udt, AttributeType.UdtArray)]
    nested = {}
    for attr in udt_cols:
        t = ctx.data_source.find_type(attr.udt_schema, attr.udt_type)
        s = ctx.data_source.find_schema(attr.udt_schema)
        if t and s:
            nested[attr] = _build_meta_for_type(ctx, s, t)

    references = _build_reference_metas(ctx, schema, table)
    key_cols = list(table.key_columns)

    if len(key_cols) == 1:
        return SingleKeyMeta(
            type_name=table.name,
            type_iri=table_iri,
            key_column=key_cols[0].name,
            row_base_iri=_uri(str(table_iri), "row"),
            attribute_item_infos=item_infos,
            attributes=list(table.columns),
            nested_metas=nested,
            references=references,
            needs_escaping=(key_cols[0].common_type != CommonType.Integer),
        )
    if len(key_cols) > 1:
        return MultiKeyMeta(
            type_name=table.name,
            type_iri=table_iri,
            key_columns=[c.name for c in key_cols],
            row_base_iri=_uri(str(table_iri), "row"),
            attribute_item_infos=item_infos,
            attributes=list(table.columns),
            nested_metas=nested,
            references=references,
        )
    return NoKeyMeta(
        type_name=table.name,
        type_iri=table_iri,
        row_base_iri=_uri(str(table_iri), "row"),
        attribute_item_infos=item_infos,
        attributes=list(table.columns),
        nested_metas=nested,
        references=references,
        _counter_key=f"{schema.name}.{table.name}",
    )


# ---------------------------------------------------------------------------
# Table converter
# ---------------------------------------------------------------------------

class OntologyTableConverter:
    def __init__(
        self,
        ctx: OntologyConversionContext,
        schema,
        table,
        writer: TurtleWriter,
    ) -> None:
        self._ctx = ctx
        self._schema = schema
        self._table = table
        self._writer = writer
        self._settings = ctx.table_settings

    def convert(self) -> None:
        from relational2rdf.siard.table_reader import SiardTableReader

        reader = SiardTableReader(
            self._ctx.data_source,
            self._schema,
            self._table,
            max_blob_length=self._settings.max_blob_length,
            max_blob_before_compression=self._settings.max_blob_before_compression,
            blob_too_large_value=self._settings.blob_too_large_value,
        )
        try:
            table_iri = self._ctx.get_table_iri(self._schema, self._table)
            meta = build_conversion_meta(self._ctx, self._schema, self._table)
            while True:
                row = reader.read_next()
                if row is None:
                    break
                row_key = meta.get_key(row)
                row_iri = _uri(str(meta.row_base_iri), row_key)
                sub = self._writer.begin_subject(row_iri)
                sub.write_type(self._ctx.row_type)
                sub.write_iri(self._ctx.has_table_pred, table_iri)
                self._write_row(meta, row, sub, row_key, row_iri)
                self._writer.end_subject(sub)
                self._writer.write_triple(table_iri, self._ctx.has_row_pred, row_iri)
        finally:
            reader.close()

    def _write_row(self, meta, row: IRow, sub, row_key: str, row_iri: URIRef) -> None:
        for attr_obj, info in meta.attribute_item_infos.items():
            cell_iri = _uri(str(row_iri), info.cell_name)
            self._writer.write_triple(row_iri, self._ctx.has_cell_pred, cell_iri)
            cell_sub = self._writer.begin_subject(cell_iri)
            cell_sub.write_type(self._ctx.cell_type)
            cell_sub.write_iri(self._ctx.has_row_pred, row_iri)
            cell_sub.write_iri(self._ctx.has_column_pred, info.attribute_iri)
            self._write_cell_value(row, meta, attr_obj, cell_sub)
            self._writer.end_subject(cell_sub)

        for ref in meta.references:
            target_key = ref.get_target_key(row)
            if target_key is None:
                continue
            target_iri = _uri(str(ref.target_row_iri), target_key)
            ref_iri = _uri(
                str(meta.type_iri), "reference", row_key,
                quote(ref.foreign_key.name, safe="")
            )
            ref_sub = self._writer.begin_subject(ref_iri)
            ref_sub.write_type(self._ctx.reference_type)
            ref_sub.write_iri(self._ctx.referenced_row_pred, target_iri)
            ref_sub.write_iri(self._ctx.is_of_key_pred, ref.foreign_key_iri)
            self._writer.end_subject(ref_sub)

            for src_attr in ref.source_attributes:
                src_info = meta.attribute_item_infos.get(src_attr)
                if src_info:
                    cell_iri = _uri(str(row_iri), src_info.cell_name)
                    self._writer.write_triple(cell_iri, self._ctx.has_reference_pred, ref_iri)
                    self._writer.write_triple(ref_iri, self._ctx.is_referenced_by_pred, cell_iri)

    def _write_cell_value(self, row: IRow, meta, attr, cell_sub) -> None:
        value = row.get_item(attr.name)
        attr_type = attr.attribute_type

        if attr_type == AttributeType.Value:
            decoded = self._handle_lob(attr.common_type, value)
            if decoded is not None:
                if attr.common_type.can_write_raw():
                    cell_sub.write_raw(self._ctx.value_pred, decoded.lower())
                else:
                    cell_sub.write_literal(self._ctx.value_pred, decoded)

        elif attr_type == AttributeType.Array:
            for item in (value or []):
                decoded = self._handle_lob(attr.common_type, item)
                if decoded is not None:
                    if attr.common_type.can_write_raw():
                        cell_sub.write_raw(self._ctx.value_pred, decoded.lower())
                    else:
                        cell_sub.write_literal(self._ctx.value_pred, decoded)

        elif attr_type == AttributeType.Udt:
            nested_meta = meta.nested_metas.get(attr)
            if nested_meta and value is not None:
                n_key = nested_meta.get_key(value)
                n_iri = _uri(str(nested_meta.row_base_iri), n_key)
                n_sub = self._writer.begin_subject(n_iri)
                self._write_row(nested_meta, value, n_sub, n_key, n_iri)
                self._writer.end_subject(n_sub)
                cell_sub.write_iri(self._ctx.value_pred, n_iri)

        elif attr_type == AttributeType.UdtArray:
            nested_meta = meta.nested_metas.get(attr)
            if nested_meta and value is not None:
                for udt_row in value:
                    n_key = nested_meta.get_key(udt_row)
                    n_iri = _uri(str(nested_meta.row_base_iri), n_key)
                    n_sub = self._writer.begin_subject(n_iri)
                    self._write_row(nested_meta, udt_row, n_sub, n_key, n_iri)
                    self._writer.end_subject(n_sub)
                    cell_sub.write_iri(self._ctx.value_pred, n_iri)

    def _handle_lob(self, common_type: CommonType, value) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, IBlob):
            stream = value.get_stream()
            if stream is None:
                return None
            if common_type == CommonType.String:
                return stream.read().decode("utf-8", errors="replace")
            if self._settings.max_blob_length is not None and value.length > self._settings.max_blob_length:
                log.warning("Blob %s too large, using placeholder", value.identifier)
                return self._settings.blob_too_large_value
            data = stream.read()
            if self._settings.max_blob_before_compression is not None and len(data) > self._settings.max_blob_before_compression:
                data = gzip.compress(data, compresslevel=9)
            return base64.b64encode(data).decode("ascii")
        return str(value) if value is not None else None
