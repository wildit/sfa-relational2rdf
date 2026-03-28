"""
AI-assisted converter context and table converter.

Mirrors Relational2Rdf.Converter.Ai.Conversion.* in the C# project.

The AI converter asks the LLM to:
  1. Map raw table/type names to human-friendly RDF type names.
  2. Map column names to RDF predicate names.
  3. Name foreign-key relationships in both directions.
  4. Detect and name many-to-many junction tables.
"""
from __future__ import annotations

import base64
import gzip
import io
import logging
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import quote

from rdflib import URIRef, Literal, Namespace
from rdflib.namespace import RDF

from relational2rdf.converter.ai.magic import AiMagic
from relational2rdf.converter.settings import AiConversionSettings, TableConversionSettings
from relational2rdf.models import AttributeType, CommonType, IBlob, IRow, ITable, ISchema
from relational2rdf.rdf.writer import TurtleWriter

log = logging.getLogger(__name__)

_COUNTER: dict[str, int] = {}


def _inc_counter(key: str) -> int:
    _COUNTER[key] = _COUNTER.get(key, 0) + 1
    return _COUNTER[key]


def _uri(base: str, *parts: str) -> URIRef:
    result = base.rstrip("/")
    for p in parts:
        result = result.rstrip("/") + "/" + quote(str(p), safe="")
    return URIRef(result)


# ---------------------------------------------------------------------------
# Conversion meta (key strategy)
# ---------------------------------------------------------------------------

@dataclass
class SingleKeyMeta:
    base_iri: str
    schema_iri: str
    type_name: str
    key_column: str
    predicate_names: dict  # IAttribute -> URIRef
    references: list
    value_attributes: list
    nested_metas: dict
    needs_escaping: bool = True

    def get_key(self, row: IRow) -> str:
        val = row.get_item(self.key_column)
        if val is None:
            return str(_inc_counter(self.base_iri))
        return str(val)


@dataclass
class MultiKeyMeta:
    base_iri: str
    schema_iri: str
    type_name: str
    key_columns: list[str]
    predicate_names: dict
    references: list
    value_attributes: list
    nested_metas: dict

    def get_key(self, row: IRow) -> str:
        parts = [str(row.get_item(c) or "") for c in self.key_columns]
        return "_".join(parts)


@dataclass
class NoKeyMeta:
    base_iri: str
    schema_iri: str
    type_name: str
    predicate_names: dict
    references: list
    value_attributes: list
    nested_metas: dict
    _counter_key: str = ""

    def get_key(self, row: IRow) -> str:
        return str(_inc_counter(self._counter_key or self.base_iri))


@dataclass
class SingleKeyReferenceMeta:
    forward_predicate: URIRef
    backward_predicate: URIRef
    fk_column: str
    target_type_iri: str

    def get_target_key(self, row: IRow) -> Optional[str]:
        val = row.get_item(self.fk_column)
        return str(val) if val is not None else None


@dataclass
class MultiKeyReferenceMeta:
    forward_predicate: URIRef
    backward_predicate: URIRef
    fk_columns: list[str]
    target_type_iri: str

    def get_target_key(self, row: IRow) -> Optional[str]:
        parts = [str(row.get_item(c) or "") for c in self.fk_columns]
        if all(p == "" for p in parts):
            return None
        return "_".join(parts)


@dataclass
class ManyToManyMeta:
    source_type_iri: str
    target_type_iri: str
    source_to_target_predicate: URIRef
    target_to_source_predicate: URIRef
    source_columns: list[str]
    target_columns: list[str]

    def get_source_key(self, row: IRow) -> str:
        return "_".join(str(row.get_item(c) or "") for c in self.source_columns)

    def get_target_key(self, row: IRow) -> str:
        return "_".join(str(row.get_item(c) or "") for c in self.target_columns)


# ---------------------------------------------------------------------------
# Conversion context
# ---------------------------------------------------------------------------

class AiConversionContext:
    def __init__(
        self,
        data_source,
        settings: AiConversionSettings,
        ai_magic: AiMagic,
    ) -> None:
        self._ds = data_source
        self._settings = settings
        self._magic = ai_magic
        self._base = settings.base_iri.rstrip("/")

        # Cache: schema_name -> dict (table_name -> friendly_name)
        self._schema_table_names: dict[str, dict[str, str]] = {}
        # Cache: schema_name -> URIRef
        self._schema_iris: dict[str, URIRef] = {}

    @property
    def data_source(self):
        return self._ds

    @property
    def ai_magic(self) -> AiMagic:
        return self._magic

    def _ensure_schema_names(self, schema_name: str) -> None:
        if schema_name in self._schema_table_names:
            return
        schema = self._ds.find_schema(schema_name)
        if schema is None:
            self._schema_table_names[schema_name] = {}
            return
        all_names = [t.name for t in schema.tables] + [t.name for t in getattr(schema, "types", [])]
        mapping = self._magic.get_rdf_friendly_names(all_names)
        self._schema_table_names[schema_name] = mapping

    def get_table_name(self, schema_name: str, table_name: str) -> str:
        self._ensure_schema_names(schema_name)
        return self._schema_table_names.get(schema_name, {}).get(table_name, table_name)

    def get_schema_iri(self, schema_name: str) -> URIRef:
        if schema_name not in self._schema_iris:
            self._schema_iris[schema_name] = URIRef(f"{self._base}/{quote(schema_name, safe='')}/")
        return self._schema_iris[schema_name]

    def get_table_iri(self, schema_name: str, table_name: str) -> str:
        friendly = self.get_table_name(schema_name, table_name)
        return f"{self._base}/{quote(schema_name, safe='')}/{quote(friendly, safe='')}"

    def get_table_predicate_iri(self, schema_name: str, table_name: str) -> str:
        return self.get_table_iri(schema_name, table_name) + "/predicate"

    def get_counter(self, key: str) -> str:
        return key


# ---------------------------------------------------------------------------
# Meta builder (async, calls LLM)
# ---------------------------------------------------------------------------

async def _build_conversion_meta_async(
    ctx: AiConversionContext,
    schema,
    table,
) -> object:
    schema_name = schema.name
    table_name = ctx.get_table_name(schema_name, table.name)

    base_iri = ctx.get_table_iri(schema_name, table.name)
    pred_iri = ctx.get_table_predicate_iri(schema_name, table.name)
    schema_iri = ctx.get_schema_iri(schema_name)

    ref_col_names = {
        ref.source_column
        for fk in table.foreign_keys
        for ref in fk.references
    }
    value_columns = [c for c in table.columns if c.name not in ref_col_names]
    udt_columns = [c for c in table.columns if c.attribute_type in (AttributeType.Udt, AttributeType.UdtArray)]

    pred_names = await _run_in_executor(
        ctx.ai_magic.get_rdf_relationship_names,
        table_name,
        [c.name for c in value_columns],
    )

    col_map = {c.name: c for c in table.columns}
    predicates: dict = {col_map[k]: URIRef(f"{pred_iri}/{quote(v, safe='')}") for k, v in pred_names.items() if k in col_map}

    nested_metas: dict = {}
    for attr in udt_columns:
        type_obj = ctx.data_source.find_type(attr.udt_schema, attr.udt_type)
        if type_obj:
            udt_schema = ctx.data_source.find_schema(attr.udt_schema)
            nested_metas[attr] = await _build_type_meta_async(ctx, udt_schema, type_obj)

    references = await _build_reference_metas_async(ctx, schema, table)
    key_cols = list(table.key_columns)

    if len(key_cols) == 1:
        return SingleKeyMeta(
            base_iri=base_iri,
            schema_iri=str(schema_iri),
            type_name=table_name,
            key_column=key_cols[0].name,
            predicate_names=predicates,
            references=references,
            value_attributes=value_columns,
            nested_metas=nested_metas,
            needs_escaping=(key_cols[0].common_type != CommonType.Integer),
        )
    if len(key_cols) > 1:
        return MultiKeyMeta(
            base_iri=base_iri,
            schema_iri=str(schema_iri),
            type_name=table_name,
            key_columns=[c.name for c in key_cols],
            predicate_names=predicates,
            references=references,
            value_attributes=value_columns,
            nested_metas=nested_metas,
        )
    return NoKeyMeta(
        base_iri=base_iri,
        schema_iri=str(schema_iri),
        type_name=table_name,
        predicate_names=predicates,
        references=references,
        value_attributes=value_columns,
        nested_metas=nested_metas,
        _counter_key=f"{schema_name}.{table.name}",
    )


async def _build_type_meta_async(ctx, schema, type_obj):
    type_name = ctx.get_table_name(schema.name, type_obj.name)
    base_iri = ctx.get_table_iri(schema.name, type_obj.name)
    pred_iri = ctx.get_table_predicate_iri(schema.name, type_obj.name)
    schema_iri = ctx.get_schema_iri(schema.name)
    attrs = ctx.data_source.get_all_attributes(type_obj)
    pred_names = await _run_in_executor(
        ctx.ai_magic.get_rdf_relationship_names,
        type_name,
        [a.name for a in attrs],
    )
    attr_map = {a.name: a for a in attrs}
    predicates = {attr_map[k]: URIRef(f"{pred_iri}/{quote(v, safe='')}") for k, v in pred_names.items() if k in attr_map}
    return NoKeyMeta(
        base_iri=base_iri,
        schema_iri=str(schema_iri),
        type_name=type_name,
        predicate_names=predicates,
        references=[],
        value_attributes=list(attrs),
        nested_metas={},
        _counter_key=f"{schema.name}.{type_obj.name}",
    )


async def _build_reference_metas_async(ctx, schema, table) -> list:
    fkeys = list(table.foreign_keys)
    if not fkeys:
        return []

    table_name = ctx.get_table_name(schema.name, table.name)
    fk_tuples = [
        (fk.name, ctx.get_table_name(fk.referenced_schema, fk.referenced_table))
        for fk in fkeys
    ]
    naming = await _run_in_executor(
        ctx.ai_magic.get_foreign_key_names, table_name, fk_tuples
    )

    metas = []
    for fk in fkeys:
        target_iri = ctx.get_table_iri(fk.referenced_schema, fk.referenced_table)
        pred_iri = ctx.get_table_predicate_iri(schema.name, table.name)
        target_pred_iri = ctx.get_table_predicate_iri(fk.referenced_schema, fk.referenced_table)
        fwd = URIRef(f"{pred_iri}/{quote(naming.forward.get(fk.name, fk.name), safe='')}")
        bwd = URIRef(f"{target_pred_iri}/{quote(naming.backward.get(fk.name, fk.name), safe='')}")
        if len(fk.references) == 1:
            metas.append(SingleKeyReferenceMeta(
                forward_predicate=fwd,
                backward_predicate=bwd,
                fk_column=fk.references[0].source_column,
                target_type_iri=target_iri,
            ))
        else:
            metas.append(MultiKeyReferenceMeta(
                forward_predicate=fwd,
                backward_predicate=bwd,
                fk_columns=[r.source_column for r in fk.references],
                target_type_iri=target_iri,
            ))
    return metas


async def _build_many_to_many_meta_async(ctx, schema, table) -> ManyToManyMeta:
    fkeys = list(table.foreign_keys)
    fk_src, fk_tgt = fkeys[0], fkeys[1]

    src_schema = ctx.data_source.find_schema(fk_src.referenced_schema)
    src_table = ctx.data_source.find_table(fk_src.referenced_schema, fk_src.referenced_table)
    tgt_schema = ctx.data_source.find_schema(fk_tgt.referenced_schema)
    tgt_table = ctx.data_source.find_table(fk_tgt.referenced_schema, fk_tgt.referenced_table)

    src_name = ctx.get_table_name(src_schema.name, src_table.name)
    tgt_name = ctx.get_table_name(tgt_schema.name, tgt_table.name)
    mid_name = ctx.get_table_name(schema.name, table.name)

    predicates = await _run_in_executor(
        ctx.ai_magic.get_many_to_many_names,
        src_name, tgt_name, mid_name, fk_src.name, fk_tgt.name,
    )

    src_pred_iri = ctx.get_table_predicate_iri(src_schema.name, src_table.name)
    tgt_pred_iri = ctx.get_table_predicate_iri(tgt_schema.name, tgt_table.name)

    return ManyToManyMeta(
        source_type_iri=ctx.get_table_iri(src_schema.name, src_table.name),
        target_type_iri=ctx.get_table_iri(tgt_schema.name, tgt_table.name),
        source_to_target_predicate=URIRef(f"{src_pred_iri}/{quote(predicates.forward, safe='')}"),
        target_to_source_predicate=URIRef(f"{tgt_pred_iri}/{quote(predicates.backward, safe='')}"),
        source_columns=[r.source_column for r in fk_src.references],
        target_columns=[r.source_column for r in fk_tgt.references],
    )


import asyncio


async def _run_in_executor(fn, *args):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, fn, *args)


# ---------------------------------------------------------------------------
# Table converter
# ---------------------------------------------------------------------------

class AiTableConverter:
    def __init__(
        self,
        ctx: AiConversionContext,
        schema,
        table,
        writer: TurtleWriter,
        settings: TableConversionSettings,
    ) -> None:
        self._ctx = ctx
        self._schema = schema
        self._table = table
        self._writer = writer
        self._settings = settings

    async def convert_async(self) -> None:
        from relational2rdf.siard.table_reader import SiardTableReader

        ref_cols = {
            ref.source_column
            for fk in self._table.foreign_keys
            for ref in fk.references
        }
        all_cols_are_fk = all(c.name in ref_cols for c in self._table.columns)
        is_m2m = len(list(self._table.foreign_keys)) == 2 and all_cols_are_fk

        reader = SiardTableReader(
            self._ctx.data_source,
            self._schema,
            self._table,
            max_blob_length=self._settings.max_blob_length,
            max_blob_before_compression=self._settings.max_blob_before_compression,
            blob_too_large_value=self._settings.blob_too_large_value,
        )
        try:
            if is_m2m:
                meta = await _build_many_to_many_meta_async(self._ctx, self._schema, self._table)
                self._convert_many_to_many(reader, meta)
            else:
                meta = await _build_conversion_meta_async(self._ctx, self._schema, self._table)
                self._convert_table(reader, meta)
        finally:
            reader.close()

    def _convert_table(self, reader, meta) -> None:
        while True:
            row = reader.read_next()
            if row is None:
                break
            key = meta.get_key(row)
            subject_iri = URIRef(f"{meta.base_iri}/{quote(key, safe='')}")
            sub = self._writer.begin_subject(subject_iri)
            sub.write_type(URIRef(f"{meta.schema_iri}{quote(meta.type_name, safe='')}"))
            self._write_row(meta, row, sub)
            self._write_references(key, meta, row, sub)
            self._writer.end_subject(sub)

    def _convert_many_to_many(self, reader, meta: ManyToManyMeta) -> None:
        while True:
            row = reader.read_next()
            if row is None:
                break
            src_key = meta.get_source_key(row)
            tgt_key = meta.get_target_key(row)
            src_iri = URIRef(f"{meta.source_type_iri}/{quote(src_key, safe='')}")
            tgt_iri = URIRef(f"{meta.target_type_iri}/{quote(tgt_key, safe='')}")
            self._writer.write_triple(src_iri, meta.source_to_target_predicate, tgt_iri)
            if self._settings.bi_directional_references:
                self._writer.write_triple(tgt_iri, meta.target_to_source_predicate, src_iri)

    def _write_references(self, src_key: str, meta, row: IRow, sub) -> None:
        for ref in meta.references:
            tgt_key = ref.get_target_key(row)
            if tgt_key is not None:
                tgt_iri = URIRef(f"{ref.target_type_iri}/{quote(tgt_key, safe='')}")
                src_iri = URIRef(f"{meta.base_iri}/{quote(src_key, safe='')}")
                sub.write_iri(ref.forward_predicate, tgt_iri)
                if self._settings.bi_directional_references:
                    self._writer.write_triple(tgt_iri, ref.backward_predicate, src_iri)

    def _write_row(self, meta, row: IRow, sub) -> None:
        for attr in meta.value_attributes:
            value = row.get_item(attr.name)
            if value is None:
                continue
            pred = meta.predicate_names.get(attr)
            if pred is None:
                continue

            attr_type = attr.attribute_type
            if attr_type == AttributeType.Value:
                self._write_primitive(attr, value, pred, sub)
            elif attr_type == AttributeType.Array:
                for item in (value or []):
                    decoded = self._handle_lob(attr.common_type, item)
                    if decoded is not None:
                        sub.write_literal(pred, decoded)
            elif attr_type == AttributeType.Udt:
                nested = meta.nested_metas.get(attr)
                if nested and value is not None:
                    k = nested.get_key(value)
                    n_iri = URIRef(f"{nested.base_iri}/{quote(k, safe='')}")
                    n_sub = self._writer.begin_subject(n_iri)
                    self._write_row(nested, value, n_sub)
                    self._writer.end_subject(n_sub)
                    sub.write_iri(pred, n_iri)
            elif attr_type == AttributeType.UdtArray:
                nested = meta.nested_metas.get(attr)
                if nested and value is not None:
                    for udt_row in value:
                        k = nested.get_key(udt_row)
                        n_iri = URIRef(f"{nested.base_iri}/{quote(k, safe='')}")
                        n_sub = self._writer.begin_subject(n_iri)
                        self._write_row(nested, udt_row, n_sub)
                        self._writer.end_subject(n_sub)
                        sub.write_iri(pred, n_iri)

    def _write_primitive(self, attr, value, pred, sub) -> None:
        decoded = self._handle_lob(attr.common_type, value)
        if decoded is None:
            return
        if attr.common_type.can_write_raw():
            sub.write_raw(pred, decoded.lower())
        else:
            sub.write_literal(pred, decoded)

    def _handle_lob(self, common_type: CommonType, value) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, IBlob):
            stream = value.get_stream()
            if stream is None:
                return None
            if common_type == CommonType.String:
                return stream.read().decode("utf-8", errors="replace")
            # Binary LOB
            if self._settings.max_blob_length is not None and value.length > self._settings.max_blob_length:
                log.warning("Blob %s too large (%d bytes), using placeholder", value.identifier, value.length)
                return self._settings.blob_too_large_value
            data = stream.read()
            if self._settings.max_blob_before_compression is not None and len(data) > self._settings.max_blob_before_compression:
                data = gzip.compress(data, compresslevel=9)
            return base64.b64encode(data).decode("ascii")
        return str(value)
