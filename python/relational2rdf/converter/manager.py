"""
ConversionsManager – orchestrates the full conversion of one data source.

For the Ontology converter the actual table conversion is CPU-bound (XML
streaming + RDF graph construction), so we use a ThreadPoolExecutor.

For the AI converter each table requires async LLM calls, so we run each
table in its own asyncio task (within the thread pool).
"""
from __future__ import annotations

import asyncio
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from relational2rdf.converter.settings import (
    AiConversionSettings,
    ConverterSettings,
    OntologySettings,
)
from relational2rdf.rdf.writer import TurtleWriter

log = logging.getLogger(__name__)

_INVALID_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def _safe_filename(name: str) -> str:
    return _INVALID_FILENAME_CHARS.sub("_", name)


class ConversionsManager:
    def __init__(
        self,
        converter_settings: ConverterSettings,
        converter_type: str,
        type_settings: OntologySettings | AiConversionSettings,
    ) -> None:
        self._settings = converter_settings
        self._converter_type = converter_type.lower()
        self._type_settings = type_settings

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def convert(self, data_source) -> str:
        output_file = self._resolve_output_path(data_source)
        log.info("Output will be written to %s", output_file)

        with TurtleWriter(output_file) as writer:
            if self._converter_type == "ontology":
                self._run_ontology(data_source, writer)
            elif self._converter_type == "ai":
                asyncio.run(self._run_ai_async(data_source, writer))
            else:
                raise ValueError(f"Unknown converter type: {self._converter_type!r}")

        return output_file

    # ------------------------------------------------------------------
    # Ontology conversion (synchronous, thread-pool per table)
    # ------------------------------------------------------------------

    def _run_ontology(self, data_source, writer: TurtleWriter) -> None:
        from relational2rdf.converter.ontology.converter import (
            OntologyConversionContext,
            OntologyTableConverter,
        )

        ctx = OntologyConversionContext(data_source, self._type_settings)
        writer.bind("siard", self._type_settings.siard_iri)
        writer.bind("base", self._type_settings.base_iri)

        tables = [
            (schema, table)
            for schema in data_source.schemas
            for table in schema.tables
        ]

        if self._settings.thread_count <= 1:
            for schema, table in tables:
                log.info("Converting %s.%s", schema.name, table.name)
                conv = OntologyTableConverter(ctx, schema, table, writer)
                conv.convert()
        else:
            # Note: rdflib Graph is not thread-safe for writes; we collect
            # per-table writers and merge at the end.
            import tempfile, os

            tmp_dir = Path(tempfile.mkdtemp())
            partial_files = []

            def convert_one(schema, table):
                out = tmp_dir / f"{schema.name}_{table.name}.ttl"
                with TurtleWriter(out) as w:
                    w.bind("siard", self._type_settings.siard_iri)
                    c = OntologyTableConverter(ctx, schema, table, w)
                    c.convert()
                return str(out)

            with ThreadPoolExecutor(max_workers=self._settings.thread_count) as pool:
                futures = {pool.submit(convert_one, s, t): (s, t) for s, t in tables}
                for future in as_completed(futures):
                    s, t = futures[future]
                    try:
                        partial_files.append(future.result())
                        log.info("Done: %s.%s", s.name, t.name)
                    except Exception as exc:
                        log.error("Error converting %s.%s: %s", s.name, t.name, exc, exc_info=True)

            # Merge all partial Turtle files into the main writer's graph
            import rdflib
            for pf in partial_files:
                writer._g.parse(pf, format="turtle")
                os.unlink(pf)
            tmp_dir.rmdir()

    # ------------------------------------------------------------------
    # AI conversion (async per-table)
    # ------------------------------------------------------------------

    async def _run_ai_async(self, data_source, writer: TurtleWriter) -> None:
        from relational2rdf.converter.ai.converter import AiConversionContext, AiTableConverter
        from relational2rdf.converter.ai.magic import AiMagic
        from relational2rdf.converter.ai.inference import get_inference_service

        cfg: AiConversionSettings = self._type_settings
        service = get_inference_service(cfg.ai_service, cfg.ai_endpoint, cfg.ai_key, cfg.ai_model)
        magic = AiMagic(service)
        ctx = AiConversionContext(data_source, cfg, magic)
        writer.bind("base", cfg.base_iri)

        tables = [
            (schema, table)
            for schema in data_source.schemas
            for table in schema.tables
        ]

        semaphore = asyncio.Semaphore(self._settings.thread_count)

        async def convert_one(schema, table):
            async with semaphore:
                log.info("Converting %s.%s", schema.name, table.name)
                conv = AiTableConverter(ctx, schema, table, writer, cfg.table_settings)
                await conv.convert_async()

        tasks = [convert_one(s, t) for s, t in tables]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for (s, t), result in zip(tables, results):
            if isinstance(result, Exception):
                log.error("Error converting %s.%s: %s", s.name, t.name, result, exc_info=False)

    # ------------------------------------------------------------------
    # Helper
    # ------------------------------------------------------------------

    def _resolve_output_path(self, data_source) -> str:
        out_dir = self._settings.output_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        filename = self._settings.file_name or f"{_safe_filename(data_source.name)}.ttl"
        if not filename.endswith(".ttl"):
            filename += ".ttl"
        return str(out_dir / filename)
