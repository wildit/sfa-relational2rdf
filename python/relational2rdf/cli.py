"""
CLI entry-point for relational2rdf (Python rewrite).

Usage examples
--------------
# Ontology converter (default)
relational2rdf siard archive.siard --table-config table-config.json

# AI converter with OpenAI
relational2rdf siard archive.siard --converter ai --ai-key sk-... --table-config table-config.json

# AI converter with LM Studio (OpenAI-compatible)
relational2rdf siard archive.siard --converter ai \\
    --ai-service openai \\
    --ai-endpoint http://localhost:1234/ \\
    --ai-model "your-loaded-model" \\
    --ai-key lm-studio \\
    --table-config table-config.json

# AI converter with Ollama
relational2rdf siard archive.siard --converter ai \\
    --ai-service ollama \\
    --ai-endpoint http://localhost:11434/ \\
    --ai-model llama3 \\
    --table-config table-config.json
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Annotated, Optional

import typer

from relational2rdf.converter.manager import ConversionsManager
from relational2rdf.converter.settings import (
    AiConversionSettings,
    ConverterSettings,
    OntologySettings,
    TableConversionSettings,
)
from relational2rdf.siard.reader import SiardFileReader

app = typer.Typer(
    name="relational2rdf",
    help="Convert relational SIARD archives to RDF knowledge graphs.",
    add_completion=False,
)


def _load_table_config(path: Optional[str]) -> TableConversionSettings:
    if path:
        with open(path, "r", encoding="utf-8") as fh:
            return TableConversionSettings.from_dict(json.load(fh))
    return TableConversionSettings()


def _setup_logging(log_file: Optional[str], log_level: str, no_console: bool) -> None:
    level = getattr(logging, log_level.upper(), logging.INFO)
    handlers: list[logging.Handler] = []
    if not no_console:
        handlers.append(logging.StreamHandler(sys.stdout))
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=handlers,
    )


# ---------------------------------------------------------------------------
# siard sub-command
# ---------------------------------------------------------------------------

@app.command("siard")
def run_siard(
    siard_file: Annotated[str, typer.Argument(help="Path to a .siard archive or directory of .siard files")],
    # Converter selection
    converter: Annotated[str, typer.Option("--converter", "-v", help="Converter to use: 'ontology' or 'ai'")] = "ontology",
    # Threading
    threads: Annotated[int, typer.Option("--threads", "-t", help="Worker threads (default: 1)")] = 1,
    # IRI
    base_iri: Annotated[str, typer.Option("--base-iri", "-i", help="Base IRI for RDF names")] = "https://ld.admin.ch/",
    # Output
    output: Annotated[str, typer.Option("--output", "-o", help="Output directory")] = "./",
    output_file: Annotated[Optional[str], typer.Option("--output-file", "-f", help="Output file name")] = None,
    # Table config
    table_config: Annotated[Optional[str], typer.Option("--table-config", "-c", help="Path to table-config.json")] = None,
    # AI options
    ai_key: Annotated[str, typer.Option("--ai-key", "-k", help="API key for AI endpoint")] = "",
    ai_endpoint: Annotated[str, typer.Option("--ai-endpoint", "-e", help="AI endpoint URL (OpenAI-compatible)")] = "https://api.openai.com/",
    ai_model: Annotated[str, typer.Option("--ai-model", "-m", help="AI model name")] = "gpt-3.5-turbo",
    ai_service: Annotated[str, typer.Option("--ai-service", "-s", help="AI service: 'openai' or 'ollama'")] = "openai",
    # Logging
    no_console: Annotated[bool, typer.Option("--no-console", help="Suppress console output")] = False,
    log_file: Annotated[Optional[str], typer.Option("--log-file", "-l", help="Path to log file")] = None,
    log_level: Annotated[str, typer.Option("--log-level", help="Log level (DEBUG/INFO/WARNING/ERROR)")] = "INFO",
) -> None:
    """Convert a SIARD archive (or directory of archives) to RDF Turtle."""

    _setup_logging(log_file, log_level, no_console)
    log = logging.getLogger("relational2rdf")

    table_settings = _load_table_config(table_config)

    converter_settings = ConverterSettings(
        thread_count=threads,
        console_output=not no_console,
        output_dir=Path(output),
        file_name=output_file,
    )

    # Build type-specific settings
    conv_lower = converter.lower()
    if conv_lower == "ontology":
        type_settings = OntologySettings(
            base_iri=base_iri,
            table_settings=table_settings,
        )
    elif conv_lower == "ai":
        type_settings = AiConversionSettings(
            base_iri=base_iri,
            ai_key=ai_key,
            ai_endpoint=ai_endpoint,
            ai_model=ai_model,
            ai_service=ai_service,
            table_settings=table_settings,
        )
    else:
        typer.echo(f"Unknown converter: {converter!r}. Choose 'ontology' or 'ai'.", err=True)
        raise typer.Exit(1)

    manager = ConversionsManager(converter_settings, conv_lower, type_settings)
    reader = SiardFileReader()

    # Accept either a single .siard file or a directory of .siard files
    target = Path(siard_file)
    if target.is_dir():
        files = list(target.glob("*.siard"))
    else:
        files = [target]

    if not files:
        typer.echo("No .siard files found.", err=True)
        raise typer.Exit(1)

    for f in files:
        log.info("Reading %s", f)
        data_source = reader.read(f)
        log.info(
            "Archive '%s': %d schemas, %d tables",
            data_source.name,
            len(data_source.schemas),
            sum(len(s.tables) for s in data_source.schemas),
        )
        output_path = manager.convert(data_source)
        log.info("Written to %s", output_path)


# ---------------------------------------------------------------------------
# Entry-point guard
# ---------------------------------------------------------------------------

def main() -> None:
    app()


if __name__ == "__main__":
    main()
