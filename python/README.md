# relational2rdf — Python implementation

Python rewrite of the [Relational2Rdf](https://github.com/SwissFederalArchives/sfa-relational2Rdf) .NET tool.
Converts relational **SIARD** archives (v1, v2, v2.1) to **RDF Turtle** knowledge graphs.

## Requirements

- Python ≥ 3.11
- pip packages: `typer`, `rdflib`, `openai`, `httpx`, `lxml`

## Installation

```bash
cd python
pip install -e .
```

Or install dependencies only:

```bash
pip install typer rdflib openai httpx lxml
```

## Usage

```
relational2rdf siard [SIARD_FILE_OR_DIR] [OPTIONS]
```

### Ontology converter (default, no AI needed)

```bash
relational2rdf siard archive.siard \
  --converter ontology \
  --table-config table-config.json \
  --output ./output/
```

### AI converter — OpenAI

```bash
relational2rdf siard archive.siard \
  --converter ai \
  --ai-service openai \
  --ai-key sk-... \
  --ai-model gpt-4o \
  --table-config table-config.json
```

### AI converter — LM Studio (local, free)

```bash
relational2rdf siard archive.siard \
  --converter ai \
  --ai-service openai \
  --ai-endpoint http://localhost:1234/ \
  --ai-model "your-loaded-model-name" \
  --ai-key lm-studio \
  --table-config table-config.json
```

> LM Studio exposes an OpenAI-compatible API on `http://localhost:1234/v1` by default.
> No code changes are needed — just point `--ai-endpoint` at it and set any non-empty `--ai-key`.

### AI converter — Ollama (local, free)

```bash
relational2rdf siard archive.siard \
  --converter ai \
  --ai-service ollama \
  --ai-endpoint http://localhost:11434/ \
  --ai-model llama3 \
  --table-config table-config.json
```

## CLI Reference

| Option | Short | Default | Description |
|---|---|---|---|
| `--converter` | `-v` | `ontology` | `ontology` or `ai` |
| `--threads` | `-t` | `1` | Worker threads |
| `--base-iri` | `-i` | `https://ld.admin.ch/` | Base IRI for RDF names |
| `--output` | `-o` | `./` | Output directory |
| `--output-file` | `-f` | *(archive name)* | Output `.ttl` filename |
| `--table-config` | `-c` | *(defaults)* | Path to `table-config.json` |
| `--ai-key` | `-k` | `` | API key |
| `--ai-endpoint` | `-e` | `https://api.openai.com/` | AI endpoint URL |
| `--ai-model` | `-m` | `gpt-3.5-turbo` | Model name |
| `--ai-service` | `-s` | `openai` | `openai` or `ollama` |
| `--no-console` | | `False` | Suppress log output to stdout |
| `--log-file` | `-l` | | Write logs to file |
| `--log-level` | | `INFO` | `DEBUG`/`INFO`/`WARNING`/`ERROR` |

## Table Config (`table-config.json`)

```json
{
  "MaxBlobLength": 134217728,
  "MaxBlobLengthBeforeCompression": 8192,
  "BlobCompressionLevel": "SmallestSize",
  "BlobToLargeErrorValue": "Error Blob was too large during conversion",
  "ConvertMetadata": false,
  "BiDirectionalReferences": true
}
```

## Architecture

```
relational2rdf/
├── cli.py                    # Typer CLI entry-point
├── models.py                 # Protocol-based abstractions (IRow, ITable, …)
├── siard/
│   ├── reader.py             # SiardFileReader
│   ├── data_source.py        # SiardDataSource (IRelationalDataSource)
│   ├── v1.py                 # SIARD v1 XML parser
│   ├── v2.py                 # SIARD v2/v2.1 XML parser
│   ├── table_reader.py       # Streaming table row reader
│   └── sql_types.py          # SQL type → CommonType mapping
├── rdf/
│   └── writer.py             # rdflib-backed Turtle writer
└── converter/
    ├── settings.py           # Settings dataclasses
    ├── manager.py            # Multi-threaded conversion orchestrator
    ├── ontology/
    │   └── converter.py      # Fixed SIARD ontology converter
    └── ai/
        ├── inference.py      # OpenAI / Ollama / LM Studio back-ends
        ├── magic.py          # LLM prompt builders
        └── converter.py      # AI-assisted table converter
```
