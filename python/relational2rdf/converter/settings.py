"""
Shared settings dataclasses used by the conversion manager and converters.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Optional
import io


class CompressionLevel(Enum):
    NoCompression = auto()
    Fastest = auto()
    Optimal = auto()
    SmallestSize = auto()


@dataclass
class TableConversionSettings:
    max_blob_length: Optional[int] = 128 * 1024 * 1024       # 128 MB
    max_blob_before_compression: Optional[int] = 8192         # 8 KB
    blob_compression_level: CompressionLevel = CompressionLevel.SmallestSize
    blob_too_large_value: str = "Error Blob was too large during conversion"
    convert_metadata: bool = False
    bi_directional_references: bool = True

    @classmethod
    def from_dict(cls, d: dict) -> "TableConversionSettings":
        level_map = {
            "NoCompression": CompressionLevel.NoCompression,
            "Fastest": CompressionLevel.Fastest,
            "Optimal": CompressionLevel.Optimal,
            "SmallestSize": CompressionLevel.SmallestSize,
        }
        return cls(
            max_blob_length=d.get("MaxBlobLength", 128 * 1024 * 1024),
            max_blob_before_compression=d.get("MaxBlobLengthBeforeCompression", 8192),
            blob_compression_level=level_map.get(
                d.get("BlobCompressionLevel", "SmallestSize"),
                CompressionLevel.SmallestSize,
            ),
            blob_too_large_value=d.get(
                "BlobToLargeErrorValue", "Error Blob was too large during conversion"
            ),
            convert_metadata=d.get("ConvertMetadata", False),
            bi_directional_references=d.get("BiDirectionalReferences", True),
        )


@dataclass
class ConverterSettings:
    thread_count: int = 1
    console_output: bool = True
    output_dir: Path = field(default_factory=lambda: Path("./"))
    file_name: Optional[str] = None


@dataclass
class OntologySettings:
    base_iri: str = "https://ld.admin.ch/"
    siard_iri: str = "http://siard.link#"
    table_settings: TableConversionSettings = field(
        default_factory=TableConversionSettings
    )


@dataclass
class AiConversionSettings:
    base_iri: str = "https://ld.admin.ch/"
    ai_key: str = ""
    ai_endpoint: str = "https://api.openai.com/v1/"
    ai_model: str = "gpt-3.5-turbo"
    ai_service: str = "openai"
    table_settings: TableConversionSettings = field(
        default_factory=TableConversionSettings
    )
