"""
Maps SQL type names (as stored in SIARD metadata) to the normalised
CommonType enum.  Mirrors SqlTypeMapping / Mappings.resx in the C# project.
"""
from __future__ import annotations

from relational2rdf.models import CommonType

# Upper-cased SQL type token -> CommonType
_MAP: dict[str, CommonType] = {
    "BIGINT": CommonType.Integer,
    "BINARY LARGE OBJECT": CommonType.Blob,
    "BINARY VARYING": CommonType.Blob,
    "BIT": CommonType.Boolean,
    "BIT VARYING": CommonType.Boolean,
    "BOOLEAN": CommonType.Boolean,
    "CHARACTER": CommonType.String,
    "CHARACTER VARYING": CommonType.String,
    "CHARACTER VARYING OBJECT": CommonType.String,
    "DATALINK": CommonType.String,
    "DATE": CommonType.Date,
    "DECIMAL": CommonType.Decimal,
    "DOUBLE PRECISION": CommonType.Decimal,
    "FLOAT": CommonType.Decimal,
    "INT": CommonType.Integer,
    "INTEGER": CommonType.Integer,
    "INTERVAL": CommonType.TimeSpan,
    "NATIONAL CHAR": CommonType.String,
    "NATIONAL CHAR VARYING": CommonType.String,
    "NATIONAL CHARACTER": CommonType.String,
    "NATIONAL CHARACTER LARGE OBJECT": CommonType.String,
    "NATIONAL CHARACTER VARYING": CommonType.String,
    "NCHAR": CommonType.String,
    "NCHAR LARGE OBJECT": CommonType.String,
    "NCHAR VARYING": CommonType.String,
    "NCLOB": CommonType.String,
    "NUMERIC": CommonType.Decimal,
    "REAL": CommonType.Decimal,
    "SMALLINT": CommonType.Integer,
    "TIME": CommonType.Time,
    "TIME WITH TIME ZONE": CommonType.TimeSpan,
    "TIME WITHOUT TIME ZONE": CommonType.TimeSpan,
    "TIMESTAMP": CommonType.DateTime,
    "TIMESTAMP WITH TIME ZONE": CommonType.DateTime,
    "TIMESTAMP WITHOUT TIME ZONE": CommonType.DateTime,
    "VARBINARY": CommonType.Blob,
    "VARCHAR": CommonType.String,
    "XML": CommonType.String,
    # Common non-standard aliases encountered in real archives
    "BLOB": CommonType.Blob,
    "CLOB": CommonType.String,
    "TEXT": CommonType.String,
    "TINYINT": CommonType.Integer,
    "MEDIUMINT": CommonType.Integer,
    "NUMBER": CommonType.Decimal,
    "DOUBLE": CommonType.Decimal,
    "CHAR": CommonType.String,
    "NVARCHAR": CommonType.String,
    "NTEXT": CommonType.String,
    "BINARY": CommonType.Blob,
}


def get_common_type(sql_type: str | None) -> CommonType:
    """Return the CommonType for *sql_type*, stripping any precision/scale."""
    if not sql_type or not sql_type.strip():
        return CommonType.Unknown

    # Strip precision, e.g. "VARCHAR(255)" -> "VARCHAR"
    paren = sql_type.find("(")
    if paren > 0:
        sql_type = sql_type[:paren]

    return _MAP.get(sql_type.strip().upper(), CommonType.Unknown)
