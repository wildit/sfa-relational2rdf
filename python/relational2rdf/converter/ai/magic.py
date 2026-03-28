"""
AiMagic – prompt builders and retry logic for LLM-assisted RDF naming.

Mirrors Relational2Rdf.Converter.Ai.Inference.AiMagic.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from relational2rdf.converter.ai.inference import IInferenceService

log = logging.getLogger(__name__)

_MAX_RETRIES = 3


class AiMagic:
    def __init__(self, service: IInferenceService) -> None:
        self._ai = service

    # ------------------------------------------------------------------
    # Table / type names
    # ------------------------------------------------------------------

    def get_rdf_friendly_names(self, names: list[str]) -> dict[str, str]:
        """Map raw database table names to clean RDF type names."""
        if not names:
            return {}

        name_list = ", ".join(names)
        prompt = f"""Given the following database table names,
{name_list}

Reply with a json object which maps the table name to a cleaned RDF type name

Example:
{{
    "l_masseinheit": "UnitOfMeasurement"
}}
"""
        return self._ai.request_json(prompt, dict)

    # ------------------------------------------------------------------
    # Column → predicate names
    # ------------------------------------------------------------------

    def get_rdf_relationship_names(
        self, table: str, columns: list[str]
    ) -> dict[str, str]:
        """Map column names for *table* to RDF predicate names with retry."""
        if not columns:
            return {}

        col_list = ", ".join(f'"{c}"' for c in columns)
        prompt = f"""Given the following column names of the table "{table}", generate clean rdf predicate names which describes the relationship between the table and the columns
columns: {col_list}

Example:
Table: Teacher
Columns: [id, age, location]

respond in the following json format:
{{
    "id": "hasId",
    "age": "hasAge",
    "location": "isLocatedIn"
}}

dont explain things, just respond in json
"""
        result: Optional[dict] = None
        for attempt in range(_MAX_RETRIES):
            try:
                partial = self._ai.request_json(prompt, dict)
                if result is None:
                    result = partial
                else:
                    result.update(partial)
                if all(c in result for c in columns):
                    break
            except Exception as exc:
                log.warning("AI attempt %d/%d failed: %s", attempt + 1, _MAX_RETRIES, exc)
                if attempt == _MAX_RETRIES - 1:
                    raise TimeoutError(
                        f"AI failed to generate rdf names after {_MAX_RETRIES} attempts"
                    ) from exc

        return result or {}

    # ------------------------------------------------------------------
    # Foreign key predicates
    # ------------------------------------------------------------------

    @dataclass
    class ForeignKeyPredicateNaming:
        forward: dict[str, str]
        backward: dict[str, str]

    def get_foreign_key_names(
        self,
        table_name: str,
        foreign_keys: list[tuple[str, str]],  # (fk_name, referenced_table)
    ) -> "AiMagic.ForeignKeyPredicateNaming":
        if not foreign_keys:
            return AiMagic.ForeignKeyPredicateNaming({}, {})

        lines = "\n".join(f"{table_name} --({fk})--> {ref}" for fk, ref in foreign_keys)
        prompt = f"""Given the following relation ships between two tables denoted as `Table1 --(Relation Name)--> Table2`
{lines}

Reply with a json object containing RDF predicates describing the relationship between those tables.
Forward should contain predicates describing the relation going out from Table1
Backward should contain predicates describing the relation going out from Table2

Example:
Teacher --(fk_id_student)--> Student
School --(fk_id_bezirk)--> District

Response:
{{
    "Forward": {{
        "fk_id_lehrer": "hasStudent",
        "fk_id_bezirk": "isInDistrict"
    }},
    "Backward": {{
        "fk_id_lehrer": "hasTeacher",
        "fk_id_bezirk": "hasSchool"
    }}
}}
"""
        raw = self._ai.request_json(prompt, dict)
        return AiMagic.ForeignKeyPredicateNaming(
            forward=raw.get("Forward", raw.get("forward", {})),
            backward=raw.get("Backward", raw.get("backward", {})),
        )

    # ------------------------------------------------------------------
    # Many-to-many relationship names
    # ------------------------------------------------------------------

    @dataclass
    class ManyToManyMapping:
        forward: str
        backward: str

    def get_many_to_many_names(
        self, table1: str, table2: str, middle_table: str, fk1: str, fk2: str
    ) -> "AiMagic.ManyToManyMapping":
        prompt = f"""Given the following names of two many to many related tables: {table1}, {table2}
As well as the name of the inbetween table: {middle_table}
And the foreign key names: {fk1}, {fk2}

Reply with a json object containing RDF predicates describing the relationship between the many to many tables

Example:
Student, Teacher
StudentTeacher
fk_id_student, fk_id_teacher
Reply:
{{
    "Forward": "hasTeacher",
    "Backward": "hasStudent"
}}
"""
        raw = self._ai.request_json(prompt, dict)
        return AiMagic.ManyToManyMapping(
            forward=raw.get("Forward", raw.get("forward", "relatesTo")),
            backward=raw.get("Backward", raw.get("backward", "relatedBy")),
        )

    # ------------------------------------------------------------------
    # Guess foreign keys (when none declared)
    # ------------------------------------------------------------------

    def guess_foreign_keys(self, source) -> list[dict]:
        """Ask the LLM to infer foreign keys from column names."""
        lines = []
        for schema in source.schemas:
            lines.append(f"{schema.name}:")
            for table in schema.tables:
                pk_cols = {c.name for c in table.key_columns}
                col_parts = []
                for c in table.columns:
                    col_parts.append(f"*{c.name}" if c.name in pk_cols else c.name)
                lines.append(f"\t- {table.name} ({', '.join(col_parts)})")
            lines.append("")

        schema_text = "\n".join(lines)
        prompt = f"""Given the following Schemas, Tables and Column names, where Primarykey columns are prefixed by an asterisk,
take an educated guess based off the names which tables are related by a foreign key.
Don't explain anything, just respond in a json format defining all keys.

{schema_text}

Example:
Schema1:
\t- Student (*Id, Name, SchoolDistrictId, SchoolId)

Schema2:
\t- School (*DistrictId, *SchoolId, Name)

```json
[
\t{{
\t\t"Name": "fk_student_school",
\t\t"FromSchema": "Schema1",
\t\t"FromTable": "Student",
\t\t"ReferencedTable": "School",
\t\t"ReferencedSchema": "Schema2",
\t\t"References": [
\t\t\t{{"SourceColumn": "SchoolDistrictId", "TargetColumn": "DistrictId"}},
\t\t\t{{"SourceColumn": "SchoolId", "TargetColumn": "SchoolId"}}
\t\t]
\t}}
]
```
"""
        return self._ai.request_json(prompt, list)
