"""
TurtleWriter – thin wrapper around rdflib that provides the same
write-once-per-triple interface used by both converters.

The writer accumulates triples in an rdflib.Graph and serialises
to Turtle on close / context-manager exit.
"""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import rdflib
from rdflib import Graph, Literal, Namespace, URIRef, XSD
from rdflib.namespace import RDF


def _safe_iri(value: str) -> str:
    """Percent-encode characters that are not valid in IRI path segments."""
    return quote(value, safe=":/#?=&+@!$'()*,;~")


def _make_uri(base: str, *parts: str) -> URIRef:
    """Append *parts* to *base*, ensuring a single slash between each."""
    result = base.rstrip("/")
    for part in parts:
        result = result.rstrip("/") + "/" + _safe_iri(part).lstrip("/")
    return URIRef(result)


class SubjectWriter:
    """
    Collects triples for a single RDF subject.
    Closed via TurtleWriter.end_subject().
    """

    def __init__(self, graph: Graph, subject: URIRef) -> None:
        self._g = graph
        self.subject = subject

    def write_type(self, type_iri: URIRef) -> None:
        self._g.add((self.subject, RDF.type, type_iri))

    def write(self, predicate: URIRef, obj: URIRef | Literal) -> None:
        self._g.add((self.subject, predicate, obj))

    def write_literal(self, predicate: URIRef, value: str) -> None:
        self._g.add((self.subject, predicate, Literal(value)))

    def write_raw(self, predicate: URIRef, value: str) -> None:
        """Write *value* as an unquoted literal (boolean / numeric)."""
        # Detect type for cleaner Turtle output
        lit = _make_raw_literal(value)
        self._g.add((self.subject, predicate, lit))

    def write_iri(self, predicate: URIRef, obj_iri: URIRef) -> None:
        self._g.add((self.subject, predicate, obj_iri))

    def write_list(self, predicate: URIRef, items: list) -> None:
        """Write an rdf:List for *items*."""
        if not items:
            self._g.add((self.subject, predicate, RDF.nil))
            return
        collection = rdflib.collection.Collection(self._g, None, items)
        self._g.add((self.subject, predicate, collection.uri))


def _make_raw_literal(value: str) -> Literal:
    v = value.strip().lower()
    if v in ("true", "false"):
        return Literal(v == "true", datatype=XSD.boolean)
    try:
        int(value)
        return Literal(int(value), datatype=XSD.integer)
    except ValueError:
        pass
    try:
        float(value)
        return Literal(float(value), datatype=XSD.decimal)
    except ValueError:
        pass
    return Literal(value)


class TurtleWriter:
    """
    Accumulates RDF triples and serialises them to a Turtle file.

    Usage::

        with TurtleWriter(path) as w:
            sub = w.begin_subject(iri)
            sub.write_type(some_type)
            sub.write_literal(pred, "hello")
            w.end_subject(sub)
            w.write_triple(s, p, o)
    """

    def __init__(self, output_path: str | Path) -> None:
        self._path = Path(output_path)
        self._g = Graph()

    # ------------------------------------------------------------------
    # Subject helpers
    # ------------------------------------------------------------------

    def begin_subject(self, iri: URIRef) -> SubjectWriter:
        return SubjectWriter(self._g, iri)

    def end_subject(self, subject: SubjectWriter) -> None:
        pass  # triples already written into graph

    # ------------------------------------------------------------------
    # Direct triple writes
    # ------------------------------------------------------------------

    def write_triple(self, s: URIRef, p: URIRef, o) -> None:
        self._g.add((s, p, o))

    # ------------------------------------------------------------------
    # Namespace binding helpers
    # ------------------------------------------------------------------

    def bind(self, prefix: str, namespace: str) -> None:
        self._g.bind(prefix, Namespace(namespace))

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._g.serialize(str(self._path), format="turtle")

    def __enter__(self) -> "TurtleWriter":
        return self

    def __exit__(self, *_) -> None:
        self.save()

    # ------------------------------------------------------------------
    # Convenience factories
    # ------------------------------------------------------------------

    @staticmethod
    def make_uri(base: str, *parts: str) -> URIRef:
        return _make_uri(base, *parts)
