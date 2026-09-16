"""Avro serialization of order messages using the shared schemas/order.avsc.

Both producer and consumer use this same schema file, so they agree on the
byte format without needing a schema registry.
"""

import io

from fastavro import schemaless_reader, schemaless_writer
from fastavro.schema import load_schema

from src.config import SCHEMA_PATH

SCHEMA = load_schema(str(SCHEMA_PATH))


def encode(order: dict) -> bytes:
    buf = io.BytesIO()
    schemaless_writer(buf, SCHEMA, order)
    return buf.getvalue()


def decode(data: bytes) -> dict:
    return schemaless_reader(io.BytesIO(data), SCHEMA)
