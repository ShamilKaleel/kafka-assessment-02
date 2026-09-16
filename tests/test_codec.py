"""Avro schema and serialization checks (no Kafka needed)."""

import pytest

from src import avro_codec
from src.config import SCHEMA_PATH


def test_schema_matches_the_assignment():
    assert SCHEMA_PATH.name == "order.avsc"
    schema = avro_codec.SCHEMA
    assert schema["name"] == "Order"
    fields = {f["name"]: f["type"] for f in schema["fields"]}
    assert fields == {"orderId": "string", "product": "string", "price": "float"}


def test_encode_decode_round_trip():
    order = {"orderId": "1001", "product": "Item1", "price": 123.45}
    decoded = avro_codec.decode(avro_codec.encode(order))
    assert decoded["orderId"] == "1001"
    assert decoded["product"] == "Item1"
    # Avro "float" is 32-bit, so the value comes back within float32 precision
    assert decoded["price"] == pytest.approx(123.45, abs=1e-4)


def test_encoded_message_is_compact_binary():
    data = avro_codec.encode({"orderId": "1001", "product": "Item1", "price": 10.0})
    assert isinstance(data, bytes)
    assert b"orderId" not in data          # no field names in the payload, just values


def test_decoding_garbage_raises():
    with pytest.raises(Exception):
        avro_codec.decode(b"this is not an avro order")
