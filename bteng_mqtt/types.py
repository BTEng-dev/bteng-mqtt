"""MQTT payload ↔ Python type system.

MQTT payloads are opaque bytes, so the *registry* declares what a topic carries
and this module is the only place that turns bytes into Python values and back.
Trees therefore never parse anything.
"""
from __future__ import annotations

import json
from enum import Enum
from typing import Any

from bteng_mqtt.exceptions import PayloadConversionError

DEFAULT_ENCODING = "utf-8"

_TRUE_WORDS = frozenset({"true", "1", "on", "yes"})
_FALSE_WORDS = frozenset({"false", "0", "off", "no"})


class MqttPayloadType(str, Enum):
    STRING = "string"
    INT    = "int"
    FLOAT  = "float"
    BOOL   = "bool"
    JSON   = "json"
    BYTES  = "bytes"


def _to_text(payload: bytes | bytearray | str) -> str:
    """Decode a payload to text, or fail with a message that names the payload."""
    if isinstance(payload, str):
        return payload
    try:
        return bytes(payload).decode(DEFAULT_ENCODING)
    except UnicodeDecodeError as exc:
        raise PayloadConversionError(
            f"payload is not valid {DEFAULT_ENCODING} text ({len(payload)} bytes): {exc}"
        ) from exc


def _to_bool(text: str) -> bool:
    word = text.strip().lower()
    if word in _TRUE_WORDS:
        return True
    if word in _FALSE_WORDS:
        return False
    raise PayloadConversionError(
        f"cannot read {text!r} as bool — expected one of "
        f"{sorted(_TRUE_WORDS | _FALSE_WORDS)}"
    )


def _to_int(text: str) -> int:
    stripped = text.strip()
    try:
        return int(stripped)
    except ValueError:
        pass
    # Accept an integral float ("7.0") — brokers and JSON producers emit those.
    try:
        number = float(stripped)
    except ValueError as exc:
        raise PayloadConversionError(f"cannot read {text!r} as int") from exc
    if number != int(number):
        raise PayloadConversionError(f"cannot read {text!r} as int — it has a fraction")
    return int(number)


def _to_float(text: str) -> float:
    try:
        return float(text.strip())
    except ValueError as exc:
        raise PayloadConversionError(f"cannot read {text!r} as float") from exc


def extract_path(data: Any, path: str) -> Any:
    """Return the value at a dotted ``path`` inside decoded JSON.

    Mapping keys and list indices both work: ``status.axes.0`` reads
    ``data["status"]["axes"][0]``.  A missing key is an error, not ``None`` — a
    silently absent field would look like a legitimate value to a tree.
    """
    current = data
    for part in path.split("."):
        if isinstance(current, dict):
            if part not in current:
                raise PayloadConversionError(
                    f"json_path {path!r}: key {part!r} not in {sorted(current)}"
                )
            current = current[part]
        elif isinstance(current, list):
            try:
                current = current[int(part)]
            except (ValueError, IndexError) as exc:
                raise PayloadConversionError(
                    f"json_path {path!r}: {part!r} is not a valid index into a "
                    f"{len(current)}-element list"
                ) from exc
        else:
            raise PayloadConversionError(
                f"json_path {path!r}: cannot descend into {type(current).__name__}"
            )
    return current


def decode(
    payload: bytes | bytearray | str,
    payload_type: MqttPayloadType,
    json_path: str | None = None,
) -> Any:
    """Turn a received payload into a Python value of the declared type.

    ``json_path`` applies to ``json`` payloads only and is ignored otherwise
    (the registry rejects the combination at load time).
    """
    if payload_type is MqttPayloadType.BYTES:
        return bytes(payload) if not isinstance(payload, str) else payload.encode(DEFAULT_ENCODING)

    text = _to_text(payload)

    if payload_type is MqttPayloadType.STRING:
        return text
    if payload_type is MqttPayloadType.BOOL:
        return _to_bool(text)
    if payload_type is MqttPayloadType.INT:
        return _to_int(text)
    if payload_type is MqttPayloadType.FLOAT:
        return _to_float(text)
    if payload_type is MqttPayloadType.JSON:
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise PayloadConversionError(f"payload is not valid JSON: {exc}") from exc
        return extract_path(data, json_path) if json_path else data

    raise PayloadConversionError(f"no decoder for payload type {payload_type!r}")


def encode(value: Any, payload_type: MqttPayloadType) -> bytes:
    """Turn a Python value into the bytes to publish.

    For ``json``, a string that already parses as JSON is passed through
    unchanged — that is what a registry ``value_map`` entry holds — while any
    other value is serialised.
    """
    if payload_type is MqttPayloadType.BYTES:
        if isinstance(value, (bytes, bytearray)):
            return bytes(value)
        return str(value).encode(DEFAULT_ENCODING)

    if payload_type is MqttPayloadType.JSON:
        if isinstance(value, (bytes, bytearray)):
            value = _to_text(value)
        if isinstance(value, str):
            try:
                json.loads(value)
            except json.JSONDecodeError:
                pass  # a bare string — serialise it below
            else:
                return value.encode(DEFAULT_ENCODING)
        try:
            return json.dumps(value).encode(DEFAULT_ENCODING)
        except (TypeError, ValueError) as exc:
            raise PayloadConversionError(f"cannot serialise {value!r} as JSON: {exc}") from exc

    if payload_type is MqttPayloadType.BOOL:
        return b"true" if coerce(value, payload_type) else b"false"
    if payload_type is MqttPayloadType.INT:
        return str(coerce(value, payload_type)).encode(DEFAULT_ENCODING)
    if payload_type is MqttPayloadType.FLOAT:
        return repr(coerce(value, payload_type)).encode(DEFAULT_ENCODING)
    if payload_type is MqttPayloadType.STRING:
        if isinstance(value, (bytes, bytearray)):
            return bytes(value)
        return str(value).encode(DEFAULT_ENCODING)

    raise PayloadConversionError(f"no encoder for payload type {payload_type!r}")


def coerce(value: Any, payload_type: MqttPayloadType) -> Any:
    """Normalise a Python value to the declared type.

    Tree XML attributes arrive as strings, so an ``expected_value="42"`` has to
    become ``42`` before it is compared with a decoded payload.  Values that are
    already of the right type pass straight through.
    """
    if payload_type is MqttPayloadType.BYTES:
        if isinstance(value, (bytes, bytearray)):
            return bytes(value)
        return str(value).encode(DEFAULT_ENCODING)

    if payload_type is MqttPayloadType.BOOL:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and value in (0, 1):
            return bool(value)
        return _to_bool(_to_text(value) if not isinstance(value, str) else value)

    if payload_type is MqttPayloadType.INT:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return _to_int(repr(value))
        return _to_int(_to_text(value))

    if payload_type is MqttPayloadType.FLOAT:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        return _to_float(_to_text(value))

    if payload_type is MqttPayloadType.STRING:
        if isinstance(value, (bytes, bytearray)):
            return _to_text(value)
        return value if isinstance(value, str) else str(value)

    if payload_type is MqttPayloadType.JSON:
        if isinstance(value, (bytes, bytearray, str)):
            text = _to_text(value)
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return text  # a plain scalar compared against a JSON string field
        return value

    raise PayloadConversionError(f"no coercion for payload type {payload_type!r}")
