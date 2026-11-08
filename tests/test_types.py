"""Payload codecs — the only place bytes become values and back."""
from __future__ import annotations

import pytest

from bteng_mqtt.exceptions import PayloadConversionError
from bteng_mqtt.types import MqttPayloadType as T
from bteng_mqtt.types import coerce, decode, encode, extract_path


class TestDecode:
    def test_string(self):
        assert decode(b"hello", T.STRING) == "hello"

    def test_string_keeps_whitespace(self):
        # A payload's exact bytes can matter; only numeric/bool parsing strips.
        assert decode(b" padded \n", T.STRING) == " padded \n"

    def test_int(self):
        assert decode(b"42", T.INT) == 42
        assert decode(b" -7 ", T.INT) == -7

    def test_int_accepts_integral_float(self):
        assert decode(b"7.0", T.INT) == 7

    def test_int_rejects_fraction(self):
        with pytest.raises(PayloadConversionError, match="fraction"):
            decode(b"7.5", T.INT)

    def test_float(self):
        assert decode(b"1.5", T.FLOAT) == 1.5
        assert decode(b"3", T.FLOAT) == 3.0

    @pytest.mark.parametrize("payload", [b"true", b"TRUE", b"1", b"on", b"yes", b" True "])
    def test_bool_true_words(self, payload):
        assert decode(payload, T.BOOL) is True

    @pytest.mark.parametrize("payload", [b"false", b"FALSE", b"0", b"off", b"no"])
    def test_bool_false_words(self, payload):
        assert decode(payload, T.BOOL) is False

    def test_bool_rejects_anything_else(self):
        with pytest.raises(PayloadConversionError, match="as bool"):
            decode(b"maybe", T.BOOL)

    def test_json_object(self):
        assert decode(b'{"a": 1}', T.JSON) == {"a": 1}

    def test_json_path(self):
        payload = b'{"status": {"mode": "running", "axes": [1, 2, 3]}}'
        assert decode(payload, T.JSON, json_path="status.mode") == "running"
        assert decode(payload, T.JSON, json_path="status.axes.1") == 2

    def test_json_path_missing_key_is_an_error(self):
        with pytest.raises(PayloadConversionError, match="not in"):
            decode(b'{"status": {}}', T.JSON, json_path="status.mode")

    def test_invalid_json(self):
        with pytest.raises(PayloadConversionError, match="valid JSON"):
            decode(b"{oops", T.JSON)

    def test_bytes_passthrough(self):
        assert decode(b"\xff\xfe", T.BYTES) == b"\xff\xfe"

    def test_non_utf8_payload_for_a_text_type(self):
        with pytest.raises(PayloadConversionError, match="valid utf-8"):
            decode(b"\xff\xfe", T.STRING)

    def test_int_rejects_words(self):
        with pytest.raises(PayloadConversionError, match="as int"):
            decode(b"twelve", T.INT)


class TestEncode:
    def test_string(self):
        assert encode("hello", T.STRING) == b"hello"

    def test_int(self):
        assert encode(42, T.INT) == b"42"
        assert encode("42", T.INT) == b"42"

    def test_float(self):
        assert encode(1.5, T.FLOAT) == b"1.5"
        assert encode("1.5", T.FLOAT) == b"1.5"

    def test_bool(self):
        assert encode(True, T.BOOL) == b"true"
        assert encode("yes", T.BOOL) == b"true"
        assert encode("off", T.BOOL) == b"false"

    def test_json_from_object(self):
        assert encode({"cmd": "stop"}, T.JSON) == b'{"cmd": "stop"}'

    def test_json_string_passes_through(self):
        # This is what a registry value_map entry holds — it must not be
        # double-encoded into a JSON string containing JSON.
        assert encode('{"cmd": "start"}', T.JSON) == b'{"cmd": "start"}'

    def test_json_bare_string_is_serialised(self):
        assert encode("start", T.JSON) == b'"start"'

    def test_json_rejects_unserialisable(self):
        with pytest.raises(PayloadConversionError, match="cannot serialise"):
            encode(object(), T.JSON)

    def test_bytes(self):
        assert encode(b"\x01\x02", T.BYTES) == b"\x01\x02"


class TestRoundTrip:
    @pytest.mark.parametrize(
        ("value", "payload_type"),
        [
            ("text", T.STRING),
            (42, T.INT),
            (-1, T.INT),
            (1.5, T.FLOAT),
            (True, T.BOOL),
            (False, T.BOOL),
            ({"a": [1, 2]}, T.JSON),
            (b"\x00\x01", T.BYTES),
        ],
    )
    def test_encode_then_decode_is_identity(self, value, payload_type):
        assert decode(encode(value, payload_type), payload_type) == value


class TestCoerce:
    def test_xml_attribute_strings_become_typed(self):
        # Tree XML gives every attribute as a string; comparisons need the type.
        assert coerce("42", T.INT) == 42
        assert coerce("1.5", T.FLOAT) == 1.5
        assert coerce("true", T.BOOL) is True

    def test_already_typed_values_pass_through(self):
        assert coerce(42, T.INT) == 42
        assert coerce(True, T.BOOL) is True
        assert coerce({"a": 1}, T.JSON) == {"a": 1}

    def test_json_string_is_parsed_when_it_can_be(self):
        assert coerce('{"a": 1}', T.JSON) == {"a": 1}

    def test_json_plain_scalar_stays_a_string(self):
        assert coerce("running", T.JSON) == "running"

    def test_bool_from_number(self):
        assert coerce(1, T.BOOL) is True
        assert coerce(0, T.BOOL) is False


class TestExtractPath:
    def test_nested_dicts_and_lists(self):
        data = {"a": {"b": [{"c": 5}]}}
        assert extract_path(data, "a.b.0.c") == 5

    def test_index_out_of_range(self):
        with pytest.raises(PayloadConversionError, match="valid index"):
            extract_path({"a": [1]}, "a.3")

    def test_cannot_descend_into_scalar(self):
        with pytest.raises(PayloadConversionError, match="cannot descend"):
            extract_path({"a": 1}, "a.b")
