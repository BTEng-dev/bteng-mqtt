"""PahoBackend glue — the parts that do not need a broker.

paho's v2 callback API is easy to get subtly wrong, and getting it wrong shows up
only against a live broker, so the conversions are pinned here.
"""
from __future__ import annotations

import pytest

from bteng_mqtt.backend.base import BackendCallbacks
from bteng_mqtt.backend.paho_backend import (
    PahoBackend,
    _make_on_connect,
    _make_on_disconnect,
    _make_on_message,
    _reason_code,
)
from bteng_mqtt.exceptions import ConnectionError
from bteng_mqtt.registry import BrokerConfig

mqtt = pytest.importorskip("paho.mqtt.client", reason="paho-mqtt not installed")
from paho.mqtt.reasoncodes import ReasonCode  # noqa: E402

CONNACK = mqtt.CONNACK >> 4


class TestReasonCode:
    def test_plain_int_passes_through(self):
        assert _reason_code(0) == 0
        assert _reason_code(5) == 5

    def test_paho_v2_reason_code_uses_value(self):
        """int(ReasonCode) raises TypeError in paho 2.x — .value is the number."""
        with pytest.raises(TypeError):
            int(ReasonCode(CONNACK, identifier=0))
        assert _reason_code(ReasonCode(CONNACK, identifier=0)) == 0
        assert _reason_code(ReasonCode(CONNACK, identifier=135)) == 135

    def test_success_and_failure_are_distinguishable(self):
        assert _reason_code(ReasonCode(CONNACK, identifier=0)) == 0
        assert _reason_code(ReasonCode(CONNACK, identifier=135)) != 0

    def test_unknown_object_falls_back_to_is_failure(self):
        class Weird:
            is_failure = True

        assert _reason_code(Weird()) == 1

        class WeirdOk:
            is_failure = False

        assert _reason_code(WeirdOk()) == 0


class TestCallbackAdapters:
    def test_on_connect_forwards_the_numeric_code(self):
        seen = []
        cb = _make_on_connect(BackendCallbacks(on_message=lambda *a: None,
                                              on_connect=seen.append))
        cb(None, None, None, ReasonCode(CONNACK, identifier=135), None)
        assert seen == [135]

    def test_on_message_unpacks_the_message(self):
        seen = []

        class Message:
            topic = "a/b"
            payload = bytearray(b"hi")
            qos = 1
            retain = True

        cb = _make_on_message(BackendCallbacks(
            on_message=lambda *args: seen.append(args)))
        cb(None, None, Message())
        assert seen == [("a/b", b"hi", 1, True)]

    def test_on_disconnect_forwards_the_code(self):
        seen = []
        cb = _make_on_disconnect(BackendCallbacks(on_message=lambda *a: None,
                                                 on_disconnect=seen.append))
        cb(None, None, None, ReasonCode(mqtt.DISCONNECT >> 4, identifier=0), None)
        assert seen == [0]


class TestConnectValidation:
    """Only synchronous failures surface from connect().

    An unreachable host does not: connect_async() defers DNS and the socket to
    paho's network thread, so a bad host shows up as the manager's CONNACK
    timeout instead. Configuration errors are the ones that raise here.
    """

    def test_bad_tls_paths_raise_our_connection_error(self):
        from bteng_mqtt.registry import TlsConfig

        backend = PahoBackend()
        config = BrokerConfig(host="127.0.0.1", tls=TlsConfig(ca_certs="/nope/ca.pem"))
        with pytest.raises(ConnectionError, match="TLS setup failed"):
            backend.connect(config, BackendCallbacks(on_message=lambda *a: None))
