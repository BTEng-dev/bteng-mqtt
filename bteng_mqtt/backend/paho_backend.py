"""paho-mqtt backend (callback API v2).

paho is imported lazily inside :meth:`PahoBackend.connect` so that importing
``bteng_mqtt`` — loading a registry, building a tree, running the mock backend —
works on a machine without paho installed.
"""
from __future__ import annotations

import logging
from typing import Any, cast

from bteng_mqtt.backend.base import BackendCallbacks, MqttBackend, PublishTicket
from bteng_mqtt.exceptions import ConnectionError, PublishError, SubscriptionError
from bteng_mqtt.registry import BrokerConfig

logger = logging.getLogger(__name__)

_PROTOCOL_NAMES = {
    "mqtt31":  "MQTTv31",
    "mqtt311": "MQTTv311",
    "mqtt5":   "MQTTv5",
}


class PahoBackend(MqttBackend):
    """MQTT I/O through paho-mqtt's threaded network loop."""

    def connect(self, config: BrokerConfig, callbacks: BackendCallbacks) -> Any:
        mqtt = _import_paho()

        protocol = getattr(mqtt, _PROTOCOL_NAMES[config.protocol])
        kwargs: dict[str, Any] = {
            "client_id": config.client_id,
            "protocol": protocol,
        }
        # clean_session is an MQTT 3 concept; v5 uses clean_start at CONNECT time
        # and paho rejects the argument outright.
        if config.protocol != "mqtt5":
            kwargs["clean_session"] = config.clean_session

        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, **kwargs)

        if config.username:
            client.username_pw_set(config.username, config.password or None)

        if config.tls is not None:
            try:
                client.tls_set(
                    ca_certs=config.tls.ca_certs or None,
                    certfile=config.tls.certfile or None,
                    keyfile=config.tls.keyfile or None,
                )
                if config.tls.insecure:
                    client.tls_insecure_set(True)
            except Exception as exc:  # bad paths, unreadable key, ...
                raise ConnectionError(f"TLS setup failed for {config.address}: {exc}") from exc

        if config.will is not None:
            client.will_set(
                config.will.topic,
                payload=config.will.payload.encode("utf-8"),
                qos=config.will.qos,
                retain=config.will.retain,
            )

        client.on_connect = _make_on_connect(callbacks)
        client.on_message = _make_on_message(callbacks)
        client.on_disconnect = _make_on_disconnect(callbacks)

        try:
            # connect_async + loop_start keeps this call non-blocking; the CONNACK
            # arrives on paho's network thread and reaches callbacks.on_connect.
            client.connect_async(config.host, config.port, keepalive=config.keepalive)
            client.loop_start()
        except Exception as exc:
            raise ConnectionError(f"Cannot reach broker {config.address}: {exc}") from exc
        return client

    def disconnect(self, client: Any) -> None:
        try:
            client.disconnect()
        finally:
            client.loop_stop()

    def publish(
        self,
        client: Any,
        topic: str,
        payload: bytes,
        qos: int = 0,
        retain: bool = False,
    ) -> PublishTicket:
        info = client.publish(topic, payload=payload, qos=qos, retain=retain)
        if info.rc != 0:
            raise PublishError(
                f"publish to {topic!r} rejected by the client (rc={info.rc}): "
                f"{_error_string(info.rc)}"
            )
        # MQTTMessageInfo satisfies PublishTicket structurally; paho is untyped,
        # so say so explicitly rather than leaking Any to the caller.
        return cast(PublishTicket, info)

    def subscribe(self, client: Any, topic_filter: str, qos: int = 0) -> None:
        result, _mid = client.subscribe(topic_filter, qos=qos)
        if result != 0:
            raise SubscriptionError(
                f"subscribe to {topic_filter!r} failed (rc={result}): {_error_string(result)}"
            )

    def unsubscribe(self, client: Any, topic_filter: str) -> None:
        result, _mid = client.unsubscribe(topic_filter)
        if result != 0:
            raise SubscriptionError(
                f"unsubscribe from {topic_filter!r} failed (rc={result}): "
                f"{_error_string(result)}"
            )

    def is_connected(self, client: Any) -> bool:
        return bool(client.is_connected())


# ── paho glue ─────────────────────────────────────────────────────────────────

def _import_paho() -> Any:
    try:
        import paho.mqtt.client as mqtt
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise ConnectionError(
            "paho-mqtt is not installed — `pip install paho-mqtt>=2.0` "
            "(or use bteng_mqtt.backend.mock.MockBackend for offline tests)"
        ) from exc
    return mqtt


def _error_string(rc: int) -> str:
    try:
        import paho.mqtt.client as mqtt
        return str(mqtt.error_string(rc))
    except Exception:  # pragma: no cover - only used to enrich a message
        return "unknown error"


def _reason_code(value: Any) -> int:
    """Normalise paho's reason code to an int.

    The v2 callback API hands over a ``ReasonCode``, which is **not**
    int-convertible — ``int(reason_code)`` raises TypeError (paho 2.1.0). Its
    numeric code is ``.value``. Some paths (and the v1 API) still pass a plain
    int, so both are handled, and an unrecognised object falls back to its
    ``is_failure`` flag rather than lying about success.
    """
    if isinstance(value, int):
        return value
    code = getattr(value, "value", None)
    if isinstance(code, int):
        return code
    return 1 if getattr(value, "is_failure", False) else 0


def _make_on_connect(callbacks: BackendCallbacks) -> Any:
    def _on_connect(_client: Any, _userdata: Any, _flags: Any, reason_code: Any,
                    _properties: Any = None) -> None:
        if callbacks.on_connect is not None:
            callbacks.on_connect(_reason_code(reason_code))
    return _on_connect


def _make_on_message(callbacks: BackendCallbacks) -> Any:
    def _on_message(_client: Any, _userdata: Any, message: Any) -> None:
        callbacks.on_message(message.topic, bytes(message.payload),
                             int(message.qos), bool(message.retain))
    return _on_message


def _make_on_disconnect(callbacks: BackendCallbacks) -> Any:
    def _on_disconnect(_client: Any, _userdata: Any, _flags: Any = None,
                       reason_code: Any = 0, _properties: Any = None) -> None:
        if callbacks.on_disconnect is not None:
            callbacks.on_disconnect(_reason_code(reason_code))
    return _on_disconnect
