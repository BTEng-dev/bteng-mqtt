"""In-process MQTT backend for tests — no broker, no paho, no network.

It behaves like a tiny broker shared by every client it creates: publishes are
routed to matching subscriptions, retained messages are stored and replayed on
subscribe, and test code can inject broker-side traffic with :meth:`inject`.

Shipped in the package (not just in ``tests/``) so consuming projects can check
their trees offline too.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from bteng_mqtt.backend.base import BackendCallbacks, MqttBackend, PublishTicket
from bteng_mqtt.exceptions import ConnectionError
from bteng_mqtt.registry import BrokerConfig
from bteng_mqtt.topics import topic_matches


@dataclass
class MockTicket:
    """Publish ticket that is complete the moment it is created."""
    rc: int = 0

    def is_published(self) -> bool:
        return True


@dataclass
class MockClient:
    """One connection to the mock broker."""
    config: BrokerConfig
    callbacks: BackendCallbacks
    connected: bool = True
    subscriptions: dict[str, int] = field(default_factory=dict)

    def matches(self, topic: str) -> bool:
        return any(topic_matches(f, topic) for f in self.subscriptions)


@dataclass
class PublishRecord:
    """What a test asserts on after a tree publishes something."""
    topic:   str
    payload: bytes
    qos:     int
    retain:  bool


class MockBackend(MqttBackend):
    """Deterministic stand-in for a broker.

    Parameters let a test drive the failure paths that a real broker would
    otherwise have to be misconfigured to produce:

    ``connect_rc``
        CONNACK reason code; non-zero makes the manager raise ``ConnectionError``.
    ``raise_on_connect``
        Exception instance raised synchronously from :meth:`connect`.
    ``auto_connack``
        When False no CONNACK is delivered, so the manager's connect times out.
        Call :meth:`send_connack` to release it.
    """

    def __init__(
        self,
        connect_rc: int = 0,
        raise_on_connect: Exception | None = None,
        auto_connack: bool = True,
    ) -> None:
        self.connect_rc = connect_rc
        self.raise_on_connect = raise_on_connect
        self.auto_connack = auto_connack
        self.clients: list[MockClient] = []
        self.retained: dict[str, PublishRecord] = {}
        self.published: list[PublishRecord] = []

    # ── MqttBackend ───────────────────────────────────────────────────────────

    def connect(self, config: BrokerConfig, callbacks: BackendCallbacks) -> Any:
        if self.raise_on_connect is not None:
            raise self.raise_on_connect
        client = MockClient(config=config, callbacks=callbacks)
        self.clients.append(client)
        if self.auto_connack:
            self.send_connack(client)
        return client

    def disconnect(self, client: Any) -> None:
        client.connected = False
        client.subscriptions.clear()
        if client in self.clients:
            self.clients.remove(client)

    def publish(
        self,
        client: Any,
        topic: str,
        payload: bytes,
        qos: int = 0,
        retain: bool = False,
    ) -> PublishTicket:
        if not client.connected:
            raise ConnectionError("mock client is not connected")
        record = PublishRecord(topic=topic, payload=bytes(payload), qos=qos, retain=retain)
        self.published.append(record)
        self._store_retained(record)
        self._route(record)
        return MockTicket()

    def subscribe(self, client: Any, topic_filter: str, qos: int = 0) -> None:
        client.subscriptions[topic_filter] = qos
        # A real broker replays retained messages on subscribe; trees rely on it.
        for record in list(self.retained.values()):
            if topic_matches(topic_filter, record.topic):
                self._deliver(client, record, retain=True)

    def unsubscribe(self, client: Any, topic_filter: str) -> None:
        client.subscriptions.pop(topic_filter, None)

    def is_connected(self, client: Any) -> bool:
        return bool(client.connected)

    # ── Test helpers ──────────────────────────────────────────────────────────

    def inject(self, topic: str, payload: bytes | str, qos: int = 0,
               retain: bool = False) -> None:
        """Publish as if a third party did — the broker side of the wire."""
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        record = PublishRecord(topic=topic, payload=bytes(payload), qos=qos, retain=retain)
        self._store_retained(record)
        self._route(record)

    def send_connack(self, client: MockClient, rc: int | None = None) -> None:
        """Deliver a CONNACK to one client (used when ``auto_connack`` is off)."""
        code = self.connect_rc if rc is None else rc
        if client.callbacks.on_connect is not None:
            client.callbacks.on_connect(code)

    def drop(self, client: MockClient, rc: int = 1) -> None:
        """Simulate the broker dropping the connection."""
        client.connected = False
        if client.callbacks.on_disconnect is not None:
            client.callbacks.on_disconnect(rc)

    def payloads_for(self, topic: str) -> list[bytes]:
        """Every payload published to a topic, oldest first."""
        return [r.payload for r in self.published if r.topic == topic]

    def reset(self) -> None:
        self.clients.clear()
        self.retained.clear()
        self.published.clear()

    # ── Internal ──────────────────────────────────────────────────────────────

    def _store_retained(self, record: PublishRecord) -> None:
        if not record.retain:
            return
        if record.payload == b"":
            self.retained.pop(record.topic, None)  # empty retained payload clears it
        else:
            self.retained[record.topic] = record

    def _route(self, record: PublishRecord) -> None:
        for client in list(self.clients):
            if client.connected and client.matches(record.topic):
                self._deliver(client, record, retain=False)

    @staticmethod
    def _deliver(client: MockClient, record: PublishRecord, retain: bool) -> None:
        client.callbacks.on_message(record.topic, record.payload, record.qos, retain)
