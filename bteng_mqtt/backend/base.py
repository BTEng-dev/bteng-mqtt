"""Abstract backend interface for MQTT client implementations.

Every method is synchronous and returns immediately: MQTT is asynchronous by
nature, so completion is reported through the callbacks in
:class:`BackendCallbacks` (CONNACK, arriving messages, disconnects) or through a
:class:`PublishTicket` the caller polls.  Nothing here may block the behavior
tree's tick loop.

Implement this class to swap paho for another library or for
:class:`bteng_mqtt.backend.mock.MockBackend`.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from bteng_mqtt.registry import BrokerConfig

# (topic, payload, qos, retain) — as delivered by the broker.
OnMessage = Callable[[str, bytes, int, bool], None]
# Reason code: 0 means success, anything else is a refusal/failure.
OnConnect = Callable[[int], None]
OnDisconnect = Callable[[int], None]


@dataclass
class BackendCallbacks:
    """Hooks the client manager installs on a backend connection."""
    on_message:    OnMessage
    on_connect:    OnConnect | None = None
    on_disconnect: OnDisconnect | None = None


@runtime_checkable
class PublishTicket(Protocol):
    """Handle for an in-flight publish.

    paho's ``MQTTMessageInfo`` satisfies this as-is: QoS 0 is published once the
    bytes are handed to the socket, QoS 1/2 once PUBACK/PUBCOMP arrives.
    """

    rc: int

    def is_published(self) -> bool: ...


class MqttBackend(ABC):
    """Pluggable MQTT I/O layer."""

    @abstractmethod
    def connect(self, config: BrokerConfig, callbacks: BackendCallbacks) -> Any:
        """Start connecting and return an opaque client handle.

        Returns before the connection is up; ``callbacks.on_connect`` reports the
        CONNACK reason code.  Raises on a synchronous failure (bad TLS files,
        unresolvable host).
        """

    @abstractmethod
    def disconnect(self, client: Any) -> None:
        """Send DISCONNECT and stop the network loop. Safe to call twice."""

    @abstractmethod
    def publish(
        self,
        client: Any,
        topic: str,
        payload: bytes,
        qos: int = 0,
        retain: bool = False,
    ) -> PublishTicket:
        """Queue a publish and return a ticket the caller polls."""

    @abstractmethod
    def subscribe(self, client: Any, topic_filter: str, qos: int = 0) -> None:
        """Subscribe the connection to a topic filter."""

    @abstractmethod
    def unsubscribe(self, client: Any, topic_filter: str) -> None:
        """Drop a subscription."""

    @abstractmethod
    def is_connected(self, client: Any) -> bool:
        """True while the transport is up."""
