"""MqttConnect — open a named broker connection."""
from __future__ import annotations

import concurrent.futures
import logging

from bteng.core.node import InputPort, NodeConfig, NodeStatus, PortDefinition
from bteng.nodes.leaf.stateful_action import StatefulActionNode

from bteng_mqtt.nodes.base import MqttNodeMixin
from bteng_mqtt.registry import BrokerConfig, TlsConfig, WillConfig, get_global_registry

logger = logging.getLogger(__name__)


class MqttConnect(MqttNodeMixin, StatefulActionNode):
    """Connect to an MQTT broker and register the connection under ``client_id``.

    Broker settings come from the registry's ``broker:`` section, so the usual
    tree line is just ``<MqttConnect client_id="main" />``.  Any port given in
    the XML overrides that section for this node only.

    SUCCESS immediately if the client id is already connected, RUNNING during
    the handshake, FAILURE if the broker refuses or never sends a CONNACK.
    """

    def __init__(self, name: str, config: NodeConfig | None = None) -> None:
        super().__init__(name, config)
        self._future: concurrent.futures.Future | None = None
        self._client_id: str = ""
        self._address: str = ""

    @classmethod
    def provided_ports(cls) -> list[PortDefinition]:
        return [
            InputPort("client_id",      "Name this connection is registered under", default="default"),
            InputPort("host",           "Broker host (overrides the registry)",     default=""),
            InputPort("port",           "Broker port",                              default=None),
            InputPort("keepalive",      "Keepalive interval in seconds",            default=None),
            InputPort("username",       "Broker username",                          default=""),
            InputPort("password",       "Broker password",                          default=""),
            InputPort("mqtt_client_id", "MQTT-level client identifier",             default=""),
            InputPort("clean_session",  "Start a clean session (MQTT 3 only)",      default=None),
            InputPort("protocol",       "mqtt311 | mqtt31 | mqtt5",                 default=""),
            InputPort("timeout",        "Seconds to wait for CONNACK",              default=None),
            InputPort("tls",            "Enable TLS with the system CA store",      default=None),
            InputPort("will_topic",     "Last-will topic",                          default=""),
            InputPort("will_payload",   "Last-will payload",                        default=""),
            InputPort("will_retain",    "Retain the last will",                     default=None),
        ]

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def on_start(self) -> NodeStatus:
        self._client_id = self.resolve_client_id()
        manager = self.manager

        if manager.is_connected(self._client_id):
            self.set_feedback_message(f"Client '{self._client_id}' already connected")
            logger.debug("client %r already connected", self._client_id)
            return NodeStatus.SUCCESS

        try:
            config = self._broker_config()
        except ValueError as exc:
            self.set_feedback_message(str(exc))
            return NodeStatus.FAILURE

        self._address = config.address
        logger.info("connecting client %r to %s", self._client_id, self._address)
        self._future = manager.connect_async(self._client_id, config)
        return NodeStatus.RUNNING

    def on_running(self) -> NodeStatus:
        if not self._future.done():
            return NodeStatus.RUNNING
        try:
            self._future.result()
        except Exception as exc:
            self.set_feedback_message(str(exc))
            logger.warning("client %r connect failed: %s", self._client_id, exc)
            return NodeStatus.FAILURE
        self.set_feedback_message(f"Client '{self._client_id}' connected to {self._address}")
        logger.info("client %r connected", self._client_id)
        return NodeStatus.SUCCESS

    def on_halted(self) -> None:
        if self._future is not None and not self._future.done():
            self._future.cancel()
        self._future = None

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _broker_config(self) -> BrokerConfig:
        """Registry ``broker:`` section, with this node's ports layered on top."""
        registry = self._config.params.get("registry") or get_global_registry()
        base = registry.broker or BrokerConfig()

        host = str(self.get_input("host", "") or "").strip() or base.host
        protocol = str(self.get_input("protocol", "") or "").strip() or base.protocol
        mqtt_client_id = str(self.get_input("mqtt_client_id", "") or "") or base.client_id
        username = str(self.get_input("username", "") or "") or base.username
        password = str(self.get_input("password", "") or "") or base.password

        return BrokerConfig(
            host=host,
            port=self._int_port("port", base.port),
            client_id=mqtt_client_id,
            keepalive=self._int_port("keepalive", base.keepalive),
            username=username,
            password=password,
            clean_session=self._bool_port("clean_session", base.clean_session),
            protocol=protocol,
            timeout=self._float_port("timeout", base.timeout),
            tls=self._tls(base.tls),
            will=self._will(base.will),
        )

    def _tls(self, fallback: TlsConfig | None) -> TlsConfig | None:
        raw = self.get_input("tls", None)
        if raw is None or raw == "":
            return fallback
        return TlsConfig() if self._truthy(raw) else None

    def _will(self, fallback: WillConfig | None) -> WillConfig | None:
        topic = str(self.get_input("will_topic", "") or "").strip()
        if not topic:
            return fallback
        return WillConfig(
            topic=topic,
            payload=str(self.get_input("will_payload", "") or ""),
            retain=self._bool_port("will_retain", False),
        )

    def _int_port(self, name: str, fallback: int) -> int:
        raw = self.get_input(name, None)
        if raw is None or raw == "":
            return fallback
        try:
            return int(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Port '{name}' must be a whole number, got {raw!r}") from exc

    def _float_port(self, name: str, fallback: float) -> float:
        raw = self.get_input(name, None)
        if raw is None or raw == "":
            return fallback
        try:
            return float(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Port '{name}' must be a number, got {raw!r}") from exc

    def _bool_port(self, name: str, fallback: bool) -> bool:
        raw = self.get_input(name, None)
        if raw is None or raw == "":
            return fallback
        return self._truthy(raw)

    @staticmethod
    def _truthy(raw: object) -> bool:
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in ("true", "1", "yes", "on")
