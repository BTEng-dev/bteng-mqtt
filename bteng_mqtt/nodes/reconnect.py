"""MqttReconnect — re-establish a client from its saved broker config."""
from __future__ import annotations

import concurrent.futures
import logging

from bteng.core.node import InputPort, NodeConfig, NodeStatus, PortDefinition
from bteng.nodes.leaf.stateful_action import StatefulActionNode

from bteng_mqtt.nodes.base import MqttNodeMixin

logger = logging.getLogger(__name__)


class MqttReconnect(MqttNodeMixin, StatefulActionNode):
    """Reconnect a client that was connected earlier in this process.

    The config saved by ``MqttConnect`` is reused, so no broker ports are needed
    here.  ``MqttDisconnect`` keeps that config on purpose, which is what makes
    a disconnect/reconnect cycle work.

    SUCCESS immediately when the client is already connected, unless
    ``force=true`` — then the connection is torn down and re-established, which
    is how a tree recovers a session it suspects is stale.

    FAILURE when no config was ever saved for this client id (nothing to
    reconnect to) or the broker refuses.

    Note that paho reconnects on its own after an unexpected drop, and the client
    manager re-issues the subscriptions when it does; this node is for the cases
    that automatic recovery does not cover.
    """

    def __init__(self, name: str, config: NodeConfig | None = None) -> None:
        super().__init__(name, config)
        self._future: concurrent.futures.Future | None = None
        self._client_id: str = ""

    @classmethod
    def provided_ports(cls) -> list[PortDefinition]:
        return [
            InputPort("client_id", "Client identifier to reconnect", default="default"),
            InputPort("force",     "Reconnect even if the client looks connected",
                      default=False),
            InputPort("timeout",   "Seconds to wait for CONNACK (default: the saved one)",
                      default=None),
        ]

    def on_start(self) -> NodeStatus:
        self._client_id = self.resolve_client_id()
        manager = self.manager

        if manager.get_client_config(self._client_id) is None:
            self.set_feedback_message(
                f"No saved config for client '{self._client_id}' — an MqttConnect "
                "must have run first"
            )
            return NodeStatus.FAILURE

        if manager.is_connected(self._client_id) and not self._force():
            self.set_feedback_message(f"Client '{self._client_id}' is already connected")
            return NodeStatus.SUCCESS

        logger.info("reconnecting client %r", self._client_id)
        self._future = manager.submit(manager.reconnect, self._client_id, self._timeout())
        return NodeStatus.RUNNING

    def on_running(self) -> NodeStatus:
        if not self._future.done():
            return NodeStatus.RUNNING
        try:
            self._future.result()
        except Exception as exc:
            self.set_feedback_message(str(exc))
            logger.warning("client %r reconnect failed: %s", self._client_id, exc)
            return NodeStatus.FAILURE
        self.set_feedback_message(f"Client '{self._client_id}' reconnected")
        logger.info("client %r reconnected", self._client_id)
        return NodeStatus.SUCCESS

    def on_halted(self) -> None:
        if self._future is not None and not self._future.done():
            self._future.cancel()
        self._future = None

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _force(self) -> bool:
        raw = self.get_input("force", False)
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in ("true", "1", "yes", "on")

    def _timeout(self) -> float | None:
        raw = self.get_input("timeout", None)
        if raw is None or raw == "":
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None
