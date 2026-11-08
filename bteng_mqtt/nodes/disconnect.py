"""MqttDisconnect — close a named broker connection."""
from __future__ import annotations

import concurrent.futures
import contextlib
import logging

from bteng.core.node import InputPort, NodeConfig, NodeStatus, PortDefinition
from bteng.nodes.leaf.stateful_action import StatefulActionNode

from bteng_mqtt.nodes.base import MqttNodeMixin

logger = logging.getLogger(__name__)


class MqttDisconnect(MqttNodeMixin, StatefulActionNode):
    """Disconnect a named client.

    SUCCESS immediately when the client was not connected, RUNNING while the
    DISCONNECT and the network thread wind down, then SUCCESS — errors are
    swallowed, because the connection is gone either way.

    The saved broker config is kept, so an ``MqttReconnect`` later on still works.
    """

    def __init__(self, name: str, config: NodeConfig | None = None) -> None:
        super().__init__(name, config)
        self._future: concurrent.futures.Future | None = None
        self._client_id: str = ""

    @classmethod
    def provided_ports(cls) -> list[PortDefinition]:
        return [
            InputPort("client_id", "Client identifier to disconnect", default="default"),
        ]

    def on_start(self) -> NodeStatus:
        self._client_id = self.resolve_client_id()
        manager = self.manager

        if not manager.is_connected(self._client_id):
            self.set_feedback_message(f"Client '{self._client_id}' was not connected")
            logger.debug("client %r was not connected", self._client_id)
            return NodeStatus.SUCCESS

        logger.info("disconnecting client %r", self._client_id)
        # unregister_client(), not disconnect(): the broker config survives so a
        # later MqttReconnect has something to reconnect with.  It runs on a
        # worker thread because stopping the network loop joins a thread.
        self._future = manager.submit(manager.unregister_client, self._client_id)
        return NodeStatus.RUNNING

    def on_running(self) -> NodeStatus:
        if not self._future.done():
            return NodeStatus.RUNNING
        with contextlib.suppress(Exception):  # best effort by design
            self._future.result()
        self.set_feedback_message(f"Client '{self._client_id}' disconnected")
        logger.info("client %r disconnected", self._client_id)
        return NodeStatus.SUCCESS

    def on_halted(self) -> None:
        if self._future is not None and not self._future.done():
            self._future.cancel()
        self._future = None
