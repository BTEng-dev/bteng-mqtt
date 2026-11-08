"""MqttUnsubscribe — drop a persistent subscription."""
from __future__ import annotations

import logging

from bteng.core.node import InputPort, NodeConfig, NodeStatus, PortDefinition
from bteng.nodes.leaf.stateful_action import StatefulActionNode

from bteng_mqtt.nodes.base import MqttNodeMixin, target_ports

logger = logging.getLogger(__name__)


class MqttUnsubscribe(MqttNodeMixin, StatefulActionNode):
    """Release a subscription an ``MqttSubscribe persistent="true"`` left open.

    Non-persistent subscribe nodes clean up after themselves, so this node is for
    the persistent ones: it closes every holder of the topic filter on that
    client, which drops the broker subscription and stops the queue from growing
    for the rest of the run.

    SUCCESS whether or not anything was subscribed — a tree tidying up should not
    fail because it already tidied up. The feedback message says which it was.
    """

    def __init__(self, name: str, config: NodeConfig | None = None) -> None:
        super().__init__(name, config)

    @classmethod
    def provided_ports(cls) -> list[PortDefinition]:
        return [
            *target_ports(),
            InputPort("clear_cached", "Also forget the topic's cached latest value",
                      default=False),
        ]

    def on_start(self) -> NodeStatus:
        client_id = self.require_connected_client()
        if client_id is None:
            return NodeStatus.FAILURE

        target = self.resolve_target()
        if target is None:
            return NodeStatus.FAILURE

        closed = self.manager.unsubscribe_all(client_id, target.topic)

        if self._flag("clear_cached"):
            # Deliberately opt-in: an MqttCondition later in the tree usually
            # still wants the last value this subscription received.
            self.manager.clear_latest(client_id)

        if closed:
            self.set_feedback_message(
                f"Unsubscribed from {target.label} ({closed} holder(s) closed)"
            )
            logger.info("unsubscribed from %s (%d holders)", target.label, closed)
        else:
            self.set_feedback_message(f"Nothing was subscribed to {target.label}")
            logger.debug("no subscription to release on %s", target.label)
        return NodeStatus.SUCCESS

    def on_running(self) -> NodeStatus:  # pragma: no cover - on_start always settles
        return NodeStatus.SUCCESS

    def on_halted(self) -> None:
        return None

    def _flag(self, port: str) -> bool:
        raw = self.get_input(port, False)
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in ("true", "1", "yes", "on")
