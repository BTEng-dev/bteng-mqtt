"""MqttCondition — a synchronous check against the last value received."""
from __future__ import annotations

import logging

from bteng.core.node import InputPort, NodeConfig, NodeStatus, PortDefinition
from bteng.nodes.leaf.condition import ConditionNode

from bteng_mqtt.nodes.base import MqttNodeMixin, target_ports

logger = logging.getLogger(__name__)


class MqttCondition(MqttNodeMixin, ConditionNode):
    """Compare a topic's most recent value with an expectation — now, no waiting.

    MQTT has no blocking read, so this node answers from the client manager's
    latest-value cache.  Something must therefore already be receiving the
    topic: either an ``MqttSubscribe persistent="true"`` earlier in the tree, or
    a retained message that arrived when it first subscribed.  With nothing
    cached the node returns FAILURE and says so — it never blocks.

    FAILURE (not an exception) is also the answer for a stale value when
    ``max_age`` is set, and for a mismatch.
    """

    def __init__(self, name: str, config: NodeConfig | None = None) -> None:
        super().__init__(name, config)

    @classmethod
    def provided_ports(cls) -> list[PortDefinition]:
        return [
            *target_ports(),
            InputPort("expected_value", "Value the topic must carry",    default=None),
            InputPort("operator",       "eq|ne|gt|lt|ge|le|contains",    default="eq"),
            InputPort("max_age",        "Fail if the cached value is older than N seconds (0 = any)",
                      default=0.0),
        ]

    def tick(self) -> NodeStatus:
        client_id = self.require_connected_client()
        if client_id is None:
            return NodeStatus.FAILURE

        target = self.resolve_target()
        if target is None:
            return NodeStatus.FAILURE

        message = self.manager.latest(client_id, target.topic)
        if message is None:
            message = self.manager.latest_matching(client_id, target.topic)
        if message is None:
            self.set_feedback_message(
                f"No value cached for {target.label} — an MqttSubscribe with "
                "persistent=\"true\" must run before this condition"
            )
            return NodeStatus.FAILURE

        try:
            max_age = float(self.get_input("max_age", 0.0))
        except (TypeError, ValueError):
            max_age = 0.0
        if max_age > 0.0 and message.age > max_age:
            self.set_feedback_message(
                f"{target.label} is stale: {message.age:.1f}s old, limit {max_age:.1f}s"
            )
            return NodeStatus.FAILURE

        try:
            value = self.decode_message(message, target)
            matched = self.compare(value, self.get_input("expected_value"), target)
        except Exception as exc:
            self.set_feedback_message(str(exc))
            logger.warning("condition on %s failed: %s", target.label, exc)
            return NodeStatus.FAILURE

        if matched:
            logger.info("condition met: %s = %r", target.label, value)
            return NodeStatus.SUCCESS

        expected = self.get_input("expected_value")
        self.set_feedback_message(
            f"Condition not met: {target.label} is {value!r}, expected {expected!r}"
        )
        return NodeStatus.FAILURE
