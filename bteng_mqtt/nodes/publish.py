"""MqttPublish — send a value to a topic."""
from __future__ import annotations

import logging
import time

from bteng.core.node import InputPort, NodeConfig, NodeStatus, PortDefinition
from bteng.nodes.leaf.stateful_action import StatefulActionNode

from bteng_mqtt.backend.base import PublishTicket
from bteng_mqtt.nodes.base import MqttNodeMixin, target_ports
from bteng_mqtt.types import encode

logger = logging.getLogger(__name__)


class MqttPublish(MqttNodeMixin, StatefulActionNode):
    """Publish a value, encoded according to the topic's declared payload type.

    QoS 0 settles as SUCCESS as soon as the payload is handed to the client.
    QoS 1 and 2 stay RUNNING until PUBACK/PUBCOMP arrives, so a tree that
    continues really has been acknowledged by the broker; ``wait_for_ack=false``
    opts out.  FAILURE on an unknown alias, a wildcard topic, a payload that
    cannot be encoded, or an acknowledgement that never comes.
    """

    def __init__(self, name: str, config: NodeConfig | None = None) -> None:
        super().__init__(name, config)
        self._ticket: PublishTicket | None = None
        self._deadline: float = 0.0
        self._label: str = ""

    @classmethod
    def provided_ports(cls) -> list[PortDefinition]:
        return [
            *target_ports(include_publish=True),
            InputPort("value",        "Value to publish (mapped through value_map)", default=None),
            InputPort("wait_for_ack", "Wait for the broker's acknowledgement (QoS>0)", default=True),
            InputPort("timeout",      "Seconds to wait for the acknowledgement",     default=5.0),
        ]

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def on_start(self) -> NodeStatus:
        client_id = self.require_connected_client()
        if client_id is None:
            return NodeStatus.FAILURE

        target = self.resolve_target(for_publish=True)
        if target is None:
            return NodeStatus.FAILURE
        self._label = target.label

        raw = self.get_input("value", None)
        if raw is None:
            self.set_feedback_message(f"Nothing to publish — port 'value' is unset ({self._label})")
            return NodeStatus.FAILURE

        if target.definition is not None:
            raw = target.definition.resolve_value(raw)

        try:
            payload = encode(raw, target.payload_type)
        except Exception as exc:
            self.set_feedback_message(f"Cannot encode {raw!r} for {self._label}: {exc}")
            return NodeStatus.FAILURE

        try:
            self._ticket = self.manager.publish(
                client_id, target.topic, payload, qos=target.qos, retain=target.retain
            )
        except Exception as exc:
            self.set_feedback_message(f"Publish to {self._label} failed: {exc}")
            logger.warning("publish %s failed: %s", self._label, exc)
            return NodeStatus.FAILURE

        logger.info("published %s = %r (qos=%d, retain=%s)",
                    self._label, raw, target.qos, target.retain)

        if target.qos == 0 or not self._wait_for_ack() or self._ticket.is_published():
            self.set_feedback_message(f"Published {raw!r} to {self._label}")
            return NodeStatus.SUCCESS

        self._deadline = time.monotonic() + self._timeout()
        return NodeStatus.RUNNING

    def on_running(self) -> NodeStatus:
        if self._ticket is not None and self._ticket.is_published():
            self.set_feedback_message(f"Publish to {self._label} acknowledged")
            return NodeStatus.SUCCESS
        if time.monotonic() >= self._deadline:
            self.set_feedback_message(
                f"Publish to {self._label} was not acknowledged within "
                f"{self._timeout():.1f}s"
            )
            logger.warning("publish %s not acknowledged", self._label)
            return NodeStatus.FAILURE
        return NodeStatus.RUNNING

    def on_halted(self) -> None:
        self._ticket = None

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _wait_for_ack(self) -> bool:
        raw = self.get_input("wait_for_ack", True)
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() not in ("false", "0", "no", "off")

    def _timeout(self) -> float:
        try:
            return float(self.get_input("timeout", 5.0))
        except (TypeError, ValueError):
            return 5.0
