"""Sleep — pause a tree for a fixed duration.

A generic helper node (nothing MQTT about it), bundled so projects that need a
timed wait between MQTT steps don't have to hand-write one. Registered by
``register_nodes()`` like the rest, and identical to the node ``bteng_opcua``
ships — a tree can move between the two integrations unchanged.
"""
from __future__ import annotations

import time

from bteng.core.node import InputPort, NodeConfig, NodeStatus, PortDefinition
from bteng.nodes.leaf.stateful_action import StatefulActionNode


class Sleep(StatefulActionNode):
    """Return RUNNING until ``duration`` seconds have elapsed, then SUCCESS."""

    def __init__(self, name: str, config: NodeConfig | None = None) -> None:
        super().__init__(name, config)
        self._deadline: float = 0.0

    @classmethod
    def provided_ports(cls) -> list[PortDefinition]:
        return [
            InputPort("duration", "Seconds to wait", default=1.0),
        ]

    def on_start(self) -> NodeStatus:
        try:
            duration = float(self.get_input("duration", 1.0))
        except (TypeError, ValueError):
            self.set_feedback_message(
                f"Sleep 'duration' must be a number, got {self.get_input('duration')!r}"
            )
            return NodeStatus.FAILURE
        if duration <= 0:
            return NodeStatus.SUCCESS
        self._deadline = time.monotonic() + duration
        return NodeStatus.RUNNING

    def on_running(self) -> NodeStatus:
        if time.monotonic() >= self._deadline:
            return NodeStatus.SUCCESS
        return NodeStatus.RUNNING

    def on_halted(self) -> None:
        self._deadline = 0.0
