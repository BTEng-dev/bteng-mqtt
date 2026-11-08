"""Test helpers for driving single nodes without building a whole tree."""
from __future__ import annotations

import time
from typing import Any

from bteng import Blackboard, NodeConfig, NodeStatus

from bteng_mqtt.registry import TopicRegistry

#: Aliases every node test can rely on — one per payload type, plus the
#: interesting cases (json_path, value_map, a wildcard, a retained topic).
DEFAULT_TOPICS = [
    {"alias": "temp",    "topic": "a/temp",  "payload_type": "float", "qos": 1},
    {"alias": "count",   "topic": "a/count", "payload_type": "int"},
    {"alias": "flag",    "topic": "a/flag",  "payload_type": "bool"},
    {"alias": "label",   "topic": "a/label", "payload_type": "string"},
    {"alias": "state",   "topic": "a/state", "payload_type": "json",
     "json_path": "status.mode"},
    {"alias": "cmd",     "topic": "a/cmd",   "payload_type": "json",
     "value_map": {"start": '{"cmd": "start"}'}},
    {"alias": "sensors", "topic": "a/sensor/+", "payload_type": "float"},
    {"alias": "sticky",  "topic": "a/sticky", "payload_type": "string", "retain": True},
]


def registry_with(topics: list[dict] | None = None, broker: dict | None = None,
                  blackboard: dict | None = None) -> TopicRegistry:
    """A registry holding the standard test aliases plus any extras."""
    registry = TopicRegistry()
    data: dict[str, Any] = {"topics": topics if topics is not None else DEFAULT_TOPICS}
    if broker is not None:
        data["broker"] = broker
    if blackboard is not None:
        data["blackboard"] = blackboard
    registry.load_dict(data)
    return registry


def make_node(node_cls, params: dict | None = None, registry: TopicRegistry | None = None,
              blackboard: Blackboard | None = None, output_ports: dict | None = None,
              name: str | None = None):
    """Instantiate one node with a blackboard and a registry, as the parser would.

    The registry travels in ``NodeConfig.params["registry"]`` — the same hook the
    nodes read — so tests never touch the global singleton.
    """
    node_params: dict[str, Any] = dict(params or {})
    node_params["registry"] = registry if registry is not None else registry_with()
    config = NodeConfig(
        blackboard=blackboard if blackboard is not None else new_blackboard(),
        params=node_params,
        output_ports=dict(output_ports or {}),
    )
    return node_cls(name or node_cls.__name__, config)


_counter = 0


def new_blackboard() -> Blackboard:
    """A fresh, uniquely named blackboard (they are process-global in bteng)."""
    global _counter
    _counter += 1
    key = f"test_bb_{_counter}"
    Blackboard.reset(key)
    return Blackboard.create(key)


def settle(node, timeout: float = 5.0, tick_interval: float = 0.005) -> NodeStatus:
    """Tick a node until it stops returning RUNNING."""
    deadline = time.monotonic() + timeout
    while True:
        status = node.execute_tick()
        if status is not NodeStatus.RUNNING:
            return status
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"{node.name} was still RUNNING after {timeout}s "
                f"(feedback: {node.feedback_message!r})"
            )
        time.sleep(tick_interval)
