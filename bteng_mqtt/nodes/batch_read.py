"""MqttBatchRead — copy several topics' latest values to the blackboard."""
from __future__ import annotations

import logging

from bteng.core.node import InputPort, NodeConfig, NodeStatus, PortDefinition
from bteng.nodes.leaf.stateful_action import StatefulActionNode

from bteng_mqtt.nodes.base import MqttNodeMixin

logger = logging.getLogger(__name__)


class MqttBatchRead(MqttNodeMixin, StatefulActionNode):
    """Read the cached latest value of several topics into the blackboard.

    Like ``MqttCondition``, this reads the client manager's cache rather than the
    network — MQTT has no request/reply read — so the topics must already be
    subscribed (``MqttSubscribe persistent="true"``, or retained messages that
    arrived when they were).  It settles in a single tick.

    ``output_keys`` defaults to the alias names, so ``topic_aliases="x,y"`` puts
    ``x`` and ``y`` on the blackboard.

    With ``require_all=true`` (the default) a topic that has never published is a
    FAILURE naming it; with ``require_all=false`` the node succeeds having written
    whatever was available, which suits an optional-telemetry snapshot.
    """

    def __init__(self, name: str, config: NodeConfig | None = None) -> None:
        super().__init__(name, config)

    @classmethod
    def provided_ports(cls) -> list[PortDefinition]:
        return [
            InputPort("client_id",     "Client identifier from MqttConnect",     default="default"),
            InputPort("topic_aliases", "Comma-separated registry aliases",       default=""),
            InputPort("output_keys",   "Comma-separated blackboard keys (default: the aliases)",
                      default=""),
            InputPort("max_age",       "Ignore values older than N seconds (0 = any)", default=0.0),
            InputPort("require_all",   "Fail if any topic has no value yet",     default=True),
        ]

    def on_start(self) -> NodeStatus:
        client_id = self.require_connected_client()
        if client_id is None:
            return NodeStatus.FAILURE

        targets = self.resolve_alias_list()
        if targets is None:
            return NodeStatus.FAILURE

        keys = self.paired_keys(targets, "output_keys")
        if keys is None:
            return NodeStatus.FAILURE

        blackboard = self._config.blackboard
        if blackboard is None:
            self.set_feedback_message("No blackboard available to write the values to")
            return NodeStatus.FAILURE

        max_age = self._number("max_age", 0.0)
        require_all = self._flag("require_all", True)
        missing: list[str] = []
        written = 0

        for target, key in zip(targets, keys, strict=True):
            message = self.manager.latest(client_id, target.topic)
            if message is None:
                message = self.manager.latest_matching(client_id, target.topic)
            if message is None:
                missing.append(f"{target.label} (never published)")
                continue
            if max_age > 0.0 and message.age > max_age:
                missing.append(f"{target.label} ({message.age:.1f}s old)")
                continue
            try:
                value = self.decode_message(message, target)
            except Exception as exc:
                self.set_feedback_message(f"Cannot decode {target.label}: {exc}")
                logger.warning("batch read %s failed: %s", target.label, exc)
                return NodeStatus.FAILURE
            blackboard.set(key, value, writer=self.uid)
            written += 1

        if missing and require_all:
            self.set_feedback_message("No usable value for: " + ", ".join(missing))
            logger.warning("batch read missing: %s", missing)
            return NodeStatus.FAILURE

        detail = f"Read {written} of {len(targets)} topic(s)"
        if missing:
            detail += f"; skipped {', '.join(missing)}"
        self.set_feedback_message(detail)
        logger.info("%s", detail)
        return NodeStatus.SUCCESS

    def on_running(self) -> NodeStatus:  # pragma: no cover - on_start always settles
        return NodeStatus.SUCCESS

    def on_halted(self) -> None:
        return None

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _number(self, port: str, fallback: float) -> float:
        try:
            return float(self.get_input(port, fallback))
        except (TypeError, ValueError):
            return fallback

    def _flag(self, port: str, fallback: bool) -> bool:
        raw = self.get_input(port, fallback)
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in ("true", "1", "yes", "on")
