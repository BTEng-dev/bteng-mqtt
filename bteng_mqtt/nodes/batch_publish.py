"""MqttBatchPublish — publish several topics in one activation."""
from __future__ import annotations

import logging
import time
from typing import Any

from bteng.core.node import InputPort, NodeConfig, NodeStatus, PortDefinition
from bteng.nodes.leaf.stateful_action import StatefulActionNode

from bteng_mqtt.backend.base import PublishTicket
from bteng_mqtt.nodes.base import MqttNodeMixin, Target
from bteng_mqtt.types import encode

logger = logging.getLogger(__name__)


class MqttBatchPublish(MqttNodeMixin, StatefulActionNode):
    """Publish to several aliased topics, then settle once.

    ``topic_aliases`` names the topics.  The values come from either

    * ``values`` — a comma-separated list of literals, one per alias; or
    * ``input_keys`` — comma-separated blackboard keys, defaulting to the alias
      names themselves, which is the common case: a tree that wrote ``x`` and
      ``y`` on the blackboard publishes both with
      ``topic_aliases="x,y"``.

    Each payload is encoded with its own topic's declared type, and each publish
    uses its own topic's QoS and retain flag.  With any QoS > 0 the node stays
    RUNNING until every acknowledgement is in, so one settle point means all of
    them landed.

    Nothing is published unless *every* value resolves and encodes — a
    half-applied batch is the failure mode worth avoiding here.
    """

    def __init__(self, name: str, config: NodeConfig | None = None) -> None:
        super().__init__(name, config)
        self._tickets: list[tuple[str, PublishTicket]] = []
        self._deadline: float = 0.0

    @classmethod
    def provided_ports(cls) -> list[PortDefinition]:
        return [
            InputPort("client_id",     "Client identifier from MqttConnect",       default="default"),
            InputPort("topic_aliases", "Comma-separated registry aliases",         default=""),
            InputPort("values",        "Comma-separated literal values (one per alias)", default=""),
            InputPort("input_keys",    "Comma-separated blackboard keys (default: the aliases)",
                      default=""),
            InputPort("qos",           "QoS for every publish (overrides each topic's)",
                      default=None),
            InputPort("retain",        "Retain every message (overrides each topic's)",
                      default=None),
            InputPort("wait_for_ack",  "Wait for every acknowledgement (QoS>0)",   default=True),
            InputPort("timeout",       "Seconds to wait for the acknowledgements",  default=5.0),
        ]

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def on_start(self) -> NodeStatus:
        client_id = self.require_connected_client()
        if client_id is None:
            return NodeStatus.FAILURE

        targets = self.resolve_alias_list()
        if targets is None:
            return NodeStatus.FAILURE

        for target in targets:
            if target.is_wildcard:
                self.set_feedback_message(
                    f"Cannot publish to wildcard topic {target.topic!r} "
                    f"(alias '{target.label}')"
                )
                return NodeStatus.FAILURE

        payloads = self._encode_all(targets)
        if payloads is None:
            return NodeStatus.FAILURE

        self._tickets = []
        for target, payload in zip(targets, payloads, strict=True):
            try:
                ticket = self.manager.publish(
                    client_id, target.topic, payload,
                    qos=target.qos, retain=target.retain,
                )
            except Exception as exc:
                self.set_feedback_message(f"Publish to {target.label} failed: {exc}")
                logger.warning("batch publish %s failed: %s", target.label, exc)
                return NodeStatus.FAILURE
            self._tickets.append((target.label, ticket))

        logger.info("published %d topic(s): %s", len(targets),
                    ", ".join(t.label for t in targets))

        if not self._wait_for_ack() or self._all_acknowledged():
            self.set_feedback_message(f"Published {len(targets)} topic(s)")
            return NodeStatus.SUCCESS

        self._deadline = time.monotonic() + self._timeout()
        return NodeStatus.RUNNING

    def on_running(self) -> NodeStatus:
        if self._all_acknowledged():
            self.set_feedback_message(f"All {len(self._tickets)} publish(es) acknowledged")
            return NodeStatus.SUCCESS
        if time.monotonic() >= self._deadline:
            pending = [label for label, ticket in self._tickets if not ticket.is_published()]
            self.set_feedback_message(
                f"Not acknowledged within {self._timeout():.1f}s: {', '.join(pending)}"
            )
            logger.warning("batch publish unacknowledged: %s", pending)
            return NodeStatus.FAILURE
        return NodeStatus.RUNNING

    def on_halted(self) -> None:
        self._tickets = []

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _encode_all(self, targets: list[Target]) -> list[bytes] | None:
        """Encode every payload up front, so a bad value publishes nothing."""
        literals = self.split_list("values")
        if literals:
            if len(literals) != len(targets):
                self.set_feedback_message(
                    f"Port 'values' has {len(literals)} item(s) but "
                    f"{len(targets)} topic(s) were given — the lists must line up"
                )
                return None
            raw_values: list[Any] = list(literals)
        else:
            keys = self.paired_keys(targets, "input_keys")
            if keys is None:
                return None
            blackboard = self._config.blackboard
            if blackboard is None:
                self.set_feedback_message(
                    "No blackboard available — pass 'values' instead of 'input_keys'"
                )
                return None
            raw_values = []
            for key in keys:
                value = blackboard.get(key)
                if value is None:
                    self.set_feedback_message(
                        f"Blackboard key {key!r} is unset — nothing to publish"
                    )
                    return None
                raw_values.append(value)

        payloads: list[bytes] = []
        for target, raw in zip(targets, raw_values, strict=True):
            value = target.definition.resolve_value(raw) if target.definition else raw
            try:
                payloads.append(encode(value, target.payload_type))
            except Exception as exc:
                self.set_feedback_message(
                    f"Cannot encode {value!r} for {target.label}: {exc}"
                )
                return None
        return payloads

    def _all_acknowledged(self) -> bool:
        return all(ticket.is_published() for _label, ticket in self._tickets)

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
