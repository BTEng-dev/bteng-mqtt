"""Shared helpers for the MQTT behavior-tree nodes.

Every node resolves the same two things — which client, and which topic — and
they all obey the same rule: a registry alias is the normal case, raw ports are
the escape hatch.  That logic lives here so the nodes stay short and behave
identically.
"""
from __future__ import annotations

import logging
import operator as _operator
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from bteng.core.node import InputPort, OutputPort, PortDefinition

from bteng_mqtt.client import Message, MqttClientManager
from bteng_mqtt.registry import TopicDefinition, get_global_registry
from bteng_mqtt.topics import is_wildcard
from bteng_mqtt.types import MqttPayloadType, coerce, decode

logger = logging.getLogger(__name__)

#: Comparison operators available to the ``operator`` port.
OPERATORS: dict[str, Callable[[Any, Any], bool]] = {
    "eq": _operator.eq,
    "ne": _operator.ne,
    "gt": _operator.gt,
    "lt": _operator.lt,
    "ge": _operator.ge,
    "le": _operator.le,
    "contains": lambda value, expected: expected in value,
}


@dataclass
class Target:
    """A resolved topic: where to talk and how to read what comes back."""
    topic:        str
    payload_type: MqttPayloadType
    qos:          int  = 0
    retain:       bool = False
    json_path:    str | None = None
    definition:   TopicDefinition | None = None

    @property
    def label(self) -> str:
        """Alias if the topic came from the registry, else the topic itself."""
        return self.definition.alias if self.definition is not None else self.topic

    @property
    def is_wildcard(self) -> bool:
        """True for a subscribe-only topic — publishing to it is an error."""
        return is_wildcard(self.topic)

    @property
    def blackboard_key(self) -> str:
        """Default blackboard key for a value read from this target."""
        return self.definition.alias if self.definition is not None else self.topic


def target_ports(include_publish: bool = False) -> list[PortDefinition]:
    """The ports every topic-addressing node shares.

    ``include_publish`` adds the ports that only make sense when sending.
    """
    ports = [
        InputPort("client_id",    "Client identifier from MqttConnect",        default="default"),
        InputPort("topic_alias",  "Registry alias for the topic",              default=""),
        InputPort("topic",        "Raw topic, when no alias is used",          default=""),
        InputPort("payload_type", "string|int|float|bool|json|bytes",          default=""),
        InputPort("json_path",    "Field to extract from a json payload",      default=""),
        InputPort("qos",          "Quality of Service 0/1/2 (overrides the registry)", default=None),
    ]
    if include_publish:
        ports.append(
            InputPort("retain", "Ask the broker to retain this message", default=None)
        )
    return ports


def value_ports() -> list[PortDefinition]:
    """Ports for a node that hands a received value to the tree."""
    return [
        InputPort("output_key", "Blackboard key for the received value", default=""),
        OutputPort("value",     "The value received from the broker"),
    ]


class MqttNodeMixin:
    """Topic/client resolution shared by every MQTT node.

    Mixed into ``StatefulActionNode`` and ``ConditionNode`` subclasses, so it
    relies on ``get_input`` / ``set_output`` / ``set_feedback_message`` from
    BTEng's ``TreeNode``.
    """

    # ── Client ────────────────────────────────────────────────────────────────

    @property
    def manager(self) -> MqttClientManager:
        return MqttClientManager.get_instance()

    def resolve_client_id(self) -> str:
        return str(self.get_input("client_id", "default"))  # type: ignore[attr-defined]

    def require_connected_client(self) -> str | None:
        """The client id, or None (with feedback set) when it is not connected."""
        client_id = self.resolve_client_id()
        if not self.manager.is_connected(client_id):
            self.set_feedback_message(  # type: ignore[attr-defined]
                f"Client '{client_id}' is not connected — put an MqttConnect "
                f"before this node. Active clients: {self.manager.active_clients()}"
            )
            return None
        return client_id

    # ── Topic ─────────────────────────────────────────────────────────────────

    def resolve_target(self, for_publish: bool = False) -> Target | None:
        """Resolve the addressed topic from an alias or from raw ports.

        Returns None and sets a feedback message when the topic cannot be
        resolved, so a caller can ``return NodeStatus.FAILURE`` directly.
        """
        alias = str(self.get_input("topic_alias", "") or "").strip()  # type: ignore[attr-defined]
        target = self._target_from_alias(alias) if alias else self._target_from_ports()
        if target is None:
            return None

        if for_publish and is_wildcard(target.topic):
            self.set_feedback_message(  # type: ignore[attr-defined]
                f"Cannot publish to wildcard topic {target.topic!r} "
                f"(alias '{target.label}')"
            )
            return None
        return target

    def _target_from_alias(self, alias: str) -> Target | None:
        registry = self._config.params.get("registry") or get_global_registry()  # type: ignore[attr-defined]
        try:
            defn = registry.resolve(alias)
        except Exception as exc:
            self.set_feedback_message(str(exc))  # type: ignore[attr-defined]
            return None
        return Target(
            topic=defn.topic,
            payload_type=defn.payload_type,
            qos=self._qos_override(defn.qos),
            retain=self._retain_override(defn.retain),
            json_path=defn.json_path,
            definition=defn,
        )

    def _target_from_ports(self) -> Target | None:
        topic = str(self.get_input("topic", "") or "").strip()  # type: ignore[attr-defined]
        if not topic:
            self.set_feedback_message(  # type: ignore[attr-defined]
                "Provide 'topic_alias' (recommended) or a raw 'topic'"
            )
            return None
        raw_type = str(self.get_input("payload_type", "") or "").strip()  # type: ignore[attr-defined]
        try:
            payload_type = MqttPayloadType(raw_type) if raw_type else MqttPayloadType.STRING
        except ValueError:
            self.set_feedback_message(  # type: ignore[attr-defined]
                f"Unknown payload_type {raw_type!r}. Supported: "
                f"{[t.value for t in MqttPayloadType]}"
            )
            return None
        json_path = str(self.get_input("json_path", "") or "").strip() or None  # type: ignore[attr-defined]
        return Target(
            topic=topic,
            payload_type=payload_type,
            qos=self._qos_override(0),
            retain=self._retain_override(False),
            json_path=json_path,
            definition=None,
        )

    def _qos_override(self, fallback: int) -> int:
        raw = self.get_input("qos", None)  # type: ignore[attr-defined]
        if raw is None or raw == "":
            return fallback
        try:
            qos = int(raw)
        except (TypeError, ValueError):
            return fallback
        return qos if qos in (0, 1, 2) else fallback

    def _retain_override(self, fallback: bool) -> bool:
        raw = self.get_input("retain", None)  # type: ignore[attr-defined]
        if raw is None or raw == "":
            return fallback
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in ("true", "1", "yes", "on")

    # ── Lists (batch nodes) ───────────────────────────────────────────────────

    def split_list(self, port: str) -> list[str]:
        """Split a comma-separated port into stripped, non-empty items."""
        raw = self.get_input(port, "")  # type: ignore[attr-defined]
        if raw is None:
            return []
        if isinstance(raw, (list, tuple)):
            return [str(item).strip() for item in raw if str(item).strip()]
        return [item.strip() for item in str(raw).split(",") if item.strip()]

    def resolve_alias_list(self, port: str = "topic_aliases") -> list[Target] | None:
        """Resolve a comma-separated alias list into targets.

        Returns None with feedback set when the port is empty or an alias is
        unknown — a batch node that silently skipped an alias would be worse than
        one that fails.
        """
        aliases = self.split_list(port)
        if not aliases:
            self.set_feedback_message(f"Port '{port}' is empty — nothing to do")  # type: ignore[attr-defined]
            return None
        targets: list[Target] = []
        for alias in aliases:
            target = self._target_from_alias(alias)
            if target is None:
                return None
            targets.append(target)
        return targets

    def paired_keys(self, targets: list[Target], port: str) -> list[str] | None:
        """Read a comma-separated key list, defaulting to the aliases themselves.

        Returns None with feedback set when the list length does not match the
        alias list.
        """
        keys = self.split_list(port)
        if not keys:
            return [target.blackboard_key for target in targets]
        if len(keys) != len(targets):
            self.set_feedback_message(  # type: ignore[attr-defined]
                f"Port '{port}' has {len(keys)} item(s) but {len(targets)} topic(s) "
                "were given — the lists must line up"
            )
            return None
        return keys

    # ── Values ────────────────────────────────────────────────────────────────

    def decode_message(self, message: Message, target: Target) -> Any:
        """Decode a received payload, or raise PayloadConversionError."""
        return decode(message.payload, target.payload_type, target.json_path)

    def store_value(self, value: Any, target: Target) -> None:
        """Publish a value to the tree: output port first, blackboard key second.

        The port mapping wins when the tree remapped ``value``; otherwise the
        value lands under ``output_key``, defaulting to the alias (or topic).
        """
        if self.set_output("value", value):  # type: ignore[attr-defined]
            return
        key = str(self.get_input("output_key", "") or "").strip()  # type: ignore[attr-defined]
        blackboard = self._config.blackboard  # type: ignore[attr-defined]
        if blackboard is not None:
            blackboard.set(key or target.blackboard_key, value, writer=self.uid)  # type: ignore[attr-defined]

    def compare(self, value: Any, expected_raw: Any, target: Target) -> bool:
        """Compare a decoded value with a tree-level expectation.

        ``expected_raw`` comes from XML as a string, so it is coerced to the
        target's payload type first: ``expected_value="42"`` matches ``42``.
        """
        name = str(self.get_input("operator", "eq") or "eq").strip().lower()  # type: ignore[attr-defined]
        compare_fn = OPERATORS.get(name)
        if compare_fn is None:
            raise ValueError(
                f"Unknown operator {name!r}. Supported: {sorted(OPERATORS)}"
            )
        expected: Any = expected_raw
        if name in ("eq", "ne", "gt", "lt", "ge", "le"):
            try:
                expected = coerce(expected_raw, target.payload_type)
            except Exception:
                expected = expected_raw  # compare as given; mismatch is the answer
        try:
            return bool(compare_fn(value, expected))
        except TypeError as exc:
            raise ValueError(
                f"Cannot compare {value!r} {name} {expected!r}: {exc}"
            ) from exc
