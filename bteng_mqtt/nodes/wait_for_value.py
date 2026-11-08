"""MqttWaitForValue — block until a topic carries the value you want."""
from __future__ import annotations

import logging
import time

from bteng.core.node import InputPort, NodeConfig, NodeStatus, PortDefinition
from bteng.nodes.leaf.stateful_action import StatefulActionNode

from bteng_mqtt.client import Subscription
from bteng_mqtt.nodes.base import MqttNodeMixin, Target, target_ports, value_ports

logger = logging.getLogger(__name__)


class MqttWaitForValue(MqttNodeMixin, StatefulActionNode):
    """Wait until a topic's value satisfies a comparison.

    The difference from ``MqttSubscribe mode=on_value`` is where it starts
    looking: this node checks the manager's cached latest value first, so a
    condition that is *already* true settles on the first tick without waiting
    for the next publish.  Set ``use_cached=false`` to require a fresh message.

    ``operator`` is one of ``eq`` (default), ``ne``, ``gt``, ``lt``, ``ge``,
    ``le``, ``contains``.  FAILURE on timeout, so ``timeout`` is the node's
    contract — ``0`` means wait forever.
    """

    def __init__(self, name: str, config: NodeConfig | None = None) -> None:
        super().__init__(name, config)
        self._subscription: Subscription | None = None
        self._target: Target | None = None
        self._deadline: float | None = None
        self._client_id: str = ""

    @classmethod
    def provided_ports(cls) -> list[PortDefinition]:
        return [
            *target_ports(),
            *value_ports(),
            InputPort("expected_value", "Value to wait for",                    default=None),
            InputPort("operator",       "eq|ne|gt|lt|ge|le|contains",           default="eq"),
            InputPort("timeout",        "Max wait in seconds; 0 = forever",     default=0.0),
            InputPort("use_cached",     "Accept the last value already received", default=True),
            InputPort("max_age",        "Reject a cached value older than N seconds (0 = any)", default=0.0),
        ]

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def on_start(self) -> NodeStatus:
        client_id = self.require_connected_client()
        if client_id is None:
            return NodeStatus.FAILURE
        self._client_id = client_id

        target = self.resolve_target()
        if target is None:
            return NodeStatus.FAILURE
        self._target = target
        self._deadline = self._compute_deadline()

        # Subscribe before checking the cache: if the cached value does not match,
        # the queue is already collecting whatever arrives next.
        try:
            self._subscription = self.manager.subscribe(client_id, target.topic, target.qos)
        except Exception as exc:
            self.set_feedback_message(f"Subscribe to {target.label} failed: {exc}")
            return NodeStatus.FAILURE

        if self._flag("use_cached", True):
            status = self._check_cached()
            if status is not None:
                return status

        return self._poll()

    def on_running(self) -> NodeStatus:
        return self._poll()

    def on_halted(self) -> None:
        self._close()

    # ── Matching ──────────────────────────────────────────────────────────────

    def _check_cached(self) -> NodeStatus | None:
        """SUCCESS if the already-known value matches; None to keep waiting."""
        assert self._target is not None
        message = self.manager.latest(self._client_id, self._target.topic)
        if message is None and self._target.topic != self._target.label:
            message = self.manager.latest_matching(self._client_id, self._target.topic)
        if message is None:
            return None

        max_age = self._number("max_age", 0.0)
        if max_age > 0.0 and message.age > max_age:
            logger.debug("cached value for %s is %.1fs old — waiting for a fresh one",
                         self._target.label, message.age)
            return None

        try:
            value = self.decode_message(message, self._target)
            matched = self.compare(value, self.get_input("expected_value"), self._target)
        except Exception as exc:
            self.set_feedback_message(str(exc))
            return None  # a bad cached value must not fail the node outright

        if not matched:
            return None
        self.store_value(value, self._target)
        self.set_feedback_message(f"{self._target.label} already satisfied: {value!r}")
        logger.info("%s already satisfied with %r", self._target.label, value)
        self._close()
        return NodeStatus.SUCCESS

    def _poll(self) -> NodeStatus:
        assert self._target is not None
        if self._subscription is None:
            return NodeStatus.FAILURE

        while (message := self._subscription.get_nowait()) is not None:
            try:
                value = self.decode_message(message, self._target)
            except Exception as exc:
                self.set_feedback_message(
                    f"Cannot decode payload from {message.topic}: {exc}"
                )
                self._close()
                return NodeStatus.FAILURE
            try:
                matched = self.compare(value, self.get_input("expected_value"), self._target)
            except ValueError as exc:
                self.set_feedback_message(str(exc))
                self._close()
                return NodeStatus.FAILURE
            if not matched:
                continue

            self.store_value(value, self._target)
            self.set_feedback_message(f"{self._target.label} reached {value!r}")
            logger.info("%s reached %r", self._target.label, value)
            self._close()
            return NodeStatus.SUCCESS

        if self._deadline is not None and time.monotonic() >= self._deadline:
            expected = self.get_input("expected_value")
            self.set_feedback_message(
                f"Timed out waiting for {self._target.label} to become {expected!r}"
            )
            logger.warning("timed out waiting for %s", self._target.label)
            self._close()
            return NodeStatus.FAILURE
        return NodeStatus.RUNNING

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _close(self) -> None:
        if self._subscription is not None:
            self._subscription.close()
            self._subscription = None

    def _compute_deadline(self) -> float | None:
        timeout = self._number("timeout", 0.0)
        return time.monotonic() + timeout if timeout > 0.0 else None

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
