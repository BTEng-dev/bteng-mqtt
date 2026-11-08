"""MqttSubscribe — wait for a message on a topic."""
from __future__ import annotations

import logging
import time

from bteng.core.node import InputPort, NodeConfig, NodeStatus, PortDefinition
from bteng.nodes.leaf.stateful_action import StatefulActionNode

from bteng_mqtt.client import Subscription
from bteng_mqtt.nodes.base import MqttNodeMixin, Target, target_ports, value_ports

logger = logging.getLogger(__name__)


class MqttSubscribe(MqttNodeMixin, StatefulActionNode):
    """Subscribe to a topic and wait for a message.

    RUNNING while waiting.  SUCCESS when

    * ``mode=on_message`` — any message arrives;
    * ``mode=on_value``   — a message whose value satisfies ``operator``
      (default ``eq``) against ``expected_value``; other messages are discarded
      and the wait continues.

    FAILURE on a missing client, an unknown alias, a subscribe error or a
    timeout (``timeout=0`` waits forever — pair it with a decorator or a tree
    timeout).

    ``persistent=true`` keeps the broker subscription (and its queue) alive
    between activations, so a value published while the tree was elsewhere still
    counts on the next activation.  Wildcard aliases are fine here.
    """

    def __init__(self, name: str, config: NodeConfig | None = None) -> None:
        super().__init__(name, config)
        self._subscription: Subscription | None = None
        self._target: Target | None = None
        self._deadline: float | None = None
        self._persistent: bool = False

    @classmethod
    def provided_ports(cls) -> list[PortDefinition]:
        return [
            *target_ports(),
            *value_ports(),
            InputPort("mode",            "on_message — any message; on_value — must match", default="on_message"),
            InputPort("expected_value",  "Value to wait for when mode=on_value",  default=None),
            InputPort("operator",        "eq|ne|gt|lt|ge|le|contains",            default="eq"),
            InputPort("timeout",         "Max wait in seconds; 0 = forever",      default=0.0),
            InputPort("persistent",      "Keep the subscription across activations", default=False),
            InputPort("ignore_retained", "Skip the broker's retained message",    default=False),
        ]

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def on_start(self) -> NodeStatus:
        client_id = self.require_connected_client()
        if client_id is None:
            return NodeStatus.FAILURE

        target = self.resolve_target()
        if target is None:
            return NodeStatus.FAILURE
        self._target = target
        self._persistent = self._flag("persistent")
        self._deadline = self._compute_deadline()

        if self._persistent and self._subscription is not None and not self._subscription.closed:
            # Reuse the live subscription — anything received while this node was
            # idle is still queued, which is the point of persistent mode.
            return self._poll()

        try:
            self._subscription = self.manager.subscribe(client_id, target.topic, target.qos)
        except Exception as exc:
            self.set_feedback_message(f"Subscribe to {target.label} failed: {exc}")
            logger.warning("subscribe %s failed: %s", target.label, exc)
            return NodeStatus.FAILURE

        logger.info("subscribed to %s (qos=%d)", target.label, target.qos)
        return self._poll()

    def on_running(self) -> NodeStatus:
        return self._poll()

    def on_halted(self) -> None:
        self._release()

    # ── Message handling ──────────────────────────────────────────────────────

    def _poll(self) -> NodeStatus:
        """Drain what has arrived; settle on the first message that qualifies."""
        assert self._target is not None
        if self._subscription is None:
            return NodeStatus.FAILURE

        ignore_retained = self._flag("ignore_retained")
        mode = str(self.get_input("mode", "on_message") or "on_message").strip().lower()

        while (message := self._subscription.get_nowait()) is not None:
            if ignore_retained and message.retain:
                logger.debug("skipping retained message on %s", message.topic)
                continue

            try:
                value = self.decode_message(message, self._target)
            except Exception as exc:
                self.set_feedback_message(
                    f"Cannot decode payload from {message.topic}: {exc}"
                )
                logger.warning("decode %s failed: %s", message.topic, exc)
                self._release()
                return NodeStatus.FAILURE

            if mode == "on_value":
                try:
                    matched = self.compare(value, self.get_input("expected_value"), self._target)
                except ValueError as exc:
                    self.set_feedback_message(str(exc))
                    self._release()
                    return NodeStatus.FAILURE
                if not matched:
                    logger.debug("discarding %r from %s — not the awaited value",
                                 value, message.topic)
                    continue

            self.store_value(value, self._target)
            self.set_feedback_message(f"Received {value!r} from {message.topic}")
            logger.info("received %s = %r", self._target.label, value)
            self._release()
            return NodeStatus.SUCCESS

        return self._check_timeout()

    def _check_timeout(self) -> NodeStatus:
        if self._deadline is not None and time.monotonic() >= self._deadline:
            label = self._target.label if self._target else "topic"
            self.set_feedback_message(f"Timed out waiting for a message on {label}")
            logger.warning("timed out waiting on %s", label)
            self._release()
            return NodeStatus.FAILURE
        return NodeStatus.RUNNING

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _release(self) -> None:
        """Drop the subscription unless the node is holding it on purpose."""
        if self._subscription is not None and not self._persistent:
            self._subscription.close()
            self._subscription = None

    def _compute_deadline(self) -> float | None:
        try:
            timeout = float(self.get_input("timeout", 0.0))
        except (TypeError, ValueError):
            timeout = 0.0
        return time.monotonic() + timeout if timeout > 0.0 else None

    def _flag(self, port: str) -> bool:
        raw = self.get_input(port, False)
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in ("true", "1", "yes", "on")
