"""MqttRequestResponse — publish a request and wait for the reply."""
from __future__ import annotations

import logging
import time

from bteng.core.node import InputPort, NodeConfig, NodeStatus, OutputPort, PortDefinition
from bteng.nodes.leaf.stateful_action import StatefulActionNode

from bteng_mqtt.client import Subscription
from bteng_mqtt.nodes.base import MqttNodeMixin, Target
from bteng_mqtt.types import MqttPayloadType, decode, encode

logger = logging.getLogger(__name__)


class MqttRequestResponse(MqttNodeMixin, StatefulActionNode):
    """Publish to a request topic and wait for a reply on the response topic.

    This is the ``OpcUaCallMethod`` analog — MQTT has no methods, so the pattern
    is a topic pair.  The registry declares it::

        - alias: calibrate
          topic: robot/req/calibrate
          payload_type: json
          response_topic: robot/res/calibrate
          response_payload_type: json
          timeout: 10.0

    and a tree calls it with one line.  The reply is decoded with
    ``response_payload_type`` (defaulting to the request's type) and lands on the
    blackboard under ``output_key``, or the alias.

    The response subscription is made **before** the request goes out, so a
    device that answers instantly cannot beat us to it.

    Correlation: the first message on the response topic after the request is
    taken as the reply.  MQTT 3.1.1 has no correlation data, so a device that
    serves several callers on one response topic can hand this node someone
    else's answer — give each caller its own response topic if that matters.
    MQTT 5 ``Correlation-Data`` support is planned separately.
    """

    def __init__(self, name: str, config: NodeConfig | None = None) -> None:
        super().__init__(name, config)
        self._subscription: Subscription | None = None
        self._target: Target | None = None
        self._response_type: MqttPayloadType = MqttPayloadType.STRING
        self._response_path: str | None = None
        self._deadline: float = 0.0

    @classmethod
    def provided_ports(cls) -> list[PortDefinition]:
        return [
            InputPort("client_id",       "Client identifier from MqttConnect",     default="default"),
            InputPort("topic_alias",     "Registry alias carrying a response_topic", default=""),
            InputPort("topic",           "Raw request topic, when no alias is used", default=""),
            InputPort("payload_type",    "Request payload type",                    default=""),
            InputPort("response_topic",  "Raw response topic (overrides the registry)", default=""),
            InputPort("response_payload_type", "Reply payload type (default: the request's)",
                      default=""),
            InputPort("response_json_path", "Field to extract from a json reply",   default=""),
            InputPort("qos",             "QoS for the request and the subscription", default=None),
            InputPort("value",           "Request payload (mapped through value_map)", default=None),
            InputPort("timeout",         "Seconds to wait for the reply (0 = forever)", default=None),
            InputPort("output_key",      "Blackboard key for the reply",            default=""),
            OutputPort("value",          "The decoded reply"),
        ]

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def on_start(self) -> NodeStatus:
        client_id = self.require_connected_client()
        if client_id is None:
            return NodeStatus.FAILURE

        target = self.resolve_target(for_publish=True)
        if target is None:
            return NodeStatus.FAILURE
        self._target = target

        response_topic = self._response_topic(target)
        if response_topic is None:
            return NodeStatus.FAILURE
        self._response_type = self._resolve_response_type(target)
        self._response_path = self._resolve_response_path(target)

        raw = self.get_input("value", None)
        if raw is None:
            self.set_feedback_message(
                f"Nothing to request — port 'value' is unset ({target.label})"
            )
            return NodeStatus.FAILURE
        if target.definition is not None:
            raw = target.definition.resolve_value(raw)
        try:
            payload = encode(raw, target.payload_type)
        except Exception as exc:
            self.set_feedback_message(f"Cannot encode {raw!r} for {target.label}: {exc}")
            return NodeStatus.FAILURE

        # Subscribe first: a device that replies immediately must not outrun us.
        try:
            self._subscription = self.manager.subscribe(client_id, response_topic, target.qos)
        except Exception as exc:
            self.set_feedback_message(f"Cannot subscribe to {response_topic}: {exc}")
            return NodeStatus.FAILURE

        try:
            self.manager.publish(client_id, target.topic, payload,
                                 qos=target.qos, retain=False)
        except Exception as exc:
            self.set_feedback_message(f"Request to {target.label} failed: {exc}")
            logger.warning("request %s failed: %s", target.label, exc)
            self._close()
            return NodeStatus.FAILURE

        timeout = self._timeout(target)
        self._deadline = time.monotonic() + timeout if timeout > 0 else 0.0
        logger.info("requested %s = %r, awaiting %s", target.label, raw, response_topic)
        return self._poll()

    def on_running(self) -> NodeStatus:
        return self._poll()

    def on_halted(self) -> None:
        self._close()

    # ── Reply handling ────────────────────────────────────────────────────────

    def _poll(self) -> NodeStatus:
        assert self._target is not None
        if self._subscription is None:
            return NodeStatus.FAILURE

        message = self._subscription.get_nowait()
        if message is not None:
            try:
                value = decode(message.payload, self._response_type, self._response_path)
            except Exception as exc:
                self.set_feedback_message(
                    f"Cannot decode the reply from {message.topic}: {exc}"
                )
                logger.warning("reply decode failed: %s", exc)
                self._close()
                return NodeStatus.FAILURE
            self.store_value(value, self._target)
            self.set_feedback_message(f"{self._target.label} replied {value!r}")
            logger.info("%s replied %r", self._target.label, value)
            self._close()
            return NodeStatus.SUCCESS

        if self._deadline and time.monotonic() >= self._deadline:
            self.set_feedback_message(
                f"No reply to {self._target.label} within "
                f"{self._timeout(self._target):.1f}s"
            )
            logger.warning("no reply to %s", self._target.label)
            self._close()
            return NodeStatus.FAILURE
        return NodeStatus.RUNNING

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _response_topic(self, target: Target) -> str | None:
        explicit = str(self.get_input("response_topic", "") or "").strip()
        if explicit:
            return explicit
        if target.definition is not None and target.definition.response_topic:
            return target.definition.response_topic
        self.set_feedback_message(
            f"No response topic for '{target.label}' — set one in the registry "
            "or pass the 'response_topic' port"
        )
        return None

    def _resolve_response_type(self, target: Target) -> MqttPayloadType:
        raw = str(self.get_input("response_payload_type", "") or "").strip()
        if raw:
            try:
                return MqttPayloadType(raw)
            except ValueError:
                logger.warning("unknown response_payload_type %r — using the request's", raw)
        if target.definition is not None:
            return target.definition.response_type
        return target.payload_type

    def _resolve_response_path(self, target: Target) -> str | None:
        explicit = str(self.get_input("response_json_path", "") or "").strip()
        if explicit:
            return explicit
        # Only inherit the request's json_path when both sides are json and the
        # registry did not declare a different reply type.
        if (target.definition is not None
                and target.definition.response_payload_type is None
                and self._response_type is MqttPayloadType.JSON):
            return target.json_path
        return None

    def _timeout(self, target: Target) -> float:
        raw = self.get_input("timeout", None)
        if raw is not None and raw != "":
            try:
                return float(raw)
            except (TypeError, ValueError):
                pass
        if target.definition is not None and target.definition.timeout is not None:
            return target.definition.timeout
        return 10.0

    def _close(self) -> None:
        if self._subscription is not None:
            self._subscription.close()
            self._subscription = None
