"""MqttPublish — encoding, QoS acknowledgement, failure paths."""
from __future__ import annotations

from bteng import NodeStatus

from bteng_mqtt.nodes.publish import MqttPublish
from tests.helpers import make_node, registry_with, settle


class TestPublish:
    def test_publishes_through_an_alias(self, connected, backend):
        node = make_node(MqttPublish, params={"topic_alias": "label", "value": "hello"})
        assert settle(node) is NodeStatus.SUCCESS
        assert backend.payloads_for("a/label") == [b"hello"]

    def test_encodes_per_the_declared_payload_type(self, connected, backend):
        node = make_node(MqttPublish, params={"topic_alias": "temp", "value": "21.5"})
        assert settle(node) is NodeStatus.SUCCESS
        assert backend.payloads_for("a/temp") == [b"21.5"]

    def test_bool_words_become_a_canonical_payload(self, connected, backend):
        node = make_node(MqttPublish, params={"topic_alias": "flag", "value": "yes"})
        assert settle(node) is NodeStatus.SUCCESS
        assert backend.payloads_for("a/flag") == [b"true"]

    def test_value_map_expands_a_tree_level_word(self, connected, backend):
        node = make_node(MqttPublish, params={"topic_alias": "cmd", "value": "start"})
        assert settle(node) is NodeStatus.SUCCESS
        assert backend.payloads_for("a/cmd") == [b'{"cmd": "start"}']

    def test_qos_and_retain_come_from_the_registry(self, connected, backend):
        node = make_node(MqttPublish, params={"topic_alias": "temp", "value": 1.0})
        assert settle(node) is NodeStatus.SUCCESS
        assert backend.published[-1].qos == 1

    def test_ports_override_qos_and_retain(self, connected, backend):
        node = make_node(MqttPublish, params={
            "topic_alias": "label", "value": "x", "qos": "2", "retain": "true",
        })
        assert settle(node) is NodeStatus.SUCCESS
        record = backend.published[-1]
        assert (record.qos, record.retain) == (2, True)

    def test_raw_topic_without_an_alias(self, connected, backend):
        node = make_node(MqttPublish, params={
            "topic": "raw/topic", "payload_type": "int", "value": "7",
        })
        assert settle(node) is NodeStatus.SUCCESS
        assert backend.payloads_for("raw/topic") == [b"7"]

    def test_value_from_the_blackboard(self, connected, backend):
        """An input port mapped to a blackboard key, as `value="{speed}"` compiles to."""
        from bteng import NodeConfig

        from tests.helpers import new_blackboard
        blackboard = new_blackboard()
        blackboard.set("speed", 1.25)
        config = NodeConfig(
            blackboard=blackboard,
            params={"topic_alias": "temp", "registry": registry_with()},
            input_ports={"value": "speed"},
        )
        node = MqttPublish("Publish", config)
        assert settle(node) is NodeStatus.SUCCESS
        assert backend.payloads_for("a/temp") == [b"1.25"]


class TestPublishFailures:
    def test_without_a_connection(self, manager):
        node = make_node(MqttPublish, params={"topic_alias": "label", "value": "x"})
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "not connected" in node.feedback_message

    def test_unknown_alias(self, connected):
        node = make_node(MqttPublish, params={"topic_alias": "nope", "value": "x"})
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "Unknown topic alias" in node.feedback_message

    def test_missing_value(self, connected):
        node = make_node(MqttPublish, params={"topic_alias": "label"})
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "'value' is unset" in node.feedback_message

    def test_wildcard_alias_cannot_be_published_to(self, connected):
        node = make_node(MqttPublish, params={"topic_alias": "sensors", "value": "1.0"})
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "wildcard" in node.feedback_message

    def test_unencodable_value(self, connected):
        node = make_node(MqttPublish, params={"topic_alias": "count", "value": "abc"})
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "Cannot encode" in node.feedback_message

    def test_neither_alias_nor_topic(self, connected):
        node = make_node(MqttPublish, params={"value": "x"})
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "Provide 'topic_alias'" in node.feedback_message

    def test_unknown_payload_type_on_a_raw_topic(self, connected):
        node = make_node(MqttPublish, params={
            "topic": "raw/x", "payload_type": "double", "value": "1",
        })
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "Unknown payload_type" in node.feedback_message


class TestAcknowledgement:
    """QoS > 0 must not report SUCCESS before the broker confirms."""

    def test_unacknowledged_publish_stays_running_then_times_out(self, connected, monkeypatch):
        class PendingTicket:
            rc = 0

            def is_published(self) -> bool:
                return False

        monkeypatch.setattr(
            connected.backend, "publish",
            lambda *a, **kw: PendingTicket(),
        )
        node = make_node(MqttPublish, params={
            "topic_alias": "temp", "value": 1.0, "timeout": "0.05",
        })
        assert node.execute_tick() is NodeStatus.RUNNING
        assert settle(node) is NodeStatus.FAILURE
        assert "not acknowledged" in node.feedback_message

    def test_wait_for_ack_false_settles_immediately(self, connected, monkeypatch):
        class PendingTicket:
            rc = 0

            def is_published(self) -> bool:
                return False

        monkeypatch.setattr(
            connected.backend, "publish",
            lambda *a, **kw: PendingTicket(),
        )
        node = make_node(MqttPublish, params={
            "topic_alias": "temp", "value": 1.0, "wait_for_ack": "false",
        })
        assert node.execute_tick() is NodeStatus.SUCCESS

    def test_qos_zero_never_waits(self, connected, monkeypatch):
        class PendingTicket:
            rc = 0

            def is_published(self) -> bool:
                return False

        monkeypatch.setattr(
            connected.backend, "publish",
            lambda *a, **kw: PendingTicket(),
        )
        node = make_node(MqttPublish, params={"topic_alias": "label", "value": "x"})
        assert node.execute_tick() is NodeStatus.SUCCESS
