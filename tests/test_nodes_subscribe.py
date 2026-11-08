"""MqttSubscribe — waiting, matching, persistence, retained messages."""
from __future__ import annotations

from bteng import NodeStatus

from bteng_mqtt.nodes.subscribe import MqttSubscribe
from tests.helpers import make_node, settle


class TestOnMessage:
    def test_success_on_the_first_message(self, connected, backend):
        node = make_node(MqttSubscribe, params={"topic_alias": "label", "timeout": "2.0"})
        assert node.execute_tick() is NodeStatus.RUNNING     # subscribed, nothing yet

        backend.inject("a/label", "hello")
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert node.blackboard.get("label") == "hello"

    def test_decoded_by_payload_type(self, connected, backend):
        node = make_node(MqttSubscribe, params={"topic_alias": "temp", "timeout": "2.0"})
        node.execute_tick()
        backend.inject("a/temp", "21.5")
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert node.blackboard.get("temp") == 21.5

    def test_json_path_is_applied(self, connected, backend):
        node = make_node(MqttSubscribe, params={"topic_alias": "state", "timeout": "2.0"})
        node.execute_tick()
        backend.inject("a/state", '{"status": {"mode": "running"}}')
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert node.blackboard.get("state") == "running"

    def test_output_key_overrides_the_alias(self, connected, backend):
        node = make_node(MqttSubscribe, params={
            "topic_alias": "label", "output_key": "greeting", "timeout": "2.0",
        })
        node.execute_tick()
        backend.inject("a/label", "hi")
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert node.blackboard.get("greeting") == "hi"

    def test_output_port_mapping_wins(self, connected, backend):
        node = make_node(
            MqttSubscribe,
            params={"topic_alias": "label", "output_key": "ignored", "timeout": "2.0"},
            output_ports={"value": "mapped"},
        )
        node.execute_tick()
        backend.inject("a/label", "hi")
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert node.blackboard.get("mapped") == "hi"
        assert node.blackboard.get("ignored") is None

    def test_wildcard_alias(self, connected, backend):
        node = make_node(MqttSubscribe, params={"topic_alias": "sensors", "timeout": "2.0"})
        node.execute_tick()
        backend.inject("a/sensor/left", "3.5")
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert node.blackboard.get("sensors") == 3.5

    def test_message_arriving_between_ticks_is_not_lost(self, connected, backend):
        """The per-subscriber queue exists exactly for this."""
        node = make_node(MqttSubscribe, params={"topic_alias": "label", "timeout": "2.0"})
        node.execute_tick()
        for text in ("first", "second"):
            backend.inject("a/label", text)
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert node.blackboard.get("label") == "first"   # oldest first, none dropped


class TestOnValue:
    def test_waits_for_the_awaited_value(self, connected, backend):
        node = make_node(MqttSubscribe, params={
            "topic_alias": "label", "mode": "on_value",
            "expected_value": "go", "timeout": "2.0",
        })
        node.execute_tick()
        backend.inject("a/label", "wait")
        assert node.execute_tick() is NodeStatus.RUNNING   # discarded, still waiting
        backend.inject("a/label", "go")
        assert node.execute_tick() is NodeStatus.SUCCESS

    def test_expected_value_is_coerced_to_the_payload_type(self, connected, backend):
        node = make_node(MqttSubscribe, params={
            "topic_alias": "count", "mode": "on_value",
            "expected_value": "7", "timeout": "2.0",
        })
        node.execute_tick()
        backend.inject("a/count", "7")
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert node.blackboard.get("count") == 7

    def test_operator_port(self, connected, backend):
        node = make_node(MqttSubscribe, params={
            "topic_alias": "temp", "mode": "on_value", "operator": "ge",
            "expected_value": "30", "timeout": "2.0",
        })
        node.execute_tick()
        backend.inject("a/temp", "20.0")
        assert node.execute_tick() is NodeStatus.RUNNING
        backend.inject("a/temp", "31.5")
        assert node.execute_tick() is NodeStatus.SUCCESS

    def test_unknown_operator_is_a_failure(self, connected, backend):
        node = make_node(MqttSubscribe, params={
            "topic_alias": "label", "mode": "on_value", "operator": "approx",
            "expected_value": "x", "timeout": "2.0",
        })
        node.execute_tick()
        backend.inject("a/label", "x")
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "Unknown operator" in node.feedback_message


class TestTimeoutAndCleanup:
    def test_timeout_is_a_failure(self, connected):
        node = make_node(MqttSubscribe, params={"topic_alias": "label", "timeout": "0.05"})
        assert settle(node) is NodeStatus.FAILURE
        assert "Timed out" in node.feedback_message

    def test_subscription_is_released_on_success(self, connected, backend):
        node = make_node(MqttSubscribe, params={"topic_alias": "label", "timeout": "2.0"})
        node.execute_tick()
        assert connected.subscriber_count("default", "a/label") == 1
        backend.inject("a/label", "x")
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert connected.subscriber_count("default", "a/label") == 0

    def test_subscription_is_released_on_timeout(self, connected):
        node = make_node(MqttSubscribe, params={"topic_alias": "label", "timeout": "0.05"})
        settle(node)
        assert connected.subscriber_count("default", "a/label") == 0

    def test_halt_releases_the_subscription(self, connected):
        node = make_node(MqttSubscribe, params={"topic_alias": "label", "timeout": "5.0"})
        node.execute_tick()
        node.halt()
        assert connected.subscriber_count("default", "a/label") == 0

    def test_undecodable_payload_fails_and_cleans_up(self, connected, backend):
        node = make_node(MqttSubscribe, params={"topic_alias": "temp", "timeout": "2.0"})
        node.execute_tick()
        backend.inject("a/temp", "not-a-number")
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "Cannot decode" in node.feedback_message
        assert connected.subscriber_count("default", "a/temp") == 0

    def test_no_connection(self, manager):
        node = make_node(MqttSubscribe, params={"topic_alias": "label"})
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "not connected" in node.feedback_message


class TestPersistent:
    def test_keeps_the_subscription_after_success(self, connected, backend):
        node = make_node(MqttSubscribe, params={
            "topic_alias": "label", "persistent": "true", "timeout": "2.0",
        })
        node.execute_tick()
        backend.inject("a/label", "one")
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert connected.subscriber_count("default", "a/label") == 1

    def test_a_message_sent_while_idle_still_counts(self, connected, backend):
        node = make_node(MqttSubscribe, params={
            "topic_alias": "label", "persistent": "true", "timeout": "2.0",
        })
        node.execute_tick()
        backend.inject("a/label", "one")
        assert node.execute_tick() is NodeStatus.SUCCESS

        # Node is idle here — the tree is elsewhere — and a message arrives.
        backend.inject("a/label", "two")
        assert node.execute_tick() is NodeStatus.SUCCESS   # new activation, queued value
        assert node.blackboard.get("label") == "two"

    def test_non_persistent_resubscribes_each_activation(self, connected, backend):
        node = make_node(MqttSubscribe, params={"topic_alias": "label", "timeout": "2.0"})
        node.execute_tick()
        backend.inject("a/label", "one")
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert connected.subscriber_count("default", "a/label") == 0

        node.execute_tick()  # fresh activation, fresh subscription
        assert connected.subscriber_count("default", "a/label") == 1


class TestRetained:
    def test_retained_message_satisfies_by_default(self, connected, backend):
        connected.publish("default", "a/label", b"stored", retain=True)
        node = make_node(MqttSubscribe, params={"topic_alias": "label", "timeout": "2.0"})
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert node.blackboard.get("label") == "stored"

    def test_ignore_retained_waits_for_a_live_message(self, connected, backend):
        connected.publish("default", "a/label", b"stored", retain=True)
        node = make_node(MqttSubscribe, params={
            "topic_alias": "label", "ignore_retained": "true", "timeout": "2.0",
        })
        assert node.execute_tick() is NodeStatus.RUNNING
        backend.inject("a/label", "live")
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert node.blackboard.get("label") == "live"
