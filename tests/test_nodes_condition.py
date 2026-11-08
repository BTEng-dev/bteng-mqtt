"""MqttWaitForValue, MqttCondition and Sleep."""
from __future__ import annotations

import time

from bteng import NodeStatus

from bteng_mqtt.nodes.condition import MqttCondition
from bteng_mqtt.nodes.sleep import Sleep
from bteng_mqtt.nodes.wait_for_value import MqttWaitForValue
from tests.helpers import make_node, settle


class TestWaitForValue:
    def test_cached_value_settles_on_the_first_tick(self, connected, backend):
        """A condition that is already true must not wait for the next publish."""
        connected.subscribe("default", "a/temp")
        backend.inject("a/temp", "42.0")

        node = make_node(MqttWaitForValue, params={
            "topic_alias": "temp", "expected_value": "42.0", "timeout": "2.0",
        })
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert node.blackboard.get("temp") == 42.0

    def test_waits_for_a_fresh_message_when_the_cache_does_not_match(self, connected, backend):
        node = make_node(MqttWaitForValue, params={
            "topic_alias": "count", "expected_value": "3", "timeout": "2.0",
        })
        assert node.execute_tick() is NodeStatus.RUNNING
        backend.inject("a/count", "1")
        assert node.execute_tick() is NodeStatus.RUNNING
        backend.inject("a/count", "3")
        assert node.execute_tick() is NodeStatus.SUCCESS

    def test_use_cached_false_requires_a_new_message(self, connected, backend):
        connected.subscribe("default", "a/label")
        backend.inject("a/label", "ready")

        node = make_node(MqttWaitForValue, params={
            "topic_alias": "label", "expected_value": "ready",
            "use_cached": "false", "timeout": "2.0",
        })
        assert node.execute_tick() is NodeStatus.RUNNING
        backend.inject("a/label", "ready")
        assert node.execute_tick() is NodeStatus.SUCCESS

    def test_max_age_rejects_a_stale_cached_value(self, connected, backend):
        connected.subscribe("default", "a/label")
        backend.inject("a/label", "old")
        time.sleep(0.05)

        node = make_node(MqttWaitForValue, params={
            "topic_alias": "label", "expected_value": "old",
            "max_age": "0.01", "timeout": "0.05",
        })
        assert settle(node) is NodeStatus.FAILURE
        assert "Timed out" in node.feedback_message

    def test_numeric_operators(self, connected, backend):
        node = make_node(MqttWaitForValue, params={
            "topic_alias": "temp", "operator": "gt",
            "expected_value": "30", "timeout": "2.0",
        })
        node.execute_tick()
        backend.inject("a/temp", "29.9")
        assert node.execute_tick() is NodeStatus.RUNNING
        backend.inject("a/temp", "30.1")
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert node.blackboard.get("temp") == 30.1

    def test_contains_operator(self, connected, backend):
        node = make_node(MqttWaitForValue, params={
            "topic_alias": "label", "operator": "contains",
            "expected_value": "err", "timeout": "2.0",
        })
        node.execute_tick()
        backend.inject("a/label", "all good")
        assert node.execute_tick() is NodeStatus.RUNNING
        backend.inject("a/label", "an error occurred")
        assert node.execute_tick() is NodeStatus.SUCCESS

    def test_timeout_reports_what_it_wanted(self, connected):
        node = make_node(MqttWaitForValue, params={
            "topic_alias": "label", "expected_value": "never", "timeout": "0.05",
        })
        assert settle(node) is NodeStatus.FAILURE
        assert "'never'" in node.feedback_message

    def test_subscription_is_released(self, connected, backend):
        node = make_node(MqttWaitForValue, params={
            "topic_alias": "label", "expected_value": "x", "timeout": "2.0",
        })
        node.execute_tick()
        assert connected.subscriber_count("default", "a/label") == 1
        backend.inject("a/label", "x")
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert connected.subscriber_count("default", "a/label") == 0

    def test_halt_releases_the_subscription(self, connected):
        node = make_node(MqttWaitForValue, params={
            "topic_alias": "label", "expected_value": "x", "timeout": "5.0",
        })
        node.execute_tick()
        node.halt()
        assert connected.subscriber_count("default", "a/label") == 0

    def test_no_connection(self, manager):
        node = make_node(MqttWaitForValue, params={"topic_alias": "label"})
        assert node.execute_tick() is NodeStatus.FAILURE


class TestCondition:
    def test_success_when_the_cached_value_matches(self, connected, backend):
        connected.subscribe("default", "a/flag")
        backend.inject("a/flag", "true")

        node = make_node(MqttCondition, params={
            "topic_alias": "flag", "expected_value": "true",
        })
        assert node.execute_tick() is NodeStatus.SUCCESS

    def test_failure_when_it_does_not_match(self, connected, backend):
        connected.subscribe("default", "a/flag")
        backend.inject("a/flag", "false")

        node = make_node(MqttCondition, params={
            "topic_alias": "flag", "expected_value": "true",
        })
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "Condition not met" in node.feedback_message

    def test_failure_with_nothing_cached_explains_the_fix(self, connected):
        node = make_node(MqttCondition, params={
            "topic_alias": "flag", "expected_value": "true",
        })
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "persistent" in node.feedback_message

    def test_it_never_blocks(self, connected):
        """A condition node must answer within one tick, cached or not."""
        node = make_node(MqttCondition, params={
            "topic_alias": "flag", "expected_value": "true",
        })
        start = time.monotonic()
        node.execute_tick()
        assert time.monotonic() - start < 0.5

    def test_retained_message_is_enough(self, connected):
        connected.publish("default", "a/label", b"ok", retain=True)
        connected.subscribe("default", "a/label")   # retained replay fills the cache

        node = make_node(MqttCondition, params={
            "topic_alias": "label", "expected_value": "ok",
        })
        assert node.execute_tick() is NodeStatus.SUCCESS

    def test_operators(self, connected, backend):
        connected.subscribe("default", "a/temp")
        backend.inject("a/temp", "45.0")

        for operator, expected, outcome in [
            ("gt", "40", NodeStatus.SUCCESS),
            ("lt", "40", NodeStatus.FAILURE),
            ("ge", "45", NodeStatus.SUCCESS),
            ("ne", "45", NodeStatus.FAILURE),
        ]:
            node = make_node(MqttCondition, params={
                "topic_alias": "temp", "operator": operator, "expected_value": expected,
            })
            assert node.execute_tick() is outcome, f"{operator} {expected}"

    def test_max_age_makes_a_stale_value_fail(self, connected, backend):
        connected.subscribe("default", "a/label")
        backend.inject("a/label", "ok")
        time.sleep(0.05)

        node = make_node(MqttCondition, params={
            "topic_alias": "label", "expected_value": "ok", "max_age": "0.01",
        })
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "stale" in node.feedback_message

    def test_json_path_condition(self, connected, backend):
        connected.subscribe("default", "a/state")
        backend.inject("a/state", '{"status": {"mode": "idle"}}')

        node = make_node(MqttCondition, params={
            "topic_alias": "state", "expected_value": "idle",
        })
        assert node.execute_tick() is NodeStatus.SUCCESS

    def test_no_connection(self, manager):
        node = make_node(MqttCondition, params={"topic_alias": "flag"})
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "not connected" in node.feedback_message


class TestSleep:
    def test_returns_running_then_success(self):
        node = make_node(Sleep, params={"duration": "0.05"})
        assert node.execute_tick() is NodeStatus.RUNNING
        assert settle(node) is NodeStatus.SUCCESS

    def test_zero_duration_settles_at_once(self):
        node = make_node(Sleep, params={"duration": "0"})
        assert node.execute_tick() is NodeStatus.SUCCESS

    def test_non_numeric_duration_fails(self):
        node = make_node(Sleep, params={"duration": "soon"})
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "must be a number" in node.feedback_message
