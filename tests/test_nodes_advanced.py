"""MqttReconnect, MqttUnsubscribe, MqttBatchPublish, MqttBatchRead,
MqttRequestResponse — the milestone 4 nodes."""
from __future__ import annotations

import time

import pytest
from bteng import NodeStatus

from bteng_mqtt.nodes.batch_publish import MqttBatchPublish
from bteng_mqtt.nodes.batch_read import MqttBatchRead
from bteng_mqtt.nodes.reconnect import MqttReconnect
from bteng_mqtt.nodes.request_response import MqttRequestResponse
from bteng_mqtt.nodes.subscribe import MqttSubscribe
from bteng_mqtt.nodes.unsubscribe import MqttUnsubscribe
from tests.helpers import DEFAULT_TOPICS, make_node, registry_with, settle

REQUEST_TOPICS = [
    *DEFAULT_TOPICS,
    {"alias": "calibrate", "topic": "a/req/calibrate", "payload_type": "json",
     "response_topic": "a/res/calibrate", "response_payload_type": "json",
     "timeout": 2.0},
    {"alias": "ping", "topic": "a/req/ping", "payload_type": "string",
     "response_topic": "a/res/ping", "timeout": 2.0},
    {"alias": "no_reply", "topic": "a/req/lonely", "payload_type": "string"},
]


class TestReconnect:
    def test_reconnects_after_a_disconnect(self, connected, backend):
        connected.unregister_client("default")
        node = make_node(MqttReconnect, params={"client_id": "default"})
        assert settle(node) is NodeStatus.SUCCESS
        assert connected.is_connected("default")

    def test_already_connected_is_a_no_op(self, connected, backend):
        node = make_node(MqttReconnect, params={"client_id": "default"})
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert "already connected" in node.feedback_message
        assert len(backend.clients) == 1

    def test_force_reconnects_a_live_client(self, connected, backend):
        node = make_node(MqttReconnect, params={"client_id": "default", "force": "true"})
        assert settle(node) is NodeStatus.SUCCESS
        assert connected.is_connected("default")
        assert len(backend.clients) == 1   # the old one was torn down

    def test_without_a_saved_config_it_fails(self, manager):
        node = make_node(MqttReconnect, params={"client_id": "never"})
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "No saved config" in node.feedback_message

    def test_subscriptions_are_reissued(self, connected, backend):
        """The broker forgets subscriptions; the manager must re-send them."""
        subscribe = make_node(MqttSubscribe, params={
            "topic_alias": "label", "persistent": "true", "timeout": "2.0",
        })
        subscribe.execute_tick()
        connected.unregister_client("default")

        reconnect = make_node(MqttReconnect, params={"client_id": "default"})
        assert settle(reconnect) is NodeStatus.SUCCESS

        backend.inject("a/label", "after")
        assert subscribe.execute_tick() is NodeStatus.SUCCESS
        assert subscribe.blackboard.get("label") == "after"


class TestUnsubscribe:
    def test_releases_a_persistent_subscription(self, connected, backend):
        subscribe = make_node(MqttSubscribe, params={
            "topic_alias": "label", "persistent": "true", "timeout": "2.0",
        })
        subscribe.execute_tick()
        assert connected.subscriber_count("default", "a/label") == 1

        node = make_node(MqttUnsubscribe, params={"topic_alias": "label"})
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert connected.subscriber_count("default", "a/label") == 0
        assert backend.clients[0].subscriptions == {}
        assert "1 holder" in node.feedback_message

    def test_nothing_subscribed_is_still_success(self, connected):
        node = make_node(MqttUnsubscribe, params={"topic_alias": "label"})
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert "Nothing was subscribed" in node.feedback_message

    def test_cached_value_survives_by_default(self, connected, backend):
        connected.subscribe("default", "a/label")
        backend.inject("a/label", "kept")
        node = make_node(MqttUnsubscribe, params={"topic_alias": "label"})
        node.execute_tick()
        assert connected.latest("default", "a/label") is not None

    def test_clear_cached_forgets_it(self, connected, backend):
        connected.subscribe("default", "a/label")
        backend.inject("a/label", "gone")
        node = make_node(MqttUnsubscribe, params={
            "topic_alias": "label", "clear_cached": "true",
        })
        node.execute_tick()
        assert connected.latest("default", "a/label") is None


class TestBatchPublish:
    def test_publishes_values_from_the_blackboard(self, connected, backend):
        node = make_node(MqttBatchPublish, params={"topic_aliases": "temp,count,label"})
        node.blackboard.set("temp", 21.5)
        node.blackboard.set("count", 7)
        node.blackboard.set("label", "hello")

        assert settle(node) is NodeStatus.SUCCESS
        assert backend.payloads_for("a/temp") == [b"21.5"]
        assert backend.payloads_for("a/count") == [b"7"]
        assert backend.payloads_for("a/label") == [b"hello"]

    def test_literal_values_port(self, connected, backend):
        node = make_node(MqttBatchPublish, params={
            "topic_aliases": "temp,count", "values": "1.5,3",
        })
        assert settle(node) is NodeStatus.SUCCESS
        assert backend.payloads_for("a/temp") == [b"1.5"]
        assert backend.payloads_for("a/count") == [b"3"]

    def test_input_keys_override_the_alias_names(self, connected, backend):
        node = make_node(MqttBatchPublish, params={
            "topic_aliases": "temp,count", "input_keys": "t,c",
        })
        node.blackboard.set("t", 2.0)
        node.blackboard.set("c", 9)
        assert settle(node) is NodeStatus.SUCCESS
        assert backend.payloads_for("a/temp") == [b"2.0"]
        assert backend.payloads_for("a/count") == [b"9"]

    def test_each_topic_keeps_its_own_qos(self, connected, backend):
        node = make_node(MqttBatchPublish, params={
            "topic_aliases": "temp,label", "values": "1.0,x",
        })
        assert settle(node) is NodeStatus.SUCCESS
        by_topic = {r.topic: r.qos for r in backend.published}
        assert by_topic["a/temp"] == 1     # registry says qos 1
        assert by_topic["a/label"] == 0

    def test_value_map_still_applies(self, connected, backend):
        node = make_node(MqttBatchPublish, params={
            "topic_aliases": "cmd", "values": "start",
        })
        assert settle(node) is NodeStatus.SUCCESS
        assert backend.payloads_for("a/cmd") == [b'{"cmd": "start"}']

    def test_a_bad_value_publishes_nothing_at_all(self, connected, backend):
        """Half-applied batches are the failure mode worth preventing."""
        node = make_node(MqttBatchPublish, params={
            "topic_aliases": "label,count", "values": "ok,not-a-number",
        })
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "Cannot encode" in node.feedback_message
        assert backend.published == []

    def test_mismatched_list_lengths(self, connected):
        node = make_node(MqttBatchPublish, params={
            "topic_aliases": "temp,count", "values": "1.0",
        })
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "must line up" in node.feedback_message

    def test_unset_blackboard_key(self, connected):
        node = make_node(MqttBatchPublish, params={"topic_aliases": "temp"})
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "is unset" in node.feedback_message

    def test_empty_alias_list(self, connected):
        node = make_node(MqttBatchPublish, params={"topic_aliases": ""})
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "is empty" in node.feedback_message

    def test_unknown_alias(self, connected):
        node = make_node(MqttBatchPublish, params={
            "topic_aliases": "temp,nope", "values": "1.0,2.0",
        })
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "Unknown topic alias" in node.feedback_message

    def test_wildcard_alias_is_refused(self, connected):
        node = make_node(MqttBatchPublish, params={
            "topic_aliases": "sensors", "values": "1.0",
        })
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "wildcard" in node.feedback_message

    def test_pending_acknowledgement_times_out(self, connected, monkeypatch):
        class PendingTicket:
            rc = 0

            def is_published(self) -> bool:
                return False

        monkeypatch.setattr(connected.backend, "publish", lambda *a, **kw: PendingTicket())
        node = make_node(MqttBatchPublish, params={
            "topic_aliases": "temp", "values": "1.0", "timeout": "0.05",
        })
        assert node.execute_tick() is NodeStatus.RUNNING
        assert settle(node) is NodeStatus.FAILURE
        assert "Not acknowledged" in node.feedback_message


class TestBatchRead:
    @pytest.fixture
    def primed(self, connected, backend):
        for topic, payload in [("a/temp", "21.5"), ("a/count", "7"), ("a/label", "hi")]:
            connected.subscribe("default", topic)
            backend.inject(topic, payload)
        return connected

    def test_copies_cached_values_to_the_blackboard(self, primed):
        node = make_node(MqttBatchRead, params={"topic_aliases": "temp,count,label"})
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert node.blackboard.get("temp") == 21.5
        assert node.blackboard.get("count") == 7
        assert node.blackboard.get("label") == "hi"

    def test_output_keys_override_the_alias_names(self, primed):
        node = make_node(MqttBatchRead, params={
            "topic_aliases": "temp,count", "output_keys": "t,c",
        })
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert node.blackboard.get("t") == 21.5
        assert node.blackboard.get("c") == 7

    def test_missing_value_fails_by_default(self, connected):
        node = make_node(MqttBatchRead, params={"topic_aliases": "temp"})
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "never published" in node.feedback_message

    def test_require_all_false_takes_what_it_can(self, primed):
        node = make_node(MqttBatchRead, params={
            "topic_aliases": "temp,flag", "require_all": "false",
        })
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert node.blackboard.get("temp") == 21.5
        assert node.blackboard.get("flag") is None
        assert "skipped" in node.feedback_message

    def test_max_age_skips_stale_values(self, primed):
        time.sleep(0.05)
        node = make_node(MqttBatchRead, params={
            "topic_aliases": "temp", "max_age": "0.01",
        })
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "old" in node.feedback_message

    def test_undecodable_value_fails(self, connected, backend):
        connected.subscribe("default", "a/temp")
        backend.inject("a/temp", "warm-ish")
        node = make_node(MqttBatchRead, params={"topic_aliases": "temp"})
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "Cannot decode" in node.feedback_message

    def test_settles_in_one_tick(self, primed):
        node = make_node(MqttBatchRead, params={"topic_aliases": "temp"})
        assert node.execute_tick() is NodeStatus.SUCCESS


class TestRequestResponse:
    def test_reply_is_decoded_and_stored(self, connected, backend):
        node = make_node(
            MqttRequestResponse,
            params={"topic_alias": "calibrate", "value": '{"axis": 1}'},
            registry=registry_with(REQUEST_TOPICS),
        )
        assert node.execute_tick() is NodeStatus.RUNNING
        assert backend.payloads_for("a/req/calibrate") == [b'{"axis": 1}']

        backend.inject("a/res/calibrate", '{"ok": true}')
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert node.blackboard.get("calibrate") == {"ok": True}

    def test_response_type_defaults_to_the_request_type(self, connected, backend):
        node = make_node(
            MqttRequestResponse,
            params={"topic_alias": "ping", "value": "hello"},
            registry=registry_with(REQUEST_TOPICS),
        )
        node.execute_tick()
        backend.inject("a/res/ping", "pong")
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert node.blackboard.get("ping") == "pong"

    def test_subscription_precedes_the_request(self, connected, backend):
        """A device replying instantly must not be able to outrun the subscribe."""
        replies = []

        original_publish = connected.backend.publish

        def reply_immediately(client, topic, payload, qos=0, retain=False):
            ticket = original_publish(client, topic, payload, qos=qos, retain=retain)
            if topic == "a/req/ping":
                replies.append(topic)
                backend.inject("a/res/ping", "instant")
            return ticket

        connected.backend.publish = reply_immediately  # type: ignore[method-assign]
        try:
            node = make_node(
                MqttRequestResponse,
                params={"topic_alias": "ping", "value": "hi"},
                registry=registry_with(REQUEST_TOPICS),
            )
            assert node.execute_tick() is NodeStatus.SUCCESS
            assert node.blackboard.get("ping") == "instant"
        finally:
            connected.backend.publish = original_publish  # type: ignore[method-assign]
        assert replies == ["a/req/ping"]

    def test_json_path_on_the_reply(self, connected, backend):
        node = make_node(
            MqttRequestResponse,
            params={"topic_alias": "calibrate", "value": "{}",
                    "response_json_path": "result.state"},
            registry=registry_with(REQUEST_TOPICS),
        )
        node.execute_tick()
        backend.inject("a/res/calibrate", '{"result": {"state": "done"}}')
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert node.blackboard.get("calibrate") == "done"

    def test_timeout_from_the_registry(self, connected):
        node = make_node(
            MqttRequestResponse,
            params={"topic_alias": "ping", "value": "hi", "timeout": "0.05"},
            registry=registry_with(REQUEST_TOPICS),
        )
        assert settle(node) is NodeStatus.FAILURE
        assert "No reply" in node.feedback_message

    def test_alias_without_a_response_topic(self, connected):
        node = make_node(
            MqttRequestResponse,
            params={"topic_alias": "no_reply", "value": "hi"},
            registry=registry_with(REQUEST_TOPICS),
        )
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "No response topic" in node.feedback_message

    def test_response_topic_port_supplies_one(self, connected, backend):
        node = make_node(
            MqttRequestResponse,
            params={"topic_alias": "no_reply", "value": "hi",
                    "response_topic": "a/res/lonely", "timeout": "2.0"},
            registry=registry_with(REQUEST_TOPICS),
        )
        node.execute_tick()
        backend.inject("a/res/lonely", "answered")
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert node.blackboard.get("no_reply") == "answered"

    def test_missing_value(self, connected):
        node = make_node(
            MqttRequestResponse,
            params={"topic_alias": "ping"},
            registry=registry_with(REQUEST_TOPICS),
        )
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "'value' is unset" in node.feedback_message

    def test_undecodable_reply(self, connected, backend):
        node = make_node(
            MqttRequestResponse,
            params={"topic_alias": "calibrate", "value": "{}", "timeout": "2.0"},
            registry=registry_with(REQUEST_TOPICS),
        )
        node.execute_tick()
        backend.inject("a/res/calibrate", "not json")
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "Cannot decode the reply" in node.feedback_message

    def test_subscription_is_released_on_success(self, connected, backend):
        node = make_node(
            MqttRequestResponse,
            params={"topic_alias": "ping", "value": "hi", "timeout": "2.0"},
            registry=registry_with(REQUEST_TOPICS),
        )
        node.execute_tick()
        assert connected.subscriber_count("default", "a/res/ping") == 1
        backend.inject("a/res/ping", "pong")
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert connected.subscriber_count("default", "a/res/ping") == 0

    def test_halt_releases_the_subscription(self, connected):
        node = make_node(
            MqttRequestResponse,
            params={"topic_alias": "ping", "value": "hi", "timeout": "5.0"},
            registry=registry_with(REQUEST_TOPICS),
        )
        node.execute_tick()
        node.halt()
        assert connected.subscriber_count("default", "a/res/ping") == 0

    def test_no_connection(self, manager):
        node = make_node(
            MqttRequestResponse,
            params={"topic_alias": "ping", "value": "hi"},
            registry=registry_with(REQUEST_TOPICS),
        )
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "not connected" in node.feedback_message
