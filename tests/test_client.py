"""Client manager — connections, routing, refcounted subscriptions, caches."""
from __future__ import annotations

import pytest

from bteng_mqtt import MockBackend, MqttClientManager
from bteng_mqtt.exceptions import ClientNotFoundError, ConnectionError
from bteng_mqtt.registry import BrokerConfig


class TestConnect:
    def test_connect_registers_the_client(self, manager, config):
        manager.connect("main", config)
        assert manager.is_connected("main")
        assert manager.active_clients() == ["main"]

    def test_connect_is_idempotent(self, manager, config, backend):
        manager.connect("main", config)
        manager.connect("main", config)
        assert len(backend.clients) == 1

    def test_broker_refusal_raises(self, config):
        manager = MqttClientManager(backend=MockBackend(connect_rc=5))
        try:
            with pytest.raises(ConnectionError, match="refused the connection"):
                manager.connect("main", config)
            assert not manager.is_connected("main")
        finally:
            manager.close()

    def test_missing_connack_times_out(self):
        manager = MqttClientManager(backend=MockBackend(auto_connack=False))
        try:
            with pytest.raises(ConnectionError, match="no CONNACK"):
                manager.connect("main", BrokerConfig(timeout=0.2))
        finally:
            manager.close()

    def test_synchronous_backend_failure_is_wrapped(self, config):
        manager = MqttClientManager(backend=MockBackend(raise_on_connect=OSError("no route")))
        try:
            with pytest.raises(ConnectionError, match="no route"):
                manager.connect("main", config)
        finally:
            manager.close()

    def test_connect_async_returns_a_pollable_future(self, manager, config):
        future = manager.connect_async("main", config)
        assert future.result(timeout=5) is None
        assert manager.is_connected("main")

    def test_several_named_clients_coexist(self, manager, config):
        manager.connect("a", config)
        manager.connect("b", config)
        assert sorted(manager.active_clients()) == ["a", "b"]


class TestDisconnect:
    def test_disconnect_forgets_the_config(self, connected):
        connected.disconnect("default")
        assert not connected.is_connected("default")
        assert connected.get_client_config("default") is None

    def test_disconnect_is_safe_when_absent(self, manager):
        manager.disconnect("never-existed")  # no raise

    def test_unregister_keeps_the_config_for_reconnect(self, connected):
        connected.unregister_client("default")
        assert not connected.is_connected("default")
        assert connected.get_client_config("default") is not None

    def test_reconnect_after_unregister(self, connected, backend):
        connected.unregister_client("default")
        connected.reconnect("default")
        assert connected.is_connected("default")
        assert len(backend.clients) == 1

    def test_reconnect_without_a_config_fails(self, manager):
        with pytest.raises(ConnectionError, match="No saved config"):
            manager.reconnect("nope")

    def test_get_client_of_unknown_id(self, manager):
        with pytest.raises(ClientNotFoundError, match="Active clients"):
            manager.get_client("nope")


class TestPublish:
    def test_publish_reaches_the_backend(self, connected, backend):
        ticket = connected.publish("default", "a/b", b"7", qos=1)
        assert ticket.is_published()
        assert backend.payloads_for("a/b") == [b"7"]

    def test_publish_carries_qos_and_retain(self, connected, backend):
        connected.publish("default", "a/b", b"1", qos=2, retain=True)
        record = backend.published[-1]
        assert (record.qos, record.retain) == (2, True)

    def test_publish_needs_a_client(self, manager):
        with pytest.raises(ClientNotFoundError):
            manager.publish("nope", "a/b", b"1")

    def test_retained_payload_is_stored_and_cleared(self, connected, backend):
        connected.publish("default", "a/b", b"1", retain=True)
        assert "a/b" in backend.retained
        connected.publish("default", "a/b", b"", retain=True)
        assert "a/b" not in backend.retained


class TestSubscribe:
    def test_message_reaches_the_subscriber(self, connected, backend):
        sub = connected.subscribe("default", "a/b")
        backend.inject("a/b", "hello")
        message = sub.get_nowait()
        assert message is not None
        assert message.payload == b"hello"
        assert message.topic == "a/b"

    def test_wildcard_subscription(self, connected, backend):
        sub = connected.subscribe("default", "a/+")
        backend.inject("a/b", "1")
        backend.inject("a/c", "2")
        backend.inject("other", "3")
        assert [m.topic for m in sub.drain()] == ["a/b", "a/c"]

    def test_queue_buffers_between_ticks(self, connected, backend):
        """The point of per-subscriber queues: nothing is lost while not polling."""
        sub = connected.subscribe("default", "a/b")
        for i in range(3):
            backend.inject("a/b", str(i))
        assert [m.payload for m in sub.drain()] == [b"0", b"1", b"2"]
        assert sub.empty

    def test_two_subscribers_share_one_broker_subscription(self, connected, backend):
        first = connected.subscribe("default", "a/b")
        second = connected.subscribe("default", "a/b")
        assert connected.subscriber_count("default", "a/b") == 2
        assert list(backend.clients[0].subscriptions) == ["a/b"]

        backend.inject("a/b", "x")
        assert first.get_nowait() is not None
        assert second.get_nowait() is not None

    def test_broker_subscription_survives_until_the_last_holder_closes(self, connected, backend):
        first = connected.subscribe("default", "a/b")
        second = connected.subscribe("default", "a/b")
        first.close()
        assert list(backend.clients[0].subscriptions) == ["a/b"]
        second.close()
        assert backend.clients[0].subscriptions == {}
        assert connected.subscriber_count("default", "a/b") == 0

    def test_closed_subscription_stops_receiving(self, connected, backend):
        sub = connected.subscribe("default", "a/b")
        sub.close()
        backend.inject("a/b", "x")
        assert sub.get_nowait() is None

    def test_close_is_idempotent(self, connected):
        sub = connected.subscribe("default", "a/b")
        sub.close()
        sub.close()
        assert sub.closed

    def test_context_manager_closes(self, connected, backend):
        with connected.subscribe("default", "a/b") as sub:
            assert not sub.closed
        assert sub.closed
        assert backend.clients[0].subscriptions == {}

    def test_qos_upgrade_resubscribes(self, connected, backend):
        connected.subscribe("default", "a/b", qos=0)
        connected.subscribe("default", "a/b", qos=2)
        assert backend.clients[0].subscriptions["a/b"] == 2

    def test_clear_discards_pending_messages(self, connected, backend):
        sub = connected.subscribe("default", "a/b")
        backend.inject("a/b", "stale")
        sub.clear()
        assert sub.get_nowait() is None

    def test_retained_message_arrives_on_subscribe(self, connected, backend):
        connected.publish("default", "a/b", b"stored", retain=True)
        sub = connected.subscribe("default", "a/b")
        message = sub.get_nowait()
        assert message is not None
        assert message.retain is True

    def test_subscribe_needs_a_client(self, manager):
        with pytest.raises(ClientNotFoundError):
            manager.subscribe("nope", "a/b")

    def test_resubscribe_after_reconnect(self, connected, backend):
        """paho reconnects on its own and the broker forgot the subscription."""
        sub = connected.subscribe("default", "a/b")
        connected.unregister_client("default")
        connected.reconnect("default")
        backend.inject("a/b", "after")
        message = sub.get_nowait()
        assert message is not None
        assert message.payload == b"after"


class TestLatestCache:
    def test_latest_is_recorded(self, connected, backend):
        connected.subscribe("default", "a/b")
        backend.inject("a/b", "42")
        message = connected.latest("default", "a/b")
        assert message is not None
        assert message.payload == b"42"
        assert message.age >= 0

    def test_latest_keeps_only_the_newest(self, connected, backend):
        connected.subscribe("default", "a/b")
        backend.inject("a/b", "old")
        backend.inject("a/b", "new")
        assert connected.latest("default", "a/b").payload == b"new"

    def test_latest_is_none_before_anything_arrives(self, connected):
        assert connected.latest("default", "a/b") is None

    def test_latest_matching_uses_wildcards(self, connected, backend):
        connected.subscribe("default", "a/#")
        backend.inject("a/b", "1")
        backend.inject("a/c", "2")
        message = connected.latest_matching("default", "a/+")
        assert message is not None
        assert message.payload == b"2"

    def test_cache_is_per_client(self, manager, config, backend):
        manager.connect("a", config)
        manager.connect("b", config)
        manager.subscribe("a", "t")
        backend.inject("t", "1")
        assert manager.latest("a", "t") is not None
        assert manager.latest("b", "t") is None

    def test_disconnect_clears_the_cache(self, connected, backend):
        connected.subscribe("default", "a/b")
        backend.inject("a/b", "1")
        connected.disconnect("default")
        assert connected.latest("default", "a/b") is None

    def test_clear_latest(self, connected, backend):
        connected.subscribe("default", "a/b")
        backend.inject("a/b", "1")
        connected.clear_latest("default")
        assert connected.latest("default", "a/b") is None


class TestSingleton:
    def test_get_instance_is_stable(self):
        try:
            assert MqttClientManager.get_instance() is MqttClientManager.get_instance()
        finally:
            MqttClientManager.reset_instance()

    def test_set_instance_injects_a_mock(self, manager):
        assert MqttClientManager.get_instance() is manager

    def test_reset_instance_disconnects_everything(self, manager, config, backend):
        manager.connect("main", config)
        MqttClientManager.reset_instance()
        assert backend.clients == []

    def test_submit_runs_on_a_worker_thread(self, manager):
        assert manager.submit(lambda x: x * 2, 21).result(timeout=5) == 42
