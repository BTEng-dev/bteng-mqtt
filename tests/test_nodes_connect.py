"""MqttConnect / MqttDisconnect — connection lifecycle from a tree."""
from __future__ import annotations

from bteng import NodeStatus

from bteng_mqtt import MockBackend, MqttClientManager
from bteng_mqtt.nodes.connect import MqttConnect
from bteng_mqtt.nodes.disconnect import MqttDisconnect
from tests.helpers import make_node, registry_with, settle


class TestConnect:
    def test_takes_the_broker_from_the_registry(self, manager, backend):
        """The point of the registry: the tree line is just a client_id."""
        registry = registry_with(broker={"host": "broker.local", "port": 8883})
        node = make_node(MqttConnect, params={"client_id": "main"}, registry=registry)

        assert settle(node) is NodeStatus.SUCCESS
        assert manager.is_connected("main")
        assert backend.clients[0].config.address == "broker.local:8883"

    def test_ports_override_the_registry(self, manager, backend):
        registry = registry_with(broker={"host": "broker.local", "port": 8883})
        node = make_node(
            MqttConnect,
            params={"client_id": "main", "host": "127.0.0.1", "port": "1883"},
            registry=registry,
        )

        assert settle(node) is NodeStatus.SUCCESS
        assert backend.clients[0].config.address == "127.0.0.1:1883"

    def test_credentials_and_will_reach_the_backend(self, manager, backend):
        node = make_node(MqttConnect, params={
            "client_id": "main",
            "username": "operator",
            "password": "secret",
            "mqtt_client_id": "bteng-1",
            "will_topic": "bteng/status",
            "will_payload": "offline",
            "will_retain": "true",
        })

        assert settle(node) is NodeStatus.SUCCESS
        config = backend.clients[0].config
        assert (config.username, config.password) == ("operator", "secret")
        assert config.client_id == "bteng-1"
        assert config.will is not None
        assert (config.will.topic, config.will.retain) == ("bteng/status", True)

    def test_tls_flag(self, manager, backend):
        node = make_node(MqttConnect, params={"client_id": "main", "tls": "true"})
        assert settle(node) is NodeStatus.SUCCESS
        assert backend.clients[0].config.tls is not None

    def test_already_connected_succeeds_on_the_first_tick(self, connected, backend):
        node = make_node(MqttConnect, params={"client_id": "default"})
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert "already connected" in node.feedback_message
        assert len(backend.clients) == 1

    def test_refusal_is_a_failure_with_the_reason(self, config):
        instance = MqttClientManager(backend=MockBackend(connect_rc=5))
        MqttClientManager.set_instance(instance)
        try:
            node = make_node(MqttConnect, params={"client_id": "main", "timeout": "1.0"})
            assert settle(node) is NodeStatus.FAILURE
            assert "refused" in node.feedback_message
        finally:
            MqttClientManager.reset_instance()

    def test_running_while_the_handshake_is_pending(self):
        backend = MockBackend(auto_connack=False)
        instance = MqttClientManager(backend=backend)
        MqttClientManager.set_instance(instance)
        try:
            node = make_node(MqttConnect, params={"client_id": "main", "timeout": "5.0"})
            assert node.execute_tick() is NodeStatus.RUNNING
            assert node.execute_tick() is NodeStatus.RUNNING   # still waiting

            backend.send_connack(backend.clients[0])
            assert settle(node) is NodeStatus.SUCCESS
        finally:
            MqttClientManager.reset_instance()

    def test_bad_numeric_port_fails_with_a_readable_message(self, manager):
        node = make_node(MqttConnect, params={"client_id": "main", "port": "eighteen"})
        assert node.execute_tick() is NodeStatus.FAILURE
        assert "must be a whole number" in node.feedback_message

    def test_halt_cancels_a_pending_connect(self):
        backend = MockBackend(auto_connack=False)
        instance = MqttClientManager(backend=backend)
        MqttClientManager.set_instance(instance)
        try:
            node = make_node(MqttConnect, params={"client_id": "main", "timeout": "5.0"})
            assert node.execute_tick() is NodeStatus.RUNNING
            node.halt()
        finally:
            MqttClientManager.reset_instance()


class TestDisconnect:
    def test_disconnects_a_live_client(self, connected, backend):
        node = make_node(MqttDisconnect, params={"client_id": "default"})
        assert settle(node) is NodeStatus.SUCCESS
        assert not connected.is_connected("default")
        assert backend.clients == []

    def test_keeps_the_config_so_reconnect_stays_possible(self, connected):
        node = make_node(MqttDisconnect, params={"client_id": "default"})
        assert settle(node) is NodeStatus.SUCCESS
        assert connected.get_client_config("default") is not None

    def test_not_connected_is_success_not_failure(self, manager):
        node = make_node(MqttDisconnect, params={"client_id": "never"})
        assert node.execute_tick() is NodeStatus.SUCCESS
        assert "was not connected" in node.feedback_message
