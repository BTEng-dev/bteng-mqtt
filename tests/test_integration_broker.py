"""Integration layer — the real paho backend against a real broker.

Skipped unless something is listening, so the suite still runs anywhere:

    BTENG_MQTT_BROKER=127.0.0.1:1883 pytest tests/ -m broker

Start a broker without root using amqtt (see CLAUDE.md):

    pip install amqtt && amqtt -c broker.yaml &

Everything here is deliberately end-to-end: MockBackend already covers the
logic, so these tests exist to catch the things a mock cannot — paho's callback
signatures, real QoS acknowledgement, broker-side retained storage, and what a
real UNSUBSCRIBE does.
"""
from __future__ import annotations

import json
import os
import socket
import threading
import time

import pytest
from bteng import NodeStatus

from bteng_mqtt import MqttClientManager
from bteng_mqtt.nodes.publish import MqttPublish
from bteng_mqtt.nodes.request_response import MqttRequestResponse
from bteng_mqtt.nodes.subscribe import MqttSubscribe
from bteng_mqtt.registry import BrokerConfig
from tests.helpers import make_node, registry_with, settle

pytestmark = pytest.mark.broker

DEFAULT_ADDRESS = "127.0.0.1:1883"
CLIENT_ID = "bteng-mqtt-itest"


def broker_address() -> tuple[str, int]:
    raw = os.environ.get("BTENG_MQTT_BROKER", DEFAULT_ADDRESS)
    host, _, port = raw.partition(":")
    return host or "127.0.0.1", int(port or 1883)


def broker_is_live(host: str, port: int, timeout: float = 2.0) -> bool:
    """TCP probe, not an MQTT handshake — enough to tell "not started" from a
    protocol failure, without paying for a session."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def live_broker() -> BrokerConfig:
    host, port = broker_address()
    if not broker_is_live(host, port):
        pytest.skip(f"no MQTT broker listening at {host}:{port} "
                    "(set BTENG_MQTT_BROKER, or start one: amqtt -c broker.yaml)")
    return BrokerConfig(host=host, port=port, client_id=CLIENT_ID, timeout=10.0)


@pytest.fixture
def live_manager(live_broker):
    """A manager on the real paho backend, connected and cleaned up.

    The backend is passed explicitly rather than left to the default: another
    test that forgot to clear ``set_default_backend_factory`` would otherwise
    hand this fixture a MockBackend, and the failure would look like a broker
    problem.
    """
    from bteng_mqtt.backend.paho_backend import PahoBackend

    MqttClientManager.reset_instance()
    manager = MqttClientManager(backend=PahoBackend())
    MqttClientManager.set_instance(manager)
    manager.connect("default", live_broker)
    yield manager
    MqttClientManager.reset_instance()


@pytest.fixture
def unique_prefix(request) -> str:
    """A per-test topic prefix, so retained state cannot leak between tests."""
    return f"bteng_mqtt_itest/{os.getpid()}/{request.node.name}"


def wait_for(predicate, timeout: float = 5.0, interval: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


class TestClientManager:
    def test_connect_and_disconnect(self, live_manager):
        assert live_manager.is_connected("default")
        live_manager.disconnect("default")
        assert not live_manager.is_connected("default")

    def test_publish_and_receive_round_trip(self, live_manager, unique_prefix):
        topic = f"{unique_prefix}/round_trip"
        subscription = live_manager.subscribe("default", topic, qos=1)
        live_manager.publish("default", topic, b"hello", qos=1)

        assert wait_for(lambda: not subscription.empty), "no message came back"
        message = subscription.get_nowait()
        assert message is not None
        assert message.payload == b"hello"

    @pytest.mark.parametrize("qos", [0, 1, 2])
    def test_every_qos_level(self, live_manager, unique_prefix, qos):
        topic = f"{unique_prefix}/qos{qos}"
        subscription = live_manager.subscribe("default", topic, qos=qos)
        ticket = live_manager.publish("default", topic, f"q{qos}".encode(), qos=qos)

        assert wait_for(ticket.is_published), f"QoS {qos} was never acknowledged"
        assert wait_for(lambda: not subscription.empty)
        assert subscription.get_nowait().payload == f"q{qos}".encode()

    def test_retained_message_is_replayed_on_subscribe(self, live_manager, unique_prefix):
        topic = f"{unique_prefix}/retained"
        live_manager.publish("default", topic, b"stored", qos=1, retain=True)
        time.sleep(0.3)  # let the broker store it

        subscription = live_manager.subscribe("default", topic, qos=1)
        assert wait_for(lambda: not subscription.empty), "retained message not replayed"
        message = subscription.get_nowait()
        assert message.payload == b"stored"
        assert message.retain is True

        live_manager.publish("default", topic, b"", qos=1, retain=True)  # clear it
        time.sleep(0.2)

    def test_wildcard_subscription(self, live_manager, unique_prefix):
        subscription = live_manager.subscribe("default", f"{unique_prefix}/+", qos=1)
        time.sleep(0.3)  # the SUBACK has to land before the publishes
        live_manager.publish("default", f"{unique_prefix}/a", b"1", qos=1)
        live_manager.publish("default", f"{unique_prefix}/b", b"2", qos=1)

        received: dict[str, bytes] = {}

        def both_arrived() -> bool:
            for message in subscription.drain():
                received[message.topic] = message.payload
            return len(received) >= 2

        assert wait_for(both_arrived), f"only got {received}"
        assert received[f"{unique_prefix}/a"] == b"1"
        assert received[f"{unique_prefix}/b"] == b"2"

    def test_unsubscribe_stops_delivery(self, live_manager, unique_prefix):
        topic = f"{unique_prefix}/unsub"
        subscription = live_manager.subscribe("default", topic, qos=1)
        live_manager.publish("default", topic, b"first", qos=1)
        assert wait_for(lambda: not subscription.empty)
        subscription.drain()

        live_manager.unsubscribe_all("default", topic)
        time.sleep(0.3)
        live_manager.publish("default", topic, b"second", qos=1)
        time.sleep(0.5)
        assert subscription.empty, "message arrived after UNSUBSCRIBE"

    def test_latest_cache_is_filled(self, live_manager, unique_prefix):
        topic = f"{unique_prefix}/cache"
        live_manager.subscribe("default", topic, qos=1)
        live_manager.publish("default", topic, b"42", qos=1)

        assert wait_for(lambda: live_manager.latest("default", topic) is not None)
        assert live_manager.latest("default", topic).payload == b"42"

    def test_reconnect_reissues_subscriptions(self, live_manager, unique_prefix):
        """The broker forgets subscriptions on a new session; we must re-send."""
        topic = f"{unique_prefix}/resub"
        subscription = live_manager.subscribe("default", topic, qos=1)
        live_manager.unregister_client("default")
        live_manager.reconnect("default")

        live_manager.publish("default", topic, b"after", qos=1)
        assert wait_for(lambda: not subscription.empty), "subscription was not reissued"
        assert subscription.get_nowait().payload == b"after"

    def test_refused_credentials_or_bad_port_is_a_clean_error(self, live_broker):
        from bteng_mqtt.backend.paho_backend import PahoBackend
        from bteng_mqtt.exceptions import ConnectionError

        manager = MqttClientManager(backend=PahoBackend())
        try:
            bad = BrokerConfig(host=live_broker.host, port=1, timeout=2.0)
            with pytest.raises(ConnectionError):
                manager.connect("bad", bad)
        finally:
            manager.close()


class TestNodesAgainstRealBroker:
    def test_publish_then_subscribe_node(self, live_manager, unique_prefix):
        registry = registry_with([
            {"alias": "temp", "topic": f"{unique_prefix}/temp",
             "payload_type": "float", "qos": 1},
        ])
        publish = make_node(MqttPublish,
                            params={"topic_alias": "temp", "value": "21.5",
                                    "retain": "true"},
                            registry=registry)
        assert settle(publish) is NodeStatus.SUCCESS

        subscribe = make_node(MqttSubscribe,
                              params={"topic_alias": "temp", "timeout": "5.0"},
                              registry=registry)
        assert settle(subscribe) is NodeStatus.SUCCESS
        assert subscribe.blackboard.get("temp") == 21.5

        live_manager.publish("default", f"{unique_prefix}/temp", b"", qos=1, retain=True)

    def test_request_response_node_against_a_real_responder(self, live_manager,
                                                            unique_prefix, live_broker):
        """A thread plays the device, over the real broker, in a real session."""
        import paho.mqtt.client as mqtt

        request_topic = f"{unique_prefix}/req"
        response_topic = f"{unique_prefix}/res"
        ready = threading.Event()

        device = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="itest-device")

        def on_connect(_c, _u, _f, _rc, _p=None):
            device.subscribe(request_topic, qos=1)

        def on_subscribe(_c, _u, _mid, _rcs, _p=None):
            # Ready means SUBACK, not CONNACK: a request published between the
            # two is simply lost, and the test would flake.
            ready.set()

        def on_message(_c, _u, message):
            payload = json.loads(message.payload.decode())
            device.publish(response_topic,
                           json.dumps({"status": "ok", "echo": payload}).encode(),
                           qos=1)

        device.on_connect = on_connect
        device.on_subscribe = on_subscribe
        device.on_message = on_message
        device.connect(live_broker.host, live_broker.port, keepalive=30)
        device.loop_start()
        try:
            assert ready.wait(timeout=10), "the test device never subscribed"

            registry = registry_with([
                {"alias": "calibrate", "topic": request_topic, "payload_type": "json",
                 "response_topic": response_topic, "response_payload_type": "json",
                 "timeout": 10.0},
            ])
            node = make_node(
                MqttRequestResponse,
                params={"topic_alias": "calibrate", "value": '{"axis": 1}',
                        "response_json_path": "status"},
                registry=registry,
            )
            assert settle(node, timeout=15.0) is NodeStatus.SUCCESS
            assert node.blackboard.get("calibrate") == "ok"
        finally:
            device.loop_stop()
            device.disconnect()


class TestDiscoveryAgainstRealBroker:
    def test_discover_infers_types_from_live_traffic(self, live_manager,
                                                     unique_prefix, live_broker):
        from bteng_mqtt.discovery import DiscoverSpec, discover

        def publish_traffic():
            time.sleep(0.4)
            for i in range(3):
                live_manager.publish("default", f"{unique_prefix}/temp",
                                     f"{20.5 + i}".encode(), qos=1)
                live_manager.publish("default", f"{unique_prefix}/count",
                                     str(i).encode(), qos=1)
                live_manager.publish("default", f"{unique_prefix}/state",
                                     b'{"mode": "run"}', qos=1)
                time.sleep(0.1)

        thread = threading.Thread(target=publish_traffic, daemon=True)
        thread.start()

        spec = DiscoverSpec(broker=live_broker, filters=[f"{unique_prefix}/#"],
                            duration=2.0)
        definitions, _warnings, observations = discover(spec)
        thread.join(timeout=5)

        by_topic = {d["topic"]: d["payload_type"] for d in definitions}
        assert by_topic[f"{unique_prefix}/temp"] == "float"
        assert by_topic[f"{unique_prefix}/state"] == "json"
        assert observations
