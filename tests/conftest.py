"""Shared fixtures — every test in this suite runs offline against MockBackend.

No broker, no paho, no network: the mock backend is a tiny in-process broker, so
the whole suite is deterministic and fast.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from bteng_mqtt import (
    BrokerConfig,
    MockBackend,
    MqttClientManager,
    TopicRegistry,
    reset_global_registry,
)

ROOT = Path(__file__).resolve().parent.parent
EXAMPLE_CONFIG = ROOT / "examples" / "config" / "mqtt_config.yaml"


@pytest.fixture
def backend() -> MockBackend:
    return MockBackend()


@pytest.fixture
def manager(backend: MockBackend):
    """A manager wired to the mock backend, torn down after the test.

    Also installed as the singleton so code that calls ``get_instance()`` — the
    BT nodes, from milestone 2 on — sees the mock.
    """
    instance = MqttClientManager(backend=backend)
    MqttClientManager.set_instance(instance)
    yield instance
    MqttClientManager.reset_instance()


@pytest.fixture
def config() -> BrokerConfig:
    return BrokerConfig(host="127.0.0.1", port=1883, client_id="test", timeout=2.0)


@pytest.fixture
def connected(manager: MqttClientManager, config: BrokerConfig) -> MqttClientManager:
    """A manager with one connected client under the id ``"default"``."""
    manager.connect("default", config)
    return manager


@pytest.fixture
def example_registry() -> TopicRegistry:
    registry = TopicRegistry()
    registry.load_file(EXAMPLE_CONFIG)
    return registry


@pytest.fixture(autouse=True)
def clean_global_registry():
    reset_global_registry()
    yield
    reset_global_registry()
