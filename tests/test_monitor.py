"""Monitor — decoded tailing, including the messages that fail to decode."""
from __future__ import annotations

import threading

import pytest
import yaml

from bteng_mqtt import MockBackend, MqttClientManager
from bteng_mqtt.cli import main
from bteng_mqtt.client import Message
from bteng_mqtt.monitor import describe, monitor
from bteng_mqtt.registry import TopicRegistry
from tests.helpers import DEFAULT_TOPICS


@pytest.fixture
def registry() -> TopicRegistry:
    registry = TopicRegistry()
    registry.load_dict({
        "broker": {"host": "127.0.0.1", "port": 1883, "timeout": 2.0},
        "topics": DEFAULT_TOPICS,
    })
    return registry


class TestDescribe:
    def test_decoded_value_and_type(self, registry):
        line = describe(Message("a/temp", b"21.5", timestamp=0.0),
                        registry.resolve("temp"))
        assert "temp = 21.5" in line
        assert "[float]" in line

    def test_json_path_is_applied(self, registry):
        line = describe(Message("a/state", b'{"status": {"mode": "run"}}', timestamp=0.0),
                        registry.resolve("state"))
        assert "'run'" in line

    def test_undecodable_payload_is_reported_not_raised(self, registry):
        """Seeing exactly this is the point of monitoring."""
        line = describe(Message("a/temp", b"warm", timestamp=0.0),
                        registry.resolve("temp"))
        assert "[! float:" in line
        assert "'warm'" in line

    def test_retained_is_marked(self, registry):
        line = describe(Message("a/temp", b"1.0", retain=True, timestamp=0.0),
                        registry.resolve("temp"))
        assert "(retained)" in line

    def test_unaliased_topic(self):
        line = describe(Message("who/knows", b"x", timestamp=0.0), None)
        assert "[no alias]" in line


class TestMonitor:
    def test_prints_decoded_messages(self, manager, backend, registry):
        lines: list[str] = []
        threading.Timer(0.05, lambda: backend.inject("a/temp", "21.5")).start()

        printed = monitor(registry, aliases=["temp"], duration=0.3,
                          manager=manager, emit=lines.append)

        assert printed == 1
        assert any("temp = 21.5" in line for line in lines)

    def test_watches_every_alias_by_default(self, manager, backend, registry):
        lines: list[str] = []
        threading.Timer(0.05, lambda: backend.inject("a/label", "hi")).start()
        threading.Timer(0.08, lambda: backend.inject("a/count", "3")).start()

        printed = monitor(registry, duration=0.3, manager=manager, emit=lines.append)

        assert printed == 2

    def test_extra_raw_filter(self, manager, backend, registry):
        lines: list[str] = []
        threading.Timer(0.05, lambda: backend.inject("unknown/topic", "x")).start()

        printed = monitor(registry, aliases=["temp"], filters=["unknown/#"],
                          duration=0.3, manager=manager, emit=lines.append)

        assert printed == 1
        assert any("[no alias]" in line for line in lines)

    def test_cleans_up(self, manager, backend, registry):
        monitor(registry, aliases=["temp"], duration=0.05, manager=manager,
                emit=lambda _line: None)
        assert manager.active_clients() == []

    def test_nothing_to_watch(self, manager):
        empty = TopicRegistry()
        empty.load_dict({"topics": []})
        assert monitor(empty, duration=0.05, manager=manager,
                       emit=lambda _line: None) == 0

    def test_unknown_alias_raises_before_connecting(self, manager, registry):
        from bteng_mqtt.exceptions import TopicAliasError

        with pytest.raises(TopicAliasError):
            monitor(registry, aliases=["nope"], duration=0.05, manager=manager,
                    emit=lambda _line: None)
        assert manager.active_clients() == []


class TestMonitorCli:
    @pytest.fixture
    def mock_everywhere(self):
        backend = MockBackend()
        MqttClientManager.set_default_backend_factory(lambda: backend)
        yield backend
        MqttClientManager.set_default_backend_factory(None)
        MqttClientManager.reset_instance()

    @pytest.fixture
    def config_file(self, tmp_path):
        path = tmp_path / "mqtt_config.yaml"
        path.write_text(yaml.safe_dump({
            "broker": {"host": "127.0.0.1", "port": 1883, "timeout": 2.0},
            "topics": DEFAULT_TOPICS,
        }), encoding="utf-8")
        return path

    def test_monitor_command(self, mock_everywhere, config_file, capsys):
        threading.Timer(0.05, lambda: mock_everywhere.inject("a/temp", "21.5")).start()
        assert main(["monitor", "-n", str(config_file), "-a", "temp",
                     "-d", "0.3", "--plain"]) == 0
        assert "temp = 21.5" in capsys.readouterr().out

    def test_missing_config(self, mock_everywhere, tmp_path):
        assert main(["monitor", "-n", str(tmp_path / "absent.yaml"), "-d", "0.1"]) == 2

    def test_unknown_alias_is_a_clean_error(self, mock_everywhere, config_file, capsys):
        assert main(["monitor", "-n", str(config_file), "-a", "nope",
                     "-d", "0.1", "--plain"]) == 2
        assert "monitor failed" in capsys.readouterr().err
