"""Topic registry — loading, validation and alias resolution."""
from __future__ import annotations

import json

import pytest

from bteng_mqtt.exceptions import TopicAliasError
from bteng_mqtt.registry import (
    BrokerConfig,
    TopicDefinition,
    TopicRegistry,
    get_global_registry,
    load_global,
    reset_global_registry,
)
from bteng_mqtt.types import MqttPayloadType as T

MINIMAL = {
    "topics": [
        {"alias": "temp", "topic": "a/b/temp", "payload_type": "float", "qos": 1},
    ]
}


class TestLoading:
    def test_minimal_dict(self):
        registry = TopicRegistry()
        registry.load_dict(MINIMAL)
        defn = registry.resolve("temp")
        assert defn.topic == "a/b/temp"
        assert defn.payload_type is T.FLOAT
        assert defn.qos == 1

    def test_defaults(self):
        registry = TopicRegistry()
        registry.load_dict({"topics": [{"alias": "x", "topic": "a/x"}]})
        defn = registry.resolve("x")
        assert defn.payload_type is T.STRING
        assert defn.qos == 0
        assert defn.retain is False
        assert defn.json_path is None
        assert defn.value_map == {}

    def test_missing_topics_section(self):
        with pytest.raises(ValueError, match="'topics' list"):
            TopicRegistry().load_dict({"broker": {}})

    def test_topics_must_be_a_list(self):
        with pytest.raises(ValueError, match="must be a list"):
            TopicRegistry().load_dict({"topics": {"alias": "x"}})

    def test_missing_alias_or_topic(self):
        for bad in ({"topic": "a/b"}, {"alias": "x"}):
            with pytest.raises(ValueError, match="missing required field"):
                TopicRegistry().load_dict({"topics": [bad]})

    def test_unknown_payload_type_lists_the_valid_ones(self):
        with pytest.raises(ValueError, match="Supported:"):
            TopicRegistry().load_dict(
                {"topics": [{"alias": "x", "topic": "a/x", "payload_type": "double"}]}
            )

    def test_yaml_file(self, tmp_path):
        path = tmp_path / "topics.yaml"
        path.write_text("topics:\n  - alias: x\n    topic: a/x\n", encoding="utf-8")
        registry = TopicRegistry()
        registry.load_file(path)
        assert registry.has("x")

    def test_json_file(self, tmp_path):
        path = tmp_path / "topics.json"
        path.write_text(json.dumps(MINIMAL), encoding="utf-8")
        registry = TopicRegistry()
        registry.load_file(path)
        assert registry.resolve("temp").payload_type is T.FLOAT

    def test_unsupported_suffix(self, tmp_path):
        path = tmp_path / "topics.toml"
        path.write_text("nope", encoding="utf-8")
        with pytest.raises(ValueError, match="Unsupported file format"):
            TopicRegistry().load_file(path)

    def test_missing_file(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            TopicRegistry().load_file(tmp_path / "absent.yaml")

    def test_second_load_merges_and_overrides(self):
        registry = TopicRegistry()
        registry.load_dict(MINIMAL)
        registry.load_dict({"topics": [
            {"alias": "temp", "topic": "a/b/temp2"},
            {"alias": "other", "topic": "a/b/other"},
        ]})
        assert registry.resolve("temp").topic == "a/b/temp2"
        assert sorted(registry.all_aliases()) == ["other", "temp"]


class TestLookup:
    def test_unknown_alias_names_the_known_ones(self):
        registry = TopicRegistry()
        registry.load_dict(MINIMAL)
        with pytest.raises(TopicAliasError, match=r"Registered aliases: \['temp'\]"):
            registry.resolve("nope")

    def test_has(self):
        registry = TopicRegistry()
        registry.load_dict(MINIMAL)
        assert registry.has("temp")
        assert not registry.has("temp2")


class TestValidation:
    def test_json_path_requires_json(self):
        with pytest.raises(ValueError, match="json_path needs payload_type"):
            TopicRegistry().load_dict({"topics": [
                {"alias": "x", "topic": "a/x", "payload_type": "float", "json_path": "a.b"}
            ]})

    def test_wildcard_alias_is_allowed(self):
        registry = TopicRegistry()
        registry.load_dict({"topics": [{"alias": "any", "topic": "a/+/c"}]})
        assert registry.resolve("any").is_wildcard

    def test_wildcard_alias_cannot_retain(self):
        with pytest.raises(ValueError, match="retain is meaningless"):
            TopicRegistry().load_dict({"topics": [
                {"alias": "any", "topic": "a/#", "retain": True}
            ]})

    def test_illegal_filter_is_rejected(self):
        with pytest.raises(ValueError, match="last level"):
            TopicRegistry().load_dict({"topics": [{"alias": "bad", "topic": "a/#/c"}]})

    def test_qos_range(self):
        with pytest.raises(ValueError, match="qos must be 0, 1 or 2"):
            TopicRegistry().load_dict({"topics": [
                {"alias": "x", "topic": "a/x", "qos": 3}
            ]})

    def test_response_payload_type_needs_a_response_topic(self):
        with pytest.raises(ValueError, match="without a response_topic"):
            TopicRegistry().load_dict({"topics": [
                {"alias": "x", "topic": "a/x", "response_payload_type": "json"}
            ]})


class TestTopicDefinition:
    def test_value_map_translates_a_tree_level_word(self):
        defn = TopicDefinition(
            alias="cmd", topic="a/cmd", payload_type=T.JSON,
            value_map={"start": '{"cmd": "start"}'},
        )
        assert defn.resolve_value("start") == '{"cmd": "start"}'

    def test_value_map_leaves_unmapped_values_alone(self):
        defn = TopicDefinition(alias="cmd", topic="a/cmd", value_map={"start": "1"})
        assert defn.resolve_value("stop") == "stop"
        assert defn.resolve_value(7) == 7

    def test_response_type_defaults_to_the_request_type(self):
        defn = TopicDefinition(alias="c", topic="a/req", payload_type=T.JSON,
                               response_topic="a/res")
        assert defn.response_type is T.JSON


class TestBrokerSection:
    def test_defaults(self):
        assert BrokerConfig.from_dict(None).host == "127.0.0.1"
        assert BrokerConfig.from_dict({}).port == 1883

    def test_parsed_from_a_registry(self):
        registry = TopicRegistry()
        registry.load_dict({
            "broker": {"host": "broker.local", "port": 8883, "username": "u",
                       "password": "p", "tls": True},
            "topics": MINIMAL["topics"],
        })
        broker = registry.broker
        assert broker is not None
        assert broker.address == "broker.local:8883"
        assert broker.tls is not None
        assert broker.tls.insecure is False

    def test_absent_broker_section_is_none(self):
        registry = TopicRegistry()
        registry.load_dict(MINIMAL)
        assert registry.broker is None

    def test_tls_mapping(self):
        broker = BrokerConfig.from_dict({"tls": {"ca_certs": "/ca.pem", "insecure": True}})
        assert broker.tls is not None
        assert broker.tls.ca_certs == "/ca.pem"
        assert broker.tls.insecure is True

    def test_will_section(self):
        broker = BrokerConfig.from_dict(
            {"will": {"topic": "bteng/status", "payload": "offline", "retain": True}}
        )
        assert broker.will is not None
        assert broker.will.topic == "bteng/status"
        assert broker.will.retain is True

    def test_will_needs_a_topic(self):
        with pytest.raises(ValueError, match="needs a 'topic'"):
            BrokerConfig.from_dict({"will": {"payload": "x"}})

    def test_unknown_protocol(self):
        with pytest.raises(ValueError, match="unknown protocol"):
            BrokerConfig.from_dict({"protocol": "mqtt7"})

    def test_port_range(self):
        with pytest.raises(ValueError, match="port out of range"):
            BrokerConfig.from_dict({"port": 99999})


class TestBlackboardSection:
    def test_seeds_are_exposed(self):
        registry = TopicRegistry()
        registry.load_dict({**MINIMAL, "blackboard": {"speed": 1.5, "name": "x"}})
        assert registry.blackboard == {"speed": 1.5, "name": "x"}

    def test_blackboard_copy_is_defensive(self):
        registry = TopicRegistry()
        registry.load_dict({**MINIMAL, "blackboard": {"speed": 1.5}})
        registry.blackboard["speed"] = 99
        assert registry.blackboard["speed"] == 1.5


class TestGlobalRegistry:
    def test_load_global_and_reset(self, tmp_path):
        path = tmp_path / "topics.yaml"
        path.write_text("topics:\n  - alias: x\n    topic: a/x\n", encoding="utf-8")
        load_global(path)
        assert get_global_registry().has("x")
        reset_global_registry()
        assert not get_global_registry().has("x")


class TestShippedExample:
    """The documented example must stay loadable — it is also the README's proof."""

    def test_example_config_loads(self, example_registry):
        assert example_registry.broker is not None
        assert example_registry.broker.will is not None
        assert example_registry.blackboard == {"target_speed": 1.5}

    def test_example_aliases(self, example_registry):
        assert set(example_registry.all_aliases()) == {
            "temperature", "part_count", "estop", "robot_mode",
            "conveyor_cmd", "any_sensor", "line1_all", "calibrate",
        }

    def test_example_json_path_alias(self, example_registry):
        defn = example_registry.resolve("robot_mode")
        assert defn.payload_type is T.JSON
        assert defn.json_path == "status.mode"

    def test_example_request_response_alias(self, example_registry):
        defn = example_registry.resolve("calibrate")
        assert defn.response_topic == "factory/line1/robot/res/calibrate"
        assert defn.timeout == 10.0
