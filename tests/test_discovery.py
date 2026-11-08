"""Discovery — type inference, alias derivation, sniffing, drift, and the CLI."""
from __future__ import annotations

import threading

import pytest
import yaml

from bteng_mqtt import MockBackend, MqttClientManager
from bteng_mqtt.cli import main
from bteng_mqtt.discovery import (
    DiscoverSpec,
    derive_alias,
    discover,
    drift,
    infer_definitions,
    infer_payload_type,
    preserved_sections,
    render,
    snake_case,
    sniff,
)
from bteng_mqtt.registry import TopicRegistry
from bteng_mqtt.types import MqttPayloadType as T


class TestInferPayloadType:
    def test_float(self):
        assert infer_payload_type([b"1.5", b"2.25"])[0] is T.FLOAT

    def test_int(self):
        assert infer_payload_type([b"1", b"42"])[0] is T.INT

    def test_a_single_float_sample_widens_ints(self):
        """One non-integer sample must not be dropped to keep the narrow type."""
        assert infer_payload_type([b"1", b"2", b"3.5"])[0] is T.FLOAT

    def test_bool_words(self):
        assert infer_payload_type([b"true", b"false", b"ON"])[0] is T.BOOL

    def test_zero_and_one_are_int_not_bool(self):
        assert infer_payload_type([b"0", b"1"])[0] is T.INT

    def test_json_object_with_keys(self):
        payload_type, keys = infer_payload_type([b'{"a": 1}', b'{"b": 2}'])
        assert payload_type is T.JSON
        assert keys == ["a", "b"]

    def test_json_array_has_no_keys(self):
        payload_type, keys = infer_payload_type([b"[1, 2]"])
        assert payload_type is T.JSON
        assert keys == []

    def test_bare_number_is_not_json(self):
        """`42` parses as JSON but 'int' describes it better."""
        assert infer_payload_type([b"42"])[0] is T.INT

    def test_string_fallback(self):
        assert infer_payload_type([b"running", b"idle"])[0] is T.STRING

    def test_mixed_json_and_text_is_a_string(self):
        assert infer_payload_type([b'{"a": 1}', b"idle"])[0] is T.STRING

    def test_non_utf8_is_bytes(self):
        assert infer_payload_type([b"\xff\xfe"])[0] is T.BYTES

    def test_no_samples(self):
        assert infer_payload_type([])[0] is T.STRING


class TestAliases:
    def test_snake_case(self):
        assert snake_case("TestBool") == "test_bool"
        assert snake_case("robot-state") == "robot_state"
        assert snake_case("temp") == "temp"
        assert snake_case("!!!") == "topic"

    def test_last_level_is_the_default(self):
        assert derive_alias("factory/line1/sensor/temp", set()) == "temp"

    def test_collision_prepends_the_parent_level(self):
        taken = {"temp"}
        assert derive_alias("factory/line2/sensor/temp", taken) == "sensor_temp"

    def test_deep_collision_keeps_climbing(self):
        taken = {"temp", "sensor_temp"}
        assert derive_alias("a/line2/sensor/temp", taken) == "line2_sensor_temp"

    def test_identical_topics_get_a_numeric_suffix(self):
        taken = {"temp", "a_temp"}
        assert derive_alias("a/temp", taken) == "a_temp_2"


class TestInferDefinitions:
    def test_registry_shaped_output(self):
        definitions, _ = infer_definitions({
            "a/temp": [b"1.5"],
            "a/state": [b'{"mode": "run"}'],
        })
        assert definitions == [
            {"alias": "state", "topic": "a/state", "payload_type": "json",
             "description": "json keys seen: mode"},
            {"alias": "temp", "topic": "a/temp", "payload_type": "float"},
        ]

    def test_output_loads_back_into_a_registry(self):
        """Whatever discovery writes must be something the registry accepts."""
        definitions, _ = infer_definitions({
            "a/temp": [b"1.5"], "a/n": [b"3"], "a/flag": [b"true"],
            "a/txt": [b"hi"], "a/obj": [b'{"k": 1}'], "a/raw": [b"\xff"],
        })
        registry = TopicRegistry()
        registry.load_dict({"topics": definitions})
        assert len(registry.all_aliases()) == 6

    def test_spec_aliases_win(self):
        spec = DiscoverSpec(aliases={"a/temp": "temperature"})
        definitions, _ = infer_definitions({"a/temp": [b"1.5"]}, spec)
        assert definitions[0]["alias"] == "temperature"

    def test_exclude_filters_drop_topics(self):
        spec = DiscoverSpec(exclude=["$SYS/#"])
        definitions, _ = infer_definitions(
            {"$SYS/broker/uptime": [b"1"], "a/temp": [b"1.5"]}, spec)
        assert [d["alias"] for d in definitions] == ["temp"]

    def test_single_sample_is_flagged(self):
        _, warnings = infer_definitions({"a/temp": [b"1.5"]})
        assert any("only 1 sample" in w for w in warnings)

    def test_zero_one_int_is_flagged_as_maybe_bool(self):
        _, warnings = infer_definitions({"a/flag": [b"0", b"1", b"0"]})
        assert any("only saw 0/1" in w for w in warnings)

    def test_json_array_is_flagged(self):
        _, warnings = infer_definitions({"a/list": [b"[1]", b"[2]"]})
        assert any("no keys" in w for w in warnings)

    def test_topics_are_sorted(self):
        definitions, _ = infer_definitions({"b/x": [b"1"], "a/y": [b"1"]})
        assert [d["topic"] for d in definitions] == ["a/y", "b/x"]


class TestSpec:
    def test_defaults(self):
        spec = DiscoverSpec.from_dict({})
        assert spec.filters == ["#"]
        assert spec.duration == 15.0
        assert spec.broker.port == 1883

    def test_parsed(self):
        spec = DiscoverSpec.from_dict({
            "broker": {"host": "b.local", "port": 8883},
            "filters": ["a/#", "b/+"],
            "duration": 5,
            "aliases": {"a/x": "ex"},
            "exclude": ["$SYS/#"],
        })
        assert spec.broker.address == "b.local:8883"
        assert spec.filters == ["a/#", "b/+"]
        assert spec.duration == 5.0
        assert spec.aliases == {"a/x": "ex"}

    def test_a_single_filter_string_is_accepted(self):
        assert DiscoverSpec.from_dict({"filters": "a/#"}).filters == ["a/#"]

    def test_illegal_filter_is_rejected(self):
        with pytest.raises(ValueError, match="last level"):
            DiscoverSpec.from_dict({"filters": ["a/#/b"]})

    def test_bad_qos(self):
        with pytest.raises(ValueError, match="qos must be"):
            DiscoverSpec.from_dict({"qos": 9})

    def test_shipped_example_spec_parses(self):
        from tests.conftest import ROOT

        spec = DiscoverSpec.from_file(ROOT / "examples" / "config" / "discover.yaml")
        assert spec.filters == ["factory/line1/#"]
        assert spec.aliases["factory/line1/sensor/temp"] == "temperature"


class TestSniff:
    def test_collects_traffic(self, manager, backend):
        spec = DiscoverSpec(duration=0.4)
        threading.Timer(0.05, lambda: backend.inject("a/temp", "1.5")).start()
        threading.Timer(0.10, lambda: backend.inject("a/temp", "2.5")).start()
        threading.Timer(0.15, lambda: backend.inject("a/state", "idle")).start()

        observations = sniff(spec, manager=manager)

        assert observations == {"a/temp": [b"1.5", b"2.5"], "a/state": [b"idle"]}

    def test_overlapping_filters_do_not_double_count(self, manager, backend):
        spec = DiscoverSpec(filters=["a/#", "a/temp"], duration=0.3)
        threading.Timer(0.05, lambda: backend.inject("a/temp", "1.5")).start()

        observations = sniff(spec, manager=manager)

        assert observations == {"a/temp": [b"1.5"]}

    def test_cleans_up_after_itself(self, manager, backend):
        sniff(DiscoverSpec(duration=0.05), manager=manager)
        assert manager.active_clients() == []
        assert backend.clients == []

    def test_quiet_broker_yields_nothing(self, manager):
        assert sniff(DiscoverSpec(duration=0.05), manager=manager) == {}

    def test_discover_infers_from_what_it_heard(self, manager, backend):
        threading.Timer(0.05, lambda: backend.inject("a/temp", "1.5")).start()
        definitions, warnings, observations = discover(
            DiscoverSpec(duration=0.3), manager=manager)
        assert definitions == [
            {"alias": "temp", "topic": "a/temp", "payload_type": "float"}]
        assert observations
        assert warnings  # single sample


class TestRenderAndDrift:
    DEFS = [{"alias": "temp", "topic": "a/temp", "payload_type": "float"}]

    def test_render_is_loadable_and_carries_the_caveat(self):
        text = render(self.DEFS, {}, "127.0.0.1:1883", samples=4, duration=15.0)
        assert "INFERRED" in text
        data = yaml.safe_load(text)
        assert data["topics"] == self.DEFS

    def test_render_preserves_hand_written_sections(self):
        text = render(self.DEFS, {"broker": {"host": "b"}, "blackboard": {"k": 1}},
                      "src", 1, 1.0)
        data = yaml.safe_load(text)
        assert data["broker"] == {"host": "b"}
        assert data["blackboard"] == {"k": 1}

    def test_preserved_sections_reads_everything_but_topics(self, tmp_path):
        path = tmp_path / "c.yaml"
        path.write_text(yaml.safe_dump({
            "topics": [{"alias": "x", "topic": "a/x"}],
            "broker": {"host": "b"},
        }), encoding="utf-8")
        assert preserved_sections(path) == {"broker": {"host": "b"}}

    def test_no_drift_when_they_agree(self, tmp_path):
        path = tmp_path / "c.yaml"
        path.write_text(render(self.DEFS, {}, "s", 1, 1.0), encoding="utf-8")
        assert drift(path, self.DEFS) == []

    def test_drift_lists_added_removed_and_changed(self, tmp_path):
        path = tmp_path / "c.yaml"
        path.write_text(yaml.safe_dump({"topics": [
            {"alias": "temp", "topic": "a/temp", "payload_type": "int"},
            {"alias": "gone", "topic": "a/gone", "payload_type": "string"},
        ]}), encoding="utf-8")
        report = drift(path, [
            {"alias": "temp", "topic": "a/temp", "payload_type": "float"},
            {"alias": "new", "topic": "a/new", "payload_type": "string"},
        ])
        assert any(line.startswith("+ new") for line in report)
        assert any(line.startswith("- gone") for line in report)
        assert any(line.startswith("~ temp") for line in report)

    def test_missing_file_is_drift(self, tmp_path):
        assert drift(tmp_path / "absent.yaml", self.DEFS) == [
            f"{tmp_path / 'absent.yaml'} does not exist"]


class TestDiscoverCli:
    @pytest.fixture
    def mock_everywhere(self):
        backend = MockBackend()
        MqttClientManager.set_default_backend_factory(lambda: backend)
        yield backend
        MqttClientManager.set_default_backend_factory(None)
        MqttClientManager.reset_instance()

    @pytest.fixture
    def spec_file(self, tmp_path):
        path = tmp_path / "discover.yaml"
        path.write_text(yaml.safe_dump({
            "broker": {"host": "127.0.0.1", "port": 1883, "timeout": 2.0},
            "filters": ["a/#"],
            "duration": 0.3,
        }), encoding="utf-8")
        return path

    def test_writes_a_registry(self, mock_everywhere, spec_file, tmp_path, capsys):
        threading.Timer(0.05, lambda: mock_everywhere.inject("a/temp", "1.5")).start()
        out = tmp_path / "mqtt_config.yaml"

        assert main(["discover", "-s", str(spec_file), "-o", str(out), "--plain"]) == 0

        data = yaml.safe_load(out.read_text(encoding="utf-8"))
        assert data["topics"] == [
            {"alias": "temp", "topic": "a/temp", "payload_type": "float"}]
        assert "Wrote" in capsys.readouterr().out

    def test_no_traffic_is_an_error_not_an_empty_file(self, mock_everywhere,
                                                     spec_file, tmp_path):
        out = tmp_path / "mqtt_config.yaml"
        assert main(["discover", "-s", str(spec_file), "-o", str(out), "--plain"]) == 2
        assert not out.exists()

    def test_check_passes_when_up_to_date(self, mock_everywhere, spec_file, tmp_path):
        out = tmp_path / "mqtt_config.yaml"
        threading.Timer(0.05, lambda: mock_everywhere.inject("a/temp", "1.5")).start()
        main(["discover", "-s", str(spec_file), "-o", str(out), "--plain"])

        threading.Timer(0.05, lambda: mock_everywhere.inject("a/temp", "1.5")).start()
        assert main(["discover", "-s", str(spec_file), "-o", str(out),
                     "--check", "--plain"]) == 0

    def test_check_fails_on_drift(self, mock_everywhere, spec_file, tmp_path, capsys):
        out = tmp_path / "mqtt_config.yaml"
        out.write_text(yaml.safe_dump({"topics": [
            {"alias": "temp", "topic": "a/temp", "payload_type": "int"}]}),
            encoding="utf-8")
        threading.Timer(0.05, lambda: mock_everywhere.inject("a/temp", "1.5")).start()

        assert main(["discover", "-s", str(spec_file), "-o", str(out),
                     "--check", "--plain"]) == 1
        assert "does not match" in capsys.readouterr().err

    def test_regenerating_preserves_the_broker_section(self, mock_everywhere,
                                                       spec_file, tmp_path):
        out = tmp_path / "mqtt_config.yaml"
        out.write_text(yaml.safe_dump({
            "topics": [],
            "broker": {"host": "keep.me", "port": 1883},
            "blackboard": {"speed": 1.5},
        }), encoding="utf-8")
        threading.Timer(0.05, lambda: mock_everywhere.inject("a/temp", "1.5")).start()

        main(["discover", "-s", str(spec_file), "-o", str(out), "--plain"])

        data = yaml.safe_load(out.read_text(encoding="utf-8"))
        assert data["broker"]["host"] == "keep.me"
        assert data["blackboard"] == {"speed": 1.5}

    def test_cli_overrides_the_spec(self, mock_everywhere, spec_file, tmp_path):
        out = tmp_path / "mqtt_config.yaml"
        threading.Timer(0.05, lambda: mock_everywhere.inject("b/other", "hi")).start()

        assert main(["discover", "-s", str(spec_file), "-o", str(out),
                     "-f", "b/#", "-d", "0.3", "--plain"]) == 0
        data = yaml.safe_load(out.read_text(encoding="utf-8"))
        assert data["topics"][0]["topic"] == "b/other"

    def test_works_without_a_spec_file(self, mock_everywhere, tmp_path):
        out = tmp_path / "mqtt_config.yaml"
        threading.Timer(0.05, lambda: mock_everywhere.inject("a/temp", "1.5")).start()
        assert main(["discover", "-o", str(out), "-b", "127.0.0.1:1883",
                     "-d", "0.3", "--plain"]) == 0


class TestManualEntries:
    """Hand-written topics survive a re-discover, and do not count as drift.

    Wildcards, request/response pairs and value_maps cannot be sniffed, so
    without this a project would lose them on every `discover` and its `--check`
    gate would be permanently red.
    """

    SNIFFED = [{"alias": "temp", "topic": "a/temp", "payload_type": "float"}]
    MANUAL = [{"alias": "calibrate", "topic": "a/req", "payload_type": "json",
               "response_topic": "a/res", "manual": True}]

    def test_manual_topics_are_read_back(self, tmp_path):
        from bteng_mqtt.discovery import manual_topics

        path = tmp_path / "c.yaml"
        path.write_text(yaml.safe_dump({"topics": self.SNIFFED + self.MANUAL}),
                        encoding="utf-8")
        assert manual_topics(path) == self.MANUAL

    def test_render_appends_them(self):
        text = render(self.SNIFFED, {}, "src", 1, 1.0, manual=self.MANUAL)
        topics = yaml.safe_load(text)["topics"]
        assert topics == self.SNIFFED + self.MANUAL

    def test_they_are_not_drift(self, tmp_path):
        path = tmp_path / "c.yaml"
        path.write_text(yaml.safe_dump({"topics": self.SNIFFED + self.MANUAL}),
                        encoding="utf-8")
        assert drift(path, self.SNIFFED) == []

    def test_a_real_change_is_still_drift(self, tmp_path):
        path = tmp_path / "c.yaml"
        path.write_text(yaml.safe_dump({"topics": self.SNIFFED + self.MANUAL}),
                        encoding="utf-8")
        report = drift(path, [{"alias": "temp", "topic": "a/temp",
                               "payload_type": "int"}])
        assert any(line.startswith("~ temp") for line in report)

    def test_the_registry_accepts_the_flag(self):
        registry = TopicRegistry()
        registry.load_dict({"topics": self.MANUAL})
        assert registry.resolve("calibrate").manual is True

    def test_regenerating_keeps_them(self, tmp_path, monkeypatch):
        """The end-to-end promise: discover twice, hand-written entry still there."""
        from bteng_mqtt.discovery import manual_topics, preserved_sections

        path = tmp_path / "c.yaml"
        path.write_text(render(self.SNIFFED, {"broker": {"host": "b"}},
                               "src", 1, 1.0, manual=self.MANUAL), encoding="utf-8")

        again = render(self.SNIFFED, preserved_sections(path), "src", 1, 1.0,
                       manual=manual_topics(path))
        path.write_text(again, encoding="utf-8")

        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert self.MANUAL[0] in data["topics"]
        assert data["broker"] == {"host": "b"}


class TestDriftIgnoresHumanEdits:
    """`--check` must only fire on what a sniff can actually determine.

    A reviewed registry carries qos, json_path, retain and value_map that
    discovery knows nothing about; comparing those made the gate permanently red.
    """

    COMMITTED = [{
        "alias": "temp", "topic": "a/temp", "payload_type": "float",
        "qos": 1, "retain": True, "description": "reviewed by hand",
    }]
    SNIFFED = [{"alias": "temp", "topic": "a/temp", "payload_type": "float"}]

    def _write(self, tmp_path, topics):
        path = tmp_path / "c.yaml"
        path.write_text(yaml.safe_dump({"topics": topics}), encoding="utf-8")
        return path

    def test_qos_and_json_path_are_not_drift(self, tmp_path):
        assert drift(self._write(tmp_path, self.COMMITTED), self.SNIFFED) == []

    def test_a_generated_description_is_not_drift(self, tmp_path):
        sniffed = [{**self.SNIFFED[0], "description": "json keys seen: a"}]
        assert drift(self._write(tmp_path, self.COMMITTED), sniffed) == []

    def test_a_contradicting_payload_type_is_drift(self, tmp_path):
        sniffed = [{"alias": "temp", "topic": "a/temp", "payload_type": "string"}]
        report = drift(self._write(tmp_path, self.COMMITTED), sniffed)
        assert len(report) == 1
        assert "is float in the file" in report[0]
        assert "published string" in report[0]

    def test_a_new_topic_is_drift(self, tmp_path):
        sniffed = self.SNIFFED + [
            {"alias": "extra", "topic": "a/extra", "payload_type": "int"}]
        report = drift(self._write(tmp_path, self.COMMITTED), sniffed)
        assert any("+ extra" in line and "not in the file" in line for line in report)

    def test_a_quiet_topic_is_reported_as_such(self, tmp_path):
        """It reads as a removal, so the message says what it really means."""
        committed = self.COMMITTED + [
            {"alias": "quiet", "topic": "a/quiet", "payload_type": "int"}]
        report = drift(self._write(tmp_path, committed), self.SNIFFED)
        assert any("- quiet" in line and "nothing published" in line
                   for line in report)
