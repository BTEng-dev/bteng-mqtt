"""Runtime and CLI — `bteng-mqtt run` end to end, with no broker involved.

`MqttClientManager.set_default_backend_factory` is what makes this possible:
run_trees() resets the singleton between trees, so a manager injected with
set_instance() would not survive to the second tree.
"""
from __future__ import annotations

import logging

import pytest
import yaml
from bteng import NodeStatus

from bteng_mqtt import MockBackend, MqttClientManager
from bteng_mqtt.cli import build_parser, main
from bteng_mqtt.runtime import (
    TreeRunner,
    install_logging,
    parse_bb_presets,
    run_trees,
    shutdown,
)
from tests.helpers import DEFAULT_TOPICS

GOOD_TREE = """<?xml version="1.0" encoding="UTF-8"?>
<BTEng format_version="1.0" main_tree_to_execute="good">
  <Tree ID="good">
    <Sequence name="Good">
      <MqttConnect    name="Connect"    client_id="good" />
      <MqttPublish    name="Publish"    client_id="good" topic_alias="label"
                      value="{greeting}" />
      <MqttDisconnect name="Disconnect" client_id="good" />
    </Sequence>
  </Tree>
</BTEng>
"""

FAILING_TREE = """<?xml version="1.0" encoding="UTF-8"?>
<BTEng format_version="1.0" main_tree_to_execute="bad">
  <Tree ID="bad">
    <Sequence name="Bad">
      <MqttConnect name="Connect" client_id="bad" />
      <MqttSubscribe name="WaitForNothing" client_id="bad" topic_alias="label"
                     timeout="0.1" />
    </Sequence>
  </Tree>
</BTEng>
"""

BROKEN_XML = "<BTEng><Tree ID='oops'><Sequence name='x'>"


@pytest.fixture
def mock_everywhere():
    """Every manager built from here on uses one shared MockBackend."""
    backend = MockBackend()
    MqttClientManager.set_default_backend_factory(lambda: backend)
    yield backend
    MqttClientManager.set_default_backend_factory(None)
    MqttClientManager.reset_instance()


@pytest.fixture
def project(tmp_path):
    """A miniature consuming project: config/ + trees/, no Python."""
    (tmp_path / "trees").mkdir()
    (tmp_path / "trees" / "good.xml").write_text(GOOD_TREE, encoding="utf-8")
    config = tmp_path / "mqtt_config.yaml"
    config.write_text(yaml.safe_dump({
        "broker": {"host": "127.0.0.1", "port": 1883, "timeout": 2.0},
        "topics": DEFAULT_TOPICS,
        "blackboard": {"greeting": "hello"},
    }), encoding="utf-8")
    return tmp_path


class TestRunTrees:
    def test_a_project_runs_with_no_python(self, mock_everywhere, project):
        exit_code = run_trees([project / "trees"], project / "mqtt_config.yaml",
                              pretty=False)
        assert exit_code == 0
        assert mock_everywhere.payloads_for("a/label") == [b"hello"]

    def test_registry_blackboard_section_seeds_the_value(self, mock_everywhere, project):
        """`value="{greeting}"` resolved from the registry's blackboard section."""
        run_trees([project / "trees" / "good.xml"], project / "mqtt_config.yaml",
                  pretty=False)
        assert mock_everywhere.payloads_for("a/label") == [b"hello"]

    def test_bb_preset_overrides_the_registry(self, mock_everywhere, project):
        run_trees([project / "trees" / "good.xml"], project / "mqtt_config.yaml",
                  presets=["greeting=override"], pretty=False)
        assert mock_everywhere.payloads_for("a/label") == [b"override"]

    def test_a_failing_tree_gives_a_non_zero_exit_code(self, mock_everywhere, project):
        (project / "trees" / "bad.xml").write_text(FAILING_TREE, encoding="utf-8")
        assert run_trees([project / "trees"], project / "mqtt_config.yaml",
                         pretty=False) == 1

    def test_the_node_that_failed_is_named_in_the_log(self, mock_everywhere, project, caplog):
        """The Sequence halts (and clears) the failed child, so this only works
        because TreeRunner records failures through bteng's Inspector."""
        (project / "trees" / "bad.xml").write_text(FAILING_TREE, encoding="utf-8")
        with caplog.at_level(logging.ERROR, logger="bteng_mqtt.run"):
            run_trees([project / "trees" / "bad.xml"], project / "mqtt_config.yaml",
                      pretty=False)
        logged = " ".join(record.getMessage() for record in caplog.records)
        assert "WaitForNothing" in logged
        assert "Timed out" in logged

    def test_a_broken_tree_does_not_abort_the_batch(self, mock_everywhere, project, capsys):
        (project / "trees" / "aaa_broken.xml").write_text(BROKEN_XML, encoding="utf-8")
        exit_code = run_trees([project / "trees"], project / "mqtt_config.yaml",
                              pretty=False)
        assert exit_code == 1
        # The good tree still ran, even though the broken one sorts first.
        assert mock_everywhere.payloads_for("a/label") == [b"hello"]
        assert "good.xml" in capsys.readouterr().out

    def test_missing_registry_is_reported(self, mock_everywhere, project):
        assert run_trees([project / "trees"], project / "absent.yaml", pretty=False) == 1

    def test_missing_tree_is_reported(self, mock_everywhere, project):
        assert run_trees([project / "trees" / "absent.xml"],
                         project / "mqtt_config.yaml", pretty=False) == 1

    def test_no_trees_found(self, mock_everywhere, project):
        (project / "empty").mkdir()
        assert run_trees([project / "empty"], project / "mqtt_config.yaml",
                         pretty=False) == 1

    def test_summary_is_printed(self, mock_everywhere, project, capsys):
        run_trees([project / "trees"], project / "mqtt_config.yaml", pretty=False)
        out = capsys.readouterr().out
        assert "=== Summary ===" in out
        assert "SUCCESS" in out

    def test_clients_are_closed_after_a_run(self, mock_everywhere, project):
        run_trees([project / "trees"], project / "mqtt_config.yaml", pretty=False)
        assert mock_everywhere.clients == []

    def test_bad_preset_is_reported(self, mock_everywhere, project):
        assert run_trees([project / "trees"], project / "mqtt_config.yaml",
                         presets=["nonsense"], pretty=False) == 1


class TestCli:
    def test_run_command_end_to_end(self, mock_everywhere, project):
        exit_code = main([
            "run", str(project / "trees"),
            "-n", str(project / "mqtt_config.yaml"),
            "--plain",
        ])
        assert exit_code == 0
        assert mock_everywhere.payloads_for("a/label") == [b"hello"]

    def test_bb_flag_reaches_the_tree(self, mock_everywhere, project):
        main([
            "run", str(project / "trees" / "good.xml"),
            "-n", str(project / "mqtt_config.yaml"),
            "--bb", "greeting=from-cli", "--plain",
        ])
        assert mock_everywhere.payloads_for("a/label") == [b"from-cli"]

    def test_a_command_is_required(self):
        with pytest.raises(SystemExit):
            build_parser().parse_args([])

    def test_defaults(self):
        args = build_parser().parse_args(["run", "trees/"])
        assert args.topics == "mqtt_config.yaml"
        assert args.timeout == 60.0
        assert args.plain is False


class TestPresets:
    def test_values_carry_native_types(self):
        parsed = parse_bb_presets(["n=5", "x=false", "s=abc", "f=1.5"])
        assert parsed == {"n": 5, "x": False, "s": "abc", "f": 1.5}

    def test_a_missing_equals_sign_is_an_error(self):
        with pytest.raises(ValueError, match="KEY=VALUE"):
            parse_bb_presets(["nonsense"])


class TestTreeRunner:
    def test_timeout_halts_and_fails(self, manager):
        from bteng_mqtt.nodes.sleep import Sleep
        from tests.helpers import make_node

        node = make_node(Sleep, params={"duration": "10"})
        assert TreeRunner(node, timeout=0.05, pretty=False).run() is NodeStatus.FAILURE

    def test_pretty_output_logs_one_line_per_leaf(self, manager, caplog):
        from bteng_mqtt.nodes.sleep import Sleep
        from tests.helpers import make_node

        node = make_node(Sleep, params={"duration": "0"})
        with caplog.at_level(logging.INFO, logger="bteng_mqtt.run"):
            assert TreeRunner(node, pretty=True).run() is NodeStatus.SUCCESS
        assert any("💤" in record.getMessage() for record in caplog.records)


class TestShutdown:
    def test_disconnects_and_drops_the_manager(self, mock_everywhere):
        from bteng_mqtt.registry import BrokerConfig

        manager = MqttClientManager.get_instance()
        manager.connect("leftover", BrokerConfig(timeout=2.0))
        shutdown()
        assert mock_everywhere.clients == []
        assert MqttClientManager.get_instance() is not manager


class TestLogging:
    def test_install_logging_replaces_handlers(self):
        root = logging.getLogger()
        original = list(root.handlers)
        original_level = root.level
        try:
            install_logging("DEBUG", pretty=False)
            assert len(root.handlers) == 1
            assert root.level == logging.DEBUG
        finally:
            root.handlers[:] = original
            root.setLevel(original_level)

    def test_unknown_level_falls_back_to_info(self):
        root = logging.getLogger()
        original = list(root.handlers)
        original_level = root.level
        try:
            install_logging("LOUD", pretty=False)
            assert root.level == logging.INFO
        finally:
            root.handlers[:] = original
            root.setLevel(original_level)


class TestShippedDemo:
    """The documented example must actually run."""

    def test_demo_tree_runs_against_the_example_config(self, mock_everywhere):
        from tests.conftest import EXAMPLE_CONFIG, ROOT

        tree = ROOT / "examples" / "trees" / "demo_tree.xml"
        assert run_trees([tree], EXAMPLE_CONFIG, pretty=False) == 0
        assert mock_everywhere.payloads_for("factory/line1/sensor/temp") == [b"21.5"]
