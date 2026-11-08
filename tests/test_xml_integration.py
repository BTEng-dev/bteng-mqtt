"""XML/factory bridge — the trees a consumer writes must build and run."""
from __future__ import annotations

import pytest
from bteng import NodeStatus
from bteng.factory.factory import NodeFactory

import bteng_mqtt
from bteng_mqtt import load_tree, register_nodes
from bteng_mqtt.runtime import TreeRunner
from tests.helpers import DEFAULT_TOPICS, new_blackboard

DEMO_TREE = """<?xml version="1.0" encoding="UTF-8"?>
<BTEng format_version="1.0" main_tree_to_execute="demo">
  <Tree ID="demo">
    <Sequence name="Demo">
      <MqttConnect   name="Connect"    client_id="demo" />
      <MqttPublish   name="Publish"    client_id="demo" topic_alias="temp"
                     value="21.5" retain="true" />
      <MqttSubscribe name="Read"       client_id="demo" topic_alias="temp"
                     timeout="5.0" persistent="true" />
      <MqttCondition name="IsWarm"     client_id="demo" topic_alias="temp"
                     operator="ge" expected_value="20" />
      <MqttDisconnect name="Disconnect" client_id="demo" />
    </Sequence>
  </Tree>
</BTEng>
"""

TWO_TREES = """<?xml version="1.0" encoding="UTF-8"?>
<BTEng format_version="1.0" main_tree_to_execute="second">
  <Tree ID="first">
    <Sequence name="First"><Sleep name="S1" duration="0" /></Sequence>
  </Tree>
  <Tree ID="second">
    <Sequence name="Second"><Sleep name="S2" duration="0" /></Sequence>
  </Tree>
</BTEng>
"""


@pytest.fixture
def tree_file(tmp_path):
    def write(xml: str, name: str = "tree.xml"):
        path = tmp_path / name
        path.write_text(xml, encoding="utf-8")
        return path
    return write


class TestRegisterNodes:
    def test_every_node_is_registered_under_its_xml_tag(self):
        factory = NodeFactory()
        register_nodes(factory)
        for name, _cls in bteng_mqtt.BTENG_NODES:
            assert factory.is_registered(name), name

    def test_bteng_builtins_are_registered_too(self):
        """bteng ships these but does not auto-register them."""
        factory = NodeFactory()
        register_nodes(factory)
        assert factory.is_registered("SetBlackboard")
        assert factory.is_registered("CheckBlackboard")

    def test_calling_twice_is_safe(self):
        factory = NodeFactory()
        register_nodes(factory)
        register_nodes(factory)
        assert factory.is_registered("MqttPublish")

    def test_defaults_to_the_singleton_factory(self):
        register_nodes()
        assert NodeFactory.get_instance().is_registered("MqttConnect")


class TestLoadTree:
    def test_builds_a_tree_from_xml(self, tree_file):
        root = load_tree(tree_file(DEMO_TREE), blackboard=new_blackboard())
        assert root is not None
        assert len(root.get_children()) == 5

    def test_tree_id_selects_one_of_several(self, tree_file):
        path = tree_file(TWO_TREES, "multi.xml")
        assert load_tree(path, tree_id="first", blackboard=new_blackboard()).name == "First"
        assert load_tree(path, blackboard=new_blackboard()).name == "Second"  # main_tree_to_execute

    def test_unknown_node_tag_is_an_error(self, tree_file):
        """An unregistered tag must not build silently into a no-op tree."""
        xml = DEMO_TREE.replace("MqttPublish", "MqttTeleport")
        with pytest.raises((KeyError, ValueError)) as excinfo:
            load_tree(tree_file(xml, "bad.xml"), blackboard=new_blackboard())
        assert "MqttTeleport" in str(excinfo.value)


class TestEndToEnd:
    """A whole tree, ticked by the real runner, against the mock broker."""

    def test_demo_tree_succeeds(self, manager, backend, tree_file):
        bteng_mqtt.get_global_registry().load_dict({
            "topics": DEFAULT_TOPICS,
            "broker": {"host": "127.0.0.1", "port": 1883, "timeout": 2.0},
        })
        root = load_tree(tree_file(DEMO_TREE), blackboard=new_blackboard())

        runner = TreeRunner(root, timeout=10.0, pretty=False)
        status = runner.run()

        assert status is NodeStatus.SUCCESS, runner.failures
        assert backend.payloads_for("a/temp") == [b"21.5"]
        assert root.blackboard.get("temp") == 21.5

    def test_a_failing_node_stops_the_sequence_and_is_reported(self, manager, tree_file):
        """No registry loaded, so the alias cannot resolve: Publish must fail."""
        bteng_mqtt.get_global_registry().load_dict({
            "topics": [],
            "broker": {"host": "127.0.0.1", "port": 1883, "timeout": 2.0},
        })
        root = load_tree(tree_file(DEMO_TREE), blackboard=new_blackboard())

        runner = TreeRunner(root, timeout=10.0, pretty=False)
        status = runner.run()

        assert status is NodeStatus.FAILURE
        # TreeRunner records failures as they happen: the Sequence halts its
        # children on failure, which resets the failed child's status to IDLE.
        reported = dict(runner.failures)
        assert "Publish" in reported
        assert "Unknown topic alias" in reported["Publish"]
