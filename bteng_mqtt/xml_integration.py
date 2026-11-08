"""XML and factory integration for bteng_mqtt nodes with BTEng.

Bridges the MQTT nodes into BTEng's singleton ``NodeFactory`` and its
``XMLTreeParser``, which looks up custom node types through that factory — so
registering before parsing is all a consumer has to do.

Typical usage::

    import bteng_mqtt

    bteng_mqtt.load_topics("config/mqtt_config.yaml")   # topic aliases
    root = bteng_mqtt.load_tree("trees/main_flow.xml")  # registers + parses
"""
from __future__ import annotations

from pathlib import Path

from bteng.blackboard.blackboard import Blackboard
from bteng.core.node import TreeNode
from bteng.factory.factory import NodeFactory
from bteng.xml_parser.parser import XMLTreeParser

import bteng_mqtt


def register_nodes(factory: NodeFactory | None = None) -> None:
    """Register every bteng_mqtt node with BTEng's NodeFactory.

    Each ``(name, cls)`` pair in ``bteng_mqtt.BTENG_NODES`` is registered under
    its XML tag.  Calling this more than once is safe — re-registering a name
    overwrites the entry with the same class.

    Parameters
    ----------
    factory:
        The factory to register into.  Defaults to the global singleton, which
        is what ``XMLTreeParser`` uses when none is passed explicitly.
    """
    f = factory or NodeFactory.get_instance()
    for name, cls in bteng_mqtt.BTENG_NODES:
        f.register(cls, name)

    # bteng ships SetBlackboard/CheckBlackboard but its factory does not
    # auto-register them; do it here so a tree using <SetBlackboard> builds
    # without the consumer registering anything by hand.
    try:
        from bteng.nodes.leaf.builtins import CheckBlackboard, SetBlackboard
    except ImportError:  # pragma: no cover - a bteng that lacks these builtins
        pass
    else:
        f.register(SetBlackboard, "SetBlackboard")
        f.register(CheckBlackboard, "CheckBlackboard")


def load_tree(
    path: str | Path,
    tree_id: str | None = None,
    blackboard: Blackboard | None = None,
    factory: NodeFactory | None = None,
) -> TreeNode:
    """Load a BTEng XML tree with every bteng_mqtt node pre-registered.

    Parameters
    ----------
    path:
        The ``.xml`` file, in BTEng XML format.
    tree_id:
        Which ``<Tree ID="...">`` to build when the file holds several.
        Defaults to the first, or the one named by ``main_tree_to_execute``.
    blackboard:
        An existing blackboard to use; a fresh one is created when omitted.
    factory:
        Factory to register into and parse with. Defaults to the singleton.

    Returns
    -------
    TreeNode
        The root of the parsed tree.

    Notes
    -----
    Topic aliases are resolved when a node *ticks*, not when the tree is built,
    so ``load_topics()`` may run before or after this call — but it must run
    before the tree is executed.
    """
    f = factory or NodeFactory.get_instance()
    register_nodes(f)
    parser = XMLTreeParser(factory=f)
    return parser.parse_file(str(path), tree_id=tree_id, blackboard=blackboard)
