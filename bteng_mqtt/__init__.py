"""bteng_mqtt — MQTT integration for the BTEng Behavior Tree engine.

Milestone 2 (this state) ships the foundation — payload codecs, topic registry,
backend layer, client manager — plus the core behavior-tree nodes.  The
XML/factory bridge, the tree runner and the ``bteng-mqtt`` CLI follow in
milestones 3–5; see ``docs/design.md``.
"""
from __future__ import annotations

import logging

from bteng_mqtt.backend.base import BackendCallbacks, MqttBackend, PublishTicket
from bteng_mqtt.backend.mock import MockBackend
from bteng_mqtt.client import Message, MqttClientManager, Subscription
from bteng_mqtt.exceptions import (
    ClientNotFoundError,
    ConditionNotSatisfiedError,
    ConnectionError,
    MqttError,
    PayloadConversionError,
    PublishError,
    SubscriptionError,
    TopicAliasError,
)
from bteng_mqtt.nodes.batch_publish import MqttBatchPublish
from bteng_mqtt.nodes.batch_read import MqttBatchRead
from bteng_mqtt.nodes.condition import MqttCondition
from bteng_mqtt.nodes.connect import MqttConnect
from bteng_mqtt.nodes.disconnect import MqttDisconnect
from bteng_mqtt.nodes.publish import MqttPublish
from bteng_mqtt.nodes.reconnect import MqttReconnect
from bteng_mqtt.nodes.request_response import MqttRequestResponse
from bteng_mqtt.nodes.sleep import Sleep
from bteng_mqtt.nodes.subscribe import MqttSubscribe
from bteng_mqtt.nodes.unsubscribe import MqttUnsubscribe
from bteng_mqtt.nodes.wait_for_value import MqttWaitForValue
from bteng_mqtt.registry import (
    BrokerConfig,
    TlsConfig,
    TopicDefinition,
    TopicRegistry,
    WillConfig,
    get_global_registry,
    load_global,
    reset_global_registry,
)
from bteng_mqtt.topics import is_wildcard, topic_matches
from bteng_mqtt.types import MqttPayloadType, coerce, decode, encode

# Library logging: emit plain records under the "bteng_mqtt" logger tree and
# attach a NullHandler so nothing is printed unless the host app configures
# logging.  Colour/emoji are a presentation concern and live in runtime.py only.
logging.getLogger(__name__).addHandler(logging.NullHandler())

load_topics = load_global


# XML / BTEng factory integration — imported lazily so that importing this
# package does not pull in the XML parser for users who only need the client.
def register_nodes(factory=None):  # type: ignore[no-untyped-def]
    """Register all bteng_mqtt nodes with BTEng's NodeFactory singleton.

    Delegates to :func:`bteng_mqtt.xml_integration.register_nodes`.
    """
    from bteng_mqtt.xml_integration import register_nodes as _reg
    return _reg(factory)


def load_tree(path, tree_id=None, blackboard=None, factory=None):  # type: ignore[no-untyped-def]
    """Load a BTEng XML tree with all bteng_mqtt nodes pre-registered.

    Delegates to :func:`bteng_mqtt.xml_integration.load_tree`.
    """
    from bteng_mqtt.xml_integration import load_tree as _load
    return _load(path, tree_id=tree_id, blackboard=blackboard, factory=factory)

# Registered with BTEng's NodeFactory by xml_integration.register_nodes()
# (milestone 3); the XML tag is the first element of each pair.
BTENG_NODES: list[tuple[str, type]] = [
    ("MqttConnect",         MqttConnect),
    ("MqttDisconnect",      MqttDisconnect),
    ("MqttReconnect",       MqttReconnect),
    ("MqttPublish",         MqttPublish),
    ("MqttBatchPublish",    MqttBatchPublish),
    ("MqttSubscribe",       MqttSubscribe),
    ("MqttUnsubscribe",     MqttUnsubscribe),
    ("MqttWaitForValue",    MqttWaitForValue),
    ("MqttCondition",       MqttCondition),
    ("MqttBatchRead",       MqttBatchRead),
    ("MqttRequestResponse", MqttRequestResponse),
    ("Sleep",               Sleep),
]

__all__ = [
    # Exceptions
    "MqttError",
    "ConnectionError",
    "ClientNotFoundError",
    "TopicAliasError",
    "PayloadConversionError",
    "PublishError",
    "SubscriptionError",
    "ConditionNotSatisfiedError",
    # Payload types
    "MqttPayloadType",
    "decode",
    "encode",
    "coerce",
    # Topics
    "topic_matches",
    "is_wildcard",
    # Registry
    "TopicRegistry",
    "TopicDefinition",
    "BrokerConfig",
    "TlsConfig",
    "WillConfig",
    "get_global_registry",
    "reset_global_registry",
    "load_global",
    "load_topics",
    # Client
    "MqttClientManager",
    "Subscription",
    "Message",
    # Backend
    "MqttBackend",
    "BackendCallbacks",
    "PublishTicket",
    "MockBackend",
    # BT nodes
    "MqttConnect",
    "MqttDisconnect",
    "MqttReconnect",
    "MqttPublish",
    "MqttBatchPublish",
    "MqttSubscribe",
    "MqttUnsubscribe",
    "MqttWaitForValue",
    "MqttCondition",
    "MqttBatchRead",
    "MqttRequestResponse",
    "Sleep",
    # BTEng plugin integration
    "BTENG_NODES",
    "register_nodes",
    "load_tree",
]
