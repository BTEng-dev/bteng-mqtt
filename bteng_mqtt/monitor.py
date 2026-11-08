"""Tail a broker's traffic with the registry's types applied.

``bteng-mqtt monitor`` is the "what is actually on this topic?" tool: it prints
one line per message, decoded the way the trees will decode it, which is where
a wrong ``payload_type`` or ``json_path`` shows up first.
"""
from __future__ import annotations

import contextlib
import logging
import time
from typing import Any

from bteng_mqtt.client import Message, MqttClientManager
from bteng_mqtt.registry import BrokerConfig, TopicDefinition, TopicRegistry
from bteng_mqtt.topics import topic_matches
from bteng_mqtt.types import decode

logger = logging.getLogger(__name__)

MONITOR_CLIENT_ID = "bteng-mqtt-monitor"


def describe(message: Message, definition: TopicDefinition | None) -> str:
    """One line for a received message: the decoded value, or why it could not be.

    An undecodable payload is reported, not raised — the point of monitoring is
    to see exactly this.
    """
    stamp = time.strftime("%H:%M:%S", time.localtime(message.timestamp))
    label = message.topic
    flags = " (retained)" if message.retain else ""
    if definition is None:
        preview = message.payload.decode("utf-8", errors="replace")
        return f"{stamp}  {label}{flags} = {preview!r}  [no alias]"
    try:
        value = decode(message.payload, definition.payload_type, definition.json_path)
    except Exception as exc:
        raw = message.payload.decode("utf-8", errors="replace")
        return (f"{stamp}  {label}{flags} = {raw!r}  "
                f"[! {definition.payload_type.value}: {exc}]")
    return (f"{stamp}  {definition.alias}{flags} = {value!r}  "
            f"[{definition.payload_type.value}] {label}")


def monitor(
    registry: TopicRegistry,
    aliases: list[str] | None = None,
    filters: list[str] | None = None,
    duration: float = 0.0,
    broker: BrokerConfig | None = None,
    manager: MqttClientManager | None = None,
    emit: Any = print,
) -> int:
    """Subscribe and print decoded messages until ``duration`` (0 = until Ctrl-C).

    Returns the number of messages printed.  ``aliases`` defaults to every alias
    in the registry; ``filters`` adds raw topic filters, which is how you watch
    something the registry does not know about yet.
    """
    config = broker or registry.broker or BrokerConfig()
    manager = manager or MqttClientManager.get_instance()

    chosen = aliases if aliases else registry.all_aliases()
    definitions = [registry.resolve(alias) for alias in chosen]
    topics = [definition.topic for definition in definitions]
    watch = list(dict.fromkeys(topics + list(filters or [])))
    if not watch:
        logger.error("Nothing to monitor — the registry has no topics and no "
                     "filters were given")
        return 0

    manager.connect(MONITOR_CLIENT_ID, config)
    subscriptions = []
    printed = 0
    try:
        for topic_filter in watch:
            subscriptions.append(manager.subscribe(MONITOR_CLIENT_ID, topic_filter))
        emit(f"watching {len(watch)} topic(s) on {config.address}"
             + (f" for {duration:.0f}s" if duration else " — Ctrl-C to stop"))

        deadline = time.monotonic() + duration if duration > 0 else None
        seen: set[int] = set()
        while deadline is None or time.monotonic() < deadline:
            received = False
            for subscription in subscriptions:
                for message in subscription.drain():
                    received = True
                    if id(message) in seen:
                        continue  # overlapping filters deliver the same object
                    seen.add(id(message))
                    emit(describe(message, _match(definitions, message.topic)))
                    printed += 1
            if not received:
                time.sleep(0.02)
    except KeyboardInterrupt:
        emit("")  # leave the ^C on its own line
    finally:
        for subscription in subscriptions:
            subscription.close()
        with contextlib.suppress(Exception):
            manager.disconnect(MONITOR_CLIENT_ID)

    emit(f"{printed} message(s)")
    return printed


def _match(definitions: list[TopicDefinition], topic: str) -> TopicDefinition | None:
    """The definition describing a topic: exact match first, then wildcards."""
    for definition in definitions:
        if definition.topic == topic:
            return definition
    for definition in definitions:
        if topic_matches(definition.topic, topic):
            return definition
    return None
