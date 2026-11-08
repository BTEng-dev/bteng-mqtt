# Writing a new MQTT node

The twelve bundled nodes cover the usual shapes; this is how to add another one
without re-deriving the plumbing.

## The skeleton

```python
"""MqttWhatever — one line on what it does."""
from __future__ import annotations

import logging

from bteng.core.node import InputPort, NodeConfig, NodeStatus, PortDefinition
from bteng.nodes.leaf.stateful_action import StatefulActionNode

from bteng_mqtt.nodes.base import MqttNodeMixin, target_ports, value_ports

logger = logging.getLogger(__name__)


class MqttWhatever(MqttNodeMixin, StatefulActionNode):
    """What it does, what each status means, and any caveat worth knowing."""

    def __init__(self, name: str, config: NodeConfig | None = None) -> None:
        super().__init__(name, config)
        self._subscription = None

    @classmethod
    def provided_ports(cls) -> list[PortDefinition]:
        return [
            *target_ports(),          # client_id, topic_alias, topic, payload_type, …
            *value_ports(),           # output_key + the "value" output port
            InputPort("timeout", "Max wait in seconds; 0 = forever", default=0.0),
        ]

    def on_start(self) -> NodeStatus:
        client_id = self.require_connected_client()
        if client_id is None:
            return NodeStatus.FAILURE
        target = self.resolve_target()          # for_publish=True to reject wildcards
        if target is None:
            return NodeStatus.FAILURE
        ...
        return NodeStatus.RUNNING

    def on_running(self) -> NodeStatus: ...
    def on_halted(self) -> None: ...
```

Then register it:

```python
# bteng_mqtt/nodes/__init__.py — export it
# bteng_mqtt/__init__.py       — import it and add ("MqttWhatever", MqttWhatever)
#                                to BTENG_NODES
# bteng_mqtt/runtime.py        — add an emoji to _NODE_EMOJI (optional)
```

`BTENG_NODES` is what `register_nodes()` feeds to BTEng's factory, so the XML tag
is the string in that pair.

## What the mixin gives you

| Helper | Does |
|---|---|
| `self.manager` | the `MqttClientManager` singleton |
| `require_connected_client()` | client id, or None with feedback already set |
| `resolve_target(for_publish=False)` | alias → `Target` (topic, type, qos, retain, json_path); raw `topic` + `payload_type` as the fallback; refuses wildcards when publishing |
| `resolve_alias_list("topic_aliases")` | the same for a comma-separated list |
| `paired_keys(targets, "output_keys")` | a key list defaulting to the aliases |
| `decode_message(message, target)` | payload → Python value |
| `store_value(value, target)` | output port first, then `output_key`, then the alias |
| `compare(value, expected, target)` | `eq/ne/gt/lt/ge/le/contains`, expectation coerced to the topic's type |

Use them rather than reading ports directly: it keeps every node's behaviour and
error messages consistent, which is most of what makes the XML predictable.

## Rules that are not negotiable

**Never block the tick loop.** No `sleep`, no `Future.result()`, no waiting on a
queue. Return `RUNNING` and check again next tick. Blocking work goes through
`self.manager.submit(...)`, which returns a `Future` to poll.

**Subscriptions are yours to close.** `manager.subscribe()` hands back a
`Subscription`; close it on success, on failure, on timeout and in `on_halted()`.
Leaked subscriptions keep the broker sending, and the queue growing, for the
rest of the run.

**Log plain records under `bteng_mqtt.nodes.*`.** No colour, no emoji — those
belong to `runtime.py`, so file and CI logs stay clean.

**Explain every FAILURE.** `set_feedback_message()` is what the runner prints
next to the ❌, and what a user has to debug from. Name the alias and the value.

**Say what a status means.** The class docstring should state when the node
returns SUCCESS, RUNNING and FAILURE. Half the value of these nodes is that a
reader of the XML can predict the tree's behaviour.

## Tests

`tests/helpers.py` drives one node without building a tree:

```python
from tests.helpers import make_node, registry_with, settle

def test_it_publishes(connected, backend):
    node = make_node(MqttWhatever, params={"topic_alias": "temp", "value": "1.5"})
    assert settle(node) is NodeStatus.SUCCESS
    assert backend.payloads_for("a/temp") == [b"1.5"]
```

- `connected` — a manager on `MockBackend` with one connected client
- `backend.inject(topic, payload)` — broker-side traffic
- `settle(node)` — tick until it stops returning RUNNING
- `make_node(..., registry=registry_with([...]))` — a private registry, so no test
  touches the global one

Cover the failure paths too: no connection, unknown alias, undecodable payload,
timeout, and that `halt()` releases whatever the node holds. If the node talks to
a broker in a way the mock cannot fake, add a case to
`tests/test_integration_broker.py`, which skips when no broker is listening.
