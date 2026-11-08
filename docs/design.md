# bteng_mqtt — design and plan

Target: use MQTT from a BTEng behavior tree while writing **no Python**. A
consuming project holds exactly two hand-written things — a topic registry
(YAML) and tree XML files — and runs them with `bteng-mqtt run`.

This mirrors [`bteng_opcua`](https://github.com/BTEng-dev/bteng-opcua) so the two
integrations feel like one product.

## Goal, stated precisely

```
project/
├── config/mqtt_config.yaml   # broker + topic aliases (hand-written or discovered)
└── trees/main_flow.xml       # the behavior tree
```

```bash
bteng-mqtt run trees/ -n config/mqtt_config.yaml
```

Everything else — client lifecycle, subscriptions, payload decoding, the tick
loop, blackboard plumbing, readable output, exit codes — belongs to the package.

## Package layout

```
bteng_mqtt/
├── __init__.py          # public API, BTENG_NODES, NullHandler, lazy XML helpers
├── exceptions.py        # MqttError, ConnectionError, ClientNotFoundError,
│                        # TopicAliasError, PayloadConversionError,
│                        # PublishError, ConditionNotSatisfiedError
├── types.py             # MqttPayloadType + encode()/decode() codecs
├── registry.py          # TopicRegistry, TopicDefinition, global singleton
├── client.py            # MqttClientManager: named clients, router, caches
├── backend/
│   ├── base.py          # MqttBackend ABC
│   └── paho_backend.py  # paho-mqtt v2 implementation (lazy import)
├── nodes/               # one module per BT node (see catalog)
├── xml_integration.py   # register_nodes() / load_tree()
├── runtime.py           # TreeRunner, install_logging, run_trees, shutdown
├── discovery.py         # sniff the broker, infer types, emit a registry
└── cli.py               # bteng-mqtt discover | run | monitor
```

## Topic registry

The MQTT counterpart of `nodes.yaml`. Aliases keep broker-specific topic
strings out of the trees.

```yaml
broker:
  host: 127.0.0.1
  port: 1883
  client_id: bteng
  keepalive: 60
  username: ""
  password: ""
  tls: false            # true → tls_ca / tls_cert / tls_key
  # last will, applied at connect
  will:
    topic: bteng/status
    payload: offline
    retain: true

topics:
  - alias: temperature
    topic: factory/line1/sensor/temp
    payload_type: float          # string|int|float|bool|json|bytes
    qos: 1

  - alias: robot_state
    topic: factory/line1/robot/state
    payload_type: json
    json_path: status.mode       # decode → the value at this path
    qos: 1

  - alias: conveyor_cmd
    topic: factory/line1/conveyor/cmd
    payload_type: json
    qos: 1
    retain: false
    # value_map lets a tree say value="start" and publish the real payload
    value_map:
      start: '{"cmd": "start", "speed": 1.0}'
      stop:  '{"cmd": "stop"}'

  # request/response pair (MQTT 5 style, works on 3.1.1 with a reply topic)
  - alias: calibrate
    topic: factory/line1/robot/req/calibrate
    payload_type: json
    response_topic: factory/line1/robot/res/calibrate
    response_payload_type: json
    timeout: 10.0

blackboard:          # optional seeds, same as bteng_opcua
  target_speed: 1.5
```

`payload_type` is the single source of truth for decode (broker → blackboard)
and encode (blackboard → broker). `json_path` extracts one field so trees stay
free of parsing. Wildcard topics (`+`, `#`) are legal for subscribe-only
aliases and rejected for publish.

## Node catalog

| Node | Base | Purpose |
|---|---|---|
| `MqttConnect` | StatefulAction | connect a named client (auth, TLS, LWT, keepalive), SUCCESS if already connected |
| `MqttDisconnect` | StatefulAction | clean DISCONNECT, keeps config for reconnect |
| `MqttReconnect` | StatefulAction | re-establish from the saved config |
| `MqttPublish` | StatefulAction | publish alias/topic; QoS 1/2 stay RUNNING until PUBACK/PUBCOMP |
| `MqttBatchPublish` | StatefulAction | several topics in one activation, one settle point |
| `MqttSubscribe` | StatefulAction | subscribe and wait; `mode=on_message` \| `on_value`; `persistent` keeps the subscription across activations |
| `MqttWaitForValue` | StatefulAction | wait until a topic's value satisfies `operator` (`eq/ne/gt/lt/ge/le/contains`) or time out |
| `MqttCondition` | Condition | synchronous check of the cached latest value — no waiting |
| `MqttBatchRead` | StatefulAction | copy the cached latest value of N aliases to the blackboard |
| `MqttRequestResponse` | StatefulAction | publish a request, correlate the reply on `response_topic`, time out — the `OpcUaCallMethod` analog |
| `MqttUnsubscribe` | StatefulAction | drop a persistent subscription explicitly |
| `Sleep` | StatefulAction | carried over from bteng_opcua |

Every node takes `client_id` (default `"default"`), accepts either
`topic_alias` or a raw `topic` + `payload_type`, and writes results through an
output port with an `output_key` blackboard fallback — identical to the OPC UA
nodes, so trees port across protocols by renaming tags.

## Client manager

`MqttClientManager` is a singleton (`get_instance()` / `reset_instance()` /
`set_instance()` for tests) holding, per client id:

- the paho client and its network thread (`loop_start()`),
- the saved connect config (for `MqttReconnect`),
- `topic → refcount` so N nodes on one topic mean one broker SUBSCRIBE,
- `topic → latest (payload, timestamp)` so `MqttCondition` can answer now,
- `subscriber_id → queue.SimpleQueue` so a node sees every message that arrived
  since it subscribed, not only those during its own tick.

Nodes never call paho directly and never block the tick loop: they register a
subscriber, then poll their queue / a `concurrent.futures.Future` from
`on_running()`.

## `bteng-mqtt discover`

MQTT has no information model, so discovery is a **sniff, and its output is
advisory**:

```bash
bteng-mqtt discover -b 127.0.0.1:1883 -d 30 -o config/mqtt_config.yaml
bteng-mqtt discover -s config/discover.yaml -o config/mqtt_config.yaml --check
```

Subscribe to the configured filters (default `#`) for `-d` seconds, group by
topic, infer `payload_type` from the observed payloads (JSON object → `json`
plus the flattened key list; `true/false` → `bool`; numeric → `int`/`float`;
else `string`), derive snake_case aliases from the last topic segment, and
write the `topics:` section while preserving hand-written sections (`broker:`,
`blackboard:`) exactly as `harvest` does. `--check` exits non-zero on drift.

A human must review the result: a topic that only ever carried `0` looks like
an `int` even when it is a float, and a silent topic is invisible. This is
called out in the generated file's header comment and in the README.

## CLI

```bash
bteng-mqtt run trees/ -n config/mqtt_config.yaml   # every tree, exit 1 on any failure
bteng-mqtt run trees/pick.xml -n config/mqtt_config.yaml --bb speed=1.5
bteng-mqtt discover -s config/discover.yaml -o config/mqtt_config.yaml [--check]
bteng-mqtt monitor -n config/mqtt_config.yaml [-a temperature ...]  # tail decoded values
```

`run` reuses the `bteng_opcua` behaviour: `--bb KEY=VALUE` (YAML-typed),
`--plain` for CI, `--timeout` per tree, a bad tree becomes a FAILURE instead of
aborting the batch, and a `=== Summary ===` table at the end.

## Milestones

1. ~~**Skeleton** — `pyproject.toml`, package dirs, exceptions, `types.py` codecs,
   `registry.py`, `backend/base.py` + MockBackend, `MqttClientManager`.~~
   **Done**: 152 tests, ruff + mypy clean. `topics.py` (wildcard matching) was
   added as its own module so the registry and the backends can share it without
   importing each other.
2. ~~**Core nodes** — Connect, Disconnect, Publish, Subscribe, Condition,
   WaitForValue, Sleep.~~ **Done**: 226 tests, ruff + mypy clean. Shared
   resolution lives in `nodes/base.py`; `MqttConnect` takes its broker settings
   from the registry's `broker:` section so a tree line is just a `client_id`.
3. ~~**Runtime** — `xml_integration.py`, `runtime.py`, `bteng-mqtt run`.~~
   **Done**: 259 tests, and the demo tree verified against a real broker (amqtt)
   with the paho backend. One design change: per-node reporting and failure
   collection listen on bteng's `Inspector` instead of polling `node.status`,
   because a Sequence halts (and so clears) the child that failed inside the same
   root tick — see `CLAUDE.md`.
4. ~~**Advanced nodes** — RequestResponse, BatchPublish, BatchRead, Unsubscribe,
   Reconnect; TLS and LWT on Connect.~~ **Done**: 307 tests, and all three
   example trees verified against a real broker. TLS/LWT landed with
   `MqttConnect` in milestone 2. Along the way: paho 2.x `ReasonCode` turned out
   not to be int-convertible (`.value` is the number) — pinned by
   `tests/test_backend_paho.py`.
5. ~~**Discovery** — `discovery.py`, `discover`, `--check` drift gate, `monitor`.~~
   **Done**: 370 tests, and both commands verified against live traffic. Type
   inference is a pure function (`infer_definitions`) so the rules are testable
   without a broker; `monitor` decodes with the registry's types, which is where a
   wrong `payload_type` or `json_path` shows up first.
6. **Tests** — MockBackend unit suite: **done** (370 tests, past bteng_opcua's
   ~194). Outstanding: an integration file that skips when no broker is
   listening, so the paho backend is covered in CI when one is available.
   `examples/demo_responder.py` already ships; the broker itself is `amqtt` from
   pip rather than a bundled script.
7. **Validation project** — separate `bteng_mqtt_test` project:
   `config/`, `trees/`, offline + live pytest layers, `Makefile`, `setup.sh`.
8. **Docs / CI** — README, `docs/authoring-nodes.md`, `docs/topic-registry.md`,
   `CHANGELOG.md`, GitHub Actions lint + offline tests.

## Open items

- **Broker for local runs.** `mosquitto` is not installed on this machine and
  `paho-mqtt` is absent from system Python. Plan: `examples/demo_broker.py`
  backed by `amqtt` (pip-installable, no root) as the default, with mosquitto
  or Docker honoured when present. Live tests TCP-probe the endpoint and skip.
- **MQTT 5 vs 3.1.1.** Target 3.1.1 for the first release with paho's v2 API;
  `MqttRequestResponse` uses an explicit `response_topic` in the registry, so
  it works on both. Native v5 properties (`Response-Topic`,
  `Correlation-Data`, shared subscriptions) come later behind a
  `protocol: mqtt5` broker option.
- **Retained messages.** A retained message arrives immediately on subscribe
  and can satisfy a `MqttSubscribe` with a stale value. Nodes get an
  `ignore_retained` port (default `false`, so the useful case works by default)
  and the behaviour is documented rather than guessed at.
