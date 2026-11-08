# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — unreleased

First working version: a project with a topic registry and tree XML runs MQTT
behavior trees with no Python of its own.

### Added
- **Topic registry** (`registry.py`) — broker settings, topic aliases, payload
  types, QoS/retain, `json_path`, `value_map`, request/response pairs, and a
  `blackboard:` seed section.
- **Payload codecs** (`types.py`) — `string`, `int`, `float`, `bool`, `json`,
  `bytes`, with dotted `json_path` extraction.
- **Client manager** (`client.py`) — named connections, refcounted broker
  subscriptions shared by several nodes, per-subscriber queues that buffer
  between ticks, a latest-value cache, and resubscribe-after-reconnect.
- **Backends** — `PahoBackend` (paho-mqtt v2 callback API) and `MockBackend`, an
  in-process broker with retained-message replay for offline tests.
- **Twelve BT nodes** — `MqttConnect`, `MqttDisconnect`, `MqttReconnect`,
  `MqttPublish`, `MqttBatchPublish`, `MqttSubscribe`, `MqttUnsubscribe`,
  `MqttWaitForValue`, `MqttCondition`, `MqttBatchRead`, `MqttRequestResponse`
  and `Sleep`.
- **Runtime and CLI** — `bteng-mqtt run` (tick loop, per-node reporting, client
  cleanup, `--bb` presets, exit codes), `bteng-mqtt discover` (sniff a broker
  into a registry, `--check` drift gate) and `bteng-mqtt monitor` (tail decoded
  values).
- **Examples** — a fully commented registry, three trees and a demo responder.
- **Docs** — `docs/design.md`, `docs/topic-registry.md`, `docs/discovery.md`,
  `docs/authoring-nodes.md`.

### Notes
- Requires `bteng >= 0.3.2` and Python >= 3.12, which resolves from PyPI, so
  `pip install -e .` is enough. This package is not published yet.
  Verified against bteng 0.3.2: 381 passed, 14 skipped (the broker-marked
  integration tests, with no broker listening), the same as on 0.3.1, and all
  three example trees parse identically. No MQTT tree uses a control or decorator
  node, so 0.3.2's change to bind `{key}` references on their attributes does not
  reach this package. The behaviour floor is still 0.3.0; the declared floor is
  0.3.2 because it is the first release requiring Python 3.12, and because
  **bteng 0.3.0 and 0.3.1 are yanked on PyPI** (no reason recorded), so any lower
  pin installs a yanked artifact. The CI matrix drops to `["3.12"]`.
- `discover` is a **sniff, not a harvest**: MQTT has no information model, so a
  quiet topic is invisible and every payload type is a guess. Its output is a
  draft for review, and `--check` means "look at this", not "you are wrong".
- Per-node reporting listens on bteng's `Inspector` rather than polling
  `node.status`: a `Sequence` halts (and so clears) the child that failed inside
  the same root tick, which would otherwise hide the node that broke.
- paho 2.x `ReasonCode` is not int-convertible; its numeric code is `.value`.
