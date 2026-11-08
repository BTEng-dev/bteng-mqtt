# The topic registry

One YAML file describes the broker and every topic a tree may touch. It is the
reason a tree contains no broker addresses, no topic strings and no parsing:

```xml
<MqttPublish name="Start" client_id="main" topic_alias="conveyor_cmd" value="start" />
```

Everything that line needs — which topic, what the payload looks like, which QoS,
whether to retain — comes from the registry entry for `conveyor_cmd`.

Load it with `bteng-mqtt run trees/ -n config/mqtt_config.yaml`, or from Python
with `bteng_mqtt.load_topics("config/mqtt_config.yaml")`.

## Shape

```yaml
broker:      # one connection's worth of settings
topics:      # the aliases
blackboard:  # optional values seeded before each tree runs
```

## `broker:`

`MqttConnect` reads this section, so the usual tree line is just
`<MqttConnect client_id="main" />`. Any port on the node overrides the file for
that node only.

| Key | Default | Notes |
|---|---|---|
| `host` | `127.0.0.1` | |
| `port` | `1883` | 8883 conventionally means TLS |
| `client_id` | `""` | the MQTT-level id; empty lets paho generate one |
| `keepalive` | `60` | seconds |
| `username` / `password` | `""` | omit for anonymous brokers |
| `clean_session` | `true` | MQTT 3 only; ignored for `mqtt5` |
| `protocol` | `mqtt311` | `mqtt31`, `mqtt311` or `mqtt5` |
| `timeout` | `10.0` | seconds to wait for CONNACK |
| `tls` | absent | `true` for the system CA store, or a mapping |
| `will` | absent | Last Will and Testament |

```yaml
broker:
  host: broker.example.com
  port: 8883
  username: operator
  password: ""            # pass it at runtime instead of committing it
  tls:
    ca_certs: /etc/ssl/certs/ca.pem
    certfile: ""
    keyfile: ""
    insecure: false       # true skips verification — test setups only
  will:
    topic: bteng/status
    payload: offline
    retain: true
```

## `topics:`

| Key | Required | Meaning |
|---|---|---|
| `alias` | yes | the name trees use |
| `topic` | yes | the MQTT topic; wildcards are legal for subscribe-only aliases |
| `payload_type` | no (`string`) | `string`, `int`, `float`, `bool`, `json`, `bytes` |
| `qos` | no (`0`) | 0, 1 or 2 |
| `retain` | no (`false`) | ask the broker to retain what this alias publishes |
| `json_path` | no | dotted path into a `json` payload — `status.mode`, `axes.0` |
| `value_map` | no | tree-level word → real payload |
| `response_topic` | no | makes the alias usable by `MqttRequestResponse` |
| `response_payload_type` | no | defaults to `payload_type` |
| `timeout` | no | default wait for the reply, in seconds |
| `description` | no | free text; `discover` writes the JSON keys it saw here |
| `manual` | no (`false`) | hand-written: `discover` preserves it, `--check` ignores it |

```yaml
topics:
  - alias: temperature
    topic: factory/line1/sensor/temp
    payload_type: float
    qos: 1

  - alias: robot_mode
    topic: factory/line1/robot/state
    payload_type: json
    json_path: status.mode      # the blackboard gets "running", not the object

  - alias: conveyor_cmd
    topic: factory/line1/conveyor/cmd
    payload_type: json
    manual: true
    value_map:                  # the tree says value="start"
      start: '{"cmd": "start", "speed": 1.0}'
      stop:  '{"cmd": "stop"}'

  - alias: any_sensor           # subscribe-only: publishing to it is refused
    topic: factory/line1/sensor/+
    payload_type: float
    manual: true

  - alias: calibrate            # request/response pair
    topic: factory/line1/robot/req/calibrate
    payload_type: json
    response_topic: factory/line1/robot/res/calibrate
    response_payload_type: json
    timeout: 10.0
    manual: true
```

### Payload types

`payload_type` is used in both directions: decoding what arrives, and encoding
what a tree publishes.

| Type | Reading | Writing |
|---|---|---|
| `string` | raw text, whitespace kept | `str(value)` |
| `int` | `42`, and `7.0` → `7`; a fraction is an error | `"42"` |
| `float` | any number | `"1.5"` |
| `bool` | `true/false/1/0/on/off/yes/no`, case-insensitive | `true` / `false` |
| `json` | parsed; `json_path` extracts one field | objects serialised; a string that is already JSON passes through |
| `bytes` | raw bytes, no decoding | bytes as given |

A payload that does not fit its declared type fails the node with a message
naming the topic and the value — it never reaches the blackboard half-converted.

## `blackboard:`

Seeded before every tree runs, and overridden per key by `--bb KEY=VALUE`:

```yaml
blackboard:
  target_speed: 1.5
  operator: night_shift
```

Values keep their YAML types, so `enabled: false` is a boolean, not `"false"`.

## Generating it

`bteng-mqtt discover` writes the `topics:` section from live traffic and leaves
every other section alone. It is a **sniff, not an information-model read**:
a topic that stays quiet is missing, and each type is a guess from the samples
that arrived. Mark anything you author by hand with `manual: true` so a
re-discover keeps it and `--check` stops treating it as missing.

See [discovery.md](discovery.md).
