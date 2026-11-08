# Discovering a registry

```bash
bteng-mqtt discover -s config/discover.yaml -o config/mqtt_config.yaml
bteng-mqtt discover -s config/discover.yaml -o config/mqtt_config.yaml --check
```

`discover` subscribes to a broker for a while, groups what it hears by topic,
guesses a payload type per topic, and writes the `topics:` section of a registry.

## It is a sniff, not a harvest

`bteng-opcua harvest` reads an OPC UA information model: the server states the
DataType of every node, so the generated file is authoritative. MQTT has no such
model. Everything here is inferred from whatever happened to be published during
the window, which means:

- **A quiet topic is invisible.** Nothing publishes it during the sniff, nothing
  appears in the file. A 30-second cycle needs a window longer than 30 seconds.
- **Types are guesses.** A counter that only ever sent `0` and `1` looks like a
  flag. A float that happened to be whole looks like an int.
- **Intent is invisible.** A request topic and a response topic are two unrelated
  topics to a sniffer; only a human knows they are one call.

So the output is a **draft**. Every guess worth checking is printed as a warning,
and the file itself carries the caveat in its header. Read it before committing.

## The spec

```yaml
broker:
  host: 127.0.0.1
  port: 1883
  username: ""
  password: ""

filters:            # what to listen to; "#" is everything
  - factory/line1/#

duration: 20.0      # seconds — longer than the slowest thing you care about
qos: 0

exclude:            # topics to drop from the result
  - $SYS/#

aliases:            # pin the names that matter
  factory/line1/sensor/temp: temperature
```

Anything in the spec can be overridden on the command line: `-b HOST:PORT`,
`-f FILTER` (repeatable), `-d SECONDS`, `--username`, `--password`.

## How types are inferred

| Observed | Inferred |
|---|---|
| any payload that is not valid UTF-8 | `bytes` |
| every sample is a JSON object or array | `json` (keys listed in `description`) |
| every sample is `true`/`false`/`on`/`off`/`yes`/`no` | `bool` |
| every sample parses as a whole number | `int` |
| every sample parses as a number | `float` |
| anything else | `string` |

Two deliberate choices: a bare `42` is valid JSON but types as `int`, because
that describes it better; and `0`/`1`-only topics type as `int` with a warning
suggesting `bool`, because a counter is more common than a flag that never says
"true". One non-integer sample widens `int` to `float` rather than being dropped.

Aliases come from the last topic level, snake_cased (`TestBool` → `test_bool`).
On a collision the parent level is prepended, so `a/temp` and `b/temp` become
`temp` and `b_temp`.

## Keeping hand-written entries

Wildcards, `value_map`s and request/response pairs can only be authored by a
human. Mark them so a re-discover does not delete them:

```yaml
  - alias: calibrate
    topic: robot/req/calibrate
    payload_type: json
    response_topic: robot/res/calibrate
    manual: true          # ← preserved on re-discover, ignored by --check
```

## `--check` as a gate

`--check` writes nothing and exits non-zero when the committed file disagrees
with what the broker just published. It compares only what a sniff can
determine — the set of topics and their payload types — so your reviewed `qos`,
`json_path` and `retain` never register as drift.

A red result means *look at this*, not *you are wrong*: a topic that simply
stayed quiet during the window is reported the same way as one that was removed,
and the message says so.

## Monitoring

```bash
bteng-mqtt monitor -n config/mqtt_config.yaml            # every alias
bteng-mqtt monitor -n config/mqtt_config.yaml -a temperature -a robot_mode
bteng-mqtt monitor -n config/mqtt_config.yaml -f 'factory/#' -d 30
```

`monitor` prints each message decoded exactly as the trees will decode it:

```
15:04:21  temperature = 21.5  [float] factory/line1/sensor/temp
15:04:21  robot_mode = 'running'  [json] factory/line1/robot/state
15:04:22  temperature = 'warm'  [! float: cannot read 'warm' as float] ...
```

That last line is the point: a wrong `payload_type` or `json_path` shows up here
in seconds instead of halfway through a tree.
