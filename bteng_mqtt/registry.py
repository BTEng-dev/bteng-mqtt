"""Topic registry — broker settings and topic aliases loaded from YAML/JSON.

The registry is what keeps broker-specific strings out of the trees: a tree says
``topic_alias="temperature"`` and the registry knows the topic, its payload type,
QoS and retain flag.  It is the MQTT counterpart of ``bteng_opcua``'s node
registry.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from bteng_mqtt.exceptions import TopicAliasError
from bteng_mqtt.topics import is_wildcard, validate_filter, validate_topic
from bteng_mqtt.types import MqttPayloadType


@dataclass
class TlsConfig:
    """TLS settings for a broker connection."""
    ca_certs: str = ""
    certfile: str = ""
    keyfile:  str = ""
    insecure: bool = False   # skip hostname/certificate verification

    @classmethod
    def from_value(cls, raw: Any) -> TlsConfig | None:
        """Build from ``tls: true`` (system CA store) or a mapping; None if off."""
        if raw is None or raw is False:
            return None
        if raw is True:
            return cls()
        if not isinstance(raw, dict):
            raise ValueError(f"'tls' must be a boolean or a mapping, got {raw!r}")
        return cls(
            ca_certs=str(raw.get("ca_certs", "") or ""),
            certfile=str(raw.get("certfile", "") or ""),
            keyfile=str(raw.get("keyfile", "") or ""),
            insecure=bool(raw.get("insecure", False)),
        )


@dataclass
class WillConfig:
    """Last Will and Testament, published by the broker if the client vanishes."""
    topic:   str
    payload: str = ""
    qos:     int = 0
    retain:  bool = False

    @classmethod
    def from_value(cls, raw: Any) -> WillConfig | None:
        if raw is None:
            return None
        if not isinstance(raw, dict):
            raise ValueError(f"'will' must be a mapping, got {raw!r}")
        topic = str(raw.get("topic", "") or "")
        if not topic:
            raise ValueError("'will' needs a 'topic'")
        validate_topic(topic)
        qos = int(raw.get("qos", 0))
        if qos not in (0, 1, 2):
            raise ValueError(f"will qos must be 0, 1 or 2, got {qos}")
        return cls(
            topic=topic,
            payload=str(raw.get("payload", "") or ""),
            qos=qos,
            retain=bool(raw.get("retain", False)),
        )


@dataclass
class BrokerConfig:
    """Everything needed to open one MQTT connection."""
    host:          str  = "127.0.0.1"
    port:          int  = 1883
    client_id:     str  = ""       # "" → the library lets paho generate one
    keepalive:     int  = 60
    username:      str  = ""
    password:      str  = ""
    clean_session: bool = True
    protocol:      str  = "mqtt311"   # or "mqtt5"
    timeout:       float = 10.0       # seconds to wait for CONNACK
    tls:           TlsConfig | None = None
    will:          WillConfig | None = None

    PROTOCOLS = ("mqtt311", "mqtt31", "mqtt5")

    def __post_init__(self) -> None:
        if self.protocol not in self.PROTOCOLS:
            raise ValueError(
                f"unknown protocol {self.protocol!r} — use one of {list(self.PROTOCOLS)}"
            )
        if not 1 <= self.port <= 65535:
            raise ValueError(f"port out of range: {self.port}")

    @property
    def address(self) -> str:
        """``host:port``, for log lines and error messages."""
        return f"{self.host}:{self.port}"

    @classmethod
    def from_dict(cls, raw: dict | None) -> BrokerConfig:
        raw = raw or {}
        return cls(
            host=str(raw.get("host", "127.0.0.1")),
            port=int(raw.get("port", 1883)),
            client_id=str(raw.get("client_id", "") or ""),
            keepalive=int(raw.get("keepalive", 60)),
            username=str(raw.get("username", "") or ""),
            password=str(raw.get("password", "") or ""),
            clean_session=bool(raw.get("clean_session", True)),
            protocol=str(raw.get("protocol", "mqtt311")),
            timeout=float(raw.get("timeout", 10.0)),
            tls=TlsConfig.from_value(raw.get("tls")),
            will=WillConfig.from_value(raw.get("will")),
        )


@dataclass
class TopicDefinition:
    """One aliased topic: where it is, what it carries, how it is sent."""
    alias:        str
    topic:        str
    payload_type: MqttPayloadType = MqttPayloadType.STRING
    qos:          int  = 0
    retain:       bool = False
    json_path:    str | None = None
    value_map:    dict[str, Any] = field(default_factory=dict)
    # Request/response pair — the OpcUaCallMethod analog.
    response_topic:        str | None = None
    response_payload_type: MqttPayloadType | None = None
    timeout:      float | None = None
    description:  str = ""
    #: Hand-written entry: `bteng-mqtt discover` preserves it instead of
    #: regenerating it, and `--check` does not expect to see it in the traffic.
    #: Wildcards, request/response pairs and value_maps can only be authored by
    #: a human, so without this they would be deleted on every re-discover.
    manual:       bool = False

    @property
    def is_wildcard(self) -> bool:
        """True for subscribe-only aliases (``+``/``#`` in the topic)."""
        return is_wildcard(self.topic)

    @property
    def response_type(self) -> MqttPayloadType:
        """Payload type of the reply; defaults to the request's own type."""
        return self.response_payload_type or self.payload_type

    def validate(self) -> None:
        """Raise ValueError if the definition is internally inconsistent."""
        if not self.topic:
            raise ValueError(f"alias '{self.alias}' has no topic")
        if self.is_wildcard:
            validate_filter(self.topic)
        else:
            validate_topic(self.topic)
        if self.qos not in (0, 1, 2):
            raise ValueError(f"alias '{self.alias}': qos must be 0, 1 or 2, got {self.qos}")
        if self.json_path and self.payload_type is not MqttPayloadType.JSON:
            raise ValueError(
                f"alias '{self.alias}': json_path needs payload_type 'json', "
                f"got {self.payload_type.value!r}"
            )
        if self.response_payload_type and not self.response_topic:
            raise ValueError(
                f"alias '{self.alias}': response_payload_type without a response_topic"
            )
        if self.response_topic:
            if is_wildcard(self.response_topic):
                validate_filter(self.response_topic)
            else:
                validate_topic(self.response_topic)
        if self.is_wildcard and self.retain:
            raise ValueError(
                f"alias '{self.alias}': retain is meaningless on a wildcard "
                "(subscribe-only) alias"
            )

    def resolve_value(self, value: Any) -> Any:
        """Map a tree-level value through ``value_map`` when it has an entry.

        Lets a tree say ``value="start"`` while the broker receives whatever
        payload the registry pairs with that word.
        """
        if self.value_map and isinstance(value, str) and value in self.value_map:
            return self.value_map[value]
        return value


class TopicRegistry:
    """Topic aliases plus the broker section, loaded from YAML or JSON."""

    def __init__(self) -> None:
        self._topics: dict[str, TopicDefinition] = {}
        self._broker: BrokerConfig | None = None
        self._blackboard: dict[str, Any] = {}

    # ── Loading ───────────────────────────────────────────────────────────────

    def load_file(self, path: str | Path) -> None:
        """Parse a YAML or JSON file and merge it into this registry."""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Topic registry file not found: {path}")
        text = path.read_text(encoding="utf-8")
        if path.suffix in (".yaml", ".yml"):
            data = yaml.safe_load(text)
        elif path.suffix == ".json":
            data = json.loads(text)
        else:
            raise ValueError(
                f"Unsupported file format '{path.suffix}'. Use .yaml, .yml, or .json"
            )
        if not isinstance(data, dict):
            raise ValueError(f"{path}: registry must be a mapping at the top level")
        self.load_dict(data)

    def load_dict(self, data: dict) -> None:
        """Merge a parsed registry mapping into this one.

        Later loads win: a repeated alias is replaced, and a ``broker:`` section
        overrides the previous one.
        """
        topics = data.get("topics")
        if topics is None:
            raise ValueError("Registry data must contain a top-level 'topics' list")
        if not isinstance(topics, list):
            raise ValueError(f"'topics' must be a list, got {type(topics).__name__}")

        for raw in topics:
            defn = self._parse_topic(raw)
            defn.validate()
            self._topics[defn.alias] = defn

        if "broker" in data:
            self._broker = BrokerConfig.from_dict(data.get("broker"))

        for key, value in (data.get("blackboard") or {}).items():
            self._blackboard[str(key)] = value

    # ── Lookup ────────────────────────────────────────────────────────────────

    def resolve(self, alias: str) -> TopicDefinition:
        """Return the definition for an alias. Raises TopicAliasError if absent."""
        defn = self._topics.get(alias)
        if defn is None:
            raise TopicAliasError(
                f"Unknown topic alias '{alias}'. "
                f"Registered aliases: {sorted(self._topics)}"
            )
        return defn

    def has(self, alias: str) -> bool:
        return alias in self._topics

    def all_aliases(self) -> list[str]:
        return list(self._topics)

    @property
    def broker(self) -> BrokerConfig | None:
        """The ``broker:`` section, or None when the file has none."""
        return self._broker

    @property
    def blackboard(self) -> dict[str, Any]:
        """Seed values from the ``blackboard:`` section."""
        return dict(self._blackboard)

    # ── Internal parsing ──────────────────────────────────────────────────────

    def _parse_topic(self, raw: Any) -> TopicDefinition:
        if not isinstance(raw, dict):
            raise ValueError(f"Each entry in 'topics' must be a mapping, got {raw!r}")
        for key in ("alias", "topic"):
            if not raw.get(key):
                raise ValueError(f"Topic definition missing required field '{key}': {raw}")
        alias = str(raw["alias"])
        return TopicDefinition(
            alias=alias,
            topic=str(raw["topic"]),
            payload_type=self._parse_type(raw.get("payload_type", "string"), alias),
            qos=int(raw.get("qos", 0)),
            retain=bool(raw.get("retain", False)),
            json_path=(str(raw["json_path"]) if raw.get("json_path") else None),
            value_map=dict(raw.get("value_map") or {}),
            response_topic=(str(raw["response_topic"]) if raw.get("response_topic") else None),
            response_payload_type=(
                self._parse_type(raw["response_payload_type"], alias)
                if raw.get("response_payload_type") else None
            ),
            timeout=(float(raw["timeout"]) if raw.get("timeout") is not None else None),
            description=str(raw.get("description", "") or ""),
            manual=bool(raw.get("manual", False)),
        )

    @staticmethod
    def _parse_type(raw: Any, alias: str) -> MqttPayloadType:
        try:
            return MqttPayloadType(str(raw))
        except ValueError as exc:
            valid = [t.value for t in MqttPayloadType]
            raise ValueError(
                f"Unknown payload_type '{raw}' for alias '{alias}'. Supported: {valid}"
            ) from exc


# ── Global registry singleton ─────────────────────────────────────────────────

_global_registry: TopicRegistry | None = None


def get_global_registry() -> TopicRegistry:
    """Return the process-wide shared TopicRegistry, creating it if needed."""
    global _global_registry
    if _global_registry is None:
        _global_registry = TopicRegistry()
    return _global_registry


def reset_global_registry() -> None:
    """Clear and recreate the global registry (useful in tests)."""
    global _global_registry
    _global_registry = None


def load_global(path: str | Path) -> None:
    """Load a topic registry file into the global registry."""
    get_global_registry().load_file(path)
