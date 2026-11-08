"""Exception hierarchy for bteng_mqtt.

``ConnectionError`` deliberately shadows the builtin inside this package — import
it explicitly (``from bteng_mqtt.exceptions import ConnectionError``) so it is
obvious which one a handler catches.
"""
from __future__ import annotations


class MqttError(Exception):
    """Base class for every bteng_mqtt error."""


class ConnectionError(MqttError):  # noqa: A001 - intentional shadow, see module docstring
    """Connecting to (or reconnecting to) a broker failed."""


class ClientNotFoundError(MqttError):
    """No client is registered under the requested client id."""


class TopicAliasError(MqttError):
    """A tree referenced a topic alias that the registry does not define."""


class PayloadConversionError(MqttError):
    """A payload could not be decoded to, or encoded from, the declared type."""


class PublishError(MqttError):
    """The broker rejected a publish, or it was never acknowledged."""


class SubscriptionError(MqttError):
    """A subscribe/unsubscribe request failed."""


class ConditionNotSatisfiedError(MqttError):
    """A condition node's expectation was not met."""
