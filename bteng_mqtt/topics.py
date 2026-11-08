"""Topic-string helpers — wildcard matching and validation.

Kept in its own module because both the registry (which validates topics) and
the client manager / backends (which match arriving messages against
subscription filters) need it, and neither should import the other.
"""
from __future__ import annotations

WILDCARDS = ("+", "#")


def is_wildcard(topic: str) -> bool:
    """True if the topic string is a subscription filter, not a publish topic."""
    return any(w in topic for w in WILDCARDS)


def validate_filter(topic_filter: str) -> None:
    """Raise ValueError if the string is not a legal MQTT subscription filter.

    Rules (MQTT 3.1.1 §4.7): a level is either a full wildcard, a single-level
    wildcard, or contains no wildcard at all; ``#`` may only be the last level.
    """
    if not topic_filter:
        raise ValueError("topic filter must not be empty")
    levels = topic_filter.split("/")
    for index, level in enumerate(levels):
        if level == "#":
            if index != len(levels) - 1:
                raise ValueError(
                    f"'#' must be the last level of a filter, got {topic_filter!r}"
                )
        elif level != "+" and is_wildcard(level):
            raise ValueError(
                f"level {level!r} mixes a wildcard with other characters in {topic_filter!r}"
            )


def validate_topic(topic: str) -> None:
    """Raise ValueError if the string cannot be published to."""
    if not topic:
        raise ValueError("topic must not be empty")
    if is_wildcard(topic):
        raise ValueError(f"cannot publish to a wildcard topic: {topic!r}")


def topic_matches(topic_filter: str, topic: str) -> bool:
    """True if ``topic`` is matched by the subscription filter ``topic_filter``.

    Implements MQTT 3.1.1 §4.7: ``+`` matches exactly one level, ``#`` matches
    the remaining levels including none, and neither wildcard matches a topic
    whose first level starts with ``$`` (``$SYS/...`` stays out of ``#``).
    """
    if topic_filter == topic:
        return True

    filter_levels = topic_filter.split("/")
    topic_levels = topic.split("/")

    if topic_levels[0].startswith("$") and filter_levels[0] in WILDCARDS:
        return False

    for index, level in enumerate(filter_levels):
        if level == "#":
            # Trailing '#' also matches the parent level itself (a/# matches a).
            return True
        if index >= len(topic_levels):
            return False
        if level != "+" and level != topic_levels[index]:
            return False

    return len(filter_levels) == len(topic_levels)
