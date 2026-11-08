"""MQTT wildcard matching (3.1.1 §4.7) — subtle enough to pin down."""
from __future__ import annotations

import pytest

from bteng_mqtt.topics import is_wildcard, topic_matches, validate_filter, validate_topic


class TestMatching:
    @pytest.mark.parametrize(
        ("topic_filter", "topic"),
        [
            ("a/b/c", "a/b/c"),
            ("a/+/c", "a/b/c"),
            ("+/b/c", "a/b/c"),
            ("a/#", "a/b/c"),
            ("a/#", "a/b"),
            ("a/#", "a"),          # '#' also matches the parent level
            ("#", "a/b/c"),
            ("+/+/+", "a/b/c"),
            ("sport/+", "sport/tennis"),
        ],
    )
    def test_matches(self, topic_filter, topic):
        assert topic_matches(topic_filter, topic)

    @pytest.mark.parametrize(
        ("topic_filter", "topic"),
        [
            ("a/b/c", "a/b/d"),
            ("a/+", "a/b/c"),          # '+' is exactly one level
            ("a/+/c", "a/c"),
            ("a/b", "a/b/c"),
            ("+/b", "a/c"),
            ("sport/+", "sport"),       # a trailing level must exist for '+'
            ("#", "$SYS/broker/uptime"),      # wildcards never match $-topics
            ("+/broker/uptime", "$SYS/broker/uptime"),
        ],
    )
    def test_does_not_match(self, topic_filter, topic):
        assert not topic_matches(topic_filter, topic)

    def test_dollar_topic_matches_an_explicit_filter(self):
        assert topic_matches("$SYS/broker/uptime", "$SYS/broker/uptime")
        assert topic_matches("$SYS/#", "$SYS/broker/uptime")


class TestValidation:
    def test_is_wildcard(self):
        assert is_wildcard("a/+/c")
        assert is_wildcard("a/#")
        assert not is_wildcard("a/b/c")

    def test_publish_topic_rejects_wildcards(self):
        with pytest.raises(ValueError, match="wildcard topic"):
            validate_topic("a/+/c")

    def test_publish_topic_rejects_empty(self):
        with pytest.raises(ValueError, match="must not be empty"):
            validate_topic("")

    def test_hash_must_be_last(self):
        with pytest.raises(ValueError, match="last level"):
            validate_filter("a/#/c")

    def test_wildcard_cannot_share_a_level(self):
        with pytest.raises(ValueError, match="mixes a wildcard"):
            validate_filter("a/b+/c")

    def test_valid_filters(self):
        for good in ("a/b/c", "a/+/c", "a/#", "#", "+"):
            validate_filter(good)
