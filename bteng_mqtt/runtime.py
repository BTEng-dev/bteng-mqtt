"""Run behavior trees with readable output — the application layer.

The library core logs plain ``logging`` records (see the node modules); this
module is where a *run* becomes a run: a synchronous tick loop, client cleanup,
failure reporting, and — for a terminal — colour and one emoji line per node as
it settles.  ``bteng-mqtt run`` is a thin CLI over :func:`run_trees`.

Colour/emoji live here, not in the library core, so file and CI logs stay clean;
``pretty=False`` (``--plain``) turns them off.
"""
from __future__ import annotations

import contextlib
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

import yaml
from bteng import Blackboard, NodeStatus

from bteng_mqtt import MqttClientManager, load_topics, load_tree, register_nodes
from bteng_mqtt.registry import get_global_registry

logger = logging.getLogger("bteng_mqtt.run")

# ── Colour formatting ───────────────────────────────────────────────────────────
_RESET  = "\033[0m"
_COLORS = {
    "DEBUG":    "\033[90m",
    "INFO":     "\033[36m",
    "WARNING":  "\033[33m",
    "ERROR":    "\033[31m",
    "CRITICAL": "\033[1;41m",
}


def _color_enabled(stream: Any) -> bool:
    if os.environ.get("NO_COLOR") is not None:
        return False
    if os.environ.get("FORCE_COLOR") is not None:
        return True
    return hasattr(stream, "isatty") and bool(stream.isatty())


class _ColorFormatter(logging.Formatter):
    def __init__(self, use_color: bool) -> None:
        super().__init__(fmt="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
                         datefmt="%H:%M:%S")
        self.use_color = use_color

    def format(self, record: logging.LogRecord) -> str:
        text = super().format(record)
        if not self.use_color:
            return text
        color = _COLORS.get(record.levelname, "")
        return f"{color}{text}{_RESET}" if color else text


def install_logging(level: str = "INFO", pretty: bool = True) -> None:
    """Configure the root logger for a run.

    Colours only when ``pretty`` and attached to a TTY; paho is quieted so our
    lines stand out.
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_ColorFormatter(pretty and _color_enabled(sys.stdout)))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    logging.getLogger("paho").setLevel(logging.WARNING)


# ── Per-node emoji reporting ────────────────────────────────────────────────────
_NODE_EMOJI = [
    ("MqttConnect",      "🔌"),
    ("MqttDisconnect",   "🔻"),
    ("MqttReconnect",    "🔄"),
    ("MqttBatchPublish", "📦"),
    ("MqttBatchRead",    "📚"),
    ("MqttRequestResponse", "⚙️"),
    ("MqttPublish",      "📤"),
    ("MqttUnsubscribe",  "🚫"),
    ("MqttSubscribe",    "📥"),
    ("MqttWaitForValue", "⏳"),
    ("MqttCondition",    "🔍"),
    ("Sleep",            "💤"),
    ("SetBlackboard",    "📝"),
    ("CheckBlackboard",  "🔎"),
]
_STATUS_MARK = {
    NodeStatus.SUCCESS: ("✅", logging.INFO),
    NodeStatus.FAILURE: ("❌", logging.ERROR),
}


def _emoji_for(node: Any) -> str:
    name = type(node).__name__
    for needle, emoji in _NODE_EMOJI:
        if needle in name:
            return emoji
    return "•"


def _attach_inspector(node: Any, inspector: Any) -> None:
    """Give every node in the tree the same Inspector (what bteng's executor does)."""
    node._inspector = inspector
    for child in node.get_children():
        _attach_inspector(child, inspector)


class _NodeReporter:
    """Log one emoji line as each node settles, and remember what failed.

    It listens on bteng's Inspector rather than polling node.status between
    ticks, because a Sequence halts its children the moment one fails — which
    resets that child's status to IDLE inside the same root tick.  Polling
    therefore never sees the FAILURE, and the node that actually broke would be
    missing from both the log and the failure report.
    """

    def __init__(self, root: Any, enabled: bool) -> None:
        self.enabled = enabled
        self.failures: list[tuple[str, str]] = []
        self._types: dict[str, str] = {}
        self._inspector: Any = None
        if root is None:
            return

        from bteng.introspection.inspector import Inspector

        self._inspector = Inspector()
        self._inspector.subscribe(self._on_record)
        _attach_inspector(root, self._inspector)
        # Leaves only: a Sequence failing right after its child failed would
        # otherwise log the same event twice, once without an explanation.
        self._types = _emoji_by_uid(root)

    def _on_record(self, record: Any) -> None:
        if record.status == NodeStatus.FAILURE and record.feedback_message:
            self.failures.append((record.name, record.feedback_message))
        if not self.enabled:
            return
        emoji = self._types.get(record.uid)
        if emoji is None:
            return  # a control node: its child already explained the outcome
        mark = _STATUS_MARK.get(record.status)
        if mark is None:
            return
        symbol, level = mark
        detail = ""
        if record.status == NodeStatus.FAILURE and record.feedback_message:
            detail = f" — {record.feedback_message}"
        logger.log(level, "%s %s %s%s", emoji, symbol, record.name, detail)

    def detach(self) -> None:
        """Stop listening — the tree is done with."""
        self._inspector = None


def _emoji_by_uid(node: Any, acc: dict[str, str] | None = None) -> dict[str, str]:
    """Map every *leaf* uid to its emoji, so a record can be decorated by uid.

    Control nodes are left out deliberately — they are the reporter's filter for
    "this event is worth a line".
    """
    acc = {} if acc is None else acc
    children = node.get_children()
    if children:
        for child in children:
            _emoji_by_uid(child, acc)
    else:
        acc[node.uid] = _emoji_for(node)
    return acc


class TreeRunner:
    """Tick a tree synchronously until it settles or times out."""

    def __init__(self, root: Any, tick_interval: float = 0.01, timeout: float = 60.0,
                 pretty: bool = True) -> None:
        self.root = root
        self.tick_interval = tick_interval
        self.timeout = timeout
        self.pretty = pretty
        #: Populated during run() — (node name, feedback) for every FAILURE seen.
        self.failures: list[tuple[str, str]] = []

    def run(self) -> NodeStatus:
        if self.pretty:
            logger.info("🌳 Tree execution started (timeout=%.1fs)", self.timeout)
        reporter = _NodeReporter(self.root, self.pretty)
        self.failures = reporter.failures
        start = time.monotonic()
        ticks = 0
        while True:
            status = self.root.execute_tick()
            ticks += 1
            if status != NodeStatus.RUNNING:
                if self.pretty:
                    emoji = "🎉" if status == NodeStatus.SUCCESS else "🛑"
                    elapsed = time.monotonic() - start
                    logger.info("%s Tree finished: %s  (ticks=%d, elapsed=%.2fs)",
                                emoji, status.value, ticks, elapsed)
                reporter.detach()
                return status
            if time.monotonic() - start >= self.timeout:
                self.root.halt()
                logger.error("⏰ Tree timed out after %.1fs (%d ticks)", self.timeout, ticks)
                self.failures.append((self.root.name, f"tree timed out after {self.timeout:.1f}s"))
                reporter.detach()
                return NodeStatus.FAILURE
            time.sleep(self.tick_interval)


# ── Client lifecycle + failure reporting ─────────────────────────────────────────
def shutdown() -> None:
    """Disconnect any client a tree left open, then drop the manager.

    A tree that fails before its MqttDisconnect leaves a live connection and a
    running network thread behind; closing them here keeps the next tree — and
    the process exit — clean.
    """
    manager = MqttClientManager.get_instance()
    for client_id in list(manager.active_clients()):
        with contextlib.suppress(Exception):
            manager.disconnect(client_id)
    MqttClientManager.reset_instance()


def failures(node: Any, found: list[tuple[str, str]] | None = None) -> list[tuple[str, str]]:
    """Collect feedback from nodes still showing FAILURE.

    Filtering on status keeps a successful node's incidental feedback (e.g.
    'Client connected') out of the report.

    Caveat worth knowing: a Sequence halts its children as soon as one fails,
    and halting resets that child's status to IDLE — so the node that actually
    broke is usually *not* in this list.  Prefer ``TreeRunner.failures``, which
    records every FAILURE as it happens via bteng's Inspector.  This function
    remains for inspecting a tree that was never run through TreeRunner.
    """
    found = [] if found is None else found
    if getattr(node, "status", None) == NodeStatus.FAILURE:
        message = getattr(node, "feedback_message", None) or getattr(node, "failure_reason", None)
        if message:
            found.append((getattr(node, "name", node.__class__.__name__), message))
    for child in node.get_children():
        failures(child, found)
    return found


# ── Orchestration ────────────────────────────────────────────────────────────────
def parse_bb_presets(raw: list[str]) -> dict[str, object]:
    """Parse repeated KEY=VALUE strings into a dict.

    Values go through a YAML scalar pass so they carry native types, the same as
    the registry's ``blackboard:`` section: ``x=false`` → ``False``, ``n=5`` →
    ``5``, ``s=abc`` → ``"abc"``.  Without it every preset would be a string and
    ``--bb enabled=false`` would write ``bool("false")`` == True.
    """
    result: dict[str, object] = {}
    for item in raw:
        if "=" not in item:
            raise ValueError(f"blackboard preset must be KEY=VALUE, got: {item!r}")
        key, _, value = item.partition("=")
        try:
            parsed = yaml.safe_load(value.strip())
        except yaml.YAMLError:
            parsed = value.strip()
        result[key.strip()] = parsed
    return result


def _collect_trees(paths: list[str | Path]) -> list[Path]:
    """Expand each path: a directory contributes its ``*.xml`` (sorted); a file
    is taken as-is."""
    trees: list[Path] = []
    for raw in paths:
        p = Path(raw)
        if p.is_dir():
            trees.extend(sorted(p.glob("*.xml")))
        else:
            trees.append(p)
    return trees


def run_one(path: Path, presets: dict[str, object], timeout: float, pretty: bool) -> NodeStatus:
    """Run a single tree to completion and return its final status.

    Any failure to build or tick the tree (unparseable XML, unknown node tag, a
    bad input like a non-numeric Sleep duration) becomes a FAILURE result rather
    than an exception, so one broken tree never aborts a batch.
    """
    Blackboard.reset("run")
    bb = Blackboard.create("run")
    for key, value in presets.items():
        bb.set(key, value)

    root = None
    reported: list[tuple[str, str]] = []
    try:
        root = load_tree(path, blackboard=bb)
        runner = TreeRunner(root, timeout=timeout, pretty=pretty)
        status = runner.run()
        reported = runner.failures
    except Exception as exc:
        logger.error("%s failed to run: %s", path.name, exc)
        status = NodeStatus.FAILURE
    finally:
        shutdown()

    if status != NodeStatus.SUCCESS:
        # Fall back to walking the tree when the runner never got to record
        # anything (a build error), so a failure is never silent.
        for name, message in reported or (failures(root) if root is not None else []):
            logger.error("  %s: %s", name, message)
    return status


def run_trees(paths: Any, topics_path: str | Path, presets: list[str] | None = None,
              timeout: float = 60.0, pretty: bool = True) -> int:
    """Register nodes, load the topic registry, run every tree, print a summary.

    Returns a process exit code: 0 if every tree ended in SUCCESS, else 1.

    Note: this does NOT configure logging — call :func:`install_logging` first
    (the CLI does) or let your own logging config stand.  A library caller keeps
    its handlers; only ``bteng-mqtt run`` installs the pretty one.
    """
    topics = Path(topics_path)
    if not topics.exists():
        logger.error("Topic registry not found: %s", topics)
        return 1

    trees = _collect_trees(paths if isinstance(paths, list) else [paths])
    if not trees:
        logger.error("No trees to run")
        return 1

    register_nodes()
    try:
        load_topics(topics)
    except Exception as exc:
        logger.error("Cannot load %s: %s", topics, exc)
        return 1

    # The registry's optional `blackboard:` section seeds values before every
    # tree; --bb presets override it per key.
    seed: dict[str, object] = dict(get_global_registry().blackboard)
    try:
        seed.update(parse_bb_presets(presets or []))
    except ValueError as exc:
        logger.error("%s", exc)
        return 1

    results: list[tuple[str, NodeStatus]] = []
    try:
        for path in trees:
            if not path.exists():
                logger.error("Tree not found: %s", path)
                results.append((path.name, NodeStatus.FAILURE))
                continue
            logger.info("── %s ──", path.name)
            results.append((path.name, run_one(path, seed, timeout, pretty)))
    finally:
        MqttClientManager.reset_instance()

    print("\n=== Summary ===")
    for name, status in results:
        mark = "✅" if status == NodeStatus.SUCCESS else "❌"
        print(f"  {mark if pretty else status.value[:4]} {name:<22} {status.value}")

    return 0 if all(s == NodeStatus.SUCCESS for _, s in results) else 1
