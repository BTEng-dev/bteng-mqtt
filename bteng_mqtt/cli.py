"""``bteng-mqtt`` command line interface.

    bteng-mqtt run trees/ -n config/mqtt_config.yaml         # run every tree
    bteng-mqtt run trees/pick.xml -n config/mqtt_config.yaml # run one tree
    bteng-mqtt run trees/ -n config/mqtt_config.yaml --bb speed=1.5

    bteng-mqtt discover -s config/discover.yaml -o config/mqtt_config.yaml
    bteng-mqtt discover -s config/discover.yaml -o config/mqtt_config.yaml --check
    bteng-mqtt monitor -n config/mqtt_config.yaml -a temperature

A consuming project needs no Python: the topic registry names the broker and the
topic aliases, the trees say what to do, and ``run`` runs them.

``discover`` proposes a registry from live traffic. Unlike OPC UA's harvest it is
a *sniff*, not a read of an information model, so its output is a draft for a
human to review — see the header it writes into the file.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path


def cmd_run(args: argparse.Namespace) -> int:
    from bteng_mqtt.runtime import install_logging, run_trees

    # The CLI is the app layer, so this is where logging gets configured (the
    # library core and run_trees leave the host's logging untouched).
    install_logging(args.log, pretty=not args.plain)
    return run_trees(
        args.trees,
        args.topics,
        presets=args.bb,
        timeout=args.timeout,
        pretty=not args.plain,
    )


def cmd_discover(args: argparse.Namespace) -> int:
    from bteng_mqtt.client import MqttClientManager
    from bteng_mqtt.discovery import (
        DiscoverSpec,
        discover,
        drift,
        manual_topics,
        preserved_sections,
        render,
    )
    from bteng_mqtt.registry import BrokerConfig
    from bteng_mqtt.runtime import install_logging

    install_logging(args.log, pretty=not args.plain)

    spec = DiscoverSpec.from_file(args.spec) if args.spec else DiscoverSpec()
    if args.broker:
        host, _, port = args.broker.partition(":")
        spec.broker = BrokerConfig(
            host=host or spec.broker.host,
            port=int(port) if port else spec.broker.port,
            username=args.username or spec.broker.username,
            password=args.password or spec.broker.password,
            timeout=spec.broker.timeout,
        )
    elif args.username or args.password:
        spec.broker.username = args.username or spec.broker.username
        spec.broker.password = args.password or spec.broker.password
    if args.filter:
        spec.filters = args.filter
    if args.duration is not None:
        spec.duration = args.duration

    try:
        definitions, warnings, observations = discover(spec)
    except Exception as exc:
        print(f"discover failed: {exc}", file=sys.stderr)
        return 2
    finally:
        MqttClientManager.reset_instance()

    for warning in warnings:
        print(f"warning: {warning}", file=sys.stderr)

    if not definitions:
        print(f"No traffic seen on {', '.join(spec.filters)} in "
              f"{spec.duration:.0f}s — nothing to write. Is anything publishing?",
              file=sys.stderr)
        return 2

    out = Path(args.output)
    samples = sum(len(v) for v in observations.values())

    if args.check:
        report = drift(out, definitions)
        if not report:
            print(f"{out} is up to date ({len(definitions)} topics)")
            return 0
        print(f"{out} does not match what the broker just published:",
              file=sys.stderr)
        for line in report:
            print(f"  {line}", file=sys.stderr)
        print("A quiet topic looks like a removal — review before re-running "
              "without --check.", file=sys.stderr)
        return 1

    kept = manual_topics(out)
    text = render(definitions, preserved_sections(out), spec.broker.address,
                  samples, spec.duration, manual=kept)
    out.write_text(text, encoding="utf-8")
    print(f"Wrote {out} — {len(definitions)} topics from {samples} message(s)"
          + (f", {len(kept)} hand-written entr{'y' if len(kept) == 1 else 'ies'} kept"
             if kept else ""))
    for definition in definitions:
        print(f"  {definition['alias']:<28} {definition['payload_type']:<8} "
              f"{definition['topic']}")
    if warnings:
        print(f"\n{len(warnings)} guess(es) worth reviewing — see the warnings above.")
    return 0


def cmd_monitor(args: argparse.Namespace) -> int:
    from bteng_mqtt.client import MqttClientManager
    from bteng_mqtt.monitor import monitor
    from bteng_mqtt.registry import TopicRegistry
    from bteng_mqtt.runtime import install_logging

    install_logging(args.log, pretty=not args.plain)

    registry = TopicRegistry()
    try:
        registry.load_file(args.topics)
    except Exception as exc:
        print(f"Cannot load {args.topics}: {exc}", file=sys.stderr)
        return 2

    try:
        monitor(
            registry,
            aliases=args.alias or None,
            filters=args.filter or None,
            duration=args.duration or 0.0,
        )
    except Exception as exc:
        print(f"monitor failed: {exc}", file=sys.stderr)
        return 2
    finally:
        MqttClientManager.reset_instance()
    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="bteng-mqtt", description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="command", required=True)

    r = sub.add_parser("run", help="Run behavior tree(s) against a topic registry")
    r.add_argument("trees", nargs="+",
                   help="Tree XML file(s), or a directory whose *.xml are all run")
    r.add_argument("-n", "--topics", default="mqtt_config.yaml",
                   help="Topic registry YAML (default: mqtt_config.yaml)")
    r.add_argument("--bb", action="append", default=[], metavar="KEY=VALUE",
                   help="Seed a blackboard key before each tree runs (repeatable)")
    r.add_argument("--timeout", type=float, default=60.0,
                   help="Per-tree timeout in seconds (default: 60)")
    r.add_argument("--plain", action="store_true",
                   help="Disable colour and emoji (for logs / CI)")
    r.add_argument("--log", default="INFO", help="Log level (DEBUG/INFO/WARNING)")
    r.set_defaults(func=cmd_run)

    d = sub.add_parser("discover",
                       help="Sniff a broker and propose a topic registry (review it!)")
    d.add_argument("-s", "--spec", help="Discovery spec YAML")
    d.add_argument("-o", "--output", default="mqtt_config.yaml",
                   help="Registry file to write (default: mqtt_config.yaml)")
    d.add_argument("-b", "--broker", metavar="HOST[:PORT]",
                   help="Override the spec's broker")
    d.add_argument("-f", "--filter", action="append", default=[], metavar="FILTER",
                   help="Topic filter to listen to (repeatable; default '#')")
    d.add_argument("-d", "--duration", type=float,
                   help="Seconds to listen (default: the spec's, else 15)")
    d.add_argument("--username", help="Override the spec's broker username")
    d.add_argument("--password", help="Override the spec's broker password")
    d.add_argument("--check", action="store_true",
                   help="Do not write; exit 1 if the file no longer matches the traffic")
    d.add_argument("--plain", action="store_true", help="Disable colour")
    d.add_argument("--log", default="INFO", help="Log level (DEBUG/INFO/WARNING)")
    d.set_defaults(func=cmd_discover)

    m = sub.add_parser("monitor", help="Print decoded messages as they arrive")
    m.add_argument("-n", "--topics", default="mqtt_config.yaml",
                   help="Topic registry YAML (default: mqtt_config.yaml)")
    m.add_argument("-a", "--alias", action="append", default=[],
                   help="Alias to watch (repeatable; default: every alias)")
    m.add_argument("-f", "--filter", action="append", default=[], metavar="FILTER",
                   help="Extra raw topic filter to watch (repeatable)")
    m.add_argument("-d", "--duration", type=float, default=0.0,
                   help="Stop after N seconds (default: run until Ctrl-C)")
    m.add_argument("--plain", action="store_true", help="Disable colour")
    m.add_argument("--log", default="WARNING", help="Log level (DEBUG/INFO/WARNING)")
    m.set_defaults(func=cmd_monitor)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
