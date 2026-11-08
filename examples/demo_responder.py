#!/usr/bin/env python3
"""Answer the request topic that `trees/request_response_tree.xml` calls.

A stand-in for the device that would normally be listening: it subscribes to the
request topic, and publishes a JSON reply on the response topic.

    python examples/demo_responder.py                       # defaults to :1883
    python examples/demo_responder.py --port 11883 --once
    python examples/demo_responder.py --request-topic a/req --response-topic a/res

Needs paho-mqtt and a running broker. If you have no broker, `pip install amqtt`
gives you one without root:

    amqtt -c broker.yaml    # see CLAUDE.md for a minimal broker.yaml
"""
from __future__ import annotations

import argparse
import json
import signal
import sys

DEFAULT_REQUEST_TOPIC = "factory/line1/robot/req/calibrate"
DEFAULT_RESPONSE_TOPIC = "factory/line1/robot/res/calibrate"


def build_reply(payload: bytes) -> bytes:
    """Echo the request back with a status field, as a device would."""
    try:
        request = json.loads(payload.decode("utf-8")) if payload else {}
    except (UnicodeDecodeError, json.JSONDecodeError):
        request = {"raw": repr(payload)}
    return json.dumps({"status": "ok", "request": request}).encode("utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--request-topic", default=DEFAULT_REQUEST_TOPIC,
                        help="Topic to listen on")
    parser.add_argument("--response-topic", default=DEFAULT_RESPONSE_TOPIC,
                        help="Topic to answer on")
    parser.add_argument("--once", action="store_true",
                        help="Answer a single request, then exit")
    args = parser.parse_args(argv)

    try:
        import paho.mqtt.client as mqtt
    except ImportError:
        print("paho-mqtt is required: pip install paho-mqtt", file=sys.stderr)
        return 2

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="demo-responder")
    answered = {"count": 0}

    def on_connect(_client, _userdata, _flags, reason_code, _properties=None):
        # paho 2.x passes a ReasonCode, which int() refuses; .value is the code.
        code = reason_code if isinstance(reason_code, int) else getattr(reason_code, "value", 0)
        if code != 0:
            print(f"connect refused: {reason_code}", file=sys.stderr)
            return
        client.subscribe(args.request_topic, qos=1)
        print(f"listening on {args.request_topic}, answering on {args.response_topic}",
              flush=True)

    def on_message(_client, _userdata, message):
        reply = build_reply(bytes(message.payload))
        client.publish(args.response_topic, payload=reply, qos=1)
        answered["count"] += 1
        print(f"answered request {answered['count']}: {reply.decode()}", flush=True)
        if args.once:
            client.disconnect()

    client.on_connect = on_connect
    client.on_message = on_message

    signal.signal(signal.SIGINT, lambda *_: client.disconnect())

    client.connect(args.host, args.port, keepalive=60)
    client.loop_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
