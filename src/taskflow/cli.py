"""Taskflow 的安全运维命令行，仅调用公开 Broker/Admin API。"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any

from .broker import RedisBroker, SQLiteBroker
from .errors import ValidationError


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="taskflow")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--sqlite", metavar="PATH", default="taskflow.db")
    source.add_argument("--redis-url", metavar="URL")
    parser.add_argument("--namespace", default="taskflow")
    parser.add_argument("--json", action="store_true", dest="as_json")
    parser.add_argument("--include-payload", action="store_true")
    commands = parser.add_subparsers(dest="command", required=True)
    queue = commands.add_parser("queue").add_subparsers(dest="queue_command", required=True)
    queue.add_parser("inspect").add_argument("queue")
    queue.add_parser("list-dead-letters").add_argument("queue")
    message = commands.add_parser("message").add_subparsers(dest="message_command", required=True)
    message.add_parser("inspect").add_argument("message_id")
    dlq = commands.add_parser("dlq").add_subparsers(dest="dlq_command", required=True)
    replay = dlq.add_parser("replay")
    replay.add_argument("queue")
    replay.add_argument("message_id")
    replay.add_argument("--target-queue")
    replay.add_argument("--dedup-mode", choices=("keep", "remove", "replace"), default="keep")
    replay.add_argument("--yes", action="store_true")
    commands.add_parser("health")
    args = parser.parse_args(argv)
    try:
        return asyncio.run(_run(args))
    except ValidationError as exc:
        print(f"taskflow: error: {exc}", file=sys.stderr)
        return 2


async def _run(args: argparse.Namespace) -> int:
    broker: SQLiteBroker | RedisBroker
    if args.redis_url:
        broker = RedisBroker.from_url(args.redis_url, namespace=args.namespace)
    else:
        broker = SQLiteBroker(args.sqlite)
    context = {"backend": "redis" if args.redis_url else "sqlite",
               "namespace": args.namespace if args.redis_url else None}
    async with broker:
        if args.command == "health":
            return _print({**context, "healthy": True, "probe": "backend_connectivity"}, args)
        if args.command == "queue":
            if args.queue_command == "inspect":
                return _print({**context, "queue": args.queue,
                               "stats": await broker.inspect(args.queue)}, args)
            return _print({**context, "queue": args.queue,
                           "dead_letters": await broker.admin.list_dead_letters(args.queue)}, args)
        if args.command == "message":
            value = await broker.inspect_message(args.message_id)
            if value is None:
                raise ValidationError("未找到指定消息")
            return _print({**context, "message": value}, args)
        if not args.yes:
            raise ValidationError("DLQ replay 会改变队列状态；请显式传入 --yes")
        await broker.admin.replay_dead_letter(args.queue, args.message_id, target_queue=args.target_queue,
                                              dedup_mode=args.dedup_mode)
        return _print({**context, "replayed": args.message_id,
                       "queue": args.target_queue or args.queue}, args)


def _print(value: Any, args: argparse.Namespace) -> int:
    rendered = _safe_value(value, include_payload=args.include_payload)
    if args.as_json:
        print(json.dumps(rendered, ensure_ascii=False, default=str, sort_keys=True))
    else:
        print(json.dumps(rendered, ensure_ascii=False, default=str, indent=2, sort_keys=True))
    return 0


def _safe_value(value: Any, *, include_payload: bool) -> Any:
    if is_dataclass(value):
        value = asdict(value)  # type: ignore[arg-type]  # is_dataclass 是运行时 adapter 边界
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [_safe_value(item, include_payload=include_payload) for item in value]
    if isinstance(value, dict):
        return {key: (item if key != "payload" or include_payload else "<redacted>")
                for key, item in ((key, _safe_value(item, include_payload=include_payload)) for key, item in value.items())}
    return value
