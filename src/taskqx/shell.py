"""Optional prompt_toolkit operations shell; imports its dependency only when launched."""

from __future__ import annotations

import shlex
from datetime import timedelta
from pathlib import Path
from typing import Any

from .interactive import render_json

_COMMANDS = (
    "health",
    "queues",
    "queue inspect",
    "messages",
    "message inspect",
    "payload show",
    "dlq list",
    "dlq replay",
    "dlq delete",
    "eq list",
    "eq replay",
    "eq delete",
    "consistency repair",
    "help",
    "quit",
)


def _prompt(backend: str, namespace: str | None) -> str:
    return f"taskqx[{backend}:{namespace or '-'}]> "


def _replay_options(arguments: list[str]) -> tuple[str | None, str, dict[str, Any]]:
    """Parse bounded replay options, including a complete replacement dedup key."""

    target_queue: str | None = None
    if arguments and arguments[0] not in {"keep", "remove", "replace"}:
        target_queue = arguments.pop(0)
    dedup_mode = arguments.pop(0) if arguments else "keep"
    if dedup_mode not in {"keep", "remove", "replace"}:
        raise ValueError("dedup mode 必须是 keep、remove 或 replace")
    if dedup_mode != "replace":
        if arguments:
            raise ValueError("keep/remove 不接受额外的 dedup 参数")
        return target_queue, dedup_mode, {}
    if len(arguments) != 3:
        raise ValueError("replace 需要 DEDUP_SCOPE DEDUP_KEY DEDUP_TTL_SECONDS")
    try:
        ttl = timedelta(seconds=float(arguments[2]))
    except ValueError as exc:
        raise ValueError("DEDUP_TTL_SECONDS 必须是正数") from exc
    if ttl.total_seconds() <= 0:
        raise ValueError("DEDUP_TTL_SECONDS 必须是正数")
    return (
        target_queue,
        dedup_mode,
        {
            "dedup_scope": arguments[0],
            "dedup_key": arguments[1],
            "dedup_ttl": ttl,
        },
    )


async def run_shell(broker: Any, *, backend: str, namespace: str | None) -> int:
    """Run the keyboard-first shell over public Broker/Admin APIs only."""

    from prompt_toolkit import PromptSession  # type: ignore[import-not-found]
    from prompt_toolkit.completion import (  # type: ignore[import-not-found]
        Completer,
        Completion,
    )
    from prompt_toolkit.document import Document  # type: ignore[import-not-found]
    from prompt_toolkit.history import FileHistory  # type: ignore[import-not-found]

    class BrokerCompleter(Completer):  # type: ignore[misc]
        def get_completions(self, document: Document, complete_event: Any) -> tuple[()]:
            del document, complete_event
            return ()

        async def get_completions_async(self, document: Document, complete_event: Any):
            del complete_event
            before = document.text_before_cursor
            try:
                words = shlex.split(before)
            except ValueError:
                return
            partial = "" if before.endswith(" ") else (words.pop() if words else "")
            candidates: tuple[str, ...] = _COMMANDS
            try:
                if words in (
                    ["queue", "inspect"],
                    ["messages"],
                    ["dlq", "list"],
                    ["eq", "list"],
                ):
                    page = await broker.list_queues(limit=50)
                    candidates = tuple(item.queue for item in page.items)
                elif len(words) == 3 and words[:2] in (
                    ["dlq", "replay"],
                    ["dlq", "delete"],
                ):
                    page = await broker.admin.page_dead_letters(words[2], limit=50)
                    candidates = tuple(item.message.id for item in page.items)
                elif len(words) == 3 and words[:2] in (
                    ["eq", "replay"],
                    ["eq", "delete"],
                ):
                    page = await broker.admin.page_expired(words[2], limit=50)
                    candidates = tuple(item.message.id for item in page.items)
            except Exception:  # noqa: BLE001 - completion is bounded best effort.
                return
            for candidate in candidates:
                if candidate.startswith(partial):
                    yield Completion(candidate, start_position=-len(partial))

    session: Any = PromptSession(
        history=FileHistory(str(Path.home() / ".taskqx_history")),
        completer=BrokerCompleter(),
        complete_while_typing=False,
    )
    while True:
        try:
            line = (await session.prompt_async(_prompt(backend, namespace))).strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if not line:
            continue
        try:
            command = shlex.split(line)
        except ValueError as exc:
            print(f"错误：{exc}")
            continue
        if command[0] in {"quit", "exit"}:
            return 0
        if command[0] == "help":
            print("""health
queues [CURSOR]
queue inspect QUEUE
messages QUEUE [CURSOR]
message inspect MESSAGE_ID
payload show MESSAGE_ID (显式显示敏感 payload)
dlq list QUEUE [CURSOR]
dlq replay QUEUE MESSAGE_ID [TARGET_QUEUE] [keep|remove]
dlq replay QUEUE MESSAGE_ID [TARGET_QUEUE] replace DEDUP_SCOPE DEDUP_KEY DEDUP_TTL_SECONDS
dlq delete QUEUE MESSAGE_ID
eq list QUEUE [CURSOR]
eq replay QUEUE MESSAGE_ID [TARGET_QUEUE] [keep|remove]
eq replay QUEUE MESSAGE_ID [TARGET_QUEUE] replace DEDUP_SCOPE DEDUP_KEY DEDUP_TTL_SECONDS
eq delete QUEUE MESSAGE_ID
consistency repair QUEUE (先显示 dry-run，再要求输入确认词)
quit""")
            continue
        try:
            await _execute(session, broker, command)
        except Exception as exc:  # noqa: BLE001 - retain actionable backend diagnostics in the shell
            print(f"操作失败：{type(exc).__name__}: {exc}")


async def _execute(session: Any, broker: Any, command: list[str]) -> None:
    if command == ["health"]:
        print(render_json(await broker.health_check()))
        return
    if 1 <= len(command) <= 2 and command[0] == "queues":
        print(
            render_json(
                await broker.list_queues(
                    cursor=command[1] if len(command) == 2 else None, limit=50
                )
            )
        )
        return
    if len(command) == 3 and command[:2] == ["queue", "inspect"]:
        print(render_json(await broker.observe_queue(command[2])))
        return
    if 2 <= len(command) <= 3 and command[0] == "messages":
        print(
            render_json(
                await broker.list_messages(
                    command[1],
                    cursor=command[2] if len(command) == 3 else None,
                    limit=50,
                )
            )
        )
        return
    if len(command) == 3 and command[:2] == ["message", "inspect"]:
        message = await broker.observe_message(command[2])
        if message is None:
            raise ValueError("未找到指定消息")
        print(render_json(message))
        return
    if len(command) == 3 and command[:2] == ["payload", "show"]:
        message = await broker.observe_message(command[2])
        if message is None:
            raise ValueError("未找到指定消息")
        print("警告：payload 可能包含敏感数据；以下内容不会写入命令历史。")
        print(render_json(message, include_payload=True))
        return
    if len(command) in {3, 4} and command[:2] in (["dlq", "list"], ["eq", "list"]):
        queue = command[2]
        cursor = command[3] if len(command) == 4 else None
        page = (
            await broker.admin.page_dead_letters(queue, cursor=cursor, limit=50)
            if command[0] == "dlq"
            else await broker.admin.page_expired(queue, cursor=cursor, limit=50)
        )
        print(render_json(page))
        return
    if len(command) >= 4 and command[0] in {"dlq", "eq"} and command[1] == "replay":
        source, queue, message_id = command[0], command[2], command[3]
        target_queue, dedup_mode, dedup_options = _replay_options(command[4:])
        print(
            render_json(
                {
                    "operation": f"{source.upper()} replay",
                    "source_queue": queue,
                    "target_queue": target_queue or queue,
                    "message_id": message_id,
                    "dedup_mode": dedup_mode,
                    "dedup": "<replacement configured>"
                    if dedup_options
                    else "<existing policy>",
                }
            )
        )
        confirmation = await session.prompt_async(
            f"输入 'replay {queue} {message_id}' 确认："
        )
        if confirmation != f"replay {queue} {message_id}":
            print("已取消。")
            return
        if source == "dlq":
            await broker.admin.replay_dead_letter(
                queue,
                message_id,
                target_queue=target_queue,
                dedup_mode=dedup_mode,
                **dedup_options,
            )
        else:
            await broker.admin.replay_expired(
                queue,
                message_id,
                expires_at=None,
                target_queue=target_queue,
                dedup_mode=dedup_mode,
                **dedup_options,
            )
        print(
            render_json(
                {
                    "replay_enqueued": message_id,
                    "queue": target_queue or queue,
                    "note": "业务 handler 的处理结果由后续异步执行决定。",
                }
            )
        )
        return
    if len(command) == 4 and command[0] in {"dlq", "eq"} and command[1] == "delete":
        source, queue, message_id = command[0], command[2], command[3]
        print(
            render_json(
                {
                    "operation": f"{source.upper()} delete",
                    "queue": queue,
                    "message_id": message_id,
                }
            )
        )
        confirmation = await session.prompt_async(
            f"输入 'delete {queue} {message_id}' 确认："
        )
        if confirmation != f"delete {queue} {message_id}":
            print("已取消。")
            return
        delete = (
            broker.admin.delete_dead_letter
            if source == "dlq"
            else broker.admin.delete_expired
        )
        print(
            render_json(
                {"deleted": await delete(queue, message_id), "message_id": message_id}
            )
        )
        return
    if len(command) == 3 and command[:2] == ["consistency", "repair"]:
        queue = command[2]
        proposal = await broker.repair_consistency(queue, dry_run=True)
        print(render_json(proposal))
        confirmation = await session.prompt_async(
            f"输入 'repair {queue}' 应用以上修复："
        )
        if confirmation != f"repair {queue}":
            print("已取消。")
            return
        print(render_json(await broker.repair_consistency(queue, dry_run=False)))
        return
    raise ValueError("未知命令；输入 help 查看可用命令")
