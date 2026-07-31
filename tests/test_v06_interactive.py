"""v0.6 optional interactive operations foundation."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from taskflow import MessageStatus, RedisBroker, SQLiteBroker
from taskflow.cli import main


def test_interactive_commands_reject_non_tty_without_optional_dependencies(
    tmp_path: Path,
    capsys,
) -> None:  # type: ignore[no-untyped-def]
    database = tmp_path / "operations.db"

    assert main(["tui", "--sqlite", str(database)]) == 2
    assert "需要 TTY" in capsys.readouterr().err
    assert main(["shell", "--sqlite", str(database)]) == 2
    assert "需要 TTY" in capsys.readouterr().err


def test_sqlite_readonly_observation_never_initializes_database(tmp_path: Path) -> None:
    database = tmp_path / "operations.db"

    async def seed() -> str:
        async with SQLiteBroker(database) as writer:
            return (
                await writer.submit(queue="emails", payload={"private": "value"})
            ).message_id

    message_id = asyncio.run(seed())
    before = database.read_bytes()

    async def observe() -> None:
        reader = SQLiteBroker(database)
        health = await reader.health_check()
        stats = await reader.observe_queue("emails")
        message = await reader.observe_message(message_id)
        assert health.healthy
        assert stats.ready == 1
        assert message is not None and message.payload == {"private": "value"}
        assert reader._connection is None
        await reader.close()

    asyncio.run(observe())
    assert database.read_bytes() == before


@pytest.mark.asyncio
async def test_sqlite_message_summary_does_not_decode_payload(tmp_path: Path) -> None:
    from taskflow import SerializerUnavailableError
    from tests.support import BinaryJsonSerializer

    database = tmp_path / "serialized-message.db"
    async with SQLiteBroker(database, serializer=BinaryJsonSerializer()) as writer:
        message_id = (
            await writer.submit(queue="emails", payload={"private": "value"})
        ).message_id

    reader = SQLiteBroker(database)
    try:
        page = await reader.list_message_summaries("emails")
        assert page.items[0].message_id == message_id
        assert page.items[0].serializer_name == "binary-json"
        with pytest.raises(SerializerUnavailableError):
            await reader.list_messages("emails")
    finally:
        await reader.close()


@pytest.mark.asyncio
async def test_textual_dashboard_handles_empty_queue_table() -> None:
    pytest.importorskip("textual")

    from textual.widgets import DataTable

    from taskflow.tui import build_tui_app

    async with SQLiteBroker() as broker:
        app = build_tui_app(broker, backend="sqlite", namespace=None)
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause()
            assert app.query_one("#queues", DataTable).row_count == 0
            await pilot.press("3")
            await pilot.pause()
            assert app._queue is None


@pytest.mark.asyncio
async def test_textual_dashboard_browses_bounded_redacted_pages() -> None:
    pytest.importorskip("textual")
    from rich.text import Text
    from textual.containers import VerticalScroll
    from textual.widgets import DataTable, Footer, Input, ListView, Static
    from textual.widgets._footer import FooterKey

    from taskflow.tui import build_tui_app

    async with SQLiteBroker() as broker:
        message_id = (
            await broker.submit(queue="emails", payload={"private": "value"})
        ).message_id
        delivery = await broker.consumer("emails").__anext__()
        await delivery.reject(reason="first rejection")
        for index in range(30):
            await broker.submit(queue=f"queue-{index:02}", payload={})
        app = build_tui_app(
            broker,
            backend="sqlite",
            namespace=None,
        )
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause()
            queues = app.query_one("#queues", DataTable)
            records = app.query_one("#records", DataTable)
            overall = app.query_one("#overall", Static)
            assert queues.row_count == 0
            assert records.row_count == 0
            assert "HEALTHY" in overall.render().plain
            sidebar = app.query_one("#sidebar")
            content = app.query_one("#content")
            status_panel = app.query_one("#status-panel")
            namespace_panel = app.query_one("#namespace-panel")
            namespaces = app.query_one("#namespaces", ListView)
            detail_panel = app.query_one("#detail-panel", VerticalScroll)
            status_main = app.query_one("#status-main", VerticalScroll)
            status_detail = app.query_one("#status-detail", Static)
            detail = app.query_one("#detail", Static)
            assert app.current_theme.name == "dracula"
            assert status_panel.parent is sidebar
            assert namespace_panel.parent is sidebar
            assert queues.parent is sidebar
            assert records.parent is content
            assert detail_panel.parent is content
            assert detail.parent is detail_panel
            assert detail_panel.can_focus
            assert status_main.can_focus
            assert app.query_one("#version", Static).render().plain.startswith("v")
            footer = app.query_one(Footer)
            assert all(key.key not in {"1", "3"} for key in footer.query(FooterKey))
            assert len(namespaces) == 1
            assert (
                datetime.now().astimezone().tzname()
                in app.query_one("#refreshed", Static).render().plain
            )
            assert Text.from_markup(str(status_panel.border_title)).plain == "1-Status"
            assert (
                Text.from_markup(str(namespace_panel.border_title)).plain
                == "2-Namespace"
            )
            assert Text.from_markup(str(queues.border_title)).plain == "3-Queue"
            initial_refresh = app._last_refreshed
            assert initial_refresh is not None
            await pilot.press("1")
            await pilot.pause()
            assert app._last_refreshed is initial_refresh
            assert queues.row_count == 0
            await pilot.press("3")
            await pilot.pause()
            queue_refresh = app._last_refreshed
            assert queue_refresh is not initial_refresh
            assert queues.row_count == 25
            assert records.row_count == 1
            row_key, _ = records.coordinate_to_cell_key(records.cursor_coordinate)
            assert row_key.value == message_id
            assert str(records.get_row_at(records.cursor_row)[1]).endswith("...")
            assert "状态: DEAD_LETTERED" in detail.render().plain
            assert detail.render().plain.endswith("<redacted>")
            await pilot.press("1")
            await pilot.pause()
            await pilot.press("3")
            await pilot.pause()
            assert app._last_refreshed is queue_refresh
            await pilot.press("v")
            await pilot.pause()
            assert "private" in detail.render().plain
            detail.update("\n".join(str(line) for line in range(100)))
            detail_panel.focus()
            await pilot.press("end")
            await pilot.pause()
            assert detail_panel.scroll_y > 0
            await pilot.press("?")
            await pilot.pause()
            help_content = app.screen.query_one("#help-content", Static).render().plain
            assert "1 Status" in help_content
            assert "2 Namespace" in help_content
            assert "3 Queue" in help_content
            await pilot.press("escape")
            await pilot.pause()
            await pilot.press("/")
            await pilot.pause()
            search = app.screen.query_one("#dialog-input", Input)
            assert search.placeholder.startswith("筛选当前页")
            await pilot.press("enter")
            await pilot.pause()
            await pilot.press("2")
            await pilot.pause()
            assert app.focused is namespaces
            await pilot.press("enter")
            await pilot.pause()
            await pilot.press("3")
            await pilot.pause()
            assert app.focused is queues
            await pilot.resize_terminal(60, 22)
            await pilot.pause()
            assert "vertical" in repr(app.query_one("#layout").styles.layout)
            assert records.border_title is None
            assert detail_panel.border_title is None
            await pilot.press("1")
            await pilot.pause()
            assert status_panel.has_class("active")
            assert status_main.styles.display != "none"
            assert status_detail.render().plain.startswith("状态")
            assert not namespace_panel.has_class("active")
            await pilot.press("3")
            await pilot.pause()
            assert app.focused is queues
            assert queues.has_class("active")
            await pilot.press("r")
            await pilot.pause()
            assert records.row_count == 1
            await pilot.press("c")
            await pilot.pause()
            assert (
                app.screen.query_one("#confirm-title", Static).render().plain
                == "修复队列一致性"
            )
            assert (
                "dry-run"
                in app.screen.query_one("#confirm-description", Static).render().plain
            )
            await pilot.press("n")
            await pilot.pause()
            await pilot.press("d")
            await pilot.pause()
            assert "DEAD_LETTERED" in str(records.get_row_at(records.cursor_row)[0])
            await pilot.press("x")
            await pilot.pause()
            assert (
                app.screen.query_one("#confirm-title", Static).render().plain
                == "重放队列"
            )
            await pilot.press("n")
            await pilot.pause()
            assert records.row_count == 1
            await pilot.press("x")
            await pilot.pause()
            await pilot.press("y")
            await pilot.pause()
            assert records.row_count == 1

        replayed = await broker.consumer("emails").__anext__()
        await replayed.reject(reason="second rejection")
        dead_letters = await broker.admin.page_dead_letters("emails")
        assert dead_letters.total == 1
        assert dead_letters.items[0].reason == "second rejection"



@pytest.mark.asyncio
async def test_textual_dashboard_renders_reason_with_rich_markup_characters() -> None:
    pytest.importorskip("textual")

    from textual.widgets import Static

    from taskflow.tui import build_tui_app

    reason = "[{'url': 'https://www.newbalance.com/pd/rev-iq?size=4.5'}]"
    async with SQLiteBroker() as broker:
        await broker.submit(queue="emails", payload={})
        delivery = await broker.consumer("emails").__anext__()
        await delivery.reject(reason=reason)

        app = build_tui_app(broker, backend="sqlite", namespace=None)
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.press("3")
            await pilot.pause()
            detail = app.query_one("#detail", Static).render().plain
            assert reason in detail

@pytest.mark.asyncio
async def test_textual_dashboard_deletes_selected_queue_after_confirmation() -> None:
    pytest.importorskip("textual")

    from textual.widgets import DataTable, Static

    from taskflow.tui import build_tui_app

    async with SQLiteBroker() as broker:
        await broker.submit(queue="emails", payload={"private": "value"})
        app = build_tui_app(broker, backend="sqlite", namespace=None)
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause()
            await pilot.press("3")
            await pilot.pause()
            queues = app.query_one("#queues", DataTable)
            assert queues.row_count == 1
            await pilot.press("delete")
            await pilot.pause()
            assert (
                app.screen.query_one("#confirm-title", Static).render().plain
                == "删除队列"
            )
            await pilot.press("n")
            await pilot.pause()
            assert queues.row_count == 1
            await pilot.press("delete")
            await pilot.pause()
            await pilot.press("y")
            await pilot.pause()
            assert app._queue is None
            assert queues.row_count == 0

        assert (await broker.list_queues()).total == 0


@pytest.mark.asyncio
async def test_textual_dashboard_loads_record_pages_on_demand() -> None:
    pytest.importorskip("textual")

    from textual.widgets import DataTable

    from taskflow.tui import build_tui_app

    async with SQLiteBroker() as broker:
        for number in range(26):
            await broker.submit(queue="emails", payload={"number": number})
        app = build_tui_app(broker, backend="sqlite", namespace=None)
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause()
            queues = app.query_one("#queues", DataTable)
            records = app.query_one("#records", DataTable)
            assert queues.row_count == 0
            assert records.row_count == 0
            assert app._record_kind == "messages"

            await pilot.press("3")
            await pilot.pause()
            assert queues.row_count == 1
            assert records.row_count == 25

            await pilot.press("]")
            await pilot.pause()
            assert records.row_count == 1

            await pilot.press("[")
            await pilot.pause()
            assert records.row_count == 25

            await pilot.press("d")
            await pilot.pause()
            assert records.row_count == 0


@pytest.mark.asyncio
async def test_textual_dashboard_skips_duplicate_message_summaries() -> None:
    pytest.importorskip("textual")

    from textual.widgets import DataTable

    from taskflow import Page
    from taskflow.tui import build_tui_app

    class DuplicateSummaryBroker:
        def __init__(self, wrapped: SQLiteBroker) -> None:
            self._wrapped = wrapped

        def __getattr__(self, name: str) -> object:
            return getattr(self._wrapped, name)

        async def list_message_summaries(
            self, *args: object, **kwargs: object
        ) -> Page[object]:
            page = await self._wrapped.list_message_summaries(*args, **kwargs)  # type: ignore[arg-type]
            return Page((page.items[0], page.items[0]), page.next_cursor, page.total)

    async with SQLiteBroker() as broker:
        message_id = (await broker.submit(queue="emails", payload={})).message_id
        app = build_tui_app(
            DuplicateSummaryBroker(broker), backend="sqlite", namespace=None
        )
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause()
            await pilot.press("3")
            await pilot.pause()

            records = app.query_one("#records", DataTable)
            assert records.row_count == 1
            row_key, _ = records.coordinate_to_cell_key(records.cursor_coordinate)
            assert row_key.value == message_id
            assert not any(
                "消息加载失败" in notification.message
                for notification in app._notifications
            )


@pytest.mark.asyncio
async def test_textual_dashboard_keeps_status_interactive_while_queues_load() -> None:
    pytest.importorskip("textual")

    from textual.widgets import DataTable

    from taskflow.tui import build_tui_app

    class SlowQueueBroker:
        def __init__(self, wrapped: SQLiteBroker) -> None:
            self._wrapped = wrapped
            self.queue_started = asyncio.Event()
            self.release_queues = asyncio.Event()
            self.records_started = asyncio.Event()

        def __getattr__(self, name: str) -> object:
            return getattr(self._wrapped, name)

        async def list_queues(self, **kwargs: object) -> object:
            self.queue_started.set()
            await self.release_queues.wait()
            return await self._wrapped.list_queues(**kwargs)  # type: ignore[arg-type]

        async def list_message_summaries(
            self, *args: object, **kwargs: object
        ) -> object:
            self.records_started.set()
            return await self._wrapped.list_message_summaries(*args, **kwargs)  # type: ignore[arg-type]

    async with SQLiteBroker() as broker:
        await broker.submit(queue="emails", payload={})
        slow_broker = SlowQueueBroker(broker)
        app = build_tui_app(slow_broker, backend="sqlite", namespace=None)
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause()
            await pilot.press("3")
            await asyncio.wait_for(slow_broker.queue_started.wait(), timeout=1)
            assert not slow_broker.records_started.is_set()

            await pilot.press("1")
            await pilot.pause()
            assert app._sidebar_view == "status"

            slow_broker.release_queues.set()
            await pilot.press("3")
            await asyncio.wait_for(slow_broker.records_started.wait(), timeout=1)
            await pilot.pause()
            assert app.query_one("#queues", DataTable).row_count == 1


@pytest.mark.asyncio
async def test_textual_dashboard_quits_on_ctrl_c() -> None:
    pytest.importorskip("textual")
    from taskflow.tui import build_tui_app

    async with SQLiteBroker() as broker:
        app = build_tui_app(
            broker,
            backend="sqlite",
            namespace=None,
        )
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.press("ctrl+c")
            assert app._exit


@pytest.mark.asyncio
async def test_redis_namespace_discovery_uses_schema_markers() -> None:
    class ScanRedis:
        async def scan_iter(self, *, match: str):  # type: ignore[no-untyped-def]
            assert match == "*:meta:schema_version"
            yield "alpha:meta:schema_version"
            yield b"beta:meta:schema_version"
            yield "alpha:queue:{jobs}:stream"

    broker = RedisBroker(ScanRedis(), namespace="current")
    assert await broker.list_namespaces() == ("alpha", "beta")

    broker.select_namespace("alpha")
    report = await broker.health_check()
    assert report.namespace == "alpha"


@pytest.mark.asyncio
async def test_redis_queue_listing_scans_stats_keys_not_message_records() -> None:
    class QueueMetadataRedis:
        def __init__(self) -> None:
            self.scans: list[str] = []
            self.stats_reads = 0

        async def scan(
            self, *, cursor: int, match: str, count: int
        ) -> tuple[int, list[str]]:
            self.scans.append(match)
            return 0, [
                "current:queue:{emails}:stats",
                "current:queue:{emails}:stats",
            ]

        async def hgetall(self, key: str) -> dict[str, str]:
            self.stats_reads += 1
            assert key == "current:queue:{emails}:stats"
            return {"submitted_total": "1"}

        async def zcard(self, key: str) -> int:
            return 0

        async def zrange(
            self, key: str, start: int, end: int, *, withscores: bool
        ) -> list[tuple[str, float]]:
            return []

        async def llen(self, key: str) -> int:
            return 0

    redis = QueueMetadataRedis()
    page = await RedisBroker(redis, namespace="current").list_queues()
    assert redis.scans == ["current:queue:*:stats"]
    assert [item.queue for item in page.items] == ["emails"]
    assert redis.stats_reads == 1


@pytest.mark.asyncio
async def test_redis_message_summary_uses_metadata_fields_only() -> None:
    class MetadataOnlyRedis:
        def __init__(self) -> None:
            self.metadata_reads = 0

        async def scan(
            self, *, cursor: int, match: str, count: int
        ) -> tuple[int, list[str]]:
            assert cursor == 0
            assert match == "current:message:*"
            assert count >= 10
            return 0, ["current:message:message-1", "current:message:message-1"]

        async def hmget(self, key: str, fields: tuple[str, ...]) -> list[str | None]:
            self.metadata_reads += 1
            assert key == "current:message:message-1"
            assert "envelope" not in fields
            values = {
                "queue": "emails",
                "status": "ready",
                "attempt": "1",
                "created_at": "0",
                "serializer_name": "binary-json",
                "serializer_version": "7",
            }
            return [values.get(field) for field in fields]

    redis = MetadataOnlyRedis()
    page = await RedisBroker(redis, namespace="current").list_message_summaries(
        "emails"
    )
    assert page.items[0].message_id == "message-1"
    assert page.items[0].serializer_name == "binary-json"
    assert redis.metadata_reads == 1


@pytest.mark.asyncio
async def test_sqlite_operation_pages_are_bounded_and_stable() -> None:
    async with SQLiteBroker() as broker:
        await broker.submit(queue="emails", payload={"number": 1})
        await broker.submit(queue="emails", payload={"number": 2})
        await broker.submit(queue="jobs", payload={"number": 3})

        first_queues = await broker.list_queues(limit=1)
        assert first_queues.total == 2
        assert len(first_queues.items) == 1
        assert first_queues.next_cursor is not None
        second_queues = await broker.list_queues(
            cursor=first_queues.next_cursor, limit=1
        )
        assert [item.queue for item in (*first_queues.items, *second_queues.items)] == [
            "emails",
            "jobs",
        ]

        first_messages = await broker.list_messages("emails", limit=1)
        assert first_messages.total == 2
        assert len(first_messages.items) == 1
        assert first_messages.next_cursor is not None
        second_messages = await broker.list_messages(
            "emails", cursor=first_messages.next_cursor, limit=1
        )
        assert {
            item.message.payload["number"]
            for item in (*first_messages.items, *second_messages.items)
        } == {1, 2}

        first_summaries = await broker.list_message_summaries("emails", limit=1)
        assert first_summaries.total == 2
        assert len(first_summaries.items) == 1
        assert first_summaries.next_cursor is not None
        second_summaries = await broker.list_message_summaries(
            "emails", cursor=first_summaries.next_cursor, limit=1
        )
        assert {
            item.queue for item in (*first_summaries.items, *second_summaries.items)
        } == {"emails"}

        delivery = await broker.consumer("emails").__anext__()
        await delivery.reject(reason="invalid address")
        dead_letters = await broker.admin.page_dead_letters("emails", limit=1)
        assert dead_letters.total == 1
        assert dead_letters.items[0].reason == "invalid address"
        messages = await broker.list_messages(
            "emails", status=MessageStatus.DEAD_LETTERED
        )
        assert messages.total == 1
        assert messages.items[0].last_reason == "invalid address"


@pytest.mark.asyncio
async def test_shell_lists_redacted_dlq_and_confirms_replay(capsys) -> None:  # type: ignore[no-untyped-def]
    from taskflow.shell import _execute

    class ConfirmSession:
        async def prompt_async(self, prompt: str) -> str:
            assert "replay emails" in prompt
            return f"replay emails {message_id}"

    async with SQLiteBroker() as broker:
        message_id = (
            await broker.submit(
                queue="emails", payload={"secret": "never-render-by-default"}
            )
        ).message_id
        delivery = await broker.consumer("emails").__anext__()
        await delivery.reject(reason="invalid address")

        await _execute(ConfirmSession(), broker, ["dlq", "list", "emails"])
        listing = capsys.readouterr().out
        assert "<redacted>" in listing
        assert "never-render-by-default" not in listing

        await _execute(
            ConfirmSession(), broker, ["dlq", "replay", "emails", message_id]
        )
        assert "replay_enqueued" in capsys.readouterr().out
        replayed = await broker.consumer("emails").__anext__()
        await replayed.ack()


@pytest.mark.asyncio
async def test_shell_observation_eq_and_repair_workflows(capsys) -> None:  # type: ignore[no-untyped-def]
    from taskflow.shell import _execute, _replay_options

    class ConfirmSession:
        async def prompt_async(self, prompt: str) -> str:
            if "replay" in prompt:
                return prompt.removeprefix("输入 '").removesuffix("' 确认：")
            if "delete" in prompt:
                return prompt.removeprefix("输入 '").removesuffix("' 确认：")
            assert "repair" in prompt
            return prompt.removeprefix("输入 '").removesuffix("' 应用以上修复：")

    target, mode, replacement = _replay_options(
        ["archive", "replace", "scope", "key", "60"]
    )
    assert target == "archive"
    assert mode == "replace"
    assert replacement["dedup_ttl"] == timedelta(seconds=60)
    with pytest.raises(ValueError, match="keep/remove"):
        _replay_options(["keep", "unexpected"])
    with pytest.raises(ValueError, match="DEDUP_SCOPE"):
        _replay_options(["replace"])
    with pytest.raises(ValueError, match="必须是正数"):
        _replay_options(["replace", "scope", "key", "0"])
    with pytest.raises(ValueError, match="必须是正数"):
        _replay_options(["replace", "scope", "key", "not-a-number"])

    async with SQLiteBroker() as broker:
        message_id = (
            await broker.submit(
                queue="emails", payload={"secret": "only-explicit-payload"}
            )
        ).message_id
        for command in (
            ["health"],
            ["queues"],
            ["queue", "inspect", "emails"],
            ["messages", "emails"],
            ["message", "inspect", message_id],
            ["payload", "show", message_id],
            ["consistency", "repair", "emails"],
        ):
            await _execute(ConfirmSession(), broker, command)
        output = capsys.readouterr().out
        assert "only-explicit-payload" in output
        assert "<redacted>" in output

        expired_id = (
            await broker.submit(
                queue="expired",
                payload={"value": 1},
                expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
            )
        ).message_id
        await broker.inspect("expired")
        await _execute(ConfirmSession(), broker, ["eq", "list", "expired"])
        await _execute(
            ConfirmSession(), broker, ["eq", "replay", "expired", expired_id]
        )
        replayed = await broker.consumer("expired").__anext__()
        await replayed.ack()

        delete_id = (
            await broker.submit(
                queue="expired",
                payload={"value": 2},
                expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
            )
        ).message_id
        await broker.inspect("expired")
        await _execute(ConfirmSession(), broker, ["eq", "delete", "expired", delete_id])
        assert '"deleted": true' in capsys.readouterr().out



@pytest.mark.asyncio
@pytest.mark.redis
async def test_redis_textual_and_shell_smoke(capsys) -> None:  # type: ignore[no-untyped-def]
    """Exercise the real Redis observation path through both interactive clients."""
    pytest.importorskip("textual")

    from textual.widgets import DataTable

    from taskflow.shell import _execute
    from taskflow.tui import build_tui_app

    class Session:
        async def prompt_async(self, prompt: str) -> str:
            raise AssertionError(f"unexpected protected operation: {prompt}")

    broker = RedisBroker.from_url(
        namespace=f"taskflow-v06-interactive-{uuid4()}",
        pending_recovery_seconds=0.0,
    )
    await broker.start()
    try:
        message_id = (
            await broker.submit(queue="emails", payload={"secret": "Redis-only"})
        ).message_id
        app = build_tui_app(broker, backend="redis", namespace=broker._namespace)
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause()
            await pilot.press("3")
            await pilot.pause()
            assert app.query_one("#queues", DataTable).row_count == 1
            assert app.query_one("#records", DataTable).row_count == 1
            assert app._namespace == broker._namespace
            await pilot.press("ctrl+c")
            assert app._exit

        await _execute(Session(), broker, ["queues"])
        await _execute(Session(), broker, ["message", "inspect", message_id])
        output = capsys.readouterr().out
        assert '"queue": "emails"' in output
        assert "<redacted>" in output
        assert "Redis-only" not in output
    finally:
        keys = [
            key async for key in broker._redis.scan_iter(match=f"{broker._namespace}:*")
        ]
        if keys:
            await broker._redis.unlink(*keys)
        await broker.close()