"""Optional Textual operations console; imports Textual only when launched."""

from __future__ import annotations

import asyncio
from datetime import datetime
from importlib.metadata import PackageNotFoundError, version
from typing import Any, ClassVar

try:
    _TASKFLOW_VERSION = version("taskflow")
except PackageNotFoundError:
    _TASKFLOW_VERSION = "unknown"


def _connection_label(backend: str, namespace: str | None) -> str:
    return f"{backend}:{namespace}" if namespace else backend


def _truncate(value: object, limit: int) -> str:
    text = str(value)
    return text if len(text) <= limit else f"{text[: limit - 3]}..."


def _render_health_diagnostics(report: Any) -> str:
    status_labels = {"ok": "OK", "warning": "WARNING", "error": "ERROR"}
    lines = ["[bold]诊断信息[/]"]
    for check in report.checks:
        label = status_labels[check.status]
        detail = f" · {check.detail}" if check.detail else ""
        lines.append(f"[bold]{label} {check.name}[/]{detail}")
    return "\n".join(lines)


def build_tui_app(
    broker: Any,
    *,
    backend: str,
    namespace: str | None,
    namespace_factory: Any | None = None,
) -> Any:
    """Build a bounded, read-mostly Textual operations console."""

    from rich.text import Text
    from textual.app import App, ComposeResult  # type: ignore[import-not-found]
    from textual.binding import Binding, BindingType  # type: ignore[import-not-found]
    from textual.containers import (  # type: ignore[import-not-found]
        Horizontal,
        Vertical,
        VerticalScroll,
    )
    from textual.screen import ModalScreen  # type: ignore[import-not-found]
    from textual.widgets import (  # type: ignore[import-not-found]
        DataTable,
        Footer,
        Input,
        ListItem,
        ListView,
        Static,
    )


    class TextInputModal(ModalScreen[str | None]):
        """Centered modal prompt for search and command input."""

        BINDINGS: ClassVar[list[BindingType]] = [
            Binding("escape", "cancel", "取消", show=False),
        ]
        CSS = """
        TextInputModal { align: center middle; background: transparent; }
        #input-dialog { width: 72; max-width: 90%; height: auto; border: round $primary; background: $surface; padding: 1 2; }
        #input-dialog-title { margin-bottom: 1; text-style: bold; color: $primary; }
        #dialog-input { width: 1fr; }
        """

        def __init__(self, title: str, placeholder: str) -> None:
            super().__init__()
            self._title = title
            self._placeholder = placeholder

        def compose(self) -> ComposeResult:
            with Vertical(id="input-dialog"):
                yield Static(self._title, id="input-dialog-title")
                yield Input(placeholder=self._placeholder, id="dialog-input")

        def on_mount(self) -> None:
            self.query_one("#dialog-input", Input).focus()

        def on_input_submitted(self, event: Input.Submitted) -> None:
            self.dismiss(event.value)

        def action_cancel(self) -> None:
            self.dismiss(None)

    class HelpModal(ModalScreen[None]):
        """Centered keyboard reference."""

        BINDINGS: ClassVar[list[BindingType]] = [
            Binding("escape", "dismiss_help", "关闭", show=False),
            Binding("?", "dismiss_help", "关闭", show=False),
        ]
        CSS = """
        HelpModal { align: center middle; background: transparent; }
        #help-dialog { width: 76; max-width: 90%; height: auto; border: round $primary; background: $surface; padding: 1 2; }
        #help-content { color: $text; }
        """

        def compose(self) -> ComposeResult:
            with Vertical(id="help-dialog"):
                yield Static(
                    "[bold]帮助[/]\n\n"
                    "0 Main · 1 Status · 2 Namespace · 3 Queue\n"
                    "↑↓ 预览 / Enter 选择 namespace 或 queue · r 刷新 · / 搜索 · : 命令\n"
                    "m/d/e 切换消息、DLQ、EQ · [ ] 翻页 · v 显示 payload\n"
                    "x 重放 · Delete 删除 · c 一致性修复 · q 退出\n\n"
                    "写操作会打开确认弹框；payload 仅显示在当前界面。\n"
                    "按 Esc 或 ? 关闭。",
                    id="help-content",
                )

        def action_dismiss_help(self) -> None:
            self.dismiss(None)

    class ConfirmModal(ModalScreen[bool]):
        """Small explicit confirmation prompt for mutating operations."""

        BINDINGS: ClassVar[list[BindingType]] = [
            Binding("y", "confirm", "确认", show=False),
            Binding("n", "cancel", "取消", show=False),
            Binding("escape", "cancel", "取消", show=False),
        ]
        CSS = """
        ConfirmModal { align: center middle; }
        #confirm-dialog { width: 68; max-width: 90%; height: auto; border: round $warning; background: $surface; padding: 1 2; }
        #confirm-title { margin-bottom: 1; text-style: bold; color: $warning; }
        #confirm-hint { margin-top: 1; color: $text-muted; }
        """

        def __init__(self, title: str, description: str) -> None:
            super().__init__()
            self._title = title
            self._description = description

        def compose(self) -> ComposeResult:
            with Vertical(id="confirm-dialog"):
                yield Static(self._title, id="confirm-title")
                yield Static(self._description, id="confirm-description")
                yield Static("按 y 确认 · 按 n 或 Esc 取消", id="confirm-hint")

        def action_confirm(self) -> None:
            self.dismiss(True)

        def action_cancel(self) -> None:
            self.dismiss(False)

    class OperationsApp(App[None]):
        """Keyboard-first, bounded operations console."""

        TITLE: str = "Taskflow Operations"
        PAGE_SIZE = 25
        BINDINGS: ClassVar[list[BindingType]] = [
            Binding("0", "main", "主内容", show=False),
            Binding("2", "namespaces", "命名空间", show=False),
            Binding("1", "status", "状态", show=False),
            Binding("3", "queues", "队列", show=False),
            Binding("ctrl+c", "quit", show=False),
            ("r", "refresh", "刷新"),
            ("m", "messages", "消息"),
            ("d", "dead_letters", "DLQ"),
            ("e", "expired", "EQ"),
            ("]", "next_page", "下一页"),
            ("[", "previous_page", "上一页"),
            ("/", "search", "搜索"),
            ("v", "show_payload", "显示 payload"),
            ("x", "replay", "重放"),
            ("delete", "delete_record", "删除"),
            ("c", "consistency_repair", "修复"),
            (":", "command", "命令"),
            ("?", "help", "帮助"),
            ("q", "quit", "退出"),
        ]

        CSS = """
        Screen { background: $background; color: $text; }
        #dashboard { height: 1fr; }
        #header { height: 1; padding: 0 1; background: $panel; color: $primary; }
        #statusbar { width: 1fr; }
        #activity { width: auto; color: $warning; margin-right: 1; }
        #activity.hidden { display: none; }
        #version { width: auto; text-align: right; }
        #layout { height: 1fr; padding: 0 1; }
        #sidebar { width: 32; min-width: 24; height: 1fr; margin-right: 1; }
        #content { height: 1fr; }
        .panel { border: round $secondary; border-title-color: $secondary; border-title-style: bold; margin-bottom: 0; padding: 0 1; }
        .panel.active { border: round $primary; border-title-color: $primary; }
        .panel.collapsed { border: none; border-top: round $secondary; height: 1; min-height: 1; padding: 0; }
        #queues { height: 1fr; background: transparent; }
        #status-main { height: 1fr; }
        #records { height: 2fr; }
        #detail-panel { height: 1fr; }
        #detail { width: 1fr; }
        #namespaces { height: 1fr; background: transparent; }
        #namespaces > ListItem { height: 1; }
        .namespace-item-selected { color: $primary; text-style: bold; }
        #namespaces > ListItem.-highlight .namespace-item-selected { color: $block-cursor-foreground; }
        DataTable:focus { border: round $primary; border-title-color: $primary; }
        """

        def __init__(self) -> None:
            super().__init__()
            self.theme = "dracula"
            self._namespace = namespace
            self._active_loads: dict[str, int] = {}
            self._load_generation: dict[str, int] = {}
            self._queue_cursors: list[str | None] = [None]
            self._queue_page_index = 0
            self._record_cursors: list[str | None] = [None]
            self._record_page_index = 0
            self._queue: str | None = None
            self._record_kind = "messages"
            self._records: dict[str, Any] = {}
            self._pending: tuple[str, str, str] | None = None
            self._last_refreshed: datetime | None = None
            self._search_query = ""
            self._sidebar_view = "status"
            self._activated_views: set[str] = set()

        def compose(self) -> ComposeResult:
            with Vertical(id="dashboard"):
                with Horizontal(id="header"):
                    yield Static(
                        f"[bold]TASKFLOW[/] · {_connection_label(backend, self._namespace)}",
                        id="statusbar",
                    )
                    yield Static("", id="activity", classes="hidden")
                    yield Static(f"v{_TASKFLOW_VERSION}", id="version")
                with Horizontal(id="layout"):
                    with Vertical(id="sidebar"):
                        with Vertical(id="status-panel", classes="panel"):
                            yield Static("读取中…", id="overall")
                            yield Static("等待首个快照", id="refreshed")
                        with Vertical(id="namespace-panel", classes="panel"):
                            yield ListView(id="namespaces", initial_index=0)
                        yield DataTable(
                            id="queues",
                            classes="panel",
                            cursor_type="row",
                            zebra_stripes=True,
                        )
                    with Vertical(id="content"):
                        with VerticalScroll(id="status-main", classes="panel"):
                            yield Static("诊断信息加载中…", id="status-detail")
                        yield DataTable(
                            id="records",
                            classes="panel",
                            cursor_type="row",
                            zebra_stripes=True,
                        )
                        with VerticalScroll(id="detail-panel", classes="panel"):
                            yield Static(
                                "选择一条记录查看详情；payload 保持脱敏。",
                                id="detail",
                            )
                yield Footer()

        def _record_title(self) -> Text:
            return Text(
                {
                    "messages": "0-Message",
                    "dlq": "0-Dead Letter",
                    "eq": "0-Expired Letter",
                }[self._record_kind]
            )

        def _set_sidebar_layout(self, compact: bool) -> None:
            panels = {
                "status": self.query_one("#status-panel", Vertical),
                "namespaces": self.query_one("#namespace-panel", Vertical),
                "queues": self.query_one("#queues", DataTable),
            }
            content = {
                "status": (
                    self.query_one("#overall", Static),
                    self.query_one("#refreshed", Static),
                ),
                "namespaces": (self.query_one("#namespaces", ListView),),
                "queues": (),
            }
            for view, panel in panels.items():
                active = view == self._sidebar_view
                collapsed = compact and not active
                panel.set_class(active, "active")
                panel.set_class(collapsed, "collapsed")
                panel.styles.height = "1fr" if active else (1 if compact else None)
                for widget in content[view]:
                    widget.styles.display = "none" if collapsed else "block"

            self.query_one("#records", DataTable).border_title = (
                None if compact else self._record_title()
            )
            self.query_one("#detail-panel", VerticalScroll).border_title = (
                None if compact else Text("Message Detail")
            )

        def _set_main_view(self, view: str) -> None:
            self._sidebar_view = view
            views = {
                "status": (self.query_one("#status-main", VerticalScroll),),
                "queues": (
                    self.query_one("#records", DataTable),
                    self.query_one("#detail-panel", VerticalScroll),
                ),
            }
            for name, widgets in views.items():
                for widget in widgets:
                    widget.styles.display = "block" if name == view else "none"
            self._set_sidebar_layout(self.size.width < 90 or self.size.height < 30)

        def on_resize(self, event: Any) -> None:
            """Preserve the active side panel when vertical space is scarce."""

            layout = self.query_one("#layout", Horizontal)
            sidebar = self.query_one("#sidebar", Vertical)
            content = self.query_one("#content", Vertical)
            narrow = event.size.width < 90
            layout.styles.layout = "vertical" if narrow else "horizontal"
            sidebar.styles.width = "1fr" if narrow else 32
            content.styles.width = "1fr"
            self._set_sidebar_layout(narrow or event.size.height < 30)

        def on_mount(self) -> None:
            queues = self.query_one("#queues", DataTable)
            status_main = self.query_one("#status-main", VerticalScroll)
            records = self.query_one("#records", DataTable)
            detail_panel = self.query_one("#detail-panel", VerticalScroll)
            self.query_one("#status-panel", Vertical).border_title = Text("1-Status")
            self.query_one("#namespace-panel", Vertical).border_title = Text(
                "2-Namespace"
            )
            queues.border_title = Text("3-Queue")
            status_main.border_title = Text("0-Status")
            records.border_title = self._record_title()
            detail_panel.border_title = Text("Message Detail")
            queues.add_columns(
                ("队列", "queue"),
                ("READY", "ready"),
                ("LEASED", "leased"),
                ("DLQ", "dlq"),
            )
            records.add_column("状态", key="status", width=15)
            records.add_column("消息", key="message", width=32)
            records.add_column("尝试", key="attempt", width=8)
            records.add_column("原因", key="reason", width=38)
            self._set_main_view("status")
            self._load_view_once("status")

        def _search(self) -> str:
            return self._search_query

        def _matches(self, *values: object) -> bool:
            query = self._search()
            return not query or any(query in str(value).casefold() for value in values)

        def _begin_load(self, name: str) -> int:
            generation = self._load_generation.get(name, 0) + 1
            self._load_generation[name] = generation
            self._active_loads[name] = generation
            self._update_activity()
            return generation

        def _finish_load(self, name: str, generation: int) -> None:
            if self._active_loads.get(name) == generation:
                del self._active_loads[name]
                self._update_activity()

        def _update_activity(self) -> None:
            activity = self.query_one("#activity", Static)

            labels = {"status": "状态", "queues": "队列", "records": "消息"}
            if self._active_loads:
                activity.update(
                    f"刷新中：{'、'.join(labels[name] for name in self._active_loads)}"
                )
            activity.set_class(not self._active_loads, "hidden")

        def _load_is_current(self, name: str, generation: int) -> bool:
            return self._load_generation.get(name) == generation

        async def _load_namespaces(self) -> None:
            namespace_list = self.query_one("#namespaces", ListView)
            if backend == "redis":
                available = await broker.list_namespaces()
                if self._namespace is not None and self._namespace not in available:
                    available = tuple(sorted((*available, self._namespace)))
            elif namespace_factory is not None:
                available = tuple(await namespace_factory())
            else:
                available = (self._namespace or "sqlite",)
            selected = self._namespace or ("sqlite" if backend == "sqlite" else None)
            await namespace_list.clear()
            await namespace_list.mount(
                *[
                    ListItem(
                        Static(
                            f"{value} *" if value == selected else value,
                            classes="namespace-item-selected"
                            if value == selected
                            else "",
                        ),
                        name=value,
                    )
                    for value in available
                ]
            )

        def _schedule_queues(self) -> None:
            self.run_worker(
                self._load_queues(),
                group="queue-refresh",
                exclusive=True,
                exit_on_error=False,
            )

        def _schedule_records(self) -> None:
            self.run_worker(
                self._load_records(),
                group="record-refresh",
                exclusive=True,
                exit_on_error=False,
            )

        async def _load_queues(self) -> None:
            generation = self._begin_load("queues")
            try:
                page = await broker.list_queues(
                    cursor=self._queue_cursors[self._queue_page_index],
                    limit=self.PAGE_SIZE,
                )
                table = self.query_one("#queues", DataTable)
                table.clear()
                visible = [item for item in page.items if self._matches(item.queue)]
                for item in visible:
                    table.add_row(
                        item.queue,
                        str(item.ready),
                        str(item.leased),
                        str(item.dead_letters),
                        key=item.queue,
                    )
                if self._queue is None and page.items:
                    self._queue = page.items[0].queue
                if page.next_cursor is not None:
                    self._queue_cursors = self._queue_cursors[
                        : self._queue_page_index + 1
                    ] + [page.next_cursor]
                else:
                    self._queue_cursors = self._queue_cursors[
                        : self._queue_page_index + 1
                    ]
                if self._sidebar_view == "queues":
                    self._schedule_records()
            except Exception as exc:  # noqa: BLE001 - backend failures are operator-visible
                self.notify(
                    f"队列加载失败：{type(exc).__name__}: {exc}；按 r 重试。",
                    severity="error",
                )
            finally:
                self._finish_load("queues", generation)

        async def _load_records(self) -> None:
            generation = self._begin_load("records")
            try:
                if self._queue is None:
                    table = self.query_one("#records", DataTable)
                    table.clear()
                    self._records.clear()
                    return
                cursor = self._record_cursors[self._record_page_index]
                if self._record_kind == "messages":
                    page = await broker.list_message_summaries(
                        self._queue, cursor=cursor, limit=self.PAGE_SIZE
                    )
                    rows = [
                        (
                            item.message_id,
                            item.status.value.upper(),
                            item.attempt,
                            item.last_reason or "—",
                            item,
                        )
                        for item in page.items
                    ]
                elif self._record_kind == "dlq":
                    page = await broker.admin.page_dead_letters(
                        self._queue, cursor=cursor, limit=self.PAGE_SIZE
                    )
                    rows = [
                        (
                            item.message.id,
                            "DEAD_LETTERED",
                            item.attempt,
                            item.reason,
                            item,
                        )
                        for item in page.items
                    ]
                else:
                    page = await broker.admin.page_expired(
                        self._queue, cursor=cursor, limit=self.PAGE_SIZE
                    )
                    rows = [
                        (item.message.id, "EXPIRED", item.attempt, "expired", item)
                        for item in page.items
                    ]
                if not self._load_is_current("records", generation):
                    return
                table = self.query_one("#records", DataTable)
                table.clear()
                seen_message_ids: set[str] = set()
                for message_id, status, attempt, reason, item in rows:
                    if message_id in seen_message_ids:
                        continue
                    seen_message_ids.add(message_id)
                    if self._matches(message_id, status, reason):
                        rendered_status = Text(status)
                        rendered_status.stylize(
                            {
                                "READY": "green",
                                "LEASED": "yellow",
                                "DEAD_LETTERED": "red",
                                "EXPIRED": "magenta",
                            }.get(status, "cyan"),
                            0,
                            1,
                        )
                        table.add_row(
                            rendered_status,
                            _truncate(message_id, 29),
                            str(attempt),
                            _truncate(reason, 35),
                            key=message_id,
                        )
                        self._records[message_id] = item
                if page.next_cursor is not None:
                    self._record_cursors = self._record_cursors[
                        : self._record_page_index + 1
                    ] + [page.next_cursor]
                else:
                    self._record_cursors = self._record_cursors[
                        : self._record_page_index + 1
                    ]
            except Exception as exc:  # noqa: BLE001 - backend failures are operator-visible
                self.notify(
                    f"消息加载失败：{type(exc).__name__}: {exc}；按 r 重试。",
                    severity="error",
                )
            finally:
                self._finish_load("records", generation)

        def _load_view_once(self, view: str) -> None:
            if view in self._activated_views:
                return
            self._activated_views.add(view)
            self.action_refresh(load_queues=view == "queues")

        def action_refresh(self, *, load_queues: bool | None = None) -> None:
            if load_queues is None:
                load_queues = "queues" in self._activated_views
            self.run_worker(
                self._refresh_status(),
                group="status-refresh",
                exclusive=True,
                exit_on_error=False,
            )
            if load_queues:
                self._schedule_queues()

        async def _refresh_status(self) -> None:
            generation = self._begin_load("status")
            try:
                report, _ = await asyncio.gather(
                    broker.health_check(), self._load_namespaces()
                )
                self._last_refreshed = datetime.now().astimezone()
                state = "HEALTHY" if report.healthy else "ATTENTION REQUIRED"
                timestamp = f"{self._last_refreshed:%Y-%m-%d %H:%M:%S %Z}"
                self.query_one("#overall", Static).update(
                    f"[bold]整体状态[/]\n{state}\n[dim]{len(report.checks)} 项诊断[/]"
                )
                self.query_one("#refreshed", Static).update(f"已刷新 {timestamp}")
                self.query_one("#status-detail", Static).update(
                    f"[bold]状态[/] {state}\n刷新时间: {timestamp}\n\n"
                    f"{_render_health_diagnostics(report)}"
                )
            except Exception as exc:  # noqa: BLE001 - backend diagnostics are operator-visible
                self.query_one("#overall", Static).update(
                    f"[bold]整体状态[/]\nREFRESH FAILED\n[dim]{type(exc).__name__}[/]"
                )
                self.query_one("#status-detail", Static).update(
                    f"[bold]刷新失败[/]\n{type(exc).__name__}: {exc}"
                )
                self.notify(
                    f"刷新失败：{type(exc).__name__}: {exc}；检查连接后按 r 重试。",
                    severity="error",
                )
            finally:
                self._finish_load("status", generation)

        def action_main(self) -> None:
            if self._sidebar_view == "queues":
                self.query_one("#records", DataTable).focus()
            else:
                self._set_main_view("status")
                self.query_one("#status-main", VerticalScroll).focus()

        def action_status(self) -> None:
            self._set_main_view("status")
            self._load_view_once("status")

        def action_namespaces(self) -> None:
            self._sidebar_view = "namespaces"
            self._set_sidebar_layout(self.size.width < 90 or self.size.height < 30)
            self.query_one("#namespaces", ListView).focus()

        def action_queues(self) -> None:
            self._set_main_view("queues")
            self.query_one("#queues", DataTable).focus()
            if "queues" in self._activated_views:
                self._schedule_records()
            else:
                self._load_view_once("queues")

        async def on_list_view_selected(self, event: Any) -> None:
            selected = event.item.name
            if not selected or selected == self._namespace:
                return
            if backend != "redis" or not hasattr(broker, "select_namespace"):
                self.notify("当前 backend 不支持切换 namespace。", severity="warning")
                return
            broker.select_namespace(selected)
            self._namespace = selected
            self._queue = None
            self._queue_cursors = [None]
            self._queue_page_index = 0
            self._record_cursors = [None]
            self._record_page_index = 0
            self._activated_views.discard("queues")
            self.query_one("#statusbar", Static).update(
                f"[bold]TASKFLOW[/] · {_connection_label(backend, self._namespace)}"
            )
            self.action_refresh()

        def _switch_records(self, kind: str) -> None:
            self._record_kind = kind
            self._record_cursors = [None]
            self._record_page_index = 0
            self._schedule_records()

        def action_messages(self) -> None:
            self._switch_records("messages")

        def action_dead_letters(self) -> None:
            self._switch_records("dlq")

        def action_expired(self) -> None:
            self._switch_records("eq")

        def action_next_page(self) -> None:
            if self._record_cursors and self._record_page_index + 1 < len(
                self._record_cursors
            ):
                self._record_page_index += 1
                self._schedule_records()
            elif self._queue_cursors and self._queue_page_index + 1 < len(
                self._queue_cursors
            ):
                self._queue_page_index += 1
                self._schedule_queues()

        def action_previous_page(self) -> None:
            if self._record_page_index:
                self._record_page_index -= 1
                self._schedule_records()
            elif self._queue_page_index:
                self._queue_page_index -= 1
                self._schedule_queues()

        def _selected(self) -> tuple[str, Any] | None:
            table = self.query_one("#records", DataTable)
            if table.cursor_row is None or table.row_count == 0:
                return None
            row_key, _ = table.coordinate_to_cell_key(table.cursor_coordinate)
            message_id = str(row_key.value)
            item = self._records.get(message_id)
            return (message_id, item) if item is not None else None

        def _detail(
            self,
            item: Any,
            *,
            message: Any | None = None,
            include_payload: bool = False,
        ) -> Text:
            message_id = getattr(item, "message_id", None) or item.message.id
            created_at = getattr(item, "created_at", None) or item.message.created_at
            serializer_name = (
                getattr(item, "serializer_name", None)
                or getattr(item.message, "payload_schema_name", None)
                or "json"
            )
            serializer_version = (
                getattr(item, "serializer_version", None)
                or getattr(item.message, "payload_schema_version", None)
                or "—"
            )
            status = getattr(item, "status", self._record_kind)
            status_value = str(getattr(status, "value", status)).upper()
            payload = (
                message.payload
                if include_payload and message is not None
                else "<redacted>"
            )
            reason = (
                getattr(item, "last_reason", None)
                or getattr(item, "reason", None)
                or "—"
            )
            return Text.assemble(
                ("消息", "bold"),
                f" {message_id}\n状态: {status_value}  · 尝试: {item.attempt}  · 原因: {reason}\n"
                f"创建: {created_at.isoformat()}  · serializer: {serializer_name}@{serializer_version}\n"
                f"payload: {payload}",
            )

        def _open_command(self, placeholder: str | None = None) -> None:
            self.push_screen(
                TextInputModal("命令", placeholder or "输入命令后按 Enter 执行…"),
                self._submit_command,
            )

        def _submit_command(self, value: str | None) -> None:
            if value is not None:
                self.run_worker(self._run_command(value.strip()))

        def action_search(self) -> None:
            self.push_screen(
                TextInputModal("搜索", "筛选当前页的队列或消息，按 Enter 应用…"),
                self._submit_search,
            )

        def _submit_search(self, value: str | None) -> None:
            if value is not None:
                self._search_query = value.casefold().strip()
                self._reload_search_results()

        def _reload_search_results(self) -> None:
            if self._sidebar_view != "queues":
                return
            self._schedule_queues()
            self.query_one("#records", DataTable).focus()

        def action_show_payload(self) -> None:
            selected = self._selected()
            if selected is None:
                self.notify("请先选择一条消息。", severity="warning")
                return
            self.run_worker(self._show_payload(selected))

        async def _show_payload(self, selected: tuple[str, Any]) -> None:
            message = await broker.observe_message(selected[0])
            if message is None:
                self.notify("消息已不存在，请刷新列表。", severity="warning")
                return
            self.query_one("#detail", Static).update(
                self._detail(selected[1], message=message, include_payload=True)
            )

        def _update_highlight(self, event: Any) -> None:
            if event.data_table.id == "queues":
                if event.row_key is None:
                    return
                self._queue = str(event.row_key.value)
                self._record_cursors = [None]
                self._record_page_index = 0
                if self._sidebar_view == "queues":
                    self._schedule_records()
            elif event.data_table.id == "records":
                selected = self._selected()
                if selected is not None:
                    self.query_one("#detail", Static).update(self._detail(selected[1]))

        def on_data_table_row_selected(self, event: Any) -> None:
            self._update_highlight(event)

        def on_data_table_row_highlighted(self, event: Any) -> None:
            self._update_highlight(event)

        def action_command(self) -> None:
            self._open_command()

        async def _run_command(self, command: str) -> None:
            if command in {"refresh", "r"}:
                self.action_refresh()
            elif command in {"messages", "m"}:
                self.action_messages()
            elif command in {"dlq", "d"}:
                self.action_dead_letters()
            elif command in {"eq", "e"}:
                self.action_expired()
            elif command in {"next", "n"}:
                self.action_next_page()
            elif command in {"prev", "p"}:
                self.action_previous_page()
            else:
                self.notify(
                    "未知命令；可用：refresh、messages、dlq、eq、next、prev。",
                    severity="warning",
                )

        def _queue_is_focused(self) -> bool:
            return self.focused is self.query_one("#queues", DataTable)

        def _request_confirmation(
            self,
            operation: str,
            kind: str,
            target: str,
            title: str,
            description: str,
        ) -> None:
            self._pending = (operation, kind, target)
            self.push_screen(
                ConfirmModal(title, description), self._resolve_confirmation
            )

        def _resolve_confirmation(self, confirmed: bool | None) -> None:
            if confirmed:
                self.run_worker(self._confirm_pending())
            else:
                self._pending = None

        def action_replay(self) -> None:
            if self._queue_is_focused():
                if self._queue is None:
                    self.notify("请先选择一个队列。", severity="warning")
                    return
                self._request_confirmation(
                    "replay_queue",
                    "queue",
                    self._queue,
                    "重放队列",
                    f"来源: {self._queue} 的 DLQ/EQ · 目标: {self._queue} · dedup: keep\n"
                    "将重新入队该队列全部 DLQ 与 EQ 消息。",
                )
                return
            selected = self._selected()
            if (
                selected is None
                or self._record_kind not in {"dlq", "eq"}
                or self._queue is None
            ):
                self.notify("仅能重放已选择的 DLQ/EQ 消息。", severity="warning")
                return
            message_id, _ = selected
            self._request_confirmation(
                "replay",
                self._record_kind,
                message_id,
                "重放消息",
                f"来源: {self._record_kind.upper()} · 目标: {self._queue} · dedup: keep\n"
                f"将消息 {message_id} 重新入队。",
            )

        def action_delete_record(self) -> None:
            if self._queue_is_focused():
                if self._queue is None:
                    self.notify("请先选择一个队列。", severity="warning")
                    return
                self._request_confirmation(
                    "delete_queue",
                    "queue",
                    self._queue,
                    "删除队列",
                    f"将永久删除队列 {self._queue} 的全部消息、DLQ、EQ 和 dedup 状态。",
                )
                return
            selected = self._selected()
            if (
                selected is None
                or self._record_kind not in {"dlq", "eq"}
                or self._queue is None
            ):
                self.notify("仅能删除已选择的 DLQ/EQ 消息。", severity="warning")
                return
            message_id, _ = selected
            self._request_confirmation(
                "delete",
                self._record_kind,
                message_id,
                "删除消息",
                f"来源: {self._record_kind.upper()} · 目标: {self._queue}\n"
                f"将永久删除消息 {message_id}。",
            )

        def action_consistency_repair(self) -> None:
            if self._queue is None:
                self.notify("请先选择一个队列。", severity="warning")
                return
            self.run_worker(self._preview_consistency_repair(self._queue))

        async def _preview_consistency_repair(self, queue: str) -> None:
            try:
                proposal = await broker.repair_consistency(queue)
            except Exception as exc:  # noqa: BLE001 - backend failures are operator-visible
                self.notify(
                    f"一致性检查失败：{type(exc).__name__}: {exc}；请检查状态后重试。",
                    severity="error",
                )
                return
            self._request_confirmation(
                "repair",
                "consistency",
                queue,
                "修复队列一致性",
                f"dry-run 发现 {len(proposal.repairs)} 项待修复；确认后将应用队列 {queue} 的修复。",
            )

        async def _confirm_pending(self) -> None:
            assert self._pending is not None
            operation, kind, target = self._pending
            try:
                if operation == "repair":
                    await broker.repair_consistency(target, dry_run=False)
                    self.notify("一致性修复已应用。")
                elif operation == "replay_queue":
                    dead_letters, expired = await asyncio.gather(
                        broker.admin.list_dead_letters(target),
                        broker.admin.list_expired(target),
                    )
                    for item in dead_letters:
                        await broker.admin.replay_dead_letter(
                            target, item.message.id, dedup_mode="keep"
                        )
                    for item in expired:
                        await broker.admin.replay_expired(
                            target,
                            item.message.id,
                            expires_at=None,
                            dedup_mode="keep",
                        )
                    self._record_kind = "messages"
                    self._record_cursors = [None]
                    self._record_page_index = 0
                    self.notify(
                        f"已重新入队 {len(dead_letters) + len(expired)} 条消息；业务 handler 将异步处理。"
                    )
                elif operation == "delete_queue":
                    deleted = await broker.admin.delete_queue(target)
                    self._queue = None
                    self._records.clear()
                    self.query_one("#records", DataTable).clear()
                    self.notify(f"队列已删除（{deleted} 条消息）。")
                    await self._load_queues()
                    return
                elif operation == "replay":
                    assert self._queue is not None
                    if kind == "dlq":
                        await broker.admin.replay_dead_letter(
                            self._queue, target, dedup_mode="keep"
                        )
                    else:
                        await broker.admin.replay_expired(
                            self._queue,
                            target,
                            expires_at=None,
                            dedup_mode="keep",
                        )
                    self._record_kind = "messages"
                    self._record_cursors = [None]
                    self._record_page_index = 0
                    self.notify("消息已重新入队；业务 handler 将异步处理。")
                elif kind == "dlq":
                    assert self._queue is not None
                    await broker.admin.delete_dead_letter(self._queue, target)
                    self.notify("DLQ 消息已删除。")
                else:
                    assert self._queue is not None
                    await broker.admin.delete_expired(self._queue, target)
                    self.notify("EQ 消息已删除。")
                await self._load_records()
            except Exception as exc:  # noqa: BLE001
                self.notify(
                    f"操作失败：{type(exc).__name__}: {exc}；请检查状态后重试。",
                    severity="error",
                )
            finally:
                self._pending = None

        def action_help(self) -> None:
            self.push_screen(HelpModal())

    return OperationsApp()


async def run_tui(
    broker: Any,
    *,
    backend: str,
    namespace: str | None,
    namespace_factory: Any | None = None,
) -> int:
    """Run the Textual dashboard."""

    app = build_tui_app(
        broker,
        backend=backend,
        namespace=namespace,
        namespace_factory=namespace_factory,
    )
    await app.run_async()
    return 0
