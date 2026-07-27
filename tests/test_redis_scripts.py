"""Redis Lua 文本及 Python ``EVAL`` 调用参数契约。"""
from datetime import datetime, timezone

from taskflow.broker.redis_calls import (
    RedisScriptCall,
    batch_submit_call,
    claim_call,
    due_delayed_call,
    expire_call,
    extend_lease_call,
    finish_call,
    pel_recover_call,
    reclaim_lease_call,
    replay_dead_letter_call,
    replay_expired_call,
    submit_call,
)
from taskflow.broker.redis_scripts import (
    BATCH_SUBMIT,
    CLAIM,
    DUE_DELAYED,
    EXPIRE,
    EXTEND_LEASE,
    FINISH,
    PEL_RECOVER,
    RECLAIM_LEASE,
    REPLAY_DEAD_LETTER,
    REPLAY_EXPIRED,
    SUBMIT,
)
from taskflow.submission.base import PreparedSubmission


class _Keyspace:
    def _message_key(self, message_id: str) -> str:
        return f"ns:message:{message_id}"

    def _queue_key(self, queue: str, kind: str) -> str:
        return f"ns:queue:{{{queue}}}:{kind}"

    def _group_name(self) -> str:
        return "taskflow"


def _submission(message_id: str = "m1") -> PreparedSubmission:
    return PreparedSubmission(
        message_id, "jobs", b"envelope", "ready",
        datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc), 123_000,
        "scope", "key", 45_000, 3, "json", "1", None,
    )


def test_script_call_passes_the_exact_eval_boundary() -> None:
    call = RedisScriptCall("return 1", ("key",), ("arg",))

    class Redis:
        def __init__(self) -> None:
            self.values: tuple[object, ...] | None = None

        async def eval(self, *values: object) -> int:
            self.values = values
            return 1

    import asyncio

    redis = Redis()
    assert asyncio.run(call.execute(redis)) == 1
    assert redis.values == ("return 1", 1, "key", "arg")


def test_submission_call_layouts_match_lua_keys_and_argv() -> None:
    backend, submission = _Keyspace(), _submission()
    single = submit_call(backend, submission, "dedup")
    assert single.script == SUBMIT
    assert single.keys == (
        "dedup", "ns:message:m1", "ns:queue:{jobs}:stream", "ns:queue:{jobs}:expiry",
        "ns:queue:{jobs}:eq", "ns:queue:{jobs}:stats", "ns:queue:{jobs}:ready", "ns:queue:{jobs}:delayed",
    )
    assert single.args == (
        "dedup", "m1", "45000", "ZW52ZWxvcGU=", "jobs", "ready", "3", "1767323045.0",
        "123.0", "json", "1", "1767323045.0", "0",
    )
    batch = batch_submit_call(backend, [submission, _submission("m2")], ["dedup", ""])
    assert batch.script == BATCH_SUBMIT
    assert len(batch.keys) == 16 and batch.keys[:8] == single.keys
    assert batch.args[:14] == ("2", *single.args)
    assert batch.args[14] == "" and batch.args[15] == "m2"


def test_delivery_and_maintenance_call_layouts_match_lua() -> None:
    backend = _Keyspace()
    claim = claim_call(backend, queue="jobs", message_id="m1", now=10, consumer_id="c",
                       delivery_id="d", token="t", lease_until=20, entry_id="1-0")
    assert claim.script == CLAIM
    assert claim.keys == (
        "ns:message:m1", "ns:queue:{jobs}:leases", "ns:queue:{jobs}:eq", "ns:queue:{jobs}:expiry",
        "ns:queue:{jobs}:stream", "ns:queue:{jobs}:ready",
    )
    assert claim.args == ("10", "m1", "c", "d", "t", "20", "taskflow", "1-0")
    finish = finish_call(backend, queue="jobs", message_id="m1", action="retry", delivery_id="d",
                         token="t", now=10, reason="error", error_type="ValueError",
                         retry_available_at=20, max_attempts=3)
    assert finish.script == FINISH and len(finish.keys) == 9
    assert finish.keys[0] == "ns:message:m1" and finish.keys[-1] == "ns:queue:{jobs}:delayed"
    assert finish.args == ("retry", "d", "t", "10", "m1", "error", "ValueError", "taskflow", "20", "3")
    extend = extend_lease_call(backend, queue="jobs", message_id="m1", delivery_id="d", token="t", lease_until=20)
    assert extend.script == EXTEND_LEASE and extend.args == ("d", "t", "20", "m1", "taskflow")
    assert pel_recover_call(backend, queue="jobs", message_id="m1", entry_id="1-0").args == ("taskflow", "1-0", "m1")
    assert due_delayed_call(backend, queue="jobs", message_id="m1", now=10).script == DUE_DELAYED
    assert expire_call(backend, queue="jobs", message_id="m1", now=10).args == ("10", "taskflow", "m1")
    assert reclaim_lease_call(backend, queue="jobs", message_id="m1", now=10).script == RECLAIM_LEASE


def test_replay_call_layouts_preserve_dedup_key_positions() -> None:
    backend = _Keyspace()
    dlq = replay_dead_letter_call(backend, source_queue="jobs", target_queue="repairs", message_id="m1",
                                  envelope="encoded", attempt="0", expires_at=0, now=10,
                                  old_dedup_key="old", new_dedup_key="new", keep=False, replacement_ttl=60)
    assert dlq.script == REPLAY_DEAD_LETTER
    assert dlq.keys == (
        "ns:message:m1", "ns:queue:{jobs}:dlq", "ns:queue:{repairs}:stream", "ns:queue:{repairs}:expiry",
        "ns:queue:{repairs}:ready", "old", "new",
    )
    assert dlq.args == ("m1", "encoded", "repairs", "0", "0", "10", "0", "60")
    eq = replay_expired_call(backend, source_queue="jobs", target_queue="repairs", message_id="m1",
                             envelope="encoded", expires_at=0, now=10, old_dedup_key="old",
                             new_dedup_key="new", keep=True, replacement_ttl=60)
    assert eq.script == REPLAY_EXPIRED and eq.keys[1] == "ns:queue:{jobs}:eq"
    assert eq.args == ("m1", "encoded", "0", "10", "1", "60", "repairs")


def test_batch_submit_script_keeps_eight_keys_and_thirteen_args_per_item() -> None:
    assert "index * 8" in BATCH_SUBMIT
    assert "index * 13" in BATCH_SUBMIT
    assert "return output" in BATCH_SUBMIT


def test_replay_scripts_keep_dedup_conflict_and_index_contracts() -> None:
    assert "KEYS[7]" in REPLAY_DEAD_LETTER
    assert "ARGV[8]" in REPLAY_DEAD_LETTER
    assert "LREM" in REPLAY_DEAD_LETTER and "XADD" in REPLAY_DEAD_LETTER
    assert "KEYS[7]" in REPLAY_EXPIRED
    assert "ARGV[7]" in REPLAY_EXPIRED
    assert "expires_at" in REPLAY_EXPIRED


def test_pel_recovery_script_moves_pending_entry_atomically() -> None:
    assert "XACK" in PEL_RECOVER and "XDEL" in PEL_RECOVER and "XADD" in PEL_RECOVER
    assert "reclaimed_total" in PEL_RECOVER


def test_delivery_and_maintenance_scripts_keep_transition_contracts() -> None:
    assert "lease_token" in CLAIM and "attempt" in CLAIM
    assert "last_delivery_id" in FINISH and "dead_lettered_total" in FINISH
    assert "TIME" in EXTEND_LEASE and "lease_until" in EXTEND_LEASE
    assert "delayed" in DUE_DELAYED and "XADD" in DUE_DELAYED
    assert "status_at_expiry" in EXPIRE
    assert "lease_timeout" in RECLAIM_LEASE and "reclaimed_total" in RECLAIM_LEASE
    assert "submitted_total" in SUBMIT and "serializer_version" in SUBMIT
