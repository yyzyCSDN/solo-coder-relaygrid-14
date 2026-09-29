from __future__ import annotations

import pytest

from relaygrid.restoration.execution import (
    LedgerState,
    RestorationLedger,
    StepStatus,
)
from relaygrid.switching.orders import SwitchingOrder, SwitchingStep


def step(seq, action="close", device="d1", expected="closed", hold=False, note=""):
    return SwitchingStep(seq, action, device, expected, hold, note)


def make_order(steps, order_id="ORD-1", author="alice", checker="bob", approver="carol"):
    order = SwitchingOrder(order_id)
    order.create(list(steps), author)
    order.check(checker)
    order.approve(approver)
    return order


def open_ledger(steps, *, now=0.0, operator="op", timeout=30.0, **kw):
    order = make_order(steps, **{k: v for k, v in kw.items() if k in {"order_id"}})
    return RestorationLedger.open(order, now=now, operator=operator, command_timeout_s=timeout)


# ------------------------------------------------------------------ #
# 建立与版本快照
# ------------------------------------------------------------------ #
def test_open_ledger_snapshots_approved_revision():
    order = make_order([step(1), step(2, device="d2")])
    ledger = RestorationLedger.open(order, now=0.0, operator="op")
    assert ledger.state == LedgerState.ACTIVE
    assert ledger.order_id == "ORD-1"
    assert ledger.order_revision == 1
    assert len(ledger.steps) == 2
    assert all(r.status == StepStatus.PENDING for r in ledger.step_records.values())
    opened = [e for e in ledger.evidence() if e["event"] == "ledger_opened"][0]
    assert opened["detail"]["revision"] == 1
    assert opened["detail"]["approver"] == "carol"


def test_cannot_open_ledger_from_unapproved_order():
    order = SwitchingOrder("ORD-X")
    order.create([step(1)], "alice")
    with pytest.raises(ValueError):
        RestorationLedger.open(order, now=0.0, operator="op")
    order.check("bob")
    with pytest.raises(ValueError):
        RestorationLedger.open(order, now=0.0, operator="op")


# ------------------------------------------------------------------ #
# 逐步投递 + 遥信确认
# ------------------------------------------------------------------ #
def test_happy_path_deliver_confirm_complete():
    ledger = open_ledger([step(1), step(2, device="d2")])
    # 投递第一步
    cmd1 = ledger.deliver_next(now=1.0)
    assert cmd1 is not None and cmd1.device_id == "d1"
    assert len(ledger.bus.pending) == 1
    # 第一步在途时不能投递第二步
    assert ledger.deliver_next(now=2.0) is None
    # 遥信确认第一步
    rec1 = ledger.confirm_step(1, "r1", "closed", now=3.0)
    assert rec1.status == StepStatus.CONFIRMED
    assert ledger.state == LedgerState.ACTIVE
    # 投递第二步并确认
    cmd2 = ledger.deliver_next(now=4.0)
    assert cmd2.device_id == "d2"
    rec2 = ledger.confirm_step(2, "r2", "closed", now=5.0)
    assert rec2.status == StepStatus.CONFIRMED
    assert ledger.is_complete
    assert ledger.progress == 1.0
    # 总线下无在途命令
    assert len(ledger.bus.pending) == 0
    assert len(ledger.bus.completed) == 2


def test_verify_step_has_no_device_command():
    ledger = open_ledger([step(1, action="verify", expected="closed")])
    cmd = ledger.deliver_next(now=1.0)
    assert cmd is None  # verify 不上设备
    assert len(ledger.bus.pending) == 0
    rec = ledger.confirm_step(1, "r1", "closed", now=2.0)
    assert rec.status == StepStatus.CONFIRMED
    assert ledger.is_complete


def test_verify_step_timeout_retry_keeps_no_command():
    ledger = open_ledger([step(1, action="verify", expected="closed")], timeout=10.0)
    ledger.deliver_next(now=0.0)
    ledger.expire_step(1, now=11.0)
    assert ledger.step_records[1].status == StepStatus.TIMED_OUT
    cmd = ledger.retry_step(1, now=12.0)
    assert cmd is None  # verify 重试仍不上设备
    assert ledger.step_records[1].command is None
    assert len(ledger.bus.pending) == 0
    ledger.confirm_step(1, "r1", "closed", now=13.0)
    assert ledger.step_records[1].status == StepStatus.CONFIRMED
    assert len(ledger.bus.completed) == 0  # 未投递命令，无完成记录


def test_confirm_undelivered_step_raises():
    ledger = open_ledger([step(1)])
    with pytest.raises(ValueError):
        ledger.confirm_step(1, "r1", "closed", now=1.0)


# ------------------------------------------------------------------ #
# 幂等：重复回执 / 迟到回执
# ------------------------------------------------------------------ #
def test_duplicate_receipt_is_idempotent():
    ledger = open_ledger([step(1)])
    ledger.deliver_next(now=1.0)
    ledger.confirm_step(1, "r1", "closed", now=2.0)
    # 同一回执重复上报
    ledger.confirm_step(1, "r1", "closed", now=3.0)
    rec = ledger.step_records[1]
    assert rec.status == StepStatus.CONFIRMED
    assert rec.confirmed_at == 2.0  # 首次确认时间不变
    dup = [e for e in ledger.evidence() if e["event"] == "duplicate_receipt"]
    assert len(dup) == 1


def test_late_receipt_after_confirmed_is_evidence_only():
    ledger = open_ledger([step(1)])
    ledger.deliver_next(now=1.0)
    ledger.confirm_step(1, "r1", "closed", now=2.0)
    # 另一张回执在确认后到达
    ledger.confirm_step(1, "r2", "closed", now=3.0)
    rec = ledger.step_records[1]
    assert rec.status == StepStatus.CONFIRMED
    assert rec.receipt_id == "r1"  # 仍以首张回执为准
    assert any(e["event"] == "duplicate_receipt" for e in ledger.evidence())


# ------------------------------------------------------------------ #
# 不满足预期状态 → 暂停
# ------------------------------------------------------------------ #
def test_mismatch_pauses_and_blocks_delivery():
    ledger = open_ledger([step(1), step(2, device="d2")])
    ledger.deliver_next(now=1.0)
    rec = ledger.confirm_step(1, "r1", "open", now=2.0)  # 预期 closed，实际 open
    assert rec.status == StepStatus.DELIVERED
    assert rec.observed_state == "open"
    assert ledger.is_paused
    assert ledger.pause_reason == "mismatch"
    # 暂停时不投递后续步骤
    assert ledger.deliver_next(now=3.0) is None
    # 随后遥信到位，满足预期 → 确认并恢复
    ledger.confirm_step(1, "r2", "closed", now=4.0)
    assert ledger.step_records[1].status == StepStatus.CONFIRMED
    assert ledger.state == LedgerState.ACTIVE
    assert ledger.deliver_next(now=5.0) is not None


def test_mismatch_evidence_recorded():
    ledger = open_ledger([step(1)])
    ledger.deliver_next(now=1.0)
    ledger.confirm_step(1, "r1", "open", now=2.0)
    events = [e["event"] for e in ledger.evidence()]
    assert "step_mismatched" in events
    assert "ledger_paused" in events


# ------------------------------------------------------------------ #
# 超时 → 暂停 → 重试
# ------------------------------------------------------------------ #
def test_timeout_pauses_then_retry_redelivers():
    ledger = open_ledger([step(1), step(2, device="d2")], timeout=10.0)
    ledger.deliver_next(now=0.0)
    # 未超时时标记无效
    assert ledger.expire_step(1, now=5.0).status == StepStatus.DELIVERED
    # 超时
    rec = ledger.expire_step(1, now=11.0)
    assert rec.status == StepStatus.TIMED_OUT
    assert ledger.is_paused and ledger.pause_reason == "timeout"
    assert ledger.deliver_next(now=12.0) is None
    # 重试：下发新命令
    cmd = ledger.retry_step(1, now=13.0)
    assert cmd is not None and cmd.command_id.endswith(":2")
    assert ledger.state == LedgerState.ACTIVE
    assert ledger.step_records[1].status == StepStatus.DELIVERED
    # 新命令遥信确认
    ledger.confirm_step(1, "r2", "closed", now=14.0)
    assert ledger.step_records[1].status == StepStatus.CONFIRMED
    # 后续步骤可继续
    assert ledger.deliver_next(now=15.0) is not None


def test_expire_step_idempotent():
    ledger = open_ledger([step(1)], timeout=10.0)
    ledger.deliver_next(now=0.0)
    ledger.expire_step(1, now=11.0)
    ledger.expire_step(1, now=12.0)  # 重复超时标记
    assert ledger.step_records[1].status == StepStatus.TIMED_OUT
    assert len([e for e in ledger.evidence() if e["event"] == "step_timed_out"]) == 1


# ------------------------------------------------------------------ #
# 失败 → 暂停 → 重试
# ------------------------------------------------------------------ #
def test_failure_pauses_then_retry():
    ledger = open_ledger([step(1)])
    ledger.deliver_next(now=0.0)
    rec = ledger.report_failure(1, "device rejected", now=1.0, observed_state="open")
    assert rec.status == StepStatus.FAILED
    assert ledger.is_paused and ledger.pause_reason == "failed"
    cmd = ledger.retry_step(1, now=2.0)
    assert cmd is not None
    assert ledger.step_records[1].status == StepStatus.DELIVERED
    ledger.confirm_step(1, "r2", "closed", now=3.0)
    assert ledger.is_complete


def test_retry_in_flight_is_noop():
    ledger = open_ledger([step(1)])
    cmd1 = ledger.deliver_next(now=0.0)
    # 在途时重试不重复下发
    assert ledger.retry_step(1, now=1.0) is cmd1
    assert ledger.step_records[1].attempt == 1


def test_retry_supersedes_old_command_in_queue():
    ledger = open_ledger([step(1)], timeout=10.0)
    ledger.deliver_next(now=0.0)
    assert len(ledger.bus.pending) == 1
    ledger.report_failure(1, "rejected", now=1.0)
    ledger.retry_step(1, now=2.0)
    # 旧 attempt 命令应移出队列，仅保留新命令
    assert len(ledger.bus.pending) == 1
    assert ledger.bus.pending[0].command_id.endswith(":2")
    ledger.confirm_step(1, "r2", "closed", now=3.0)
    assert len(ledger.bus.pending) == 0
    # 失败 attempt 与成功 attempt 均留痕，成功 attempt 状态为 success
    results = {c.command_id: c.status for c in ledger.bus.completed.values()}
    assert results["ledger:ORD-1:1:1:1"] == "failed"
    assert results["ledger:ORD-1:1:1:2"] == "success"


def test_retry_pending_raises():
    ledger = open_ledger([step(1)])
    with pytest.raises(ValueError):
        ledger.retry_step(1, now=1.0)


# ------------------------------------------------------------------ #
# 人工跳过
# ------------------------------------------------------------------ #
def test_skip_after_timeout_resumes():
    ledger = open_ledger([step(1), step(2, device="d2")], timeout=10.0)
    ledger.deliver_next(now=0.0)
    ledger.expire_step(1, now=11.0)
    rec = ledger.skip_step(1, "op", "field manual close", now=12.0)
    assert rec.status == StepStatus.SKIPPED
    assert rec.operator == "op" and rec.reason == "field manual close"
    assert ledger.state == LedgerState.ACTIVE
    # 后续步骤可投递
    assert ledger.deliver_next(now=13.0) is not None
    assert ledger.progress == 0.5


def test_skip_idempotent():
    ledger = open_ledger([step(1)], timeout=10.0)
    ledger.deliver_next(now=0.0)
    ledger.expire_step(1, now=11.0)
    ledger.skip_step(1, "op", "manual", now=12.0)
    ledger.skip_step(1, "op", "manual", now=13.0)  # 重复跳过
    assert ledger.step_records[1].status == StepStatus.SKIPPED
    assert len([e for e in ledger.evidence() if e["event"] == "step_skipped"]) == 1


def test_skip_confirmed_raises():
    ledger = open_ledger([step(1)])
    ledger.deliver_next(now=0.0)
    ledger.confirm_step(1, "r1", "closed", now=1.0)
    with pytest.raises(ValueError):
        ledger.skip_step(1, "op", "manual", now=2.0)


def test_skip_pending_raises():
    ledger = open_ledger([step(1)])
    with pytest.raises(ValueError):
        ledger.skip_step(1, "op", "manual", now=1.0)


# ------------------------------------------------------------------ #
# 保持点
# ------------------------------------------------------------------ #
def test_hold_point_pauses_until_released():
    ledger = open_ledger([step(1, hold=True), step(2, device="d2")])
    ledger.deliver_next(now=0.0)
    ledger.confirm_step(1, "r1", "closed", now=1.0)
    assert ledger.is_paused and ledger.pause_reason == "hold"
    assert ledger.deliver_next(now=2.0) is None
    ledger.release_hold(1, "op", now=3.0)
    assert ledger.state == LedgerState.ACTIVE
    assert ledger.deliver_next(now=4.0) is not None


def test_release_hold_idempotent():
    ledger = open_ledger([step(1, hold=True)])
    ledger.deliver_next(now=0.0)
    ledger.confirm_step(1, "r1", "closed", now=1.0)
    ledger.release_hold(1, "op", now=2.0)
    ledger.release_hold(1, "op", now=3.0)  # 重复解除
    assert ledger.is_complete


# ------------------------------------------------------------------ #
# 终止
# ------------------------------------------------------------------ #
def test_abort_blocks_delivery():
    ledger = open_ledger([step(1)])
    ledger.abort("op", "cancelled", now=1.0)
    assert ledger.state == LedgerState.ABORTED
    with pytest.raises(ValueError):
        ledger.deliver_next(now=2.0)
    with pytest.raises(ValueError):
        ledger.confirm_step(1, "r1", "closed", now=3.0)


def test_abort_idempotent():
    ledger = open_ledger([step(1)])
    ledger.abort("op", "cancelled", now=1.0)
    ledger.abort("op", "cancelled", now=2.0)
    assert len([e for e in ledger.evidence() if e["event"] == "ledger_aborted"]) == 1


# ------------------------------------------------------------------ #
# 证据与上报
# ------------------------------------------------------------------ #
def test_evidence_preserves_full_trace():
    ledger = open_ledger([step(1), step(2, device="d2")])
    ledger.deliver_next(now=0.0)
    ledger.confirm_step(1, "r1", "closed", now=1.0)
    ledger.deliver_next(now=2.0)
    ledger.confirm_step(2, "r2", "closed", now=3.0)
    events = [e["event"] for e in ledger.evidence()]
    for expected in [
        "ledger_opened",
        "command_delivered",
        "receipt_received",
        "step_confirmed",
        "ledger_completed",
    ]:
        assert expected in events
    # 每条证据都带时间戳
    assert all("timestamp" in e for e in ledger.evidence())


def test_step_summary_reports_status():
    ledger = open_ledger([step(1), step(2, device="d2")])
    ledger.deliver_next(now=0.0)
    ledger.confirm_step(1, "r1", "closed", now=1.0)
    summary = ledger.step_summary()
    assert summary[0]["status"] == "confirmed"
    assert summary[0]["receipt_id"] == "r1"
    assert summary[1]["status"] == "pending"
