"""复电命令执行账本。

把已批准的操作票逐步投递到设备命令队列（:class:`CommandBus`），并用遥信
回执确认每一步。账本对重复回执、超时、人工跳过和失败重试都做幂等处理；
任何一步的遥信不满足预期状态就暂停后续步骤。票据版本与全部执行证据以
追加式事件日志完整保留，不可篡改。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from relaygrid.operations.command_bus import Command, CommandBus
from relaygrid.switching.orders import OrderState, SwitchingOrder, SwitchingStep

# 允许投递到设备命令队列的动作；verify/test 不上设备，仅靠遥信确认。
_DISPATCHABLE_ACTIONS = {"open", "close", "trip", "reset"}


class StepStatus(str, Enum):
    PENDING = "pending"          # 尚未投递
    DELIVERED = "delivered"      # 已投递，等待遥信回执
    CONFIRMED = "confirmed"      # 遥信满足预期状态
    TIMED_OUT = "timed_out"      # 投递后超时未确认
    SKIPPED = "skipped"          # 人工跳过（接受偏差）
    FAILED = "failed"            # 设备拒绝/执行失败


class LedgerState(str, Enum):
    ACTIVE = "active"        # 正在执行
    PAUSED = "paused"        # 暂停，等待处置
    COMPLETE = "complete"    # 全部步骤确认/跳过
    ABORTED = "aborted"      # 人工终止


@dataclass(frozen=True)
class LedgerEvent:
    """追加式证据事件。detail 存放不宜用标量表达的键值对。"""

    event: str
    step_seq: int | None
    timestamp: float
    command_id: str | None = None
    receipt_id: str | None = None
    observed_state: str | None = None
    operator: str | None = None
    reason: str | None = None
    detail: tuple[tuple[str, Any], ...] = ()


@dataclass
class StepRecord:
    """步骤的物化状态（证据以 LedgerEvent 追加留存，此处仅为最新状态）。"""

    sequence: int
    action: str
    device_id: str
    expected_state: str
    hold_point: bool
    note: str
    status: StepStatus = StepStatus.PENDING
    command: Command | None = None
    receipt_id: str | None = None
    observed_state: str | None = None
    attempt: int = 0
    delivered_at: float | None = None
    confirmed_at: float | None = None
    operator: str | None = None
    reason: str | None = None

    @property
    def terminal(self) -> bool:
        return self.status in {StepStatus.CONFIRMED, StepStatus.SKIPPED}

    def deadline(self, timeout_s: float) -> float:
        if self.command is not None:
            return self.command.expires_at
        return (self.delivered_at if self.delivered_at is not None else 0.0) + timeout_s


@dataclass
class RestorationLedger:
    """复电命令执行账本。

    用 :func:`open_ledger` 从已批准的操作票建立，票据版本与步骤在建立时
    快照，随后通过 :meth:`deliver_next` 逐步投递、:meth:`confirm_step` 用
    遥信确认。所有处置均幂等，证据追加在 :attr:`events` 中。
    """

    ledger_id: str
    order_id: str
    order_revision: int
    steps: tuple[SwitchingStep, ...]
    bus: CommandBus
    created_by: str
    created_at: float
    command_timeout_s: float = 30.0
    state: LedgerState = LedgerState.ACTIVE
    pause_reason: str | None = None
    paused_at_seq: int | None = None
    step_records: dict[int, StepRecord] = field(default_factory=dict)
    events: list[LedgerEvent] = field(default_factory=list)
    aborted_by: str | None = None
    aborted_reason: str | None = None
    completed_at: float | None = None

    # ------------------------------------------------------------------ #
    # 工厂
    # ------------------------------------------------------------------ #
    @classmethod
    def open(
        cls,
        order: SwitchingOrder,
        *,
        now: float,
        operator: str,
        ledger_id: str | None = None,
        bus: CommandBus | None = None,
        command_timeout_s: float = 30.0,
    ) -> "RestorationLedger":
        """从已批准的操作票建立账本，快照其批准版本与步骤。"""
        rev = order.current
        if rev.state != OrderState.APPROVED:
            raise ValueError("order not approved")
        lid = ledger_id or f"ledger:{order.order_id}:{rev.revision}"
        steps = tuple(rev.steps)
        ledger = cls(
            ledger_id=lid,
            order_id=order.order_id,
            order_revision=rev.revision,
            steps=steps,
            bus=bus if bus is not None else CommandBus(),
            created_by=operator,
            created_at=now,
            command_timeout_s=command_timeout_s,
        )
        for s in steps:
            ledger.step_records[s.sequence] = StepRecord(
                sequence=s.sequence,
                action=s.action,
                device_id=s.device_id,
                expected_state=s.expected_state,
                hold_point=s.hold_point,
                note=s.note,
            )
        ledger._log(
            "ledger_opened",
            None,
            now,
            detail=(
                ("order_id", order.order_id),
                ("revision", rev.revision),
                ("approver", rev.approver or ""),
                ("author", rev.author),
                ("checker", rev.checker or ""),
                ("steps", len(steps)),
            ),
        )
        return ledger

    # ------------------------------------------------------------------ #
    # 投递
    # ------------------------------------------------------------------ #
    def deliver_next(self, now: float, actor: str | None = None) -> Command | None:
        """投递下一个待执行步骤到设备命令队列。

        同一时刻只允许一条命令在途；账本暂停或终止时不投递。返回投递的
        命令，若无待投递步骤则返回 None。
        """
        if self.state == LedgerState.ABORTED:
            raise ValueError("ledger aborted")
        if self.state in {LedgerState.PAUSED, LedgerState.COMPLETE}:
            return None
        if any(r.status == StepStatus.DELIVERED for r in self.step_records.values()):
            return None  # 上一条命令仍在途，等待确认
        rec = self._next_pending()
        if rec is None:
            return None
        attempt = rec.attempt + 1
        command_id = f"{self.ledger_id}:{rec.sequence}:{attempt}"
        cmd = Command(
            command_id=command_id,
            device_id=rec.device_id,
            action=rec.action,
            issued_at=now,
            expires_at=now + self.command_timeout_s,
            actor=actor if actor is not None else self.created_by,
        )
        if rec.action in _DISPATCHABLE_ACTIONS:
            self.bus.submit(cmd, now)
            rec.command = cmd
            dispatched = True
        else:
            rec.command = None  # verify/test 不上设备，仅等待遥信
            dispatched = False
        rec.status = StepStatus.DELIVERED
        rec.attempt = attempt
        rec.delivered_at = now
        self._log(
            "command_delivered" if dispatched else "step_activated",
            rec.sequence,
            now,
            command_id=command_id,
            detail=(
                ("device_id", rec.device_id),
                ("action", rec.action),
                ("attempt", attempt),
                ("dispatched", dispatched),
            ),
        )
        return cmd if dispatched else None

    # ------------------------------------------------------------------ #
    # 遥信确认
    # ------------------------------------------------------------------ #
    def confirm_step(
        self,
        sequence: int,
        receipt_id: str,
        observed_state: str,
        now: float,
        *,
        source: str = "telemetry",
    ) -> StepRecord:
        """用遥信回执确认一步。

        幂等：同一 receipt_id 重复回执不重复确认；步骤已确认后的迟到回执
        仅作证据留存。遥信满足预期状态才确认，否则暂停后续步骤。
        """
        if self.state == LedgerState.ABORTED:
            raise ValueError("ledger aborted")
        rec = self._step(sequence)
        if rec.status == StepStatus.PENDING:
            raise ValueError("step not delivered")
        if rec.status == StepStatus.CONFIRMED:
            # 迟到/重复回执：证据留存，状态不变
            self._log(
                "duplicate_receipt",
                sequence,
                now,
                receipt_id=receipt_id,
                observed_state=observed_state,
                detail=(("source", source),),
            )
            return rec
        if rec.status in {StepStatus.TIMED_OUT, StepStatus.FAILED, StepStatus.SKIPPED}:
            # 超时/失败/跳过后的迟到回执：留存证据，不自动改判
            self._log(
                "late_receipt",
                sequence,
                now,
                receipt_id=receipt_id,
                observed_state=observed_state,
                detail=(("source", source), ("status", rec.status.value)),
            )
            return rec
        # DELIVERED：本步唯一可确认状态
        self._log(
            "receipt_received",
            sequence,
            now,
            receipt_id=receipt_id,
            observed_state=observed_state,
            detail=(("source", source),),
        )
        rec.receipt_id = receipt_id
        rec.observed_state = observed_state
        if observed_state != rec.expected_state:
            self._log(
                "step_mismatched",
                sequence,
                now,
                receipt_id=receipt_id,
                observed_state=observed_state,
                detail=(("expected", rec.expected_state),),
            )
            self._pause("mismatch", sequence, now)
            return rec
        # 满足预期：确认
        rec.status = StepStatus.CONFIRMED
        rec.confirmed_at = now
        self._settle(rec.command, "success", now, observed_state)
        self._log(
            "step_confirmed",
            sequence,
            now,
            receipt_id=receipt_id,
            observed_state=observed_state,
        )
        if self.state == LedgerState.PAUSED and self.paused_at_seq == sequence:
            self._resume(now)
        if rec.hold_point:
            self._pause("hold", sequence, now)
        else:
            self._check_complete(now)
        return rec

    # ------------------------------------------------------------------ #
    # 超时 / 失败 / 跳过 / 重试
    # ------------------------------------------------------------------ #
    def expire_step(self, sequence: int, now: float) -> StepRecord:
        """标记投递后超时未确认的步骤。幂等。"""
        if self.state == LedgerState.ABORTED:
            raise ValueError("ledger aborted")
        rec = self._step(sequence)
        if rec.status != StepStatus.DELIVERED:
            return rec  # 已处置或未投递，幂等
        if now <= rec.deadline(self.command_timeout_s):
            return rec  # 尚未超时
        rec.status = StepStatus.TIMED_OUT
        self._log(
            "step_timed_out",
            sequence,
            now,
            command_id=rec.command.command_id if rec.command else None,
            detail=(("deadline", rec.deadline(self.command_timeout_s)),),
        )
        self._pause("timeout", sequence, now)
        return rec

    def report_failure(
        self,
        sequence: int,
        reason: str,
        now: float,
        *,
        observed_state: str | None = None,
    ) -> StepRecord:
        """记录设备拒绝/执行失败。幂等。"""
        if self.state == LedgerState.ABORTED:
            raise ValueError("ledger aborted")
        rec = self._step(sequence)
        if rec.status != StepStatus.DELIVERED:
            return rec  # 已处置或未投递，幂等
        rec.status = StepStatus.FAILED
        rec.reason = reason
        if observed_state is not None:
            rec.observed_state = observed_state
        self._log(
            "step_failed",
            sequence,
            now,
            command_id=rec.command.command_id if rec.command else None,
            observed_state=observed_state,
            reason=reason,
        )
        self._settle(rec.command, "failed", now, observed_state)
        self._pause("failed", sequence, now)
        return rec

    def skip_step(self, sequence: int, operator: str, reason: str, now: float) -> StepRecord:
        """人工跳过某步（接受偏差）并继续。幂等。

        已确认的步骤不可跳过；未投递的步骤不可跳过。
        """
        if self.state == LedgerState.ABORTED:
            raise ValueError("ledger aborted")
        rec = self._step(sequence)
        if rec.status == StepStatus.SKIPPED:
            return rec  # 幂等
        if rec.status == StepStatus.CONFIRMED:
            raise ValueError("step already confirmed")
        if rec.status == StepStatus.PENDING:
            raise ValueError("step not delivered")
        rec.status = StepStatus.SKIPPED
        rec.operator = operator
        rec.reason = reason
        self._remove_from_pending(rec.command)  # 防止跳过的命令被设备滞后领取
        self._log("step_skipped", sequence, now, operator=operator, reason=reason)
        self._resume(now)
        self._check_complete(now)
        return rec

    def retry_step(self, sequence: int, now: float, actor: str | None = None) -> Command | None:
        """失败/超时/不匹配后重新投递命令。幂等。

        已在途或已终态时不重复投递；仅对未确认且非待投递的步骤重新下发。
        """
        if self.state == LedgerState.ABORTED:
            raise ValueError("ledger aborted")
        rec = self._step(sequence)
        if rec.status in {StepStatus.CONFIRMED, StepStatus.SKIPPED}:
            return rec.command  # 已终态，幂等
        if rec.status == StepStatus.PENDING:
            raise ValueError("step not delivered; use deliver_next")
        if rec.status == StepStatus.DELIVERED and self.state != LedgerState.PAUSED:
            return rec.command  # 命令在途，幂等
        # 重试取代旧命令：先把旧 attempt 移出待投递队列，再下发新命令
        self._remove_from_pending(rec.command)
        attempt = rec.attempt + 1
        command_id = f"{self.ledger_id}:{sequence}:{attempt}"
        if rec.action in _DISPATCHABLE_ACTIONS:
            cmd: Command | None = Command(
                command_id=command_id,
                device_id=rec.device_id,
                action=rec.action,
                issued_at=now,
                expires_at=now + self.command_timeout_s,
                actor=actor if actor is not None else self.created_by,
            )
            self.bus.submit(cmd, now)
            rec.command = cmd
        else:
            rec.command = None  # verify/test 不上设备，仅重新等待遥信
        rec.attempt = attempt
        rec.delivered_at = now
        rec.status = StepStatus.DELIVERED
        self._log(
            "step_retried",
            sequence,
            now,
            command_id=command_id if rec.command else None,
            detail=(("attempt", attempt), ("dispatched", rec.command is not None)),
        )
        self._resume(now)
        return rec.command

    def release_hold(self, sequence: int, operator: str, now: float) -> None:
        """解除保持点，继续执行。幂等。"""
        if self.state == LedgerState.ABORTED:
            raise ValueError("ledger aborted")
        if self.state != LedgerState.PAUSED or self.pause_reason != "hold":
            return  # 未在保持点，幂等
        if self.paused_at_seq != sequence:
            return  # 保持点不在本步
        self._log("hold_released", sequence, now, operator=operator)
        self._resume(now)
        self._check_complete(now)

    def abort(self, operator: str, reason: str, now: float) -> None:
        """终止整个账本。幂等。"""
        if self.state in {LedgerState.COMPLETE, LedgerState.ABORTED}:
            return
        self.state = LedgerState.ABORTED
        self.aborted_by = operator
        self.aborted_reason = reason
        self._log("ledger_aborted", None, now, operator=operator, reason=reason)

    # ------------------------------------------------------------------ #
    # 状态与证据
    # ------------------------------------------------------------------ #
    @property
    def progress(self) -> float:
        if not self.steps:
            return 1.0
        done = sum(1 for r in self.step_records.values() if r.terminal)
        return done / len(self.steps)

    @property
    def is_complete(self) -> bool:
        return self.state == LedgerState.COMPLETE

    @property
    def is_paused(self) -> bool:
        return self.state == LedgerState.PAUSED

    def step_summary(self) -> list[dict[str, Any]]:
        """返回各步骤的最新物化状态，用于上报/展示。"""
        out = []
        for s in self.steps:
            r = self.step_records[s.sequence]
            out.append(
                {
                    "sequence": s.sequence,
                    "action": s.action,
                    "device_id": s.device_id,
                    "expected_state": s.expected_state,
                    "hold_point": s.hold_point,
                    "status": r.status.value,
                    "command_id": r.command.command_id if r.command else None,
                    "attempt": r.attempt,
                    "receipt_id": r.receipt_id,
                    "observed_state": r.observed_state,
                    "operator": r.operator,
                    "reason": r.reason,
                    "delivered_at": r.delivered_at,
                    "confirmed_at": r.confirmed_at,
                }
            )
        return out

    def evidence(self) -> list[dict[str, Any]]:
        """返回全部追加式证据事件（副本），用于审计。"""
        return [
            {
                "event": e.event,
                "step_seq": e.step_seq,
                "timestamp": e.timestamp,
                "command_id": e.command_id,
                "receipt_id": e.receipt_id,
                "observed_state": e.observed_state,
                "operator": e.operator,
                "reason": e.reason,
                "detail": dict(e.detail),
            }
            for e in self.events
        ]

    # ------------------------------------------------------------------ #
    # 内部
    # ------------------------------------------------------------------ #
    def _log(self, event: str, step_seq: int | None, timestamp: float, **kw: Any) -> None:
        self.events.append(LedgerEvent(event=event, step_seq=step_seq, timestamp=timestamp, **kw))

    def _remove_from_pending(self, command: Command | None) -> None:
        if command is None:
            return
        try:
            self.bus.pending.remove(command)
        except ValueError:
            pass  # 设备端已领取（next_for 移除），无需清理

    def _settle(
        self,
        command: Command | None,
        status: str,
        now: float,
        observed_state: str | None = None,
    ) -> None:
        """终态结算：移出待投递队列并记录结果，使总线反映真实状态。"""
        self._remove_from_pending(command)
        if command is not None:
            try:
                self.bus.complete(command, status, now, observed_state)
            except ValueError:
                pass

    def _step(self, sequence: int) -> StepRecord:
        rec = self.step_records.get(sequence)
        if rec is None:
            raise KeyError(sequence)
        return rec

    def _next_pending(self) -> StepRecord | None:
        for s in self.steps:
            rec = self.step_records[s.sequence]
            if rec.status == StepStatus.PENDING:
                return rec
        return None

    def _all_terminal(self) -> bool:
        return all(r.terminal for r in self.step_records.values())

    def _pause(self, reason: str, sequence: int, now: float) -> None:
        if (
            self.state == LedgerState.PAUSED
            and self.pause_reason == reason
            and self.paused_at_seq == sequence
        ):
            return  # 同因同步已暂停，幂等
        self.state = LedgerState.PAUSED
        self.pause_reason = reason
        self.paused_at_seq = sequence
        self._log("ledger_paused", sequence, now, detail=(("reason", reason),))

    def _resume(self, now: float) -> None:
        if self.state != LedgerState.PAUSED:
            return
        reason = self.pause_reason
        at_seq = self.paused_at_seq
        self.state = LedgerState.ACTIVE
        self.pause_reason = None
        self.paused_at_seq = None
        self._log("ledger_resumed", at_seq, now, detail=(("from_reason", reason),))

    def _check_complete(self, now: float) -> None:
        if self.state == LedgerState.ACTIVE and self._all_terminal():
            self.state = LedgerState.COMPLETE
            self.completed_at = now
            self._log("ledger_completed", None, now)
