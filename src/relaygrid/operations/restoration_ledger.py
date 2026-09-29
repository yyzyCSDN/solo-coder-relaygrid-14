"""Restoration command execution ledger.

Posts the steps of an approved switching order (操作票) to a device
:class:`~relaygrid.operations.command_bus.CommandBus` one at a time and
confirms each step against SCADA telesignals (遥信). Every state change is
recorded in an append-only, hash-chained evidence log together with the exact
ticket revision being executed. Duplicate receipts, timeouts, manual skips and
failure retries are all idempotent; any step that does not reach its expected
state halts the cursor so later steps are never posted.
"""
from __future__ import annotations
import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable

from relaygrid.operations.command_bus import Command, CommandBus
from relaygrid.scada.state import StateStore
from relaygrid.switching.orders import OrderState, SwitchingOrder, SwitchingStep


class StepState(str, Enum):
    PENDING = "pending"
    DISPATCHED = "dispatched"          # posted to the command queue
    AWAITING_SIGNAL = "awaiting_signal"  # device acked success, waiting for 遥信
    CONFIRMED = "confirmed"
    FAILED = "failed"                  # rejected/failed ack or unexpected signal
    TIMED_OUT = "timed_out"
    SKIPPED = "skipped"                # operator manual skip (recorded, not confirmed)


_TERMINAL = {StepState.CONFIRMED, StepState.SKIPPED}
_RETRYABLE = {StepState.FAILED, StepState.TIMED_OUT}
# states that must be cleared before the next step may be posted
_HALT = {StepState.DISPATCHED, StepState.AWAITING_SIGNAL, StepState.FAILED, StepState.TIMED_OUT}


@dataclass(frozen=True)
class TicketSnapshot:
    """Immutable copy of the ticket revision pinned by this ledger."""
    order_id: str
    revision: int
    state: str
    author: str
    checker: str | None
    approver: str | None
    steps: tuple[SwitchingStep, ...]


@dataclass
class StepRun:
    step: SwitchingStep
    state: StepState = StepState.PENDING
    attempts: int = 0
    command_ids: list[str] = field(default_factory=list)
    receipt_ids: list[str] = field(default_factory=list)
    observed_state: str | None = None
    failure_reason: str | None = None
    dispatched_at: float | None = None
    confirmed_at: float | None = None
    expires_at: float | None = None
    hold_released: bool = False

    def as_dict(self) -> dict:
        return {
            "sequence": self.step.sequence,
            "action": self.step.action,
            "device_id": self.step.device_id,
            "expected_state": self.step.expected_state,
            "hold_point": self.step.hold_point,
            "state": self.state.value,
            "attempts": self.attempts,
            "command_ids": list(self.command_ids),
            "receipt_ids": list(self.receipt_ids),
            "observed_state": self.observed_state,
            "failure_reason": self.failure_reason,
            "dispatched_at": self.dispatched_at,
            "confirmed_at": self.confirmed_at,
            "expires_at": self.expires_at,
            "hold_released": self.hold_released,
        }


@dataclass
class Evidence:
    seq: int
    ts: float
    kind: str
    payload: dict
    prev_hash: str
    hash: str

    def as_dict(self) -> dict:
        return {"seq": self.seq, "ts": self.ts, "kind": self.kind,
                "payload": self.payload, "prev_hash": self.prev_hash, "hash": self.hash}


def _canonical(obj) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      default=lambda o: getattr(o, "value", str(o))).encode("utf-8")


def digest_payload(payload: dict) -> str:
    return hashlib.sha256(_canonical(payload)).hexdigest()


class RestorationLedger:
    """Append-only execution ledger for one approved ticket revision."""

    def __init__(self, order: SwitchingOrder, bus: CommandBus, signals: StateStore,
                 point_of: Callable[[SwitchingStep], str] | None = None,
                 command_ttl_s: float = 60.0, signal_timeout_s: float = 30.0,
                 max_attempts: int = 3):
        rev = order.current
        if rev.state not in {OrderState.APPROVED, OrderState.EXECUTING}:
            raise ValueError("order not approved")
        if command_ttl_s <= 0 or signal_timeout_s <= 0:
            raise ValueError("timeouts must be positive")
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self.order_id = order.order_id
        self.bus = bus
        self.signals = signals
        self.point_of = point_of or (lambda s: f"{s.device_id}.{s.action}")
        self.command_ttl_s = command_ttl_s
        self.signal_timeout_s = signal_timeout_s
        self.max_attempts = max_attempts
        self.ticket = TicketSnapshot(
            order.order_id, rev.revision, rev.state.value, rev.author,
            rev.checker, rev.approver, tuple(rev.steps))
        self.runs: list[StepRun] = [StepRun(step=s) for s in self.ticket.steps]
        self._command_index: dict[str, int] = {}
        self._seen_receipts: set[str] = set()
        self.evidence: list[Evidence] = []
        self._append(0.0, "ticket_pinned", {
            "order_id": self.order_id,
            "ticket_digest": self.ticket_digest,
            "revision": self.ticket.revision,
            "state": self.ticket.state,
            "author": self.ticket.author,
            "checker": self.ticket.checker,
            "approver": self.ticket.approver,
            "steps": [{"sequence": s.sequence, "action": s.action,
                       "device_id": s.device_id, "expected_state": s.expected_state,
                       "hold_point": s.hold_point, "note": s.note} for s in self.ticket.steps],
        })

    # ------------------------------------------------------------------ core
    @property
    def ticket_digest(self) -> str:
        """Content hash of the pinned ticket revision (version evidence)."""
        t = self.ticket
        return digest_payload({
            "order_id": t.order_id, "revision": t.revision, "state": t.state,
            "author": t.author, "checker": t.checker, "approver": t.approver,
            "steps": [{"sequence": s.sequence, "action": s.action,
                       "device_id": s.device_id, "expected_state": s.expected_state,
                       "hold_point": s.hold_point, "note": s.note} for s in t.steps],
        })

    @property
    def current(self) -> StepRun | None:
        """The step the cursor is parked on, or None when the ticket is done."""
        return next((r for r in self.runs if r.state not in _TERMINAL), None)

    @property
    def is_complete(self) -> bool:
        return all(r.state in _TERMINAL for r in self.runs)

    @property
    def is_paused(self) -> bool:
        """True when the cursor cannot move forward without human action."""
        cur = self.current
        return cur is not None and cur.state in _HALT

    def blocked(self) -> StepRun | None:
        cur = self.current
        return cur if cur is not None and cur.state in _HALT else None

    def run_for(self, sequence: int) -> StepRun:
        for r in self.runs:
            if r.step.sequence == sequence:
                return r
        raise KeyError(sequence)

    def _append(self, ts: float, kind: str, payload: dict) -> Evidence:
        prev_hash = self.evidence[-1].hash if self.evidence else hashlib.sha256(b"genesis").hexdigest()
        body = {"seq": len(self.evidence), "ts": ts, "kind": kind,
                "payload": payload, "prev_hash": prev_hash}
        ev = Evidence(body["seq"], ts, kind, payload, prev_hash,
                      hashlib.sha256(_canonical(body)).hexdigest())
        self.evidence.append(ev)
        return ev

    # -------------------------------------------------------------- dispatch
    def dispatch_next(self, now: float, actor: str = "ledger") -> StepRun | None:
        """Post the current step's command to the device queue.

        Idempotent: a step already in flight or a held/uncleared step returns
        its existing run without posting a second command.
        """
        cur = self.current
        if cur is None or cur.state in _HALT:
            return cur
        if cur.state in _RETRYABLE:
            return cur  # retry() must authorize a fresh attempt
        step = cur.step
        if step.hold_point and not cur.hold_released:
            return cur  # pause at hold point until release_hold()
        if cur.attempts >= self.max_attempts:
            return cur
        attempt = cur.attempts + 1
        command_id = f"{self.order_id}.r{self.ticket.revision}.s{step.sequence}.a{attempt}"
        if command_id in cur.command_ids:
            return cur  # identical attempt already posted (duplicate call)
        expires_at = now + self.command_ttl_s
        self.bus.submit(Command(command_id, step.device_id, step.action,
                                now, expires_at, actor), now)
        cur.attempts = attempt
        cur.command_ids.append(command_id)
        cur.state = StepState.DISPATCHED
        cur.dispatched_at = now
        cur.expires_at = expires_at
        cur.failure_reason = None
        cur.observed_state = None
        self._command_index[command_id] = step.sequence
        self._append(now, "dispatched", {
            "sequence": step.sequence, "command_id": command_id, "attempt": attempt,
            "device_id": step.device_id, "action": step.action,
            "expires_at": expires_at, "actor": actor})
        return cur

    # --------------------------------------------------------------- receipts
    def ingest_bus_results(self, now: float | None = None) -> list[str]:
        """Pull every queued device result belonging to this ledger.

        Idempotent: each (command_id, status) maps to a stable receipt id, so
        re-ingesting the same bus result never changes state twice.
        """
        processed: list[str] = []
        for command_id, result in self.bus.completed.items():
            sequence = self._command_index.get(command_id)
            if sequence is None:
                continue
            receipt_id = f"{command_id}:{result.status}"
            if receipt_id in self._seen_receipts:
                continue
            self.apply_receipt(receipt_id, command_id, result.status,
                               result.completed_at, result.observed_state)
            processed.append(receipt_id)
        return processed

    def apply_receipt(self, receipt_id: str, command_id: str, status: str,
                      completed_at: float, observed_state: str | None = None) -> StepRun:
        """Apply one device receipt. Duplicate receipts are no-ops."""
        if receipt_id in self._seen_receipts:
            sequence = self._command_index.get(command_id)
            return self.run_for(sequence) if sequence is not None else self.current
        sequence = self._command_index.get(command_id)
        if sequence is None:
            raise KeyError(command_id)
        run = self.run_for(sequence)
        if command_id not in run.command_ids:
            raise KeyError(command_id)
        self._seen_receipts.add(receipt_id)
        run.receipt_ids.append(receipt_id)
        if run.state in _TERMINAL:
            self._append(completed_at, "receipt_after_terminal", {
                "sequence": sequence, "command_id": command_id,
                "status": status, "state": run.state.value})
            return run
        if status == "success":
            run.state = StepState.AWAITING_SIGNAL
            payload_kind = "ack_success"
        else:  # failed / rejected: expected state cannot be met on this attempt
            run.state = StepState.FAILED
            run.failure_reason = f"receipt_{status}"
            run.observed_state = observed_state
            payload_kind = "ack_failed"
        self._append(completed_at, payload_kind, {
            "sequence": sequence, "command_id": command_id,
            "receipt_id": receipt_id, "status": status,
            "observed_state": observed_state})
        return run

    # ------------------------------------------------------------- 遥信 check
    def reconcile_signals(self, now: float) -> StepRun | None:
        """Confirm the in-flight step when its telesignal matches expectation.

        Re-checking is idempotent. A matching signal confirms; a non-matching
        fresh signal fails the attempt; a missing/unexpected signal keeps the
        step waiting (a later timeout or skip resolves it).
        """
        cur = self.current
        if cur is None:
            return None
        point = self.point_of(cur.step)
        version = self.signals.versions.get(point, -1)
        if cur.state == StepState.DISPATCHED:
            observed = self.signals.discrete.get(point)
            if observed == cur.step.expected_state:
                self._confirm(cur, observed, now, version, via_dispatch=True)
            return cur
        if cur.state != StepState.AWAITING_SIGNAL:
            return cur
        observed = self.signals.discrete.get(point)
        if observed == cur.step.expected_state:
            self._confirm(cur, observed, now, version, via_dispatch=False)
        elif observed is not None:
            cur.state = StepState.FAILED
            cur.failure_reason = "unexpected_signal"
            cur.observed_state = observed
            self._append(now, "signal_mismatch", {
                "sequence": cur.step.sequence, "point": point,
                "expected": cur.step.expected_state, "observed": observed,
                "version": version})
        return cur

    def _confirm(self, run: StepRun, observed: str, now: float, version: int,
                 via_dispatch: bool) -> None:
        run.state = StepState.CONFIRMED
        run.observed_state = observed
        run.confirmed_at = now
        self._append(now, "signal_confirmed", {
            "sequence": run.step.sequence, "point": self.point_of(run.step),
            "expected": run.step.expected_state, "observed": observed,
            "version": version, "via_dispatch": via_dispatch})

    # -------------------------------------------------------------- timeouts
    def check_timeouts(self, now: float) -> StepRun | None:
        """Expire over-age commands or unconfirmed telesignal waits (idempotent)."""
        cur = self.current
        if cur is None or cur.state in _TERMINAL:
            return cur
        reason = None
        deadline = None
        if cur.state is StepState.DISPATCHED:
            deadline = cur.expires_at
            reason = "command_expired"
        elif cur.state is StepState.AWAITING_SIGNAL:
            deadline = (cur.dispatched_at or 0.0) + self.signal_timeout_s
            reason = "signal_timeout"
        if deadline is not None and now > deadline:
            cur.state = StepState.TIMED_OUT
            cur.failure_reason = reason
            self._append(now, "timed_out", {
                "sequence": cur.step.sequence, "deadline": deadline,
                "reason": reason})
        return cur

    # ------------------------------------------------------- human overrides
    def skip(self, sequence: int, now: float, operator: str, reason: str) -> StepRun:
        """Manually skip a step. Only the current step may be skipped.

        Repeated skips of an already-skipped step are idempotent no-ops.
        Skipping records evidence but is not a telesignal confirmation.
        """
        run = self.run_for(sequence)
        if run.state == StepState.SKIPPED:
            return run
        if run.state == StepState.CONFIRMED:
            raise ValueError("cannot skip confirmed step")
        cur = self.current
        if cur is None or run is not cur:
            raise ValueError("only the current step may be skipped")
        run.state = StepState.SKIPPED
        run.failure_reason = f"manual_skip:{reason}"
        self._append(now, "skipped", {
            "sequence": sequence, "operator": operator, "reason": reason})
        return run

    def retry(self, sequence: int, now: float, actor: str = "ledger") -> StepRun:
        """Authorize a fresh attempt of a failed/timed-out step.

        Idempotent until the new attempt is actually posted: calling retry
        again after dispatch does not consume another attempt.
        """
        run = self.run_for(sequence)
        if run.state not in _RETRYABLE:
            raise ValueError("step is not retryable")
        if run.attempts >= self.max_attempts:
            raise ValueError("max attempts exhausted")
        run.state = StepState.PENDING
        run.failure_reason = None
        run.observed_state = None
        self._append(now, "retry_authorized", {
            "sequence": sequence, "next_attempt": run.attempts + 1, "actor": actor})
        return self.dispatch_next(now, actor)

    def release_hold(self, sequence: int, now: float, operator: str) -> StepRun:
        """Release a hold point (人工放行). Idempotent."""
        run = self.run_for(sequence)
        if not run.step.hold_point:
            raise ValueError("step is not a hold point")
        if not run.hold_released:
            run.hold_released = True
            self._append(now, "hold_released",
                         {"sequence": sequence, "operator": operator})
        return run

    # ----------------------------------------------------------- orchestrate
    def pump(self, now: float, actor: str = "ledger") -> StepRun | None:
        """One idempotent settle cycle: receipts, signals, timeouts, dispatch.

        Stops with the cursor on the first step that is not confirmed/skipped.
        """
        self.ingest_bus_results(now)
        cur = self.reconcile_signals(now)
        if cur is not None and cur.state in {StepState.DISPATCHED, StepState.AWAITING_SIGNAL}:
            self.check_timeouts(now)
            cur = self.current
        if cur is not None and cur.state == StepState.PENDING:
            self.dispatch_next(now, actor)
        return self.current

    # ------------------------------------------------------------- evidence
    def verify_chain(self) -> bool:
        """Recompute every evidence hash; True when the chain is intact."""
        prev_hash = hashlib.sha256(b"genesis").hexdigest()
        for i, ev in enumerate(self.evidence):
            if ev.seq != i or ev.prev_hash != prev_hash:
                return False
            body = {"seq": ev.seq, "ts": ev.ts, "kind": ev.kind,
                    "payload": ev.payload, "prev_hash": ev.prev_hash}
            if hashlib.sha256(_canonical(body)).hexdigest() != ev.hash:
                return False
            prev_hash = ev.hash
        return True

    def evidence_for(self, sequence: int) -> list[Evidence]:
        return [e for e in self.evidence if e.payload.get("sequence") == sequence]

    def export(self) -> dict:
        """Full preservation bundle: ticket version, runs and evidence chain."""
        return {
            "order_id": self.order_id,
            "ticket_digest": self.ticket_digest,
            "ticket": {
                "order_id": self.ticket.order_id,
                "revision": self.ticket.revision,
                "state": self.ticket.state,
                "author": self.ticket.author,
                "checker": self.ticket.checker,
                "approver": self.ticket.approver,
                "steps": [{"sequence": s.sequence, "action": s.action,
                           "device_id": s.device_id, "expected_state": s.expected_state,
                           "hold_point": s.hold_point, "note": s.note}
                          for s in self.ticket.steps],
            },
            "complete": self.is_complete,
            "paused": self.is_paused,
            "steps": [r.as_dict() for r in self.runs],
            "evidence": [e.as_dict() for e in self.evidence],
            "chain_ok": self.verify_chain(),
        }
