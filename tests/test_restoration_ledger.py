from __future__ import annotations
import pytest

from relaygrid.operations.command_bus import CommandBus
from relaygrid.operations.restoration_ledger import (
    RestorationLedger, StepState,
)
from relaygrid.scada.state import StateStore
from relaygrid.switching.orders import SwitchingOrder, SwitchingStep


def make_order(steps=None, order_id="O-1"):
    order = SwitchingOrder(order_id)
    steps = steps or [
        SwitchingStep(1, "close", "br1", "closed"),
        SwitchingStep(2, "close", "br2", "closed"),
        SwitchingStep(3, "open", "br3", "open"),
    ]
    order.create(steps, author="author")
    order.check("checker")
    order.approve("approver")
    return order


def device_ack(bus, command_id, status, now, observed_state=None):
    """Simulate a device returning a receipt for a queued command."""
    command = next(c for c in bus.pending if c.command_id == command_id)
    bus.complete(command, status, now, observed_state)


def signal(signals, run, value, version):
    point = f"{run.step.device_id}.{run.step.action}"
    assert signals.apply(point, value, version) is True


def build_ledger(order=None, **kw):
    order = order or make_order()
    return RestorationLedger(order, CommandBus(), StateStore(), **kw)


# ----------------------------------------------------------------- lifecycle

def test_requires_approved_order():
    order = SwitchingOrder("O")
    order.create([SwitchingStep(1, "close", "br1", "closed")], "a")
    with pytest.raises(ValueError):
        build_ledger(order)


def test_pins_ticket_revision_and_exports_evidence():
    ledger = build_ledger()
    digest = ledger.ticket_digest
    assert digest == ledger.ticket_digest  # deterministic
    exported = ledger.export()
    assert exported["ticket"]["revision"] == 1
    assert exported["ticket"]["approver"] == "approver"
    assert len(exported["ticket"]["steps"]) == 3
    assert exported["evidence"][0]["kind"] == "ticket_pinned"
    assert exported["evidence"][0]["payload"]["ticket_digest"] == digest
    assert exported["chain_ok"] is True


# ---------------------------------------------------------------- happy path

def test_happy_path_step_by_step():
    ledger = build_ledger()
    bus, sig = ledger.bus, ledger.signals

    r1 = ledger.dispatch_next(10.0)
    assert r1.state is StepState.DISPATCHED and r1.attempts == 1
    cid = r1.command_ids[0]
    # step 2 must not be postable while step 1 is in flight
    assert ledger.current.step.sequence == 1
    assert bus.outstanding_for("br2") == []

    device_ack(bus, cid, "success", 11.0)
    ledger.ingest_bus_results(11.0)
    assert ledger.current.state is StepState.AWAITING_SIGNAL
    # 遥信 not yet matching: still paused on step 1
    ledger.reconcile_signals(12.0)
    assert ledger.current.step.sequence == 1

    sig.apply("br1.close", "closed", 1)
    ledger.reconcile_signals(12.0)
    assert ledger.current.step.sequence == 2

    r2 = ledger.dispatch_next(13.0)
    assert r2.command_ids[0].endswith(".s2.a1")
    device_ack(bus, r2.command_ids[0], "success", 14.0)
    sig.apply("br2.close", "closed", 2)
    ledger.pump(14.0)
    assert ledger.current.step.sequence == 3


def test_pump_runs_full_ticket_to_complete():
    ledger = build_ledger()
    bus, sig = ledger.bus, ledger.signals
    expected = {"br1.close": "closed", "br2.close": "closed", "br3.open": "open"}
    t = 0.0
    for i in range(6):
        t += 1.0
        run = ledger.current
        if run is None:
            break
        # simulator: devices ack and telesignals settle immediately
        for cid in list(c.command_id for c in list(bus.pending)
                        if c.command_id in ledger._command_index):
            device_ack(bus, cid, "success", t)
        point = f"{run.step.device_id}.{run.step.action}"
        if point in expected:
            sig.apply(point, expected[point], i + 1)
        ledger.pump(t)
    assert ledger.is_complete is True
    assert ledger.current is None
    assert all(r.state is StepState.CONFIRMED for r in ledger.runs)
    assert ledger.verify_chain()


# ------------------------------------------------------------- idempotency

def test_duplicate_dispatch_posts_one_command():
    ledger = build_ledger()
    r1 = ledger.dispatch_next(5.0)
    r1b = ledger.dispatch_next(6.0)
    assert r1b is r1
    assert r1.command_ids == [r1.command_ids[0]]
    assert r1.attempts == 1


def test_duplicate_receipt_is_idempotent():
    ledger = build_ledger()
    bus = ledger.bus
    r1 = ledger.dispatch_next(0.0)
    cid = r1.command_ids[0]
    device_ack(bus, cid, "failed", 1.0)
    first = ledger.ingest_bus_results(1.0)
    assert first == [f"{cid}:failed"]
    assert ledger.current.state is StepState.FAILED
    # re-ingest (bus still holds the result) and re-apply same receipt
    assert ledger.ingest_bus_results(1.0) == []
    ledger.apply_receipt(f"{cid}:failed", cid, "failed", 1.0)
    run = ledger.current
    assert run.state is StepState.FAILED
    assert run.receipt_ids == [f"{cid}:failed"]
    kinds = [e.kind for e in ledger.evidence_for(1)]
    assert kinds.count("ack_failed") == 1


def test_late_success_receipt_after_signal_confirmation_is_recorded_but_safe():
    ledger = build_ledger()
    bus, sig = ledger.bus, ledger.signals
    r1 = ledger.dispatch_next(0.0)
    cid = r1.command_ids[0]
    # telesignal arrives before the receipt
    sig.apply("br1.close", "closed", 1)
    ledger.reconcile_signals(2.0)
    assert ledger.run_for(1).state is StepState.CONFIRMED
    device_ack(bus, cid, "success", 3.0)
    ledger.ingest_bus_results(3.0)
    assert ledger.run_for(1).state is StepState.CONFIRMED
    assert any(e.kind == "receipt_after_terminal" for e in ledger.evidence_for(1))


# ------------------------------------------------------------------ pausing

def test_unexpected_signal_fails_and_halts_later_steps():
    ledger = build_ledger()
    bus, sig = ledger.bus, ledger.signals
    r1 = ledger.dispatch_next(0.0)
    device_ack(bus, r1.command_ids[0], "success", 1.0)
    ledger.ingest_bus_results(1.0)
    sig.apply("br1.close", "tripped", 1)  # wrong state
    ledger.reconcile_signals(2.0)
    assert ledger.current.state is StepState.FAILED
    assert ledger.current.failure_reason == "unexpected_signal"
    assert ledger.is_paused is True
    # dispatching must not sneak step 2 through
    again = ledger.dispatch_next(3.0)
    assert again.step.sequence == 1 and again.state is StepState.FAILED
    assert bus.outstanding_for("br2") == []


def test_rejected_receipt_halts():
    ledger = build_ledger()
    bus = ledger.bus
    r1 = ledger.dispatch_next(0.0)
    device_ack(bus, r1.command_ids[0], "rejected", 1.0)
    ledger.ingest_bus_results(1.0)
    assert ledger.current.state is StepState.FAILED
    assert ledger.current.failure_reason == "receipt_rejected"
    assert ledger.is_paused


# ---------------------------------------------------------------- timeouts

def test_command_expiry_timeout_then_retry_succeeds():
    ledger = build_ledger(command_ttl_s=10.0, signal_timeout_s=10.0)
    bus, sig = ledger.bus, ledger.signals
    r1 = ledger.dispatch_next(0.0)
    first_cid = r1.command_ids[0]
    # bus itself expires the queued command
    bus.next_for("br1", 11.0)
    ledger.check_timeouts(11.0)
    assert ledger.current.state is StepState.TIMED_OUT
    # calling timeout check again changes nothing
    ledger.check_timeouts(12.0)
    assert ledger.current.state is StepState.TIMED_OUT

    r1 = ledger.retry(1, 13.0)
    assert r1.state is StepState.DISPATCHED and r1.attempts == 2
    assert len(r1.command_ids) == 2 and r1.command_ids[0] == first_cid
    cid2 = r1.command_ids[1]
    device_ack(bus, cid2, "success", 14.0)
    sig.apply("br1.close", "closed", 1)
    ledger.pump(14.0)
    assert ledger.run_for(1).state is StepState.CONFIRMED
    assert ledger.current.step.sequence == 2


def test_signal_timeout_after_ack_then_retry():
    ledger = build_ledger(command_ttl_s=60.0, signal_timeout_s=5.0)
    bus = ledger.bus
    r1 = ledger.dispatch_next(0.0)
    device_ack(bus, r1.command_ids[0], "success", 1.0)
    ledger.ingest_bus_results(1.0)
    assert ledger.current.state is StepState.AWAITING_SIGNAL
    ledger.check_timeouts(7.0)
    assert ledger.current.state is StepState.TIMED_OUT
    assert ledger.current.failure_reason == "signal_timeout"
    ledger.retry(1, 8.0)
    assert ledger.current.state is StepState.DISPATCHED


def test_retry_idempotent_until_progress_and_exhausts():
    ledger = build_ledger(max_attempts=2)
    bus = ledger.bus
    r1 = ledger.dispatch_next(0.0)
    device_ack(bus, r1.command_ids[0], "failed", 1.0)
    ledger.ingest_bus_results(1.0)
    ledger.retry(1, 2.0)  # posts attempt 2
    assert ledger.current.attempts == 2
    device_ack(bus, ledger.current.command_ids[1], "failed", 3.0)
    ledger.ingest_bus_results(3.0)
    with pytest.raises(ValueError):
        ledger.retry(1, 4.0)
    assert ledger.current.state is StepState.FAILED


def test_retry_requires_failed_or_timed_out():
    ledger = build_ledger()
    ledger.dispatch_next(0.0)
    with pytest.raises(ValueError):
        ledger.retry(1, 1.0)


# ------------------------------------------------------------- manual skip

def test_manual_skip_is_idempotent_and_advances():
    ledger = build_ledger()
    bus, sig = ledger.bus, ledger.signals
    ledger.dispatch_next(0.0)
    device_ack(bus, ledger.current.command_ids[0], "failed", 1.0)
    ledger.ingest_bus_results(1.0)
    ledger.skip(1, 2.0, operator="op", reason="field-verified-closed")
    assert ledger.run_for(1).state is StepState.SKIPPED
    # repeated skip is a no-op (same state, no extra evidence)
    n_before = len(ledger.evidence)
    ledger.skip(1, 3.0, operator="op", reason="again")
    assert len(ledger.evidence) == n_before
    # next step proceeds; skipped step is not marked confirmed
    assert ledger.current.step.sequence == 2
    assert ledger.run_for(1).confirmed_at is None
    ledger.dispatch_next(4.0)


def test_skip_only_current_step_and_not_confirmed():
    ledger = build_ledger()
    bus, sig = ledger.bus, ledger.signals
    ledger.dispatch_next(0.0)
    with pytest.raises(ValueError):
        ledger.skip(2, 1.0, "op", "ahead")  # not current
    device_ack(bus, ledger.current.command_ids[0], "success", 1.0)
    sig.apply("br1.close", "closed", 1)
    ledger.pump(2.0)
    with pytest.raises(ValueError):
        ledger.skip(1, 3.0, "op", "already done")


# -------------------------------------------------------------- hold points

def test_hold_point_blocks_until_released():
    steps = [
        SwitchingStep(1, "close", "br1", "closed"),
        SwitchingStep(2, "close", "br2", "closed", hold_point=True),
    ]
    ledger = build_ledger(make_order(steps, "O-H"))
    bus, sig = ledger.bus, ledger.signals

    r1 = ledger.dispatch_next(0.0)
    device_ack(bus, r1.command_ids[0], "success", 1.0)
    sig.apply("br1.close", "closed", 1)
    ledger.pump(2.0)
    # now at held step 2: dispatch is a no-op
    cur = ledger.dispatch_next(3.0)
    assert cur.state is StepState.PENDING
    assert bus.outstanding_for("br2") == []
    with pytest.raises(ValueError):
        ledger.release_hold(1, 3.0, "op")  # not a hold point
    ledger.release_hold(2, 4.0, "op")
    # releasing twice adds no evidence
    n = len(ledger.evidence)
    ledger.release_hold(2, 4.5, "op")
    assert len(ledger.evidence) == n
    cur = ledger.dispatch_next(5.0)
    assert cur.state is StepState.DISPATCHED


# ----------------------------------------------------------- evidence chain

def test_evidence_chain_detects_tampering():
    ledger = build_ledger()
    ledger.dispatch_next(0.0)
    assert ledger.verify_chain() is True
    ledger.evidence[1].payload["device_id"] = "fake-device"
    assert ledger.verify_chain() is False


def test_stale_signal_versions_ignored():
    ledger = build_ledger()
    sig = ledger.signals
    ledger.dispatch_next(0.0)
    assert sig.apply("br1.close", "closed", 5) is True
    # stale (older) version must not overwrite observed state
    assert sig.apply("br1.close", "open", 4) is False
    device_ack(ledger.bus, ledger.current.command_ids[0], "success", 1.0)
    ledger.pump(2.0)
    assert ledger.run_for(1).state is StepState.CONFIRMED
