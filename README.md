# RelayGrid

RelayGrid is a deterministic Python toolkit for distribution-grid operations.
It covers feeder topology, protection, voltage assessment, DER islanding,
switching safety, outage accounting, crew logistics, asset thermal limits,
telemetry quality, storage dispatch, and restoration sequencing.

The package is self-contained and uses only the Python standard library.

## Install and run

```bash
python -m pip install -e ".[test]"
relaygrid demo
python -m pytest -q
```

The demo builds a four-bus feeder with two open ties, reports its energized
state, safely closes the sectionalizing branch, and emits a JSON summary.

## Public API

The top-level package exports the feeder data model and `GridSimulator`:

```python
from relaygrid import Branch, Bus, FeederSnapshot, GridSimulator, SwitchState
```

Domain modules remain independently importable so applications can combine
topology, protection, telemetry, restoration, reliability, planning, and field
operations without depending on a web framework or database.

## Restoration command ledger

`relaygrid.operations.restoration_ledger.RestorationLedger` pins one approved
switching-order revision (`SwitchingOrder`, with a content-hash digest) and
posts its steps to a `CommandBus` strictly in sequence, confirming each step
against `StateStore` telesignals before advancing:

- one cursor; any unconfirmed step (`dispatched` / `awaiting_signal` /
  `failed` / `timed_out`) pauses all later steps;
- duplicate dispatches and duplicate device receipts (stable
  `command_id:status` receipt keys) are no-ops; command TTL and
  telesignal-wait timeouts are idempotent;
- manual skips (`skip`) and failure retries (`retry`, bounded by
  `max_attempts`, fresh attempt id per retry) are idempotent; hold points
  require explicit `release_hold`;
- every transition is appended to a SHA-256 hash-chained evidence log, and
  `export()` preserves the full ticket version, per-step runs, and evidence;
  `verify_chain()` detects tampering.

## License

RelayGrid is released under the MIT License.
