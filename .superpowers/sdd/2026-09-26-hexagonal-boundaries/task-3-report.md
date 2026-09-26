# Task 3 implementation report

## Result

Split the new application boundary into queue-owning `SlowlogIntake` and
state-owning `DiagnoseIncident`. Intake settles micro-batched slowlogs and emits
an immutable `StartIncident`; diagnosis creates and reloads repository state,
runs the analyzer in a thread with timeout protection, finalizes and delivers
the report, and invokes its completion cleanup callback. `RunManualDiagnosis`
creates the same command directly without a Kafka or queue dependency.

## Changed files

- Added `src/cluster_doctor/application/commands.py` with frozen
  `StartIncident`.
- Added `src/cluster_doctor/application/use_cases/slowlog_intake.py` with the
  queue, batching, quiet-period settling, and capped retrigger policy.
- Added `src/cluster_doctor/application/use_cases/diagnose_incident.py` with
  state creation, analyzer invocation, forced terminal closure, report merge
  and delivery, and completion cleanup.
- Added `src/cluster_doctor/application/use_cases/manual_diagnosis.py` for
  direct manual invocation.
- Added the framework-free domain `SlowlogTrigger`, and changed
  `InflowTracker` to consume it instead of the Kafka event type.
- Added focused application behavior tests under `tests/application/`.

## TDD evidence

- RED: `uv run pytest -q tests/application` failed during collection before the
  implementation with three expected `ModuleNotFoundError` errors for
  `cluster_doctor.application.commands`.
- GREEN focused verification:
  `uv run pytest -q tests/application tests/incident tests/ingestion/kafka tests/architecture/test_dependency_rules.py`
  — `123 passed in 1.67s`.
- Lint and whitespace verification:
  `uvx ruff check src/cluster_doctor/application/commands.py src/cluster_doctor/application/use_cases src/cluster_doctor/domain/incident/models.py src/cluster_doctor/domain/incident/inflow.py tests/application`
  and `git diff --check` both passed.
- Required full verification: `uv run pytest -q` — `559 passed in 9.63s`.

## Behavioral coverage

The focused tests cover micro-batching, quiet-period window extension, arrivals
during analysis, the three-retrigger cap, failed-analysis retrigger suppression,
repository-backed analyzer state, analyzer exception, timeout, cancellation,
fallback report finalization/delivery, and direct manual diagnosis.

## Self-review

- `StartIncident` is frozen and contains the immutable incident plus observed
  start/end timestamps and settling duration.
- `SlowlogIntake` only accepts framework-free triggers and does not import
  Kafka. It keeps arrivals during analysis queued for the same successful-only,
  delayed retrigger policy as the prior service.
- `DiagnoseIncident` passes only `Incident` to `IncidentAnalyzer`; the analyzer
  reloads state through its injected repository, and the lifecycle reloads it
  before terminal closure so partial persisted work remains available.
- Timeout and cancellation closure override a late analyzer state, while report
  delivery remains best effort and always runs.

## Concerns

The legacy trigger service and incident runner remain in place for the staged
bootstrap migration in Task 5. The new use cases are independently tested now;
Task 5 will replace the legacy composition path with these public application
entry points.

## Review-fix addendum

### RED/GREEN evidence

- RED: `uv run pytest -q tests/application/test_slowlog_intake.py tests/application/test_diagnose_incident.py tests/application/test_manual_diagnosis.py`
  produced `5 failed, 10 passed`. The failures showed the missing
  `max_settling_wait_seconds` constructor parameter, an analyzer invocation
  after its full budget had already been spent settling, and the old
  single-datetime manual interface rejecting sequences.
- GREEN: `uv run pytest -q tests/application` — `15 passed in 0.31s`.
- Full regression: `uv run pytest -q` — `563 passed in 8.51s`.
- Static verification: the focused `uvx ruff check` and `git diff --check`
  commands completed successfully.

### Review fixes

- `SlowlogIntake` now accepts `max_settling_wait_seconds`, defaults it to
  `MAX_TOTAL_WAIT_SECONDS`, and limits each quiet-period sleep to the remaining
  settling budget (as well as the single-wait cap). Continuous arrivals now
  begin diagnosis at that cap rather than holding the incident indefinitely.
- `DiagnoseIncident` subtracts `StartIncident.settling_wait_seconds` from the
  incident timeout before creating its deadline. An exhausted budget closes the
  incident as failed and skips the analyzer.
- `RunManualDiagnosis.handle` now accepts a non-empty `Sequence[datetime]`,
  creates one manual incident for the minimum/maximum observed window, records
  a current UTC receive time, and rejects empty input with `ValueError`.
