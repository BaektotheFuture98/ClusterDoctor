# ClusterDoctor

ClusterDoctor consumes Elasticsearch slowlog triggers from Kafka, waits for the
event stream to settle, and writes one HTML diagnosis report per incident. It is
a long-running consumer; it does not expose an HTTP diagnosis endpoint.

## Architecture

Dependencies point inward:

```text
inbound adapters → application → domain ← outbound adapters
                         ↑
                    bootstrap composition
```

The domain contains incident state, guardrails, time ranges, evidence, report
types, and the pure `merge_window_reports` policy. It imports neither the
application nor adapters. Application use cases coordinate ports and contain no
Kafka, LangChain, LangGraph, DeepAgents, or LiteLLM imports. `bootstrap` is the
only composition root.

### Ports

Application ports are:

- `SlowlogHandler`: accepts a parsed `SlowlogTrigger` from an inbound adapter.
- `IncidentAnalyzer`: analyzes one immutable `Incident` and persists progress
  through `IncidentStateRepository`.
- `IncidentStateRepository` and `ArtifactStore`: preserve lifecycle state,
  evidence, reports, and observations.
- `LogRepository`, `ClusterRepository`, `NodeResolver`, and `NodeLogFetcher`:
  obtain diagnosis inputs.
- `ReportPublisher`: delivers the final rendered report.

### Adapters

- Inbound: `adapters.inbound.kafka.KafkaConsumerAdapter` decodes Kafka records
  and calls the `SlowlogHandler` port. Its legitimate `aiokafka` dependency is
  confined to this adapter.
- Outbound: ClickHouse implements `LogRepository`; Elasticsearch implements
  `ClusterRepository` and `NodeResolver`; SSH implements `NodeLogFetcher`;
  in-memory persistence implements state and artifact ports; HTML reporting
  implements `ReportPublisher`.
- `adapters.outbound.deepagents` is one composite outbound adapter. Its public
  API is `DeepAgentsConfig` plus `build_deepagents_incident_analyzer`. The
  factory accepts configuration and the application port objects, then creates
  the structured LiteLLM caller, `ReportWriter`, metric thresholds,
  `DiagnosisSeams`, and `DeepAgentIncidentAnalyzer` internally. Bootstrap does
  not import its runtime, supervisor, diagnosis, or pipeline internals.

## Lifecycle

```text
Kafka record
  → KafkaConsumerAdapter parses SlowlogTrigger
  → SlowlogIntake batches and waits for quiet input
  → StartIncident
  → DiagnoseIncident creates IncidentState
  → IncidentAnalyzer port (DeepAgents composite adapter)
  → application finalizes and publishes the report
  → cleanup of transient state and artifacts
```

`SlowlogIntake` owns micro-batching, quiet-period settling, and retriggering.
`DiagnoseIncident` owns state creation, timeout and cancellation closure,
fallback report finalization, output mapping, publication, and cleanup.

The DeepAgents adapter creates an incident-scoped Main Agent graph. Its tool
loop lists candidate windows, proposes an allowed window, delegates the admitted
work to the diagnosis subagent, finalizes the report, and finishes the incident.
Guardrails enforce time and analysis budgets in runtime code. The diagnosis
subagent collects evidence, writes and validates each window report, and stores
intermediate results through injected ports.

Window report merging remains pure domain policy in
`domain.incident.report_merge.merge_window_reports`. The application
`finalize_incident_report` service performs the I/O around that policy: it loads
window reports from `ArtifactStore`, merges them, and persists the final report.
The DeepAgents finalization tool and `DiagnoseIncident` fallback both use that
service.

Candidate projections intentionally remain adapter-local: the DeepAgents prompt
and reporting adapter each format their own representation. Application does not
provide presentation formatting.

## Running

Configure the required Kafka, ClickHouse, Elasticsearch, SSH, LLM, and report
directory settings, then start the consumer:

```bash
uv run python -m cluster_doctor.main
```

For one direct diagnosis without Kafka or queue internals, create a
`StartIncident` through the public manual use case via:

```bash
uv run python scripts/run_diagnosis.py --at "2026-09-26T14:00:00" --dry-run
```

The manual path uses the same `DiagnoseIncident` and DeepAgents composition as
the consumer, while bypassing only Kafka intake and settling.

## Verification

```bash
uv run pytest -q tests/architecture/test_dependency_rules.py
uv run pytest -q
git diff --check
uv run python -c "import cluster_doctor; import cluster_doctor.bootstrap.dependencies"
```
