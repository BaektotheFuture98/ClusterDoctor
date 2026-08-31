# ClusterGuard

ClusterGuard diagnoses Elasticsearch clusters on its own. It consumes ES slow-log messages
from Kafka in real time, waits for the burst to settle, then runs an LLM agent that decides
what to investigate — checking cluster health, pulling the relevant ClickHouse rows, and
analysing them minute by minute — and prints a diagnosis report.

It is a long-running consumer, not a request/response service. Nothing calls it; it reacts
to slow-logs arriving.

> An earlier version exposed a `POST /api/v1/diagnosis` HTTP API where the caller supplied
> the time window. That layer is gone. The agent now chooses the window itself from the
> trigger's timestamps. If you are looking for the endpoints, they were removed along with
> `DiagnosisService` and the `single`/`graph` `ANALYSIS_MODE` switch.

## How it works

```
ES slowlog 발생
  → Filebeat                                    (실측 +16초)
  → ES 데이터스트림 logs-elasticsearch.slowlog-default
  → Kafka Elasticsearch Source Connector
  → Kafka slowlog 토픽 ─┬→ ClusterGuard consume  (트리거)
                        └→ ClickHouse slowlog_v2 (실측 합계 +31초)
```

The connector reads *from* Elasticsearch and writes *to* Kafka. Both branches leave the same
connector output, so a message reaching ClusterGuard is in ClickHouse at roughly the same
moment — the batch window is more than enough for it to land.

Once triggered:

1. **Batch** — `SlowlogTriggerService` collects arrivals for `MICRO_BATCH_SECONDS`, then
   starts the agent once. Slow-logs arrive in bursts; diagnosing each one separately would
   analyse the same incident dozens of times.
2. **Wait for the burst to end** — the agent alternates `sleep(30)` and
   `check_new_slowlogs()`, and only proceeds once it sees **zero new arrivals twice in a
   row**. One zero is not enough: the connector's polling interval produces a false lull
   mid-incident. Capped at 5 minutes.
3. **Analyse** — `analyze_logs(start, end)` queries the three ClickHouse sources for the
   window, splits them into one-minute buckets, and issues one LLM call per non-empty
   bucket, then one more to synthesise the buckets into a report.
4. **Report** — printed through `StdoutNotifier`.

Everything the agent can do is one of six tools: `cluster_health`,
`explain_unassigned_shards`, `get_index_summary`, `check_new_slowlogs`, `sleep`,
`analyze_logs`. Filesystem access is denied outright.

### Why minute buckets

A single prompt holding a whole window has to sample, and sampling loses the oldest logs
first. Splitting by minute keeps each bucket under the cap, so every line reaches the model.
The synthesis step then sees the per-minute summaries plus the evidence lines each minute
picked — never the raw logs again, which would reintroduce the truncation.

Empty minutes are skipped and cost nothing. A single minute failing (rate limit, filtered
response) does not sink the diagnosis: it is marked `[분석 실패]` and the synthesis prompt is
told, so the gap appears in the report instead of being hidden. If *every* minute fails, the
run fails rather than returning something that reads like "nothing wrong".

### Slow-logs are read by occurrence time

`slowlog_v2` has two timestamps: `ch_ingested_at` (when ClickHouse stored the row) and
`_source.@timestamp` (when ES actually logged the slow query). Queries filter on the latter.
Measured lag between them is 23–41 s, and a lag crossing a minute boundary is enough to drop
the very slow-log that caused the trigger — the same one-minute window returns different
rows depending on which column you filter (measured: 4 rows vs 2).

## Requirements

- Python >= 3.13
- [uv](https://docs.astral.sh/uv/)
- A reachable **Kafka** cluster carrying the slow-log topic
- A reachable **ClickHouse** instance holding the slow-log / query-log / node-metric tables
- A reachable **Elasticsearch** cluster (the agent's first step is `cluster_health()`)
- A **Gemini** API key
- Network access to `openaipublic.blob.core.windows.net` on the very first boot — see
  "Deployment notes"

## Configuration

Read from environment variables, or a `.env` file in the project root, by
`src/cluster_doctor/infrastructure/config/settings.py`. Copy `.env.example` to `.env` and
fill in real values — never commit real keys.

Startup fails loudly if a required variable is missing, rather than surfacing the failure on
the first slow-log. The error names only *which* settings are at fault — never their values,
so a config failure cannot write `CLICKHOUSE_PASSWORD` into a log aggregator.

| Variable | Required | Default | Notes |
|---|---|---|---|
| `GEMINI_API_KEY` | yes | — | Passed to litellm as a parameter, never in a URL. |
| `GEMINI_MODEL` | no | `gemini-2.5-flash` | |
| `CLICKHOUSE_URL` | yes | — | e.g. `jdbc:clickhouse://host:8123/default`. The `jdbc:` prefix is optional. The database in the path becomes the session default, so table names are unqualified. |
| `CLICKHOUSE_USER` | no | `default` | |
| `CLICKHOUSE_PASSWORD` | no | `` (empty) | |
| `CLICKHOUSE_SLOWLOG_TABLE` | no | `slowlog_v2` | |
| `CLICKHOUSE_LOG_TABLE` | no | `log` | |
| `CLICKHOUSE_NODE_METRIC_TABLE` | no | `es_node_metric` | |
| `ES_HOST` | yes | — | Comma-separated hosts. Rejected at startup if blank. |
| `ES_PORT` | no | `9200` | |
| `ES_USER` | no | `` (empty) | Basic auth is skipped entirely when empty. |
| `ES_PASSWORD` | no | `` (empty) | |
| `KAFKA_BOOTSTRAP_SERVERS` | no | `localhost:9092` | |
| `KAFKA_TOPIC` | no | `slowlog` | |
| `KAFKA_GROUP_ID` | no | `clusterdoctor` | |
| `MICRO_BATCH_SECONDS` | no | `10` | How long to collect arrivals before starting the agent. |
| `LITELLM_LOCAL_MODEL_COST_MAP` | no | `True` | Read by litellm directly, not by `Settings`. Set at import time by `litellm_client.py`; stops litellm fetching cost data from GitHub. |

A test asserts `.env.example` documents nothing `Settings` ignores. That check exists
because four settings (`FLUSH_INTERVAL_SECONDS`, `FLUSH_MAX_SIZE`, `LOOKBACK_*`) were once
documented but never read — an operator setting them got hardcoded defaults with no warning.

### Fixed limits

Compile-time constants, not environment variables. Several can silently drop or refuse work,
so they are listed here for operators reading logs.

| Limit | Value | Defined in | Notes |
|---|---|---|---|
| Analysis window | 10 minutes | `domain/model/time_range.py` (`MAX_TIME_RANGE_DURATION`) | `analyze_logs` refuses a longer window and tells the agent to narrow it. Bounds ClickHouse fan-out and LLM cost per call. |
| `analyze_logs` calls per run | 6 | `infrastructure/outbound/llm/deepagent/tools.py` (`_MAX_ANALYZE_CALLS`) | The most expensive tool — one call issues one LLM request per minute in the window. Past the cap it returns a refusal string without querying anything. |
| Agent wait budget | 60 s per `sleep`, 300 s cumulative | same file (`_MAX_SLEEP_SECONDS`, `_MAX_WAIT_SECONDS`) | Analysis has not started while the agent waits, so the queue only grows. Past the cumulative cap `sleep` returns immediately without waiting. |
| Consecutive retriggers | 3 | `application/service/slowlog_trigger_service.py` (`_MAX_CONSECUTIVE_RETRIGGERS`) | Only successful runs retrigger, and each retrigger waits `MICRO_BATCH_SECONDS` first. |
| Rows per minute-segment per source | 10,000 | `infrastructure/outbound/clickhouse/clickhouse_log_adapter.py` (`_MAX_ROWS_PER_SEGMENT_PER_SOURCE`) | Applied as `LIMIT` on each per-source query, per one-minute segment. There is no `ORDER BY`, so on a hit ClickHouse returns an arbitrary subset — and the count reported to the LLM is the capped one. A `WARNING` naming the source and segment is logged whenever a query returns at the cap. |
| Keywords shown per query-log line | 5 | `infrastructure/outbound/llm/langgraph/prompts.py` (`_MAX_KEYWORDS_SHOWN`) | Some queries carry 200+ keywords, making a single prompt line 2,000+ characters. The rest are summarised as `외 N개`; the domain object keeps all of them. |
| LLM output tokens | 1,024 per minute, 8,192 for synthesis | `infrastructure/outbound/llm/langgraph/nodes.py` | |
| LLM request timeout | 120 s | `infrastructure/outbound/llm/litellm_client.py` (`_REQUEST_TIMEOUT_SECONDS`) | |
| ClickHouse send/receive timeout | 30 s | `infrastructure/config/dependencies.py` | Without it a hung ClickHouse could pin a worker thread indefinitely. |

### Retries are disabled on purpose

Both LLM layers have retries turned off — `num_retries=0` for litellm, `max_retries=1` for
`ChatGoogleGenerativeAI` (that library reads `0` as "use the Google SDK default", so `1` is
how you actually disable them).

Failures on this path are overwhelmingly 429s from the Gemini free tier's **250,000 input
tokens per minute** cap. Resending the same enormous prompt cannot succeed — the quota is
what failed — and only multiplies consumption. Measured: one 5-minute window costs 513,122
input tokens; with retries that becomes 2,052,488, or 821 % of the minute's quota.

The trade-off is that transient 5xx errors are not retried either. That is affordable
because a failed minute degrades into `[분석 실패]` rather than failing the run.

## Install

```
uv sync
```

## Run

```
uv run python -m cluster_doctor.main
```

This starts the Kafka consumer and blocks. There is no HTTP port. Logs go to stderr and to
`logs/app.log`.

To exercise it without waiting for a real slow-log, publish a synthetic message:

```
uv run python scripts/produce_test_message.py
```

## Test

```
uv run pytest -q
```

146 tests, ~5 s. No test touches a real LLM, Kafka, ClickHouse, or Elasticsearch.

## Data sources

`analyze_logs` reads three ClickHouse tables and merges them into one timeline.

| Table | Time column | Type | Carries |
|---|---|---|---|
| `slowlog_v2` | `_source.@timestamp` | `DateTime64(9)` (JSON sub-column) | Queries over the ES slow-log threshold: index, node, `took`, hit count, shard count, the query source, and the `x-opaque-id` header holding service/project/company/user |
| `log` | `reg_date` | `DateTime('Asia/Seoul')` | Every ES query execution: host, runtime, success, command, keywords, company, user |
| `es_node_metric` | `reg_date` | `DateTime('Asia/Seoul')` | Per-node CPU, memory, JVM heap, and search/write queue and rejection counts |

**Timezones are uniformly KST.** The ClickHouse server's `timezone()` is `Asia/Seoul`, and
`slowlog_v2`'s column inherits it by not specifying one. All three return timezone-aware KST
values, so merging and sorting them is safe.

Only the named JSON sub-columns of `_source` are selected, not the whole document — the full
document is 2,668 characters per row of which about 27 % is diagnostic; the rest
(`host.mac`, `agent.ephemeral_id`, `host.os.kernel`, …) is prompt tokens for nothing.

## Deployment notes

Measured against a real install, not inferred from documentation.

### First boot on an air-gapped network

`import litellm` fetches the tiktoken `cl100k_base` encoding from
`openaipublic.blob.core.windows.net`. **That request has no exception handling.** With a cold
cache and no route to that host, the import stalls for roughly 19.5 s and then dies with an
uncaught `ProxyError` — the app does not start at all, and the failure looks nothing like a
configuration problem.

If you build a container image, warm the cache at build time:

    RUN python -c "import litellm"

Otherwise the first boot needs access to that host. The cache persists on disk, so later
boots do not.

`LITELLM_LOCAL_MODEL_COST_MAP=True` removes a *separate* call to GitHub for cost data. It
does not help with the tiktoken fetch.

### Trimming image size (optional)

litellm ships roughly 39 MB this service never touches:

    RUN rm -rf /path/to/site-packages/litellm/proxy/_experimental/out \
               /path/to/site-packages/litellm/proxy/swagger \
               /path/to/site-packages/litellm/rust_bridge/_native.pyd || true

The `|| true` is deliberate: if litellm reorganises its layout this step should quietly
become a no-op rather than **breaking the build**.

Do not delete the rest of `litellm/proxy/`. It looks like admin-only tooling, but real
`completion()` calls route through it.

Saves disk only — startup time is unchanged, since those files are not on the import path.

## Known limits

- **The free tier's token quota is still reachable.** A busy 5-minute window costs about
  513,000 input tokens against a 250,000-per-minute cap. Retries and retrigger storms are
  fixed; the underlying prompt size is not. The remaining levers are aggregating
  `node_metric` (107 near-identical lines per minute) and filtering `es_query_log` by
  runtime.
- **A partly-failed analysis is treated as a success.** The degraded flag is set only when
  *every* minute bucket fails. Since the quota resets each minute, partial failure is the
  common shape, and those runs still retrigger. The obvious fix — degrade on *any* failure —
  is wrong: degrading both suppresses the retrigger *and* discards the report, so a
  nine-of-ten-good diagnosis would be thrown away. Doing it properly means separating
  "produced nothing" from "produced something partial", which the `str` return type of
  `LlmAnalyzer.analyze` cannot express today.
- **Hitting the `analyze_logs` call cap is also treated as a success.** Same asymmetry, a
  different cause: the refusal string does not set the degraded flag, so a report written
  from partial coverage looks identical to a complete one.
- **`check_new_slowlogs` drains the queue before analysis starts.** If a run then dies hard,
  the drained entries are gone and no retrigger fires; the incident waits for the next
  slow-log.
