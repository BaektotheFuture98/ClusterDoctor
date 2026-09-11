# ClusterGuard

ClusterGuard diagnoses Elasticsearch clusters on its own. It consumes ES slow-log messages
from Kafka in real time, waits for the burst to settle, then runs an LLM agent that decides
what to investigate — checking cluster health, pulling the relevant ClickHouse rows,
analysing them minute by minute, and reading node logs where they point — and writes a
diagnosis report as a self-contained HTML file.

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
   bucket, then one more to synthesise the buckets into a report. The same call also pulls
   master-node logs for the window so the synthesis step can line them up against the
   per-minute spikes.
4. **Investigate further** — only when the cluster is not green or a node looks implicated:
   index summaries, allocation explanations, and node logs (see "Node logs come from two
   places").
5. **Report** — `HtmlFileNotifier` writes one HTML file per diagnosis into `REPORT_DIR`.

Everything the agent can do is one of eight tools: `analyze_logs`, `check_new_slowlogs`,
`sleep`, `cluster_health`, `explain_unassigned_shards`, `get_index_summary`,
`search_node_logs`, `get_node_logs`. Filesystem access is denied outright.

Limits are enforced by the tools, not by the prompt. The prompt can ask the model to stop
at six `analyze_logs` calls and the model can ignore it; deepagents' `recursion_limit` is
9,999, so the framework will not stop it either. Each tool clamps its own budget.

### Why minute buckets

A single prompt holding a whole window has to sample, and sampling loses the oldest logs
first. Splitting by minute keeps each bucket under the cap, so every line reaches the model.
The synthesis step then sees the per-minute summaries plus the evidence lines each minute
picked — never the raw logs again, which would reintroduce the truncation.

Empty minutes are skipped and cost nothing. A single minute failing (rate limit, filtered
response) does not sink the diagnosis: it is marked `[분석 실패]` and the synthesis prompt is
told, so the gap appears in the report instead of being hidden. If *every* minute fails, the
run fails rather than returning something that reads like "nothing wrong".

### Node logs come from two places

Master-node logs are in ClickHouse; data-node logs are not. So the agent takes two routes,
and the prompt walks them in this order:

| | Source | Tool | Why |
|---|---|---|---|
| Master node | ClickHouse node-log table | `search_node_logs` | No SSH, no `GET /_nodes` round-trip to find an IP, and a `level` column instead of a severity regex |
| Data node | SSH to the node | `get_node_logs` | Those logs are not ingested anywhere |

The link between the two is the master log itself: cluster-level events name the node that
is in trouble, and `get_node_logs(node_id)` resolves that name through
`GET /_nodes/<id>` (IP, log directory, cluster name) and opens the SSH connection itself.
One tool call, no separate lookup.

`analyze_logs` also pulls master logs for its own window automatically and hands them to
the synthesis step — that is the only path that puts master events and per-minute slow-log
spikes in front of the same model, which is what "explain the correlation" needs.

**Master logs are selected by logger, not by level.** The events this exists to surface —
shard relocation, node-left, leader election, allocation failure — are logged at **INFO**
by Elasticsearch, so a `WARN,ERROR` filter drops exactly what matters. But opening INFO is
worse: measured on the real table, 9 of 10 INFO rows were ML maintenance, expired-data
deletion, and mapping-change noise, and during an incident per-shard INFO lines drown the
row cap. So the query is `level IN (WARN,ERROR) OR logger IN (<cluster-event loggers>)` —
coverage goes up and volume goes down.

Two details that only measurement could have found: the stored `logger` values carry
trailing padding (`"o.e.t.TransportService    "`), so comparisons use `trimBoth(logger)`;
and they are the abbreviated form (`o.e.c.r.a.AllocationService`), not the full class name.

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
- A reachable **ClickHouse** instance holding the slow-log / query-log / node-metric /
  node-log tables
- A reachable **Elasticsearch** cluster (the agent's first step is `cluster_health()`)
- An API key for **one** LLM provider — Gemini or NVIDIA NIM, selected by `LLM_PROVIDER`
- Optional: **SSH** credentials for the ES nodes. Without them data-node logs are
  unavailable and that branch of the investigation is skipped
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
| `LLM_PROVIDER` | no | `gemini` | `gemini` or `nvidia_nim`. An unknown value is rejected at startup rather than surfacing as a `KeyError` on the first call. |
| `GEMINI_API_KEY` | only if selected | — | Passed to litellm as a parameter, never in a URL. |
| `GEMINI_MODEL` | no | `gemini-3.5-flash-lite` | |
| `NVIDIA_API_KEY` | only if selected | — | |
| `NVIDIA_MODEL` | no | `google/gemma-4-31b-it` | No entry in litellm's local cost map, so cost tracking reads 0. Does not support `reasoning_effort` — see "Sectioned CoT". |
| `CLICKHOUSE_URL` | yes | — | e.g. `jdbc:clickhouse://host:8123/packetbeat`. The `jdbc:` prefix is optional. **The database in the path must be the one holding the four tables below** — a same-named table in another database is read successfully and returns stale rows, with no error anywhere. |
| `CLICKHOUSE_USER` | no | `default` | |
| `CLICKHOUSE_PASSWORD` | no | `` (empty) | |
| `CLICKHOUSE_SLOWLOG_TABLE` | no | `slowlog_v2` | |
| `CLICKHOUSE_LOG_TABLE` | no | `log` | |
| `CLICKHOUSE_NODE_METRIC_TABLE` | no | `es_node_metric` | |
| `CLICKHOUSE_NODE_LOG_TABLE` | no | `loki_logs` | Current ingestion scope is **master-node logs only**. |
| `ES_HOST` | yes | — | Comma-separated hosts. Rejected at startup if blank. |
| `ES_PORT` | no | `9200` | |
| `ES_USER` | no | `` (empty) | Basic auth is skipped entirely when empty. |
| `ES_PASSWORD` | no | `` (empty) | |
| `SSH_USER` | no | `` (empty) | Used only for data-node logs. |
| `SSH_PASSWORD` | no | `` (empty) | |
| `SSH_PORT` | no | `22` | |
| `KAFKA_BOOTSTRAP_SERVERS` | no | `localhost:9092` | |
| `KAFKA_TOPIC` | no | `slowlog` | |
| `KAFKA_GROUP_ID` | no | `clusterdoctor` | Offsets are keyed by `(group, topic, partition)`, so changing the topic alone is safe — the new topic has no committed offset and `auto_offset_reset=latest` starts at the end. |
| `MICRO_BATCH_SECONDS` | no | `10` | How long to collect arrivals before starting the agent. |
| `REPORT_DIR` | no | `reports` | Where diagnosis HTML files are written. One file per diagnosis. Relative paths resolve against the working directory. |
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
| Master-log lines per `analyze_logs` call | 80 | `.../deepagent/tools.py` (`_MASTER_LOG_MAX_LINES`) | Injected into the synthesis prompt on every call, so this multiplies by up to 6. Narrow because the logger whitelist already restricts what can match. |
| Node-log rows per `search_node_logs` call | 300 default, 2,000 hard cap | `application/port/outbound/log_repository.py` (`DEFAULT_NODE_LOG_LIMIT`, `MAX_NODE_LOG_LIMIT`) | The clamp lives in the port so the tool and the adapter agree on the effective value — otherwise a request for 5,000 returns 2,000 rows and the tool reports no truncation. Rows are `ORDER BY timestamp`, so the **earliest** survive: an incident's onset matters more than its tail. |
| SSH node-log lines | 300 per call, `tail -n 2000` server-side | `infrastructure/outbound/ssh/node_log_fetcher.py` | Opposite bias to the ClickHouse path — `tail` keeps the **latest** lines. |
| LLM output tokens | 1,024 per minute, 8,192 for synthesis | `infrastructure/outbound/llm/langgraph/nodes.py` | |
| LLM request timeout | 120 s | `infrastructure/outbound/llm/litellm_client.py` (`_REQUEST_TIMEOUT_SECONDS`) | |
| ClickHouse send/receive timeout | 30 s | `infrastructure/config/dependencies.py` | Without it a hung ClickHouse could pin a worker thread indefinitely. |

### Retries are disabled on purpose

Both LLM layers have retries turned off — `num_retries=0` for litellm and `max_retries=0`
for `ChatLiteLLM`. (It used to be `1` when the orchestrator ran on
`ChatGoogleGenerativeAI`, because that library reads `0` as "use the Google SDK default".
`ChatLiteLLM` has no such special case, so `0` means what it says.)

Failures on this path are overwhelmingly 429s — a rate limit. Resending the same enormous
prompt cannot succeed, because what failed was the quota, not the request; it only
multiplies consumption. Measured against the Gemini free tier's **250,000 input tokens per
minute**: one 5-minute window costs 513,122 input tokens, and with retries that becomes
2,052,488, or 821 % of the minute's quota. Retries also amplify twice over — the
orchestrator is a tool loop, and a successful run can retrigger up to three times.

> Those numbers are Gemini's. NVIDIA NIM's limits differ and are not documented here.

Because `LlmApiError` carries only the status code (response bodies and request URLs can
contain the API key), a 429 alone does not say *which* limit was hit. So the client logs a
whitelisted set of rate-limit headers before raising:

```
WARNING  LLM provider=nvidia_nim rejected the request with status=429;
         retry-after=40 x-ratelimit-limit-tokens=250000 x-ratelimit-remaining-tokens=0
```

`x-ratelimit-*-tokens` means the prompt is too big; `x-ratelimit-*-requests` means the
calls are too close together. Nothing else from the exception is logged — not the body,
not the URL, not `str(exc)`.

The trade-off of disabling retries is that transient 5xx errors are not retried either.
That is affordable because a failed minute degrades into `[분석 실패]` rather than failing
the run.

### Sectioned CoT

`google/gemma-4-31b-it` does not support `reasoning_effort`, so reasoning cannot be raised
through a thinking budget. The synthesis prompt asks for two blocks instead —
`===추론===` then `===리포트===` — and `_strip_reasoning` keeps only the second, so
operators see the report and not the working. If the delimiter is missing the whole
response is used, with a warning: a report with reasoning mixed in beats an empty one.

Splitting this into two calls (one to reason, one to write) was rejected — it doubles the
input tokens, and that doubling is itself multiplied by retriggers.

## Install

```
uv sync
```

## Run

```
uv run python -m cluster_doctor.main
```

This starts the Kafka consumer and blocks. There is no HTTP port. Logs go to stderr and to
`logs/app.log`; diagnosis reports go to `REPORT_DIR` as HTML.

To exercise it without waiting for a real slow-log, publish a synthetic message:

```
uv run python scripts/produce_test_message.py
```

## Test

```
uv run pytest -q
```

248 tests, ~7 s. No test touches a real LLM, Kafka, ClickHouse, Elasticsearch, or SSH.

## Reports

One HTML file per diagnosis, written by `HtmlFileNotifier`:

```
reports/report-20260910-171103.html
```

The report the model produces is **plain text** (the prompt forbids markdown), in a fixed
shape: `1. 섹션 제목`, a `──` rule, `•` bullets, indented `-` details. The notifier parses
that into headings, a table of contents, nested lists, and `Critical`/`Warning`/`Info`
badges — and always appends the raw text in a `<details>` block, so nothing is lost if the
model breaks format. If no section headings are found at all it renders the raw text only
rather than inventing a structure.

Everything is HTML-escaped. Reports quote ES query sources and log lines verbatim, which
means end-user search terms and `<`/`&` reach the page; the file is also self-contained
(no web fonts, no CDN) so it opens on an air-gapped host, and it carries both light and
dark palettes plus print styles.

A write failure never loses the diagnosis: the full text is logged instead. Characters that
cannot be encoded — an unpaired surrogate from a provider response, say — are replaced at
the door, because otherwise the file write *and* the fallback log both fail.

`reports/` is git-ignored. Reports carry operational index names, query sources, and
company/user identifiers.

## Data sources

`analyze_logs` reads three ClickHouse tables and merges them into one timeline; a fourth
holds node logs and is queried separately.

| Table | Time column | Type | Carries |
|---|---|---|---|
| `slowlog_v2` | `_source.@timestamp` | `DateTime64(9)` (JSON sub-column) | Queries over the ES slow-log threshold: index, node, `took`, hit count, shard count, the query source, and the `x-opaque-id` header holding service/project/company/user |
| `log` | `reg_date` | `DateTime('Asia/Seoul')` | Every ES query execution: host, runtime, success, command, keywords, company, user |
| `es_node_metric` | `reg_date` | `DateTime('Asia/Seoul')` | Per-node CPU, memory, JVM heap, and search/write queue and rejection counts |
| `loki_logs` | `timestamp` | `DateTime64(9, 'Asia/Seoul')` | Node log lines: `node`, `node_role`, `level`, `detected_level`, `logger`, `filename`, `host`, `line`. **Master nodes only, at present.** |

**Timezones are uniformly KST.** The ClickHouse server's `timezone()` is `Asia/Seoul`, and
`slowlog_v2`'s column inherits it by not specifying one. All of them return timezone-aware
KST values, so merging and sorting them is safe.

### Reading node metrics

`os_mem` is `os.mem.used_percent` from `GET _nodes/stats` — **OS memory including the page
cache**. Elasticsearch deliberately uses spare RAM for the filesystem cache, so 95–99 % is
normal, not a symptom. A report once called that out as "메모리 사용률이 95%~99%로 매우
높게 유지됨", which is why the metric is now rendered as `os_mem(캐시포함)=98%` and three
prompts say so explicitly. Real memory pressure is `jvm_heap` together with GC warnings or
`search_rejected`.

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
- **An SSH failure discards the whole report.** `get_node_logs` sets the degraded flag, and
  the analyzer promotes that to `LlmApiError` — so a diagnosis whose slow-log analysis
  finished cleanly is thrown away because a *supplementary* log collection failed, and the
  retrigger is suppressed too. `search_node_logs` deliberately does not do this. The two
  should be reconciled.
- **Zero master-log rows is indistinguishable from "not ingested yet".** The SSH fallback
  inside `analyze_logs` fires only when the ClickHouse query *fails*, because with the
  logger whitelist in place zero rows is the normal outcome for a healthy window — falling
  back on zero would mean an ES round-trip plus a fresh SSH connection on every one of up
  to six calls. The cost is that an ingestion gap reads as "no master-side events".
- **Data-node logs depend on SSH being configured.** Nothing warns at startup if
  `SSH_USER` is empty; the gap only shows when the agent reaches that step.
- **The tool set and prompts have not been exercised against a real LLM since the node-log
  work landed.** Unit tests invoke the tools directly, which does not prove the model
  selects them or fills their arguments correctly.
