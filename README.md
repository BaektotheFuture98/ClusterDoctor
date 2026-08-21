# ClusterDoctor

ClusterDoctor is a diagnosis service for Elasticsearch/infrastructure operators. It pulls
recent slow-log, query-log, and node-metric rows out of ClickHouse for a requested time
window, hands them to an LLM, and returns a natural-language diagnosis of what was
happening in the cluster during that window.

Two LLM providers are supported behind one adapter (`litellm`): **Google Gemini** and
**NVIDIA Build**. Pick one with `LLM_PROVIDER`; only that provider's API key is required.

## Requirements

- Python >= 3.13
- [uv](https://docs.astral.sh/uv/)
- A reachable ClickHouse instance holding the slow-log / query-log / node-metric tables
- An API key for whichever provider `LLM_PROVIDER` selects (Gemini or NVIDIA Build)
- Network access to `openaipublic.blob.core.windows.net` on the very first boot --
  see "Deployment notes" below before deploying to an air-gapped network

## Configuration

Settings are read from environment variables (or a `.env` file in the project root) by
`src/cluster_doctor/config/settings.py`. Copy `.env.example` to `.env` and fill in real
values -- never commit real keys.

Exactly one provider is active at a time, and **only the selected provider's key is
required**. Setting `LLM_PROVIDER=nvidia` with no `GEMINI_API_KEY` at all boots fine, and
vice versa. Selecting a provider whose key is empty fails the boot.

| Variable | Required | Default | Notes |
|---|---|---|---|
| `CLICKHOUSE_URL` | yes | -- | e.g. `jdbc:clickhouse://host:8123/default`. The `jdbc:` prefix is optional. |
| `LLM_PROVIDER` | no | `gemini` | `gemini` or `nvidia`. Any other value fails validation. |
| `GEMINI_API_KEY` | if selected | -- | Passed to litellm as a parameter, never in a URL. |
| `GEMINI_MODEL` | no | `gemini-2.5-flash` | |
| `NVIDIA_API_KEY` | if selected | -- | NVIDIA Build key. Same handling as above. |
| `NVIDIA_MODEL` | no | `meta/llama-3.3-70b-instruct` | |
| `GEMINI_BASE_URL` | no | `https://generativelanguage.googleapis.com/v1beta` | **Currently unused** -- litellm builds the endpoint itself. Kept for backward compatibility. |
| `ANALYSIS_MODE` | no | `single` | `single` or `graph`. Orthogonal to `LLM_PROVIDER` -- see "Analysis modes" below. |
| `LITELLM_LOCAL_MODEL_COST_MAP` | no | `True` | Set by the adapter module at import time. Stops litellm fetching cost data from GitHub on import. |
| `CLICKHOUSE_USER` | no | `default` | |
| `CLICKHOUSE_PASSWORD` | no | `` (empty) | |
| `CLICKHOUSE_SLOWLOG_TABLE` | no | `slowlog_v2` | |
| `CLICKHOUSE_LOG_TABLE` | no | `log` | |
| `CLICKHOUSE_NODE_METRIC_TABLE` | no | `es_node_metric` | |

Startup fails loudly (the process exits) if a required variable is missing, rather than
surfacing the failure on the first request. The startup error names only *which* settings
are missing or invalid -- never their values, so a config failure cannot write
`CLICKHOUSE_PASSWORD` into an aggregated log store.

### Fixed limits

These are compile-time constants, not environment variables. Both can silently drop or
fail work, so they are documented here for operators reading logs.

| Limit | Value | Defined in | Notes |
|---|---|---|---|
| Query window | 10 minutes | `domain/model/time_range.py` (`MAX_TIME_RANGE_DURATION`) | A longer `[from, to)` window is rejected with `400` before any query runs. Was 1 hour; tightened because `graph` mode issues one LLM call per non-empty minute. See "Time window limit" below. |
| Rows per minute-segment per source | 10,000 | `adapter/outbound/clickhouse/clickhouse_log_adapter.py` (`_MAX_ROWS_PER_SEGMENT_PER_SOURCE`) | Applied as `LIMIT` on each of the three per-source queries, for each one-minute segment of the window. There is no `ORDER BY`, so on a hit ClickHouse returns an arbitrary subset -- and the count reported to the LLM is the capped one, not the true total. A `WARNING` naming the source and the segment is logged whenever a query comes back at the cap. |
| ClickHouse send/receive timeout | 30 seconds | `config/dependencies.py` (`_CLICKHOUSE_SEND_RECEIVE_TIMEOUT_SECONDS`) | Without it a hung ClickHouse could pin a worker thread indefinitely. A query exceeding it fails the request rather than returning partial results. |

## Install

```
uv sync
```

## Run

```
uv run python -m cluster_doctor.main
```

This starts uvicorn on `0.0.0.0:8082`. Importing the module (e.g. in tests) does not start
a server -- only running it as `__main__` does.

Equivalently, once dependencies are installed, `uv run uvicorn cluster_doctor.main:app
--host 0.0.0.0 --port 8082` starts the same app.

## Test

```
uv run pytest -v
```

## Analysis modes

`ANALYSIS_MODE` decides *how* the logs are put to the LLM. It is orthogonal to
`LLM_PROVIDER`, which decides *who* is asked -- `graph` mode uses the same provider and
key underneath.

| Mode | Calls per request | Behaviour |
|---|---|---|
| `single` (default) | 1 | Every log line goes into one prompt. |
| `graph` | 1 per non-empty minute, plus 1 | Each minute is analysed on its own, then the per-minute results are synthesised into the final report. |

**Why `graph` exists.** `single` mode samples at most 200 entries per source
(`prompt_builder.py`), and the ClickHouse adapter hands over logs sorted newest-first. So
the sampling keeps the *most recent* 200 and silently drops everything older. For a source
producing 200 entries a minute, a 10-minute window reaches the model as roughly its last
minute -- and the prompt header still reads "총 2000건 중 200건 샘플", so the model believes
it saw a representative sample rather than one slice.

`graph` mode removes that loss structurally: a single minute's logs sit below the cap, so
each minute is passed in full. The synthesis step then receives the per-minute summaries
plus the evidence lines each minute selected -- never the raw logs again, which would
reintroduce the same truncation.

Costs and caveats, all measured:

- Minute analyses run **concurrently** -- LangGraph's synchronous `invoke` executes
  fan-out branches in threads, so wall-clock stays close to a single call rather than
  multiplying by the number of minutes.
- Empty minutes are skipped entirely; they cost nothing.
- A single minute failing (rate limit, filtered response) does not sink the request. That
  minute is marked `[분석 실패]` and the synthesis prompt is told about it, so the gap
  appears in the report instead of being hidden. If *every* minute fails, the request
  fails rather than returning a report that reads like "nothing wrong".
- `langgraph` pulls in 15 packages (`langchain-core`, `langsmith`, `orjson`, ...).

## Deployment notes

Both items below were measured against a real install, not inferred from documentation.

### First boot on an air-gapped network

`import litellm` fetches the tiktoken `cl100k_base` encoding from
`openaipublic.blob.core.windows.net`. **That request has no exception handling.** With a
cold cache and no route to that host, the import stalls for roughly 19.5 seconds and then
dies with an uncaught `ProxyError` -- so the app does not start at all, and the failure
looks nothing like a configuration problem.

If you build a container image, warm the cache at build time:

    RUN python -c "import litellm"

If you deploy without an image, the first boot must have access to that host. The cache
persists on disk, so later boots do not need the network.

`LITELLM_LOCAL_MODEL_COST_MAP=True` removes a *separate* call to GitHub for cost data. It
does not help with the tiktoken fetch above.

### Trimming image size (optional)

litellm ships roughly 39MB this service never touches. These can be deleted at container
build time; Gemini and NVIDIA calls were confirmed still working afterwards:

    RUN rm -rf /path/to/site-packages/litellm/proxy/_experimental/out \
               /path/to/site-packages/litellm/proxy/swagger \
               /path/to/site-packages/litellm/rust_bridge/_native.pyd || true

The `|| true` is deliberate: if litellm reorganises its directory layout, this step should
quietly become a no-op and leave the image at full size rather than **breaking the build**.

Do not delete the rest of `litellm/proxy/`. It looks like admin-only tooling, but real
`completion()` calls route through it.

This saves disk only -- startup time is unchanged, since those files are not on the import
path.

## Time window limit

A diagnosis request's `[from, to)` window may not exceed **10 minutes**. A longer window is
rejected with `400` before any per-source ClickHouse log query is issued -- confirmed via
the app's `TestClient` with an 11-minute window:

```
POST /api/v1/diagnosis
{"from": "2026-08-20T02:00:00", "to": "2026-08-20T02:10:01"}

400
{"error":"조회 기간은 최대 0:10:00를 초과할 수 없습니다"}
```

## Endpoints

Both endpoints accept the same request body: `from` and `to` are ISO-8601 timestamps
(the window must be <= 10 minutes), and `model` optionally overrides the selected provider's
default model (`GEMINI_MODEL` / `NVIDIA_MODEL`) for that
one request. The examples below use `curl` against a running instance (see "Run" above);
the request/response shapes shown were captured by exercising the app in-process (the
same way the test suite does) with a mocked use case, since this sandbox has no reachable
ClickHouse/LLM backend to run a fully live end-to-end request against.

### `POST /api/v1/diagnosis`

Returns the diagnosis as JSON.

```
$ curl -s -X POST http://localhost:8082/api/v1/diagnosis \
    -H "Content-Type: application/json" \
    -d '{"from": "2026-08-20T02:09:00", "to": "2026-08-20T02:10:00"}'

{
  "from": "2026-08-20T02:09:00",
  "to": "2026-08-20T02:10:00",
  "analyzedAt": "2026-08-20T02:10:05",
  "totalLogs": 416,
  "logLevelCounts": {"SUCCESS": 308, "FAIL": 1, "METRIC": 107},
  "report": "진단 결과 텍스트"
}
```

### `POST /api/v1/diagnosis/text`

Same inputs, returns just the diagnosis text as `text/plain`.

```
$ curl -s -X POST http://localhost:8082/api/v1/diagnosis/text \
    -H "Content-Type: application/json" \
    -d '{"from": "2026-08-20T02:09:00", "to": "2026-08-20T02:10:00"}'

진단 결과 텍스트
```

Interactive API docs (Swagger UI) are available at `/docs` while the app is running, and
the raw OpenAPI schema at `/openapi.json` -- both confirmed by booting the app locally.
