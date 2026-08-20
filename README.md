# ClusterDoctor

ClusterDoctor is a diagnosis service for Elasticsearch/infrastructure operators. It pulls
recent slow-log, query-log, and node-metric rows out of ClickHouse for a requested time
window, hands them to a Gemini LLM, and returns a natural-language diagnosis of what was
happening in the cluster during that window.

## Requirements

- Python >= 3.13
- [uv](https://docs.astral.sh/uv/)
- A reachable ClickHouse instance holding the slow-log / query-log / node-metric tables
- A Gemini API key

## Configuration

Settings are read from environment variables (or a `.env` file in the project root) by
`src/cluster_doctor/config/settings.py`. Copy `.env.example` to `.env` and fill in real
values -- never commit real keys.

| Variable | Required | Default | Notes |
|---|---|---|---|
| `GEMINI_API_KEY` | yes | -- | Sent as a header, never in the URL. |
| `CLICKHOUSE_URL` | yes | -- | e.g. `jdbc:clickhouse://host:8123/default`. The `jdbc:` prefix is optional. |
| `GEMINI_MODEL` | no | `gemini-2.5-flash` | |
| `GEMINI_BASE_URL` | no | `https://generativelanguage.googleapis.com/v1beta` | |
| `CLICKHOUSE_USER` | no | `default` | |
| `CLICKHOUSE_PASSWORD` | no | `` (empty) | |
| `CLICKHOUSE_SLOWLOG_TABLE` | no | `slowlog_v2` | |
| `CLICKHOUSE_LOG_TABLE` | no | `log` | |
| `CLICKHOUSE_NODE_METRIC_TABLE` | no | `es_node_metric` | |

Startup fails loudly (the process exits) if a required variable is missing, rather than
surfacing the failure on the first request.

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

## Time window limit

A diagnosis request's `[from, to)` window may not exceed **1 hour**. A longer window is
rejected with `400` before any per-source ClickHouse log query is issued -- confirmed via
the app's `TestClient` with a 61-minute window:

```
POST /api/v1/diagnosis
{"from": "2026-08-20T02:00:00", "to": "2026-08-20T03:00:01"}

400
{"error":"조회 기간은 최대 1:00:00를 초과할 수 없습니다"}
```

## Endpoints

Both endpoints accept the same request body: `from` and `to` are ISO-8601 timestamps
(the window must be <= 1 hour), and `model` optionally overrides `GEMINI_MODEL` for that
one request. The examples below use `curl` against a running instance (see "Run" above);
the request/response shapes shown were captured by exercising the app in-process (the
same way the test suite does) with a mocked use case, since this sandbox has no reachable
ClickHouse/Gemini backend to run a fully live end-to-end request against.

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
