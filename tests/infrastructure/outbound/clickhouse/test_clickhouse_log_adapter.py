import logging
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock

from cluster_doctor.infrastructure.outbound.clickhouse.clickhouse_log_adapter import (
    _MAX_ROWS_PER_SEGMENT_PER_SOURCE,
    ClickHouseLogAdapter,
    _split_by_minute,
)
from cluster_doctor.domain.model.clickhouse.query_log_entry import QueryLogEntry
from cluster_doctor.domain.model.kafka.slowlog_entry import SlowlogEntry
from cluster_doctor.domain.model.clickhouse.node_metric_entry import NodeMetricEntry
from cluster_doctor.contracts.time_range import TimeRange

# ``TimeRange``??naive datetime??嫄곕??쒕떎. 援ш컙??留뚮뱾?댁쭊 ?먮━?먯꽌 嫄곗젅?섏?
# ?딆쑝硫??쒖갭 ?ㅼ쓽 李⑥쭛???곗닔?먯꽌 ?곗?湲??뚮Ц?대떎. ???뚯씪???쒓컖???꾨?
# aware?ъ빞 ?섍퀬, ?댁쁺?먯꽌 ClickHouse媛 ?뚮젮二쇰뒗 媛믩룄 洹몃젃??
KST = timezone(timedelta(hours=9))

TR       = TimeRange(start=datetime(2026, 8, 20, 2, 9, 0, tzinfo=KST), end=datetime(2026, 8, 20, 2, 10, 0, tzinfo=KST))
TR_MULTI = TimeRange(start=datetime(2026, 8, 20, 2, 9, 30, tzinfo=KST), end=datetime(2026, 8, 20, 2, 11, 15, tzinfo=KST))

# slowlog row 怨꾩빟: ?ㅼ젣 ?쒕쾭?먯꽌 ?뺤씤???쒖꽌쨌??낆씠??
#   0=諛쒖깮 ?쒓컖(_source.`@timestamp`, aware), 1=?몃뜳?ㅻ챸, 2=?몃뱶紐? 3=took,
#   4=total_hits, 5=total_shards(int), 6=x-opaque-id, 7=荑쇰━ ?먮Ц
SLOWLOG_ROW = (
    datetime(2026, 8, 20, 2, 9, 5, tzinfo=KST),
    "app_index_v1_20250721",
    "node-a01",
    "32.4s",
    "68 hits",
    902,
    "service=web,project=search_app,env=prod,company=1,user=2,action=count",
    '{"size":0,"query":{"query_string":{"query":"?앹꽑"}}}',
)


def _make_client(slowlog_rows=None, query_rows=None, metric_rows=None):
    client = MagicMock()

    def side_effect(query, parameters=None):
        result = MagicMock()
        q = query.lower()
        if "from slowlog_v2" in q:
            result.result_rows = slowlog_rows or []
        elif "from es_node_metric" in q:
            result.result_rows = metric_rows or []
        elif "from log " in q:
            result.result_rows = query_rows or []
        else:
            raise AssertionError(f"unrecognized query in test double: {query!r}")
        return result

    client.query.side_effect = side_effect
    return client


def test_split_exact_one_minute():
    segs = _split_by_minute(TR)
    assert len(segs) == 1
    assert segs[0].start == datetime(2026, 8, 20, 2, 9, 0, tzinfo=KST)
    assert segs[0].end   == datetime(2026, 8, 20, 2, 10, 0, tzinfo=KST)


def test_split_crosses_two_boundaries():
    segs = _split_by_minute(TR_MULTI)
    assert len(segs) == 3
    assert segs[0].start == datetime(2026, 8, 20, 2, 9, 30, tzinfo=KST)
    assert segs[0].end   == datetime(2026, 8, 20, 2, 10, 0, tzinfo=KST)
    assert segs[1].start == datetime(2026, 8, 20, 2, 10, 0, tzinfo=KST)
    assert segs[1].end   == datetime(2026, 8, 20, 2, 11, 0, tzinfo=KST)
    assert segs[2].start == datetime(2026, 8, 20, 2, 11, 0, tzinfo=KST)
    assert segs[2].end   == datetime(2026, 8, 20, 2, 11, 15, tzinfo=KST)


def test_fetch_logs_maps_slowlog():
    client = _make_client(slowlog_rows=[SLOWLOG_ROW])
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric", "es_node_log")
    sl      = [l for l in adapter.fetch_logs(TR) if l.source == "slowlog"]

    assert len(sl) == 1
    e = sl[0]
    assert isinstance(e, SlowlogEntry)
    assert e.timestamp    == datetime(2026, 8, 20, 2, 9, 5, tzinfo=KST)
    assert e.index_name   == "app_index_v1_20250721"
    assert e.node         == "node-a01"
    assert e.took         == "32.4s"       # ?먮┛ ?뺣룄
    assert e.total_hits   == "68 hits"     # 寃곌낵??
    assert e.total_shards == 902           # 議고쉶???ㅻ뱶 ????int濡??⑤뒗??
    assert "company=1"    in e.opaque_id   # company/user 洹??
    assert "?앹꽑"          in e.query       # 荑쇰━ ?먮Ц


def test_slowlog_projects_named_subcolumns_instead_of_the_whole_source():
    # _source瑜??듭㎏濡?媛?몄삤硫???媛吏媛 ?숈떆??源⑥쭊??
    #  1) clickhouse-connect??JSON ??낆쓣 dict濡??뚮젮以??-> LogEntry.message??
    #     str 怨꾩빟??源⑥?怨??꾨＼?꾪듃??dict repr???ㅻ┛??
    #  2) host.mac/agent.ephemeral_id/host.os.kernel 媛숈? 吏꾨떒怨?臾닿????꾨뱶媛
    #     ?됰떦 2.7KB 以?73%瑜?李⑥??쒕떎(?ㅼ륫).
    # ?뚯뒪???붾툝? ?대뒓 履쎈룄 ?ы쁽?섏? 紐삵븯誘濡?SQL???ъ쁺??吏곸젒 寃利앺븳??
    client  = _make_client()
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric", "es_node_log")
    adapter.fetch_logs(TR)

    sql = next(
        c.args[0] for c in client.query.call_args_list if "slowlog_v2" in c.args[0].lower()
    )
    select = sql.lower().split("from", 1)[0]
    assert "_source.elasticsearch.slowlog.took" in select
    assert "_source.elasticsearch.index.name" in select
    # ?듭㎏ ?ъ쁺(SELECT ... _source, ... / SELECT _source FROM)???⑥븘 ?덉쑝硫????쒕떎.
    assert not re.search(r"[\s,]_source\s*(,|$)", select), select


def test_slowlog_is_filtered_by_occurrence_time_not_ingestion_time():
    # ch_ingested_at? ClickHouse ?곸옱 ?쒓컖?대떎. ?ㅼ륫 吏?곗씠 23~41珥덈씪
    # 遺?寃쎄퀎瑜??섍린硫??몃━嫄곕? ?좊컻??洹?slowlog媛 議고쉶 援ш컙?먯꽌 鍮좎?怨?
    # 遺??⑥쐞 踰꾪궥???쒓컖 ?쇰꺼???듭㎏濡?諛由곕떎.
    client  = _make_client()
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric", "es_node_log")
    adapter.fetch_logs(TR)

    sql = next(
        c.args[0] for c in client.query.call_args_list if "slowlog_v2" in c.args[0].lower()
    )
    where = sql.lower().split("where", 1)[1]
    assert "@timestamp" in where
    assert "ch_ingested_at" not in where


QUERY_ROW = (
    datetime(2026, 8, 20, 2, 9, 10, tzinfo=KST), "host1", Decimal("0.5"), "Y", "GET",
    "svc", "prod", "proj", "cls1", ["kwd", "kwd2"], "acme", "alice",
)


def test_fetch_logs_maps_query_log_success():
    client = _make_client(query_rows=[QUERY_ROW])
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric", "es_node_log")
    ql      = [l for l in adapter.fetch_logs(TR) if l.source == "es_query_log"]

    assert len(ql) == 1
    e = ql[0]
    assert isinstance(e, QueryLogEntry)
    assert e.success  is True
    assert e.service  == "svc"
    assert e.host     == "host1"
    assert e.cmd      == "GET"
    assert e.project  == "proj"
    assert e.run_time == Decimal("0.5")     # Decimal 洹몃?濡? 臾몄옄?댁씠 ?꾨땲??
    assert e.company  == "acme"
    assert e.user     == "alice"


def test_query_log_keywords_become_a_hashable_tuple():
    # ClickHouse??list瑜?以?? frozen dataclass?먯꽌 list ?꾨뱶???댁떆瑜?源⑤쑉由곕떎.
    client = _make_client(query_rows=[QUERY_ROW])
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric", "es_node_log")
    e = [l for l in adapter.fetch_logs(TR) if l.source == "es_query_log"][0]

    assert e.keywords == ("kwd", "kwd2")
    assert hash(e) is not None


def test_fetch_logs_maps_query_log_fail():
    row    = (*QUERY_ROW[:3], "N", *QUERY_ROW[4:])
    client = _make_client(query_rows=[row])
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric", "es_node_log")
    ql      = [l for l in adapter.fetch_logs(TR) if l.source == "es_query_log"]
    assert ql[0].success is False


def test_fetch_logs_maps_node_metric():
    row    = (datetime(2026, 8, 20, 2, 9, 0, tzinfo=KST), "node1", "10.0.0.1", 30, 60, 15, 70, 2, 0, 0, 1, 0, 0)
    client = _make_client(metric_rows=[row])
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric", "es_node_log")
    metrics = [l for l in adapter.fetch_logs(TR) if l.source == "node_metric"]

    assert len(metrics) == 1
    e = metrics[0]
    assert isinstance(e, NodeMetricEntry)
    assert e.node_name             == "node1"
    assert e.node_ip               == "10.0.0.1"
    assert e.os_cpu_percent        == 30      # int濡??⑥븘 ?꾧퀎移?鍮꾧탳媛 媛?ν븯??
    assert e.jvm_heap_used_percent == 70
    assert e.search_active         == 2
    assert e.write_active          == 1


def test_fetch_logs_queries_each_source_per_minute_segment():
    client  = _make_client()
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric", "es_node_log")
    adapter.fetch_logs(TR_MULTI)
    assert client.query.call_count == 9


def test_fetch_logs_slowlog_query_has_limit():
    client  = _make_client()
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric", "es_node_log")
    adapter.fetch_logs(TR)
    slowlog_calls = [
        call for call in client.query.call_args_list if "slowlog_v2" in call.args[0].lower()
    ]
    assert slowlog_calls
    for call in slowlog_calls:
        assert f"LIMIT {_MAX_ROWS_PER_SEGMENT_PER_SOURCE}" in call.args[0]


def test_fetch_logs_query_log_query_has_limit():
    client  = _make_client()
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric", "es_node_log")
    adapter.fetch_logs(TR)
    log_calls = [
        call for call in client.query.call_args_list if "from log " in call.args[0].lower()
    ]
    assert log_calls
    for call in log_calls:
        assert f"LIMIT {_MAX_ROWS_PER_SEGMENT_PER_SOURCE}" in call.args[0]


def test_fetch_logs_node_metric_query_has_limit():
    client  = _make_client()
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric", "es_node_log")
    adapter.fetch_logs(TR)
    metric_calls = [
        call for call in client.query.call_args_list if "es_node_metric" in call.args[0].lower()
    ]
    assert metric_calls
    for call in metric_calls:
        assert f"LIMIT {_MAX_ROWS_PER_SEGMENT_PER_SOURCE}" in call.args[0]


def _slowlog_rows(count):
    return [SLOWLOG_ROW] * count


def test_warns_when_a_segment_query_returns_exactly_the_limit(caplog):
    # The LIMIT has no ORDER BY, so hitting the cap means ClickHouse dropped
    # an arbitrary subset with no signal in the result -- and _build_prompt
    # then reports the capped count to the model as if it were the total.
    client  = _make_client(slowlog_rows=_slowlog_rows(_MAX_ROWS_PER_SEGMENT_PER_SOURCE))
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric", "es_node_log")

    with caplog.at_level(logging.WARNING):
        adapter.fetch_logs(TR)

    truncation_warnings = [
        r for r in caplog.records
        if r.levelno == logging.WARNING and "LIMIT" in r.getMessage()
    ]
    assert len(truncation_warnings) == 1, [r.getMessage() for r in caplog.records]
    message = truncation_warnings[0].getMessage()
    # Operators need to know *which* source and *which* segment truncated;
    # a bare "truncation happened" line is not actionable.
    assert "slowlog" in message
    assert str(_MAX_ROWS_PER_SEGMENT_PER_SOURCE) in message
    assert TR.start.isoformat() in message
    assert TR.end.isoformat() in message


def test_does_not_warn_when_a_segment_query_stays_below_the_limit(caplog):
    client  = _make_client(slowlog_rows=_slowlog_rows(_MAX_ROWS_PER_SEGMENT_PER_SOURCE - 1))
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric", "es_node_log")

    with caplog.at_level(logging.WARNING):
        adapter.fetch_logs(TR)

    assert [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING] == []


def test_warns_once_per_truncated_segment_and_source(caplog):
    # TR_MULTI spans three one-minute segments; every slowlog segment
    # truncates, so each must be reported separately -- one aggregate
    # warning would hide which minute of the window is affected.
    client  = _make_client(slowlog_rows=_slowlog_rows(_MAX_ROWS_PER_SEGMENT_PER_SOURCE))
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric", "es_node_log")

    with caplog.at_level(logging.WARNING):
        adapter.fetch_logs(TR_MULTI)

    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 3, warnings
    assert len(set(warnings)) == 3, "each warning must name its own segment"


def test_fetch_logs_sorted_descending():
    t2     = datetime(2026, 8, 20, 2, 9, 30, tzinfo=KST)
    client = _make_client(
        slowlog_rows=[SLOWLOG_ROW],
        query_rows=[(t2, "h", Decimal("0.1"), "Y", "GET", "s", "e", "p", "c", ["k"], None, None)],
    )
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric", "es_node_log")
    logs    = adapter.fetch_logs(TR)
    times   = [l.timestamp for l in logs]
    assert times == sorted(times, reverse=True)

