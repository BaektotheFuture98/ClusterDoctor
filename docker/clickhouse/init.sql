CREATE DATABASE IF NOT EXISTS packetbeat;
USE packetbeat;

CREATE TABLE IF NOT EXISTS slowlog_v2
(
    _source Tuple(
        `@timestamp` DateTime64(3, 'Asia/Seoul'),
        elasticsearch Tuple(
            `index` Tuple(name String),
            node Tuple(name String),
            slowlog Tuple(
                took String,
                total_hits String,
                total_shards UInt32,
                id String,
                source String
            )
        )
    )
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS log
(
    reg_date DateTime64(3, 'Asia/Seoul'),
    host String,
    run_time Float64,
    success String,
    cmd String,
    service String,
    env String,
    project String,
    cluster String,
    keyword Array(String),
    company String,
    user String
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS es_node_metric
(
    reg_date DateTime64(3, 'Asia/Seoul'),
    node_name String,
    node_ip String,
    os_cpu_percent UInt32,
    os_mem_used_percent UInt32,
    process_cpu_percent UInt32,
    jvm_heap_used_percent UInt32,
    search_active UInt32,
    search_queue UInt32,
    search_rejected UInt32,
    write_active UInt32,
    write_queue UInt32,
    write_rejected UInt32
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS loki_logs
(
    timestamp DateTime64(3, 'Asia/Seoul'),
    node String,
    node_role String,
    level String,
    detected_level String,
    logger String,
    filename String,
    host String,
    line String
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO slowlog_v2 VALUES
(
    (
        '2026-01-15 10:00:00.000',
        (
            ('orders-v1'),
            ('es-data-01'),
            (
                '8.2s',
                '250 hits',
                12,
                'service=web,project=compose-test,env=local,company=test,user=test',
                '{"size":10,"query":{"match":{"customer_id":"fixture"}}}'
            )
        )
    )
);

INSERT INTO log VALUES
(
    '2026-01-15 10:00:01.000',
    'es-data-01',
    8.2,
    'N',
    'GET /orders-v1/_search',
    'checkout',
    'local',
    'compose-test',
    'elasticsearch',
    ['customer_id', 'orders'],
    'test',
    'test'
);

INSERT INTO es_node_metric VALUES
(
    '2026-01-15 10:00:02.000',
    'es-data-01',
    '127.0.0.1',
    82,
    91,
    76,
    92,
    4,
    180,
    3,
    0,
    0,
    0
);
