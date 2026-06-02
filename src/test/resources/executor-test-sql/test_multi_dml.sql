-- 两条 DML，应该抛异常

CREATE TEMPORARY TABLE source_table (
    id   BIGINT,
    name STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);

CREATE TEMPORARY TABLE sink_a (
    name STRING,
    cnt  BIGINT
) WITH (
    'connector' = 'print'
);

CREATE TEMPORARY TABLE sink_b (
    id  BIGINT
) WITH (
    'connector' = 'print'
);

INSERT INTO sink_a
SELECT name, COUNT(*) AS cnt
FROM source_table
GROUP BY name;

INSERT INTO sink_b
SELECT id
FROM source_table;
