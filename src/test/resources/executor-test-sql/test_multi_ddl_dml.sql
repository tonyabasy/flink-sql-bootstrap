-- 混合 DDL + SET + DML 脚本
SET 'table.exec.mini-batch.enabled' = 'false';

CREATE TEMPORARY TABLE source_table (
    id   BIGINT,
    name STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);

CREATE TEMPORARY TABLE sink_table (
    name  STRING,
    cnt   BIGINT
) WITH (
    'connector' = 'print'
);

INSERT INTO sink_table
SELECT name, COUNT(*) AS cnt
FROM source_table
GROUP BY name;
