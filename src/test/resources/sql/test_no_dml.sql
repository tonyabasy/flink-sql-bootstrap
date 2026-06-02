-- 只有 DDL，没有 DML

CREATE TEMPORARY TABLE t1 (
    id   BIGINT,
    name STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);

CREATE TEMPORARY TABLE t2 (
    name STRING,
    cnt  BIGINT
) WITH (
    'connector' = 'print'
);
