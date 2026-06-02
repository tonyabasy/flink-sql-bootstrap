-- Case 2: 双 Source + Union + Sink
CREATE TABLE source1 (
    id BIGINT,
    name STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);
CREATE TABLE source2 (
    id BIGINT,
    name STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);
CREATE TABLE sink (
    id BIGINT,
    name STRING
) WITH (
    'connector' = 'blackhole'
);
INSERT INTO sink
    SELECT * FROM source1
    UNION ALL
    SELECT * FROM source2;
