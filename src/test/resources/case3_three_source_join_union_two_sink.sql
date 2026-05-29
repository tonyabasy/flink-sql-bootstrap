-- Case 3: 三 Source + Join + Union + Sink 到两个地方
CREATE TABLE source1 (
    id BIGINT,
    name STRING,
    proctime AS PROCTIME()
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);
CREATE TABLE source2 (
    id BIGINT,
    score DOUBLE,
    proctime AS PROCTIME()
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);
CREATE TABLE source3 (
    name STRING,
    score DOUBLE
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);
CREATE TABLE sink1 (
    name STRING,
    score DOUBLE
) WITH (
    'connector' = 'blackhole'
);
CREATE TABLE sink2 (
    name STRING,
    score DOUBLE,
    id BIGINT
) WITH (
    'connector' = 'blackhole'
);
EXECUTE STATEMENT SET
BEGIN
    INSERT INTO sink1
        SELECT name, score FROM (
            SELECT s1.name, s2.score FROM source1 s1 JOIN source2 s2 ON s1.id = s2.id
            UNION ALL
            SELECT name, score FROM source3
        );
    INSERT INTO sink2
        SELECT name, score, id FROM (
            SELECT s1.name, s2.score, s1.id FROM source1 s1 JOIN source2 s2 ON s1.id = s2.id
            UNION ALL
            SELECT name, score, CAST(NULL AS BIGINT) FROM source3
        );
END;
