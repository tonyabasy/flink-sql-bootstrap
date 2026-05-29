-- Case 1: 双 Source + Join + Sink 到两个地方
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
CREATE TABLE sink1 (
    id BIGINT,
    name STRING,
    score DOUBLE
) WITH (
    'connector' = 'blackhole'
);
CREATE TABLE sink2 (
    name STRING,
    score DOUBLE
) WITH (
    'connector' = 'blackhole'
);
EXECUTE STATEMENT SET
BEGIN
    INSERT INTO sink1
        SELECT s1.id, s1.name, s2.score
        FROM source1 s1 JOIN source2 s2 ON s1.id = s2.id;
    INSERT INTO sink2
        SELECT s1.name, s2.score
        FROM source1 s1 JOIN source2 s2 ON s1.id = s2.id;
END;
