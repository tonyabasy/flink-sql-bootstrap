-- 注释和引号测试：注释内的分号不应触发切分
/*
 * 多行注释中的分号;
 * 不应该被切分
 */
CREATE TEMPORARY TABLE source_table (
    id   BIGINT,
    name STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);

-- 单行注释中的分号; 也不应被切分

CREATE TEMPORARY TABLE sink_table (
    name  STRING,
    cnt   BIGINT
) WITH (
    'connector' = 'print'
);

INSERT INTO sink_table
SELECT name, COUNT(*) AS cnt
FROM source_table
-- WHERE name <> 'Alice;Bob'
GROUP BY name;
