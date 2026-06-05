-- datagen 自动生成句子 → 分词 → 分组计数 → print 输出
-- 本脚本自包含，无需外部 Catalog、资源或 UDF 依赖

CREATE TEMPORARY TABLE source_table (
  sentence STRING
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '100'
);

CREATE TEMPORARY TABLE sink_table (
  word STRING,
  cnt BIGINT
) WITH (
  'connector' = 'print'
);

INSERT INTO sink_table
SELECT word, COUNT(*) AS cnt
FROM source_table
CROSS JOIN UNNEST(SPLIT(sentence, ' ')) AS t(word)
GROUP BY word;