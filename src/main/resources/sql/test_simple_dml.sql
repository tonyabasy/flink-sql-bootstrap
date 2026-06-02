-- 最简单的 Word Count：datagen → print
CREATE TEMPORARY TABLE source_table (
    sentence STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);

CREATE TEMPORARY TABLE sink_table (
    word   STRING,
    cnt    BIGINT
) WITH (
    'connector' = 'print'
);

INSERT INTO sink_table
SELECT word, COUNT(*) AS cnt
FROM source_table
CROSS JOIN UNNEST(SPLIT(sentence, ' ')) AS t(word)
GROUP BY word;
