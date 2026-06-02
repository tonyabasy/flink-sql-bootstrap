-- 最简单的 Word Count：datagen 生成句子 → 按单词 split → 分组计数 → print
INSERT INTO sink_table
SELECT my_reverse(my_substring(word,0,5)) as word, COUNT(*) AS cnt
FROM source_table
CROSS JOIN UNNEST(SPLIT(sentence, ' ')) AS t(word)
GROUP BY word;