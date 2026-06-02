-- 最简单的 Word Count：datagen 生成句子 → 按单词 split → 分组计数 → print
INSERT INTO dws_word_count
SELECT my_reverse(my_substring(word,0,2)) as word, COUNT(*) AS cnt
FROM ods_words
CROSS JOIN UNNEST(SPLIT(sentence, ' ')) AS t(word)
GROUP BY my_reverse(my_substring(word,0,2));