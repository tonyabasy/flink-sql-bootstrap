-- 语法错误

CREATE TEMPORARY TABLE source_table (
    id   BIGINT,
    name STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);

-- 这行的语法是错的
QELEEEECT * FROM source_table;
