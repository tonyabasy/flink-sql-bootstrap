/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.lanting.flink.sql.bootstrap.catalog;

import com.lanting.flink.sql.bootstrap.util.JSON;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.types.AbstractDataType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * 将 {@link CatalogEntity} 转换为 Flink 可直接使用的
 * {@link GenericInMemoryCatalog}。
 */
public final class CatalogEntityFactory {

    private CatalogEntityFactory() {
    }


    public static GenericInMemoryCatalog from(String catalogStr) throws JsonProcessingException {
        CatalogEntity snapshot = JSON.parseObject(catalogStr, CatalogEntity.class);
        return from(snapshot);
    }

    /**
     * 直接从 {@link CatalogEntity} 构建内存 Catalog。
     */
    public static GenericInMemoryCatalog from(CatalogEntity entity) {
        GenericInMemoryCatalog catalog = new GenericInMemoryCatalog(
                entity.getCatalogName(),
                entity.getDatabaseName()
        );

        try {
            ensureDatabase(catalog, entity.getDatabaseName());
            registerTables(catalog, entity);
            registerViews(catalog, entity);
            registerUdfs(catalog, entity);
        } catch (Exception e) {
            throw new CatalogException(
                    "Failed to restore catalog from snapshot [" +
                            entity.getSnapshotId() + "]", e);
        }

        return catalog;
    }


    private static void registerTables(
            GenericInMemoryCatalog catalog,
            CatalogEntity snapshot) throws Exception {

        for (TableEntity t : snapshot.getTables()) {
            ObjectPath path = new ObjectPath(t.database(), t.name());
            ensureDatabase(catalog, t.database());
            CatalogTable table = toCatalogTable(t);
            catalog.createTable(path, table, false);
        }
    }

    /**
     * 将 {@link TableEntity} 转换为 {@link CatalogTable}。
     *
     * <p>Schema 构建顺序很重要：
     * <ol>
     *   <li>先物理列和元数据列（按原始顺序）</li>
     *   <li>计算列在所有物理列之后</li>
     *   <li>Watermark 放在最后（通过 rowtime 列名引用）</li>
     *   <li>主键在 Watermark 之后</li>
     * </ol>
     */
    static CatalogTable toCatalogTable(TableEntity t) {
        Schema.Builder schema = Schema.newBuilder();

        for (ColumnEntity col : t.columns()) {

            if (col.isComputed()) {
                // AS <expr> 列 — Flink 从表达式推导类型
                schema.columnByExpression(col.name(), col.computedExpr());
            } else if (col.isMetadata()) {
                // METADATA [FROM 'key'] 列
                // Metadata 列是 Flink SQL 里一种特殊的列类型，用 METADATA 关键字声明，它的值来自 connector 暴露的元数据，而不是消息体本身。
                // CREATE TABLE user_events (
                //    user_id     BIGINT,
                //    event_type  STRING,
                //
                //    -- 这三列是 metadata 列，值来自 Kafka 消息头，不在 JSON payload 里
                //    kafka_topic     STRING    METADATA FROM 'topic'         VIRTUAL,
                //    kafka_partition INT       METADATA FROM 'partition'     VIRTUAL,
                //    kafka_offset    BIGINT    METADATA FROM 'offset'        VIRTUAL,
                //    kafka_timestamp TIMESTAMP(3) METADATA FROM 'timestamp'
                //) WITH (
                //    'connector' = 'kafka',
                //    ...
                AbstractDataType<?> dt = parseType(col.type());
                if (col.metadataKey() != null) {
                    schema.columnByMetadata(col.name(), dt, col.metadataKey(),
                            col.virtual());
                } else {
                    schema.columnByMetadata(col.name(), dt, col.virtual());
                }

            } else {
                // 普通物理列
                AbstractDataType<?> dt = parseType(col.type());
                // nullable 通过 DataType 包装器表达
                schema.column(col.name(),
                        col.nullable() ? dt : dt.notNull());
            }

            // 每列注释（Flink 2.x Schema.Builder 支持）
            if (col.comment() != null) {
                schema.withComment(col.comment());
            }
        }

        if (t.watermark() != null) {
            schema.watermark(
                    t.watermark().rowtimeColumn(),
                    t.watermark().expression()
            );
        }

        if (t.primaryKey() != null) {
            PrimaryKeyEntity pk = t.primaryKey();
            String[] cols = pk.columnNames().toArray(String[]::new);

            if (pk.constraintName() != null && !pk.constraintName().isBlank()) {
                schema.primaryKeyNamed(pk.constraintName(), cols);
            } else {
                schema.primaryKey(cols);
            }
        }

        // 防御性拷贝，保持 snapshot 对象不可变
        Map<String, String> options = new HashMap<>(t.options());

        CatalogTable.Builder builder = CatalogTable.newBuilder()
                .schema(schema.build())
                .options(options);

        if (t.partitionKeys() != null && !t.partitionKeys().isEmpty()) {
            builder.partitionKeys(t.partitionKeys());
        }

        if (t.comment() != null) {
            builder.comment(t.comment());
        }

        return builder.build();
    }


    private static void registerViews(
            GenericInMemoryCatalog catalog,
            CatalogEntity snapshot) throws Exception {

        for (ViewEntity v : snapshot.getViews()) {
            ensureDatabase(catalog, v.database());
            ObjectPath path = new ObjectPath(v.database(), v.name());

            // Flink 2.x CatalogView.of() — schema 传 null，由 query 运行时推导
            CatalogView view = CatalogView.of(
                    null,                       // null — Flink 从 expandedQuery 推导 schema
                    v.comment(),
                    v.expandedQuery(),   // originalQuery
                    v.expandedQuery(),    // expandedQuery（snapshot 里已是展开形式）
                    Collections.emptyMap()
            );

            catalog.createTable(path, view, false);
        }
    }


    private static void registerUdfs(
            GenericInMemoryCatalog catalog,
            CatalogEntity snapshot) throws Exception {

        for (UdfEntity udf : snapshot.getUdfs()) {
            String dbName = udf.database() != null ? udf.database() : snapshot.getDatabaseName();
            ensureDatabase(catalog, dbName);
            ObjectPath path = new ObjectPath(dbName, udf.name());
            CatalogFunction fn = new CatalogFunctionImpl(
                    udf.className(),
                    "PYTHON".equalsIgnoreCase(udf.functionLanguage())
                            ? FunctionLanguage.PYTHON
                            : FunctionLanguage.JAVA
            );
            catalog.createFunction(path, fn, false);
        }
    }


    private static void ensureDatabase(
            GenericInMemoryCatalog catalog,
            String dbName) throws DatabaseAlreadyExistException {

        if (!catalog.databaseExists(dbName)) {
            catalog.createDatabase(
                    dbName,
                    new CatalogDatabaseImpl(Map.of(), null),
                    /* 已存在时忽略 */ true
            );
        }
    }

    /**
     * 将 Flink SQL 类型字符串解析为 {@link AbstractDataType}。
     *
     * <p>委托给 {@code DataTypes.of(String)}，该方法处理所有
     * 内置类型，包括复杂类型（ARRAY, MAP, ROW）。
     *
     * <p>可接受的字符串示例：
     * <ul>
     *   <li>{@code "BIGINT"}</li>
     *   <li>{@code "DECIMAL(10, 2)"}</li>
     *   <li>{@code "TIMESTAMP(3)"}</li>
     *   <li>{@code "ROW<id BIGINT, name STRING>"}</li>
     *   <li>{@code "ARRAY<STRING>"}</li>
     * </ul>
     */
    private static AbstractDataType<?> parseType(String typeStr) {
        // DataTypes.of(String) 是 Flink Table API 中解析类型字符串的规范方式
        // 它支持完整的 Flink SQL 类型语法
        return DataTypes.of(typeStr);
    }
}