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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * {@link TableEntity} 中的一列。
 *
 * <p>类型系统覆盖：
 * <ul>
 *   <li>基本类型：BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、
 *       DECIMAL(p,s)、STRING、BYTES、DATE、TIME(p)、TIMESTAMP(p)、
 *       TIMESTAMP_LTZ(p)</li>
 *   <li>复杂类型：ARRAY&lt;T&gt;, MAP&lt;K,V&gt;, ROW&lt;f T, …&gt;</li>
 * </ul>
 * 所有类型都用纯字符串 {@code type} 表达，由
 * {@link CatalogEntityFactory} 通过 {@code DataTypes.of(String)} 解析。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@Accessors(fluent = true)
@AllArgsConstructor
@NoArgsConstructor
public class ColumnEntity {

    @JsonProperty("name")
    String name;

    /**
     * Flink SQL 类型字符串，例如 "BIGINT"、"STRING"、"TIMESTAMP(3)"、
     * "ROW<id BIGINT, name STRING>"。
     */
    @JsonProperty("type")
    String type;
    @JsonProperty("nullable")
    boolean nullable;
    /**
     * 只读的 metadata 列必须声明为 VIRTUAL，以便在 INSERT INTO 操作中将它们排除在外。
     */
    @JsonProperty("virtual")
    boolean virtual;
    @JsonProperty("comment")
    String comment;

    /**
     * 当列定义为 AS <expr> 时为 true。
     */
    @JsonProperty("isComputed")
    boolean isComputed;

    /**
     * 计算列的 AS 表达式，否则为 null。
     * 例如："PROCTIME()" 或 "CAST(ts AS TIMESTAMP_LTZ(3))"。
     */
    @JsonProperty("computedExpr")
    String computedExpr;

    /**
     * 当列定义为 METADATA [FROM 'key'] 时为 true。
     */
    @JsonProperty("isMetadata")
    boolean isMetadata;
    @JsonProperty("metadataKey")
    String metadataKey;

    /**
     * 仅对应携带 WATERMARK 的 rowtime 列时非 null。
     * 仅作参考保留；实际的 watermark 策略在表级别的 {@link WatermarkEntity} 中声明。
     */
    @JsonProperty("watermarkExpr")
    String watermarkExpr;
}