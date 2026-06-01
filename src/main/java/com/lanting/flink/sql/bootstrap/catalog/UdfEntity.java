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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * {@link CatalogEntity} 中的用户自定义函数条目。
 *
 * <p>{@code kind} 取值为：{@code SCALAR}、{@code TABLE}、{@code AGGREGATE} 之一。
 * {@code jarRef} 是 UDF JAR 在 {@code pipeline.jars} 中出现的文件名。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@Accessors(fluent = true)
@AllArgsConstructor
@NoArgsConstructor
public class UdfEntity {
    @JsonProperty("database")
    String database;
    @JsonProperty("name")
    String name;
    @JsonProperty("kind")
    String kind;
    @JsonProperty("className")
    String className;
    @JsonProperty("description")
    String description;
    @JsonProperty("functionLanguage")
    String functionLanguage;
    @JsonProperty("jarRef")
    String jarRef;
    /**
     * 可选的类型推断提示，按原样转发为 {@code FunctionDefinition} 属性。
     * Null 表示 Flink 通过反射从类推断类型。
     */
    @JsonProperty("typeInference")
    Map<String, String> typeInference;
}
