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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Catalog 快照的 JSON 根对象，包含表、视图和 UDF 的完整元数据定义。
 *
 * <p>自包含设计——所有 DDL 信息内置于快照中，运行时无需访问外部元数据中心即可恢复
 * {@link org.apache.flink.table.catalog.GenericInMemoryCatalog}。
 *
 * @see com.lanting.flink.sql.bootstrap.catalog.CatalogEntityFactory
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CatalogEntity {
    @JsonProperty("version")
    int version;
    @JsonProperty("snapshotId")
    String snapshotId;
    @JsonProperty("catalogName")
    String catalogName;
    @JsonProperty("databaseName")
    String databaseName;
    @JsonProperty("flinkVersion")
    String flinkVersion;
    @JsonProperty("createdAt")
    long createdAt;
    @JsonProperty("tables")
    List<TableEntity> tables;
    @JsonProperty("views")
    List<ViewEntity> views;
    @JsonProperty("udfs")
    List<UdfEntity> udfs;
}