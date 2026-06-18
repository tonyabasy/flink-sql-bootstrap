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
package com.lanting.flink.sql.bootstrap;

import static org.junit.jupiter.api.Assertions.*;

import com.lanting.flink.sql.bootstrap.catalog.CatalogEntity;
import com.lanting.flink.sql.bootstrap.resource.ResourceEntity;
import com.lanting.flink.sql.bootstrap.util.JSON;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Verify that the JSON examples shown in the documentation
 * can be successfully parsed by the actual data model classes.
 */
class DocExamplesTest {

    private static String loadResource(String path) {
        try (Scanner scanner = new Scanner(
                DocExamplesTest.class.getClassLoader().getResourceAsStream(path),
                StandardCharsets.UTF_8.name())) {
            scanner.useDelimiter("\\A");
            return scanner.hasNext() ? scanner.next() : "";
        }
    }

    @Test
    @DisplayName("文档示例 — 资源调优 JSON 可被正确解析")
    void parseResourceHintJson() {
        String json = loadResource("doc-example-resource.json");

        ResourceEntity resource = JSON.parseObject(json, ResourceEntity.class);
        assertNotNull(resource);
        assertEquals(1, resource.getVersion());
        assertEquals(2, resource.getDefaultParallelism());
        assertEquals(2, resource.getOperators().size());

        assertEquals("1_source", resource.getOperators().get(0).getUid());
        assertEquals("stateless", resource.getOperators().get(0).getResource().getProfile());

        assertEquals("5_group-aggregate", resource.getOperators().get(1).getUid());
        assertEquals(1.0, resource.getOperators().get(1).getResource().getCpu());
        assertEquals("2048m", resource.getOperators().get(1).getResource().getHeap());
        assertEquals("256m", resource.getOperators().get(1).getResource().getManaged());
    }

    @Test
    @DisplayName("文档示例 — Catalog 快照 JSON 可被正确解析")
    void parseCatalogSnapshotJson() {
        String json = loadResource("doc-example-catalog.json");

        CatalogEntity catalog = JSON.parseObject(json, CatalogEntity.class);
        assertNotNull(catalog);
        assertEquals(1, catalog.getVersion());
        assertEquals("example-word-count", catalog.getSnapshotId());
        assertEquals("platform", catalog.getCatalogName());
        assertEquals("default", catalog.getDatabaseName());

        assertEquals(1, catalog.getTables().size());
        assertEquals("ods_words", catalog.getTables().get(0).name());
        assertEquals(2, catalog.getTables().get(0).columns().size());

        assertTrue(catalog.getTables().get(0).columns().get(1).isComputed());
        assertEquals("PROCTIME()", catalog.getTables().get(0).columns().get(1).computedExpr());

        assertNotNull(catalog.getTables().get(0).primaryKey());
        assertEquals("id", catalog.getTables().get(0).primaryKey().columnNames().get(0));
        assertTrue(catalog.getTables().get(0).primaryKey().enforced());

        assertNotNull(catalog.getTables().get(0).watermark());
        assertEquals("ts", catalog.getTables().get(0).watermark().rowtimeColumn());

        assertEquals("datagen", catalog.getTables().get(0).options().get("connector"));

        assertEquals(1, catalog.getViews().size());
        assertEquals("v_latest_words", catalog.getViews().get(0).name());
        assertTrue(catalog.getViews().get(0).expandedQuery().contains("SELECT sentence"));

        assertEquals(1, catalog.getUdfs().size());
        assertEquals("my_reverse", catalog.getUdfs().get(0).name());
        assertEquals("SCALAR", catalog.getUdfs().get(0).kind());
        assertEquals("JAVA", catalog.getUdfs().get(0).functionLanguage());
    }
}
