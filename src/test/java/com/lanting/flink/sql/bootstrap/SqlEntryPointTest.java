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

import java.io.FileNotFoundException;
import java.net.URI;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SqlEntryPointTest {

    @Test
    @DisplayName("当前防御：classpath: 路径含 .. 被 SecurityException 拦截")
    void rejectDotDot() {
        URI evil = URI.create("classpath:../../../etc/passwd");
        List<URI> deps = Collections.singletonList(evil);

        SecurityException ex = assertThrows(SecurityException.class,
                () -> SqlEntryPoint.resolveClasspathDependencies(deps));
        assertTrue(ex.getMessage().contains(".."));
    }

    // ─── readFromClasspathUtf8 ─────────────────────────────────────────────

    @Test
    @DisplayName("正向：读取已有 classpath 资源返回非空内容")
    void readExistingResource() throws Exception {
        URI uri = URI.create("classpath:executor-test-sql/test_simple_dml.sql");
        String content = SqlEntryPoint.readFromClasspathUtf8(uri);
        assertNotNull(content);
        assertTrue(content.contains("CREATE TEMPORARY TABLE"));
    }

    @Test
    @DisplayName("逆向：.. 路径逃逸抛 SecurityException")
    void rejectDotDotResource() {
        URI uri = URI.create("classpath:subdir/../../etc/passwd");
        assertThrows(SecurityException.class,
                () -> SqlEntryPoint.readFromClasspathUtf8(uri));
    }

    @Test
    @DisplayName("逆向：/ 开头抛 SecurityException")
    void rejectAbsoluteResource() {
        URI uri = URI.create("classpath:/etc/passwd");
        assertThrows(SecurityException.class,
                () -> SqlEntryPoint.readFromClasspathUtf8(uri));
    }

    @Test
    @DisplayName("逆向：资源不存在抛 FileNotFoundException")
    void rejectNonExistentResource() {
        URI uri = URI.create("classpath:nonexistent/resource.sql");
        assertThrows(FileNotFoundException.class,
                () -> SqlEntryPoint.readFromClasspathUtf8(uri));
    }

    @Test
    @DisplayName("边界：.. 被 normalize 消掉后仍在 classpath 内，可正常读取")
    void normalizeSafePath() throws Exception {
        URI uri = URI.create(
                "classpath:executor-test-sql/../executor-test-sql/test_simple_dml.sql");
        String content = SqlEntryPoint.readFromClasspathUtf8(uri);
        assertNotNull(content);
        assertTrue(content.contains("CREATE TEMPORARY TABLE"));
    }
}
