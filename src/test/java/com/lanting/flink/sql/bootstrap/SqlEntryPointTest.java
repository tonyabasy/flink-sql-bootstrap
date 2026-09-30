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

    // ─── --xxx-b64 传输 ─────────────────────────────────────────────

    private static String b64(String text) {
        return java.util.Base64.getUrlEncoder()
                .encodeToString(text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("正向：--script-b64 解码回原始脚本（含换行与单引号）")
    void scriptB64RoundTrip() throws Exception {
        String sql = "-- 注释\nCREATE TABLE t (x STRING) WITH ('connector' = 'datagen');\n";
        SqlEntryPoint.ArgsContent args =
                SqlEntryPoint.parseOptions(new String[]{"--script-b64", b64(sql)});
        assertEquals(sql, args.script);
    }

    @Test
    @DisplayName("正向：--catalog-b64 / --resource-b64 解码")
    void catalogAndResourceB64() throws Exception {
        String catalog = "{\"version\":1,\"tables\":[]}";
        String resource = "{\"version\":1,\"operators\":[]}";
        SqlEntryPoint.ArgsContent args = SqlEntryPoint.parseOptions(new String[]{
                "--script-b64", b64("SELECT 1;"),
                "--catalog-b64", b64(catalog),
                "--resource-b64", b64(resource)});
        assertEquals(catalog, args.catalog);
        assertEquals(resource, args.resource);
    }

    @Test
    @DisplayName("逆向：--script 与 --script-b64 互斥")
    void scriptAndB64AreMutuallyExclusive() {
        assertThrows(IllegalArgumentException.class, () ->
                SqlEntryPoint.parseOptions(new String[]{
                        "--script", "SELECT 1;", "--script-b64", b64("SELECT 1;")}));
    }

    @Test
    @DisplayName("逆向：非法 Base64 报错并指明选项")
    void invalidBase64Rejected() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                SqlEntryPoint.parseOptions(new String[]{"--script-b64", "!!!not-base64!!!"}));
        assertTrue(e.getMessage().contains("script-b64"));
    }
}
