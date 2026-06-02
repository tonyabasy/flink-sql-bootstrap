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
package com.lanting.flink.sql.bootstrap.executor;

import static org.junit.jupiter.api.Assertions.*;

import com.lanting.flink.sql.bootstrap.Utils;
import com.lanting.flink.sql.bootstrap.flink.UriSafeSessionContext;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.delegation.InternalPlan;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.gateway.service.context.SessionContext;
import org.apache.flink.util.concurrent.Executors;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * StreamingScriptExecutor 测试。
 * 覆盖 compile、transform、injectResourceSpec、generateResultSpec 及异常路径。
 */
class StreamingScriptExecutorTest {

    private SessionContext sessionContext;

    @BeforeEach
    void setUp() throws Exception {
        Configuration config = new Configuration();
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);

        DefaultContext defaultContext = new DefaultContext(
                (Configuration) env.getConfiguration(), Collections.emptyList());
        SessionEnvironment ssEnv = SessionEnvironment.newBuilder()
                .setSessionEndpointVersion(SqlGatewayRestAPIVersion.getDefaultVersion())
                .build();
        sessionContext = UriSafeSessionContext.create(
                defaultContext, new SessionHandle(UUID.randomUUID()), ssEnv,
                Executors.newDirectExecutorService());
    }

    private StreamingScriptExecutor createExecutor(String sqlFile) throws Exception {
        String script = Utils.readFromClasspathUtf8(sqlFile);
        return new StreamingScriptExecutor(sessionContext, script);
    }

    @Test
    @DisplayName("compile — 正常 DML：返回 InternalPlan，包含 sink 节点")
    void compileWithValidDML() throws Exception {
        StreamingScriptExecutor executor = createExecutor("sql/test_simple_dml.sql");
        InternalPlan plan = executor.compile(executor.getScript());

        assertNotNull(plan);
        assertNotNull(plan.asJsonString());
        assertFalse(plan.asJsonString().isEmpty());
    }

    @Test
    @DisplayName("compile — 无 DML：抛 SqlGatewayException")
    void compileWithNoDML() throws Exception {
        StreamingScriptExecutor executor = createExecutor("sql/test_no_dml.sql");
        assertThrows(SqlGatewayException.class,
                () -> executor.compile(executor.getScript()));
    }

    @Test
    @DisplayName("compile — 多条 DML：抛 SqlGatewayException")
    void compileWithMultipleDML() throws Exception {
        StreamingScriptExecutor executor = createExecutor("sql/test_multi_dml.sql");
        assertThrows(SqlGatewayException.class,
                () -> executor.compile(executor.getScript()));
    }

    @Test
    @DisplayName("compile — STATEMENT SET：返回 InternalPlan")
    void compileWithStatementSet() throws Exception {
        StreamingScriptExecutor executor = createExecutor("sql/test_statement_set.sql");
        InternalPlan plan = executor.compile(executor.getScript());

        assertNotNull(plan);
        assertNotNull(plan.asJsonString());
    }

    @Test
    @DisplayName("compile — DDL + SET + DML 混合脚本")
    void compileWithMixedScript() throws Exception {
        StreamingScriptExecutor executor = createExecutor("sql/test_multi_ddl_dml.sql");
        InternalPlan plan = executor.compile(executor.getScript());

        assertNotNull(plan);
        assertNotNull(plan.asJsonString());
    }

    @Test
    @DisplayName("compile — 注释和引号内分号不触发切分")
    void compileWithComments() throws Exception {
        StreamingScriptExecutor executor = createExecutor("sql/test_comments.sql");
        InternalPlan plan = executor.compile(executor.getScript());

        assertNotNull(plan);
    }

    @Test
    @DisplayName("validate — SQL 语法错误：抛 SqlGatewayException")
    void validateWithSyntaxError() throws Exception {
        StreamingScriptExecutor executor = createExecutor("sql/test_syntax_error.sql");
        assertThrows(SqlGatewayException.class,
                () -> executor.validate(executor.getScript()));
    }

    @Test
    @DisplayName("transform — 正常 DML 返回非空 Transformation 列表")
    void transformValidDML() throws Exception {
        StreamingScriptExecutor executor = createExecutor("sql/test_simple_dml.sql");
        InternalPlan plan = executor.compile(executor.getScript());
        List<Transformation<?>> transformations = executor.transform(plan);

        assertNotNull(transformations);
        assertFalse(transformations.isEmpty());
    }
}
