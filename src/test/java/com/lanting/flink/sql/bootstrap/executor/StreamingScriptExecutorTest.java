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
import com.lanting.flink.sql.bootstrap.exception.SqlCompileException;
import com.lanting.flink.sql.bootstrap.exception.SqlValidateException;
import com.lanting.flink.sql.bootstrap.flink.UriSafeSessionContext;
import com.lanting.flink.sql.bootstrap.resource.OperatorEntity;
import com.lanting.flink.sql.bootstrap.resource.OperatorResourceSpec;
import com.lanting.flink.sql.bootstrap.resource.ResourceEntity;
import com.lanting.flink.sql.bootstrap.util.JSON;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.table.delegation.InternalPlan;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
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
    void setUp() {
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

    @Test
    @DisplayName("compile — 正常 DML：返回 InternalPlan，包含 sink 节点")
    void compileWithValidDML() {
        StreamingScriptExecutor executor = createExecutor("executor-test-sql/test_simple_dml.sql");
        InternalPlan plan = executor.compile(executor.getScript());

        assertNotNull(plan);
        assertNotNull(plan.asJsonString());
        assertFalse(plan.asJsonString().isEmpty());
    }

    @Test
    @DisplayName("compile — 无 DML：抛 SqlCompileException")
    void compileWithNoDML() {
        StreamingScriptExecutor executor = createExecutor("executor-test-sql/test_no_dml.sql");
        assertThrows(SqlCompileException.class,
                () -> executor.compile(executor.getScript()));
    }

    @Test
    @DisplayName("compile — 多条 DML：抛 SqlValidateException（第二 DML 在 validate 阶段被拦截）")
    void compileWithMultipleDML() {
        StreamingScriptExecutor executor = createExecutor("executor-test-sql/test_multi_dml.sql");
        assertThrows(SqlValidateException.class,
                () -> executor.compile(executor.getScript()));
    }

    @Test
    @DisplayName("compile — STATEMENT SET：返回 InternalPlan")
    void compileWithStatementSet() {
        StreamingScriptExecutor executor = createExecutor("executor-test-sql/test_statement_set.sql");
        InternalPlan plan = executor.compile(executor.getScript());

        assertNotNull(plan);
        assertNotNull(plan.asJsonString());
    }

    @Test
    @DisplayName("compile — DDL + SET + DML 混合脚本")
    void compileWithMixedScript() {
        StreamingScriptExecutor executor = createExecutor("executor-test-sql/test_multi_ddl_dml.sql");
        InternalPlan plan = executor.compile(executor.getScript());

        assertNotNull(plan);
        assertNotNull(plan.asJsonString());
    }

    @Test
    @DisplayName("compile — 注释和引号内分号不触发切分")
    void compileWithComments() {
        StreamingScriptExecutor executor = createExecutor("executor-test-sql/test_comments.sql");
        InternalPlan plan = executor.compile(executor.getScript());

        assertNotNull(plan);
    }

    @Test
    @DisplayName("validate — SQL 语法错误：抛 SqlValidateException")
    void validateWithSyntaxError() {
        StreamingScriptExecutor executor = createExecutor("executor-test-sql/test_syntax_error.sql");
        assertThrows(SqlValidateException.class,
                () -> executor.validate(executor.getScript()));
    }

    @Test
    @DisplayName("transform — 正常 DML 返回非空 Transformation 列表")
    void transformValidDML() {
        StreamingScriptExecutor executor = createExecutor("executor-test-sql/test_simple_dml.sql");
        InternalPlan plan = executor.compile(executor.getScript());
        List<Transformation<?>> transformations = executor.transform(plan);

        assertNotNull(transformations);
        assertFalse(transformations.isEmpty());
    }

    // ─── injectResourceSpec ──────────────────────────────────────────────

    @Test
    @DisplayName("injectResourceSpec — UID 精确匹配设置并行度")
    void injectByUid() {
        StreamingScriptExecutor executor = createExecutor("executor-test-sql/test_simple_dml.sql");
        List<Transformation<?>> transformations = compileAndTransform(executor);
        String uid = firstPhysical(transformations).getUid();

        ResourceEntity spec = executor.generateResultSpec();
        for (OperatorEntity op : spec.getOperators()) {
            if (uid.equals(op.getUid())) {
                op.setParallelism(7);
            }
        }
        executor.injectResourceSpec(transformations, JSON.toJSONString(spec));

        assertEquals(7, firstPhysical(transformations).getParallelism());
    }

    @Test
    @DisplayName("injectResourceSpec — 名称兜底匹配")
    void injectByName() {
        StreamingScriptExecutor executor = createExecutor("executor-test-sql/test_simple_dml.sql");
        List<Transformation<?>> transformations = compileAndTransform(executor);
        String name = firstPhysical(transformations).getName();

        ResourceEntity spec = executor.generateResultSpec();
        for (OperatorEntity op : spec.getOperators()) {
            if (name.equals(op.getName())) {
                op.setUid(null); // 去掉 UID 走名称匹配路径
                op.setParallelism(5);
            }
        }
        executor.injectResourceSpec(transformations, JSON.toJSONString(spec));

        assertEquals(5, firstPhysical(transformations).getParallelism());
    }

    @Test
    @DisplayName("injectResourceSpec — 无匹配 UID 则抛异常")
    void injectNoMatch() {
        StreamingScriptExecutor executor = createExecutor("executor-test-sql/test_simple_dml.sql");
        List<Transformation<?>> transformations = compileAndTransform(executor);

        String json = "{\"version\":1,\"operators\":[{\"uid\":\"nonexistent\",\"parallelism\":99}]}";
        assertThrows(IllegalArgumentException.class,
                () -> executor.injectResourceSpec(transformations, json));
    }

    @Test
    @DisplayName("injectResourceSpec — 资源注入 (CPU + Heap Memory)")
    void injectResource() {
        StreamingScriptExecutor executor = createExecutor("executor-test-sql/test_simple_dml.sql");
        List<Transformation<?>> transformations = compileAndTransform(executor);
        String uid = firstPhysical(transformations).getUid();

        ResourceEntity spec = executor.generateResultSpec();
        for (OperatorEntity op : spec.getOperators()) {
            if (uid.equals(op.getUid())) {
                com.lanting.flink.sql.bootstrap.resource.OperatorResourceSpec res =
                        new com.lanting.flink.sql.bootstrap.resource.OperatorResourceSpec(
                                1.5, "2048 MB", null, null, Collections.emptyMap());
                op.setResource(res);
            }
        }
        executor.injectResourceSpec(transformations, JSON.toJSONString(spec));

        assertTrue(firstPhysical(transformations).getSlotSharingGroup().isPresent());
        assertEquals(1.5,
                firstPhysical(transformations).getSlotSharingGroup().get().getCpuCores().orElse(0d));
    }

    @Test
    @DisplayName("injectResourceSpec — profile 预置规格解析")
    void injectProfile() {
        StreamingScriptExecutor executor = createExecutor("executor-test-sql/test_simple_dml.sql");
        List<Transformation<?>> transformations = compileAndTransform(executor);
        String uid = firstPhysical(transformations).getUid();

        ResourceEntity spec = executor.generateResultSpec();
        for (OperatorEntity op : spec.getOperators()) {
            if (uid.equals(op.getUid())) {
                op.setResource(OperatorResourceSpec.SMALL);
            }
        }
        executor.injectResourceSpec(transformations, JSON.toJSONString(spec));

        assertTrue(firstPhysical(transformations).getSlotSharingGroup().isPresent());
        assertEquals(0.25,
                firstPhysical(transformations).getSlotSharingGroup().get().getCpuCores().orElse(0d));
    }

    @Test
    @DisplayName("injectResourceSpec — Chain 策略注入")
    void injectChainStrategy() {
        StreamingScriptExecutor executor = createExecutor("executor-test-sql/test_simple_dml.sql");
        List<Transformation<?>> transformations = compileAndTransform(executor);
        String uid = firstPhysical(transformations).getUid();

        ResourceEntity spec = executor.generateResultSpec();
        for (OperatorEntity op : spec.getOperators()) {
            if (uid.equals(op.getUid())) {
                op.setChainStrategy("HEAD");
            }
        }
        executor.injectResourceSpec(transformations, JSON.toJSONString(spec));
    }

    @Test
    @DisplayName("injectResourceSpec — 非法 Chain 策略抛异常")
    void injectInvalidChainStrategy() {
        StreamingScriptExecutor executor = createExecutor("executor-test-sql/test_simple_dml.sql");
        List<Transformation<?>> transformations = compileAndTransform(executor);
        String uid = firstPhysical(transformations).getUid();

        ResourceEntity spec = executor.generateResultSpec();
        for (OperatorEntity op : spec.getOperators()) {
            if (uid.equals(op.getUid())) {
                op.setChainStrategy("END");
            }
        }
        assertThrows(IllegalArgumentException.class,
                () -> executor.injectResourceSpec(transformations, JSON.toJSONString(spec)));
    }

    @Test
    @DisplayName("injectResourceSpec — 空字符串跳过注入")
    void injectEmptyResource() {
        StreamingScriptExecutor executor = createExecutor("executor-test-sql/test_simple_dml.sql");
        List<Transformation<?>> transformations = compileAndTransform(executor);
        ResourceEntity spec = executor.generateResultSpec();
        int count = spec.getOperators().size();

        executor.injectResourceSpec(transformations, "");

        // 空字符串跳过，不改变任何东西
        assertEquals(count, executor.generateResultSpec().getOperators().size());
    }

    @Test
    @DisplayName("injectResourceSpec — 非法 JSON 抛异常")
    void injectBadJson() {
        StreamingScriptExecutor executor = createExecutor("executor-test-sql/test_simple_dml.sql");
        List<Transformation<?>> transformations = compileAndTransform(executor);

        assertThrows(RuntimeException.class,
                () -> executor.injectResourceSpec(transformations, "{bad}"));
    }

    // ─── generateResultSpec ──────────────────────────────────────────────

    @Test
    @DisplayName("generateResultSpec — 输出合法结构")
    void generateSpecStructure() {
        StreamingScriptExecutor executor = createExecutor("executor-test-sql/test_simple_dml.sql");
        ResourceEntity spec = executor.generateResultSpec();

        assertTrue(spec.getVersion() > 0);
        assertNotNull(spec.getOperators());
        assertFalse(spec.getOperators().isEmpty());
    }

    @Test
    @DisplayName("generateResultSpec — UID 与 Transformation 一致")
    void generateSpecUids() {
        StreamingScriptExecutor executor = createExecutor("executor-test-sql/test_simple_dml.sql");
        ResourceEntity spec = executor.generateResultSpec();

        for (OperatorEntity op : spec.getOperators()) {
            assertNotNull(op.getUid(), "UID must not be null for " + op.getName());
            assertNotNull(op.getName(), "Name must not be null for " + op.getUid());
        }
    }

    @Test
    @DisplayName("generateResultSpec — 只包含 PhysicalTransformation")
    void generateSpecOnlyPhysical() {
        StreamingScriptExecutor executor = createExecutor("executor-test-sql/test_simple_dml.sql");
        ResourceEntity spec = executor.generateResultSpec();
        List<Transformation<?>> all = executor.allTransformations(executor.getTransformations());

        long physicalCount = all.stream()
                .filter(t -> t instanceof PhysicalTransformation).count();
        assertEquals(physicalCount, spec.getOperators().size());
    }


    // ─── helpers ──────────────────────────────────────────────────────────

    private StreamingScriptExecutor createExecutor(String sqlFile) {
        String script = Utils.readFromClasspathUtf8(sqlFile);
        return new StreamingScriptExecutor(sessionContext, script);
    }

    private List<Transformation<?>> compileAndTransform(StreamingScriptExecutor executor) {
        InternalPlan plan = executor.compile(executor.getScript());
        return executor.transform(plan);
    }

    private static <T extends Transformation<?>> T firstPhysical(List<Transformation<?>> transformations) {
        for (Transformation<?> t : transformations) {
            if (t instanceof PhysicalTransformation) {
                @SuppressWarnings("unchecked")
                T casted = (T) t;
                return casted;
            }
        }
        throw new IllegalStateException("No PhysicalTransformation found");
    }
}
