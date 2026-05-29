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
package com.lanting.flink.sql.bootstrap.util;

import com.lanting.flink.sql.bootstrap.executor.StreamingScriptExecutor;
import com.lanting.flink.sql.bootstrap.flink.UriSafeSessionContext;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.delegation.InternalPlan;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.gateway.service.context.SessionContext;
import org.apache.flink.util.concurrent.Executors;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * DAG 打印测试，覆盖四种典型拓扑。
 * {@code @Disabled} — 仅手动触发，不参与 {@code mvn test}。
 */
@Disabled
class PrintUtilsTest {

    private static List<Transformation<?>> toTransformations(String scriptPath) throws Exception {
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

        DefaultContext defaultContext = new DefaultContext(
                (Configuration) env.getConfiguration(), Collections.emptyList());
        SessionEnvironment ssEnv = SessionEnvironment.newBuilder()
                .setSessionEndpointVersion(SqlGatewayRestAPIVersion.getDefaultVersion())
                .build();
        SessionContext sessionContext = UriSafeSessionContext.create(
                defaultContext, new SessionHandle(UUID.randomUUID()), ssEnv,
                Executors.newDirectExecutorService());

        try (AutoCloseable ignored = sessionContext::close) {
            String script = Files.readString(Paths.get("src/test/resources", scriptPath));
            StreamingScriptExecutor executor = new StreamingScriptExecutor(sessionContext, script);
            InternalPlan plan = executor.compile(script);
            return executor.transform(plan);
        }
    }

    @Test
    void case1TwoSourceJoinTwoSink(TestInfo info) throws Exception {
        System.out.println("\n========== " + info.getDisplayName() + " ==========");
        List<Transformation<?>> transformations = toTransformations("case1_two_source_join_two_sink.sql");
        DAGPrinter.print(transformations);
    }

    @Test
    void case2TwoSourceUnion(TestInfo info) throws Exception {
        System.out.println("\n========== " + info.getDisplayName() + " ==========");
        List<Transformation<?>> transformations = toTransformations("case2_two_source_union.sql");
        DAGPrinter.print(transformations);
    }

    @Test
    void case3ThreeSourceJoinUnionTwoSink(TestInfo info) throws Exception {
        System.out.println("\n========== " + info.getDisplayName() + " ==========");
        List<Transformation<?>> transformations = toTransformations("case3_three_source_join_union_two_sink.sql");
        DAGPrinter.print(transformations);
    }

    @Test
    void case4OneSourceOneSink(TestInfo info) throws Exception {
        System.out.println("\n========== " + info.getDisplayName() + " ==========");
        List<Transformation<?>> transformations = toTransformations("case4_one_source_one_sink.sql");
        DAGPrinter.print(transformations);
    }
}
