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
package com.lanting.flink.sql.bootstrap.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.PlannerFactoryUtil;
import org.apache.flink.table.gateway.service.context.SessionContext;
import org.apache.flink.table.gateway.service.operation.OperationExecutor;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.resource.ResourceManager;

import java.lang.reflect.Method;

/**
 * Application Mode 兼容的 OperationExecutor，用于向下兼容 Flink 1.20.x。
 *
 * <p>Flink 2.x 中 {@code ClientUtils.executeProgram()} 在 Application Mode 下
 * 会通过 {@code StreamContextEnvironment.setAsContext()} 注入
 * {@code EmbeddedExecutorServiceLoader}，父类的
 * {@code new StreamExecutionEnvironment(config, classloader)} 能自动获取该 loader，
 * 不存在此问题。
 *
 * <p>但在 Flink 1.20 中，父类构造的 {@link StreamExecutionEnvironment} 使用
 * {@code DefaultExecutorServiceLoader}，通过 SPI 发现
 * {@code EmbeddedExecutorFactory} 后，因其 {@code isCompatibleWith()}
 * 永远返回 {@code false} 而无法匹配，导致 {@code No ExecutorFactory found}。
 *
 * <p>为在两个大版本间均可运行，本类重写
 * {@link #getTableEnvironment(ResourceManager, Configuration)}，
 * 改用 {@link StreamExecutionEnvironment#getExecutionEnvironment(Configuration)}
 * 创建 env，通过 {@code StreamContextEnvironment} 获取已注入的
 * {@code EmbeddedExecutorServiceLoader}，绕过 SPI 兼容性检查。
 */
public class ApplicationOperationExecutor extends OperationExecutor {

    protected final SessionContext sessionContext;
    private final Configuration executionConfig;

    public ApplicationOperationExecutor(SessionContext context, Configuration executionConfig) {
        super(context, executionConfig);
        this.sessionContext = context;
        this.executionConfig = executionConfig;
    }

    @Override
    public TableEnvironmentInternal getTableEnvironment(
            ResourceManager resourceManager, Configuration customConfig) {
        // checks the value of RUNTIME_MODE
        Configuration operationConfig = sessionContext.getSessionConf().clone();
        operationConfig.addAll(executionConfig);
        operationConfig.addAll(customConfig);
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().withConfiguration(operationConfig).build();

        // 使用 getExecutionEnvironment() 而非 new StreamExecutionEnvironment()，
        // 确保 Application Mode 下通过 StreamContextEnvironment 拿到
        // EmbeddedExecutorServiceLoader，绕过 EmbeddedExecutorFactory.isCompatibleWith()
        // 永远返回 false 的 SPI 兼容性问题。
        StreamExecutionEnvironment streamExecEnv =
                StreamExecutionEnvironment.getExecutionEnvironment(operationConfig);

        TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.setRootConfiguration(sessionContext.getDefaultContext().getFlinkConfig());
        tableConfig.addConfiguration(operationConfig);

        final Executor executor =
                lookupExecutor(streamExecEnv, sessionContext.getUserClassloader());
        return createStreamTableEnvironment(
                streamExecEnv,
                settings,
                tableConfig,
                executor,
                sessionContext.getSessionState().catalogManager,
                sessionContext.getSessionState().moduleManager,
                resourceManager,
                sessionContext.getSessionState().functionCatalog.copy(resourceManager));
    }

    private static Executor lookupExecutor(
            StreamExecutionEnvironment executionEnvironment, ClassLoader userClassLoader) {
        try {
            final ExecutorFactory executorFactory =
                    FactoryUtil.discoverFactory(
                            userClassLoader,
                            ExecutorFactory.class,
                            ExecutorFactory.DEFAULT_IDENTIFIER);
            final Method createMethod =
                    executorFactory
                            .getClass()
                            .getMethod("create", StreamExecutionEnvironment.class);

            return (Executor) createMethod.invoke(executorFactory, executionEnvironment);
        } catch (Exception e) {
            throw new TableException(
                    "Could not instantiate the executor. Make sure a planner module is on the classpath",
                    e);
        }
    }

    private TableEnvironmentInternal createStreamTableEnvironment(
            StreamExecutionEnvironment env,
            EnvironmentSettings settings,
            TableConfig tableConfig,
            Executor executor,
            CatalogManager catalogManager,
            ModuleManager moduleManager,
            ResourceManager resourceManager,
            FunctionCatalog functionCatalog) {

        final Planner planner =
                PlannerFactoryUtil.createPlanner(
                        executor,
                        tableConfig,
                        resourceManager.getUserClassLoader(),
                        moduleManager,
                        catalogManager,
                        functionCatalog);

        return new StreamTableEnvironmentImpl(
                catalogManager,
                moduleManager,
                resourceManager,
                functionCatalog,
                tableConfig,
                env,
                planner,
                executor,
                settings.isStreamingMode());
    }
}
