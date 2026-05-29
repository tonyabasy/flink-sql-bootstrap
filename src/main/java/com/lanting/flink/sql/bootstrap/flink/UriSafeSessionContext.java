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
import org.apache.flink.table.gateway.api.endpoint.EndpointVersion;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.gateway.service.context.SessionContext;
import org.apache.flink.table.gateway.service.operation.OperationManager;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.MutableURLClassLoader;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * {@link SessionContext} 的变体，修复 FLINK-39687 中 {@code URI → URL} 转换问题。
 *
 * <p>Flink 2.2.0 的 {@link SessionContext#create} 在构建 {@code userClassLoader} 时
 * 直接调用 {@code defaultContext.getDependencies().toArray(new URL[0])}，
 * 但 {@link java.net.URI} 不是 {@link java.net.URL} 的子类，非空列表会抛
 * {@link java.lang.ArrayStoreException}。
 *
 * <p>本类将每个 {@link URI} 逐个转换为 {@link URL} 后再传入 classloader，
 * 使 {@link DefaultContext} 的依赖机制恢复正常。
 *
 * @author wangzhao
 * @since 2026-05-15
 */
public class UriSafeSessionContext extends SessionContext {
    protected UriSafeSessionContext(DefaultContext defaultContext, SessionHandle sessionId, EndpointVersion endpointVersion, Configuration sessionConf, URLClassLoader classLoader, SessionState sessionState, OperationManager operationManager) {
        super(defaultContext, sessionId, endpointVersion, sessionConf, classLoader, sessionState, operationManager);
    }

    public static SessionContext create(DefaultContext defaultContext,
                                        SessionHandle sessionId,
                                        SessionEnvironment environment,
                                        ExecutorService operationExecutorService) {
        Configuration configuration =
                SessionContext.initializeConfiguration(defaultContext, environment, sessionId);

        final MutableURLClassLoader userClassLoader =
                FlinkUserCodeClassLoaders.create(
                        toURL(defaultContext.getDependencies()),
                        SessionContext.class.getClassLoader(),
                        configuration);
        final ResourceManager resourceManager = new ResourceManager(configuration, userClassLoader);

        return new UriSafeSessionContext(
                defaultContext,
                sessionId,
                environment.getSessionEndpointVersion(),
                configuration,
                userClassLoader,
                SessionContext.initializeSessionState(environment, configuration, resourceManager),
                new OperationManager(operationExecutorService));
    }

    public static URL[] toURL(List<URI> dependencies) {
        return dependencies.stream().map(uri -> {
            try {
                return uri.toURL();
            } catch (MalformedURLException e) {
                throw new RuntimeException("Invalid dependency URI: " + uri, e);
            }
        }).toArray(URL[]::new);
    }
}
