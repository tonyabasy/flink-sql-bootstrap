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

/**
 * {@link ClassLoader} 工具方法。
 *
 * @author wangzhao
 * @since 2026-05-28
 */
public class ClassUtils {

    /**
     * 获取当前可用的 ClassLoader，按优先级依次尝试：
     * <ol>
     *   <li>线程上下文 ClassLoader（{@link Thread#getContextClassLoader()}）</li>
     *   <li>加载本类的 ClassLoader（{@link Class#getClassLoader()}）</li>
     *   <li>系统 ClassLoader（{@link ClassLoader#getSystemClassLoader()}）</li>
     * </ol>
     *
     * @return 可用的 ClassLoader，仅在系统 ClassLoader 也无法访问时返回 {@code null}
     */
    public static ClassLoader getDefaultClassLoader() {
        ClassLoader cl = null;
        try {
            cl = Thread.currentThread().getContextClassLoader();
        } catch (Throwable ex) {
            // 无法访问线程上下文 ClassLoader，尝试下一个
        }
        if (cl == null) {
            // 回退到加载本类的 ClassLoader
            cl = ClassUtils.class.getClassLoader();
            if (cl == null) {
                // getClassLoader() 返回 null 表示由 Bootstrap ClassLoader 加载
                try {
                    cl = ClassLoader.getSystemClassLoader();
                } catch (Throwable ex) {
                    // 连系统 ClassLoader 也拿不到，调用方自行处理 null
                }
            }
        }
        return cl;
    }
}
