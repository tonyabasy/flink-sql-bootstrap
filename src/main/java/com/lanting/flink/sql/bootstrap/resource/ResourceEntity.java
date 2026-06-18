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
package com.lanting.flink.sql.bootstrap.resource;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 对应 resource-hint.json 的顶层结构，描述对整个 Flink 作业中各个算子的资源配置。
 *
 * <h3>JSON 示例</h3>
 * <pre>{@code
 * {
 *   "version": 1,
 *   "defaultParallelism": 2,
 *   "operators": [
 *     {
 *       "uid": "1_source",
 *       "name": "user_events[1]",
 *       "parallelism": 2,
 *       "chainStrategy": "ALWAYS",
 *       "resource": {
 *         "profile": "stateless"
 *       }
 *     },
 *     {
 *       "uid": "4_group-aggregate",
 *       "name": "GroupAggregate[4]",
 *       "parallelism": 1,
 *       "resource": {
 *         "cpu": 1.0,
 *         "heap": "2048m",
 *         "managed": "256m"
 *       }
 *     }
 *   ]
 * }
 * }</pre>
 *
 * <p>{@code defaultParallelism} 为 0 表示不做全局覆盖，以代码或 Flink 配置中的并行度为准。
 *
 * @author wangzhao
 * @since 2026-05-26
 */
@Data
@NoArgsConstructor
@JsonInclude(Include.NON_DEFAULT)
public class ResourceEntity {

    private int version;
    /** 全局默认并行度，0 表示不启用。仅当算子未单独配置并行度时生效。 */
    private int defaultParallelism;
    private List<OperatorEntity> operators;

    /**
     * 根据 UID 查找对应的算子配置规则。
     *
     * @param uid Transformation UID
     * @return 匹配的 OperatorSpec，未找到返回 null
     */
    public OperatorEntity findByUid(String uid) {
        if (uid == null || operators == null) {
            return null;
        }
        return operators.stream()
                .filter(h -> uid.equals(h.getUid()))
                .findFirst()
                .orElse(null);
    }

    /**
     * 根据算子名称查找对应的算子配置规则（名称匹配作为 UID 匹配的兜底策略）。
     *
     * <p>用于 {@code --init-resource} 模式生成初始配置后，用户可以通过算子名称
     * 匹配并覆盖自动生成的 UID。
     *
     * @param name Transformation 名称
     * @return 匹配的 OperatorSpec，未找到返回 null
     */
    public OperatorEntity findByName(String name) {
        if (name == null || operators == null) {
            return null;
        }
        return operators.stream()
                .filter(h -> name.equals(h.getName()))
                .findFirst()
                .orElse(null);
    }
}
