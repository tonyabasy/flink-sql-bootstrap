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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 单个算子的资源配置规则，通过 {@link #uid} 与 {@code Transformation.getUid()} 精确匹配。
 *
 * @author wangzhao
 * @since 2026-05-26
 */
@Data
@NoArgsConstructor
@JsonInclude(Include.NON_NULL)
public class OperatorSpec {
    /** 用于匹配 Transformation 的 UID，同时也是稳定 UID 的声明（覆盖 Flink 自动生成的值）。 */
    private String uid;
    /** 仅供人读，不参与匹配。 */
    private String name;
    /** 并行度，-1 表示不做变更。 */
    private int parallelism = -1;
    /** Chain 策略，对应 {@code ChainingStrategy} 枚举名，null 表示不做变更。 */
    private String chainStrategy;
    /** CPU + 内存资源规格，null 表示不做变更。 */
    private OperatorResourceSpec resource;
}
