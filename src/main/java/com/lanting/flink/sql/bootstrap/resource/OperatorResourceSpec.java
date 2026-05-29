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

import org.apache.flink.configuration.MemorySize;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * CPU + 内存资源规格。
 *
 * <p>支持两种配置方式：
 * <ol>
 *   <li><b>预置规格</b>：设置 {@code profile} 字段（如 {@code "small"}），自动匹配预置常量。</li>
 *   <li><b>显式值</b>：直接设置 {@code cpuCores}、{@code heapMemory} 等字段。</li>
 * </ol>
 * 通过 {@link #resolve()} 统一解析为具体的资源值。
 *
 * @author wangzhao
 * @since 2026-05-26
 */
@Data
@NoArgsConstructor
@JsonInclude(Include.NON_EMPTY)
public class OperatorResourceSpec {

    // ---- 预置规格常量 ----

    /**
     * 小杯：0.25 CPU, 512 MB heap
     */
    public static final OperatorResourceSpec SMALL = new OperatorResourceSpec(
            "small", 0.25, "512 MB", null, null);

    /**
     * 普通杯：0.5 CPU, 1 GB heap
     */
    public static final OperatorResourceSpec NORMAL = new OperatorResourceSpec(
            "normal", 0.5, "1024 MB", null, null);

    /**
     * 大杯：1.0 CPU, 2 GB heap, 256 MB managed
     */
    public static final OperatorResourceSpec LARGE = new OperatorResourceSpec(
            "large", 1.0, "2048 MB", null, "256 MB");

    /**
     * 超大杯：2.0 CPU, 4 GB heap, 512 MB managed
     */
    public static final OperatorResourceSpec XLARGE = new OperatorResourceSpec(
            "xlarge", 2.0, "4096 MB", null, "512 MB");

    private static final Map<String, OperatorResourceSpec> STANDARD = new LinkedHashMap<>();

    static {
        STANDARD.put("small", SMALL);
        STANDARD.put("normal", NORMAL);
        STANDARD.put("large", LARGE);
        STANDARD.put("xlarge", XLARGE);
    }

    /**
     * 预置规格名称，如 "small"、"normal"、"large"、"xlarge"。设置后优先于显式值。
     */
    private String uniqName;
    private Double cpuCores;
    private String heapMemory;
    private String offHeapMemory;
    private String managedMemory;
    private Map<String, Double> externalResources;

    OperatorResourceSpec(String uniqName, Double cpuCores, String heapMemory, String offHeapMemory,
                         String managedMemory) {
        this.uniqName = uniqName;
        this.cpuCores = cpuCores;
        this.heapMemory = heapMemory;
        this.offHeapMemory = offHeapMemory;
        this.managedMemory = managedMemory;
        this.externalResources = Collections.emptyMap();
    }

    public OperatorResourceSpec(Double cpuCores, String heapMemory, String offHeapMemory,
                                String managedMemory, Map<String, Double> externalResources) {
        this.uniqName = generateUniqName(this);
        this.cpuCores = cpuCores;
        this.heapMemory = heapMemory;
        this.offHeapMemory = offHeapMemory;
        this.managedMemory = managedMemory;
        this.externalResources = externalResources;
    }

    public String getUniqName() {
        if (uniqName == null) {
            uniqName = generateUniqName(this);
        }
        return uniqName;
    }

    /**
     * 解析为具体的资源值。若设置了 {@code profile}，则替换为对应的预置规格字段；
     * 否则保持原显式值。
     *
     * @return 解析后的 OperatorResource（新对象，不修改原对象）
     */
    public OperatorResourceSpec resolve() {
        return uniqName != null && STANDARD.get(uniqName.toLowerCase(Locale.ROOT)) == null ?
                STANDARD.get(uniqName.toLowerCase(Locale.ROOT)) : this;
    }

    /**
     * 生成资源的确定性签名字符串。相同资源 → 相同签名，不同资源 → 不同签名。
     * 预置规格返回固定签名（如 "preset:small"），显式值通过 cpu + 内存拼装。
     *
     * <p>内存值会先经 {@link MemorySize#parse(String)} 规范化，
     * 保证 {@code "512 MB"} 和 {@code "512m"} 得到相同签名。
     *
     * @return 资源签名，如 {@code "cpu0.25-heap524288000b"} 或 {@code "preset:small"}
     */
    public static String generateUniqName(OperatorResourceSpec optResource) {
        StringBuilder sb = new StringBuilder();
        sb.append("cpu").append(optResource.cpuCores);

        if (optResource.heapMemory != null && !optResource.heapMemory.isEmpty()) {
            sb.append("-heap").append(MemorySize.parse(optResource.heapMemory).getBytes()).append('b');
        } else {
            sb.append("-heap0b");
        }
        if (optResource.offHeapMemory != null && !optResource.offHeapMemory.isEmpty()) {
            sb.append("-offheap").append(MemorySize.parse(optResource.offHeapMemory).getBytes()).append('b');
        } else {
            sb.append("-offheap0b");
        }
        if (optResource.managedMemory != null && !optResource.managedMemory.isEmpty()) {
            sb.append("-managed").append(MemorySize.parse(optResource.managedMemory).getBytes()).append('b');
        } else {
            sb.append("-managed0b");
        }
        if (optResource.externalResources != null && !optResource.externalResources.isEmpty()) {
            optResource.externalResources.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> sb.append('-').append(e.getKey()).append('=').append(e.getValue()));
        }
        return DigestUtils.md5Hex(sb.toString());
    }

    public static OperatorResourceSpec findInStandardSpec(String profile) {
        if (profile == null) {
            return null;
        }
        return STANDARD.get(profile.trim().toLowerCase(Locale.ROOT));
    }
}
