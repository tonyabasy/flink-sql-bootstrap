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
import java.util.Objects;

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
 *   <li><b>预置规格</b>：设置 {@code profile} 字段（如 {@code "stateless"}），自动匹配预置常量。</li>
 *   <li><b>显式值</b>：直接设置 {@code cpu}、{@code heap} 等字段。</li>
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
     * 无状态算子：0.5 CPU, 512 MB heap — filter, map, simple transform
     */
    public static final OperatorResourceSpec STATELESS = new OperatorResourceSpec(
            "stateless", 0.5, "512 MB", null, null);

    /**
     * 有状态算子：1.0 CPU, 2 GB heap, 256 MB managed — window, deduplicate
     */
    public static final OperatorResourceSpec STATEFUL = new OperatorResourceSpec(
            "stateful", 1.0, "2048 MB", null, "256 MB");

    /**
     * 双流 JOIN：1.0 CPU, 4 GB heap, 512 MB managed — interval join, lookup join
     */
    public static final OperatorResourceSpec JOIN_HEAVY = new OperatorResourceSpec(
            "join_heavy", 1.0, "4096 MB", null, "512 MB");

    /**
     * Sink 算子：0.5 CPU, 1 GB heap — jdbc sink, file sink
     */
    public static final OperatorResourceSpec SINK = new OperatorResourceSpec(
            "sink", 0.5, "1024 MB", null, null);

    private static final Map<String, OperatorResourceSpec> STANDARD = new LinkedHashMap<>();

    static {
        STANDARD.put("stateless", STATELESS);
        STANDARD.put("stateful", STATEFUL);
        STANDARD.put("join_heavy", JOIN_HEAVY);
        STANDARD.put("sink", SINK);
    }

    /**
     * 预置规格名称，如 "stateless"、"stateful"、"join_heavy"、"sink"。设置后优先于显式值。
     */
    private String profile;
    private Double cpu;
    private String heap;
    private String offHeap;
    private String managed;
    private Map<String, Double> external;

    OperatorResourceSpec(String profile, Double cpu, String heap, String offHeap,
                         String managed) {
        this.profile = profile;
        this.cpu = cpu;
        this.heap = heap;
        this.offHeap = offHeap;
        this.managed = managed;
        this.external = Collections.emptyMap();
    }

    public OperatorResourceSpec(Double cpu, String heap, String offHeap,
                                String managed, Map<String, Double> external) {
        this.cpu = cpu;
        this.heap = heap;
        this.offHeap = offHeap;
        this.managed = managed;
        this.external = external;
        this.profile = generateName(this);
    }

    /**
     * 解析为具体的资源值。若设置了 {@code profile}，则替换为对应的预置规格字段；
     * 否则保持原显式值。
     *
     * @return 解析后的 OperatorResource（新对象，不修改原对象）
     */
    public OperatorResourceSpec resolve() {
        if (profile == null) {
            profile = generateName(this);
            return this;
        }
        OperatorResourceSpec standardSpec = STANDARD.get(profile.toLowerCase(Locale.ROOT));
        return standardSpec != null ? standardSpec : this;
    }

    /**
     * 生成资源的确定性签名字符串。相同资源 → 相同签名，不同资源 → 不同签名。
     * 预置规格返回固定签名（如 "preset:small"），显式值通过 cpu + 内存拼装。
     *
     * <p>内存值会先经 {@link MemorySize#parse(String)} 规范化，
     * 保证 {@code "512 MB"} 和 {@code "512m"} 得到相同签名。
     *
     * @return 资源签名，如 {@code "stateless"}（预置规格）或 MD5 哈希（自定义规格）
     */
    public static String generateName(OperatorResourceSpec optResource) {
        // 匹配预置规格：资源值与 STANDARD 之一完全一致时返回预置名称
        for (Map.Entry<String, OperatorResourceSpec> entry : STANDARD.entrySet()) {
            if (resourceEquals(entry.getValue(), optResource)) {
                return entry.getKey();
            }
        }
        // 自定义规格：计算 MD5 签名
        StringBuilder sb = new StringBuilder();
        sb.append("cpu").append(optResource.cpu);

        if (optResource.heap != null && !optResource.heap.isEmpty()) {
            sb.append("-heap").append(MemorySize.parse(optResource.heap).getBytes()).append('b');
        } else {
            sb.append("-heap0b");
        }
        if (optResource.offHeap != null && !optResource.offHeap.isEmpty()) {
            sb.append("-offheap").append(MemorySize.parse(optResource.offHeap).getBytes()).append('b');
        } else {
            sb.append("-offheap0b");
        }
        if (optResource.managed != null && !optResource.managed.isEmpty()) {
            sb.append("-managed").append(MemorySize.parse(optResource.managed).getBytes()).append('b');
        } else {
            sb.append("-managed0b");
        }
        if (optResource.external != null && !optResource.external.isEmpty()) {
            optResource.external.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> sb.append('-').append(e.getKey()).append('=').append(e.getValue()));
        }
        return DigestUtils.md5Hex(sb.toString());
    }

    /**
     * 判断两个规格的资源值（CPU、内存、外部资源）是否完全一致。
     */
    public static boolean resourceEquals(OperatorResourceSpec a, OperatorResourceSpec b) {
        return Objects.equals(a.cpu, b.cpu)
                && Objects.equals(a.heap, b.heap)
                && Objects.equals(a.offHeap, b.offHeap)
                && Objects.equals(a.managed, b.managed)
                && externalEquals(a.external, b.external);
    }

    /**
     * 比较两个外部资源 map，null 与空 map 视为等价。
     */
    private static boolean externalEquals(Map<String, Double> a, Map<String, Double> b) {
        if (a == b) {
            return true;
        }
        boolean aEmpty = a == null || a.isEmpty();
        boolean bEmpty = b == null || b.isEmpty();
        if (aEmpty && bEmpty) {
            return true;
        }
        if (aEmpty || bEmpty) {
            return false;
        }
        return a.equals(b);
    }

    public static OperatorResourceSpec findInStandardSpec(String profile) {
        if (profile == null) {
            return null;
        }
        return STANDARD.get(profile.trim().toLowerCase(Locale.ROOT));
    }
}
