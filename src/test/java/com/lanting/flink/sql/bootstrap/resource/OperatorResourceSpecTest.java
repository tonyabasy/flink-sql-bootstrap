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

import static org.junit.jupiter.api.Assertions.*;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * OperatorResourceSpec.generateName 测试。
 * 覆盖正向、逆向、边界场景，暴露现有代码问题。
 */
class OperatorResourceSpecTest {

    // ─── 正向 ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("全字段填充：所有字段参与拼接")
    void allFields() {
        Map<String, Double> external = new LinkedHashMap<>();
        external.put("gpu", 1.0);
        external.put("fpga", 2.0);
        OperatorResourceSpec spec = new OperatorResourceSpec(1.0, "2048 MB", "256 MB", "128 MB", external);
        String name = OperatorResourceSpec.generateName(spec);

        assertNotNull(name);
        assertEquals(32, name.length());
    }

    @Test
    @DisplayName("仅 CPU + Heap：匹配预置规格时返回预置名称")
    void onlyCpuAndHeap() {
        OperatorResourceSpec spec = new OperatorResourceSpec(0.5, "1024 MB", null, null, null);
        String name = OperatorResourceSpec.generateName(spec);

        // 0.5 CPU + 1024 MB heap 匹配 NORMAL 预置规格
        assertEquals("normal", name);
    }

    @Test
    @DisplayName("带 external：key 排序无关插入顺序")
    void withExternal() {
        Map<String, Double> inserted = new LinkedHashMap<>();
        inserted.put("gpu", 2.0);
        inserted.put("fpga", 1.0);
        OperatorResourceSpec spec = new OperatorResourceSpec(1.0, "512 MB", null, null, inserted);
        String name1 = OperatorResourceSpec.generateName(spec);

        // 颠倒插入顺序
        Map<String, Double> reversed = new LinkedHashMap<>();
        reversed.put("fpga", 1.0);
        reversed.put("gpu", 2.0);
        OperatorResourceSpec spec2 = new OperatorResourceSpec(1.0, "512 MB", null, null, reversed);
        String name2 = OperatorResourceSpec.generateName(spec2);

        assertEquals(name1, name2);
    }

    @Test
    @DisplayName("相同输入两次：签名一致（确定性）")
    void deterministic() {
        OperatorResourceSpec spec = new OperatorResourceSpec(1.0, "1024 MB", null, null, null);
        String name1 = OperatorResourceSpec.generateName(spec);
        String name2 = OperatorResourceSpec.generateName(spec);

        assertEquals(name1, name2);
    }

    @Test
    @DisplayName("预置规格 SMALL：返回固定名称 'small'")
    void presetSmall() {
        String name = OperatorResourceSpec.generateName(OperatorResourceSpec.SMALL);
        assertEquals("small", name);
    }

    @Test
    @DisplayName("预置规格 NORMAL：返回固定名称 'normal'")
    void presetNormal() {
        String name = OperatorResourceSpec.generateName(OperatorResourceSpec.NORMAL);
        assertEquals("normal", name);
    }

    @Test
    @DisplayName("预置规格 LARGE：返回固定名称 'large'")
    void presetLarge() {
        String name = OperatorResourceSpec.generateName(OperatorResourceSpec.LARGE);
        assertEquals("large", name);
    }

    @Test
    @DisplayName("预置规格 XLARGE：返回固定名称 'xlarge'")
    void presetXlarge() {
        String name = OperatorResourceSpec.generateName(OperatorResourceSpec.XLARGE);
        assertEquals("xlarge", name);
    }

    // ─── 逆向 ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName(" cpu 为 null：拼接为 'null' 字符串，不抛异常")
    void nullCpu() {
        OperatorResourceSpec spec = new OperatorResourceSpec();
        spec.setCpu(null);
        spec.setHeap("512 MB");
        // 不会 NPE，append(Double) 走 append(Object) 拼出 "null"
        String name = OperatorResourceSpec.generateName(spec);
        assertNotNull(name);
        assertEquals(32, name.length());
    }

    @Test
    @DisplayName("所有内存字段为 null：全部走默认 0 分支，不抛异常")
    void allMemoryNull() {
        OperatorResourceSpec spec = new OperatorResourceSpec();
        spec.setCpu(1.0);
        spec.setHeap(null);
        spec.setOffHeap(null);
        spec.setManaged(null);
        // 不应抛异常
        String name = OperatorResourceSpec.generateName(spec);
        assertNotNull(name);
        assertEquals(32, name.length());
    }

    @Test
    @DisplayName("external 为 null：跳过外部资源，不抛异常")
    void nullExternal() {
        OperatorResourceSpec spec = new OperatorResourceSpec();
        spec.setCpu(1.0);
        spec.setHeap("512 MB");
        spec.setExternal(null);
        // 不应抛 NPE
        String name = OperatorResourceSpec.generateName(spec);
        assertNotNull(name);
    }

    @Test
    @DisplayName("external 为空 map：跳过外部资源，不抛异常")
    void emptyExternal() {
        OperatorResourceSpec spec = new OperatorResourceSpec();
        spec.setCpu(1.0);
        spec.setHeap("512 MB");
        spec.setExternal(new LinkedHashMap<>());
        String name = OperatorResourceSpec.generateName(spec);
        assertNotNull(name);
    }

    // ─── 边界 ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("内存单位归一化：'512 MB' 与 '512m' 返回相同签名")
    void memoryUnitNormalization() {
        OperatorResourceSpec spec1 = new OperatorResourceSpec(1.0, "512 MB", null, null, null);
        OperatorResourceSpec spec2 = new OperatorResourceSpec(1.0, "512m", null, null, null);
        assertEquals(
                OperatorResourceSpec.generateName(spec1),
                OperatorResourceSpec.generateName(spec2));
    }

    @Test
    @DisplayName(" cpu = 0.0：不应抛异常")
    void zeroCpu() {
        OperatorResourceSpec spec = new OperatorResourceSpec();
        spec.setCpu(0.0);
        spec.setHeap("512 MB");
        // 不应抛异常
        String name = OperatorResourceSpec.generateName(spec);
        assertNotNull(name);
        assertEquals(32, name.length());
    }

    @Test
    @DisplayName("heap 为空字符串：走 else 分支拼 -heap0b，不抛异常")
    void emptyHeap() {
        OperatorResourceSpec spec = new OperatorResourceSpec();
        spec.setCpu(1.0);
        spec.setHeap("");
        // "".isEmpty() == true → 不走 MemorySize.parse()，走 else 分支
        String name = OperatorResourceSpec.generateName(spec);
        assertNotNull(name);
        assertEquals(32, name.length());
    }
}
