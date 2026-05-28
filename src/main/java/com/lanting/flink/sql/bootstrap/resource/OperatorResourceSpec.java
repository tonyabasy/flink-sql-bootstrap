package com.lanting.flink.sql.bootstrap.resource;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import lombok.Data;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.configuration.MemorySize;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

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
@JsonInclude(Include.NON_EMPTY)
public class OperatorResourceSpec {

    // ---- 预置规格常量 ----

    /**
     * 小杯：0.25 CPU, 512 MB heap
     */
    public static final OperatorResourceSpec SMALL = new OperatorResourceSpec(
            0.25, "512 MB", null, null, "small");

    /**
     * 普通杯：0.5 CPU, 1 GB heap
     */
    public static final OperatorResourceSpec NORMAL = new OperatorResourceSpec(
            0.5, "1024 MB", null, null, "normal");

    /**
     * 大杯：1.0 CPU, 2 GB heap, 256 MB managed
     */
    public static final OperatorResourceSpec LARGE = new OperatorResourceSpec(
            1.0, "2048 MB", null, "256 MB", "large");

    /**
     * 超大杯：2.0 CPU, 4 GB heap, 512 MB managed
     */
    public static final OperatorResourceSpec XLARGE = new OperatorResourceSpec(
            2.0, "4096 MB", null, "512 MB", "xlarge");

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
    private final String uniqName;
    private final Double cpuCores;
    private final String heapMemory;
    private final String offHeapMemory;
    private final String managedMemory;
    private final Map<String, Double> externalResources;

    OperatorResourceSpec(Double cpuCores, String heapMemory, String offHeapMemory,
                         String managedMemory, String uniqName) {
        this.cpuCores = cpuCores;
        this.heapMemory = heapMemory;
        this.offHeapMemory = offHeapMemory;
        this.managedMemory = managedMemory;
        this.externalResources = Collections.emptyMap();
        this.uniqName = uniqName;
    }

    public OperatorResourceSpec(Double cpuCores, String heapMemory, String offHeapMemory,
                                String managedMemory, Map<String, Double> externalResources) {
        this.cpuCores = cpuCores;
        this.heapMemory = heapMemory;
        this.offHeapMemory = offHeapMemory;
        this.managedMemory = managedMemory;
        this.externalResources = externalResources;
        this.uniqName = generateUniqName(this);
    }

    /**
     * 解析为具体的资源值。若设置了 {@code profile}，则替换为对应的预置规格字段；
     * 否则保持原显式值。
     *
     * @return 解析后的 OperatorResource（新对象，不修改原对象）
     */
    public OperatorResourceSpec resolve() {
        if (uniqName != null && !uniqName.isEmpty()) {
            OperatorResourceSpec preset = STANDARD.get(uniqName.toLowerCase(Locale.ROOT));
            if (preset != null) {
                return preset;
            }
            throw new IllegalArgumentException(
                    "Unknown resource profile: '" + uniqName
                            + "'. Available profiles: " + STANDARD.keySet());
        }
        return this;
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
