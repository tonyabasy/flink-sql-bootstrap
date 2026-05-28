package com.lanting.flink.sql.bootstrap.catalog;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Map;

/**
 * {@link CatalogEntity} 中的用户自定义函数条目。
 *
 * <p>{@code kind} 取值为：{@code SCALAR}、{@code TABLE}、{@code AGGREGATE} 之一。
 * {@code jarRef} 是 UDF JAR 在 {@code pipeline.jars} 中出现的文件名；
 * Cluster Adapter 负责在提交前上传 JAR。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@Accessors(fluent = true)
@AllArgsConstructor
@NoArgsConstructor
public class UdfEntity {
    @JsonProperty("database")
    String database;
    @JsonProperty("name")
    String name;
    @JsonProperty("kind")
    String kind;
    @JsonProperty("className")
    String className;
    @JsonProperty("description")
    String description;
    @JsonProperty("functionLanguage")
    String functionLanguage;
    @JsonProperty("jarRef")
    String jarRef;
    /**
     * 可选的类型推断提示，按原样转发为 {@code FunctionDefinition} 属性。
     * Null 表示 Flink 通过反射从类推断类型。
     */
    @JsonProperty("typeInference")
    Map<String, String> typeInference;
}
