package com.lanting.flink.sql.bootstrap.catalog;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * 表级别的 watermark 声明。
 * 对应：{@code WATERMARK FOR <rowtimeColumn> AS <expr>}
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@Accessors(fluent = true)
@AllArgsConstructor
@NoArgsConstructor
public class WatermarkEntity {
    @JsonProperty("rowtimeColumn")
    String rowtimeColumn;
    @JsonProperty("expression")
    String expression;
}
