package com.lanting.flink.sql.bootstrap.catalog;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * 表级别的主键声明。
 * {@code enforced = false} 对应 {@code NOT ENFORCED}。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@Accessors(fluent = true)
@AllArgsConstructor
@NoArgsConstructor
public class PrimaryKeyEntity {
    @JsonProperty("constraintName")
    String constraintName;
    @JsonProperty("columnNames")
    List<String> columnNames;
    @JsonProperty("enforced")
    boolean enforced;
}
