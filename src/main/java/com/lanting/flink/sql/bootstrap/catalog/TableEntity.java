package com.lanting.flink.sql.bootstrap.catalog;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Map;

/**
 * {@link CatalogEntity} 中的表或物化视图定义。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@Accessors(fluent = true)
@AllArgsConstructor
@NoArgsConstructor
public class TableEntity {

    @JsonProperty("database")
    String database;
    @JsonProperty("name")
    String name;
    @JsonProperty("comment")
    String comment;
    @JsonProperty("columns")
    List<ColumnEntity> columns;
    @JsonProperty("watermark")
    WatermarkEntity watermark;
    @JsonProperty("primaryKey")
    PrimaryKeyEntity primaryKey;
    @JsonProperty("partitionKeys")
    List<String> partitionKeys;
    @JsonProperty("options")
    Map<String, String> options;
}
