package com.lanting.flink.sql.bootstrap.catalog;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * {@link CatalogEntity} 中的视图定义。
 * {@code expandedQuery} 是完全展开的 SQL 查询（无变量替换残留）。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@Accessors(fluent = true)
@AllArgsConstructor
@NoArgsConstructor
public class ViewEntity {
    @JsonProperty("database")
    String database;
    @JsonProperty("name")
    String name;
    @JsonProperty("comment")
    String comment;
    @JsonProperty("expandedQuery")
    String expandedQuery;
}
