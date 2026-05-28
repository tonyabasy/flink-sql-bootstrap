package com.lanting.flink.sql.bootstrap.catalog;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * META-INF/catalog-snapshot.json 的根对象。
 *
 * <p>表示发布时捕获的完全展开、自包含的 DDL 状态。
 * 序列化到作业 JAR 后，此对象是 Flink Catalog 的唯一事实来源；
 * 运行时无需访问 MySQL 或 LantingFS。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@Accessors(fluent = true)
@AllArgsConstructor
@NoArgsConstructor
public class CatalogEntity {

    public static final CatalogEntity EMPTY;

    static {
        EMPTY = new CatalogEntity();
        EMPTY.version = 1;
        EMPTY.snapshotId = "empty";
        EMPTY.catalogName = "platform";
        EMPTY.databaseName = "default";
        EMPTY.flinkVersion = "unknown";
    }


    @JsonProperty("version")
    int version;
    @JsonProperty("snapshotId")
    String snapshotId;
    @JsonProperty("catalogName")
    String catalogName;
    @JsonProperty("databaseName")
    String databaseName;
    @JsonProperty("flinkVersion")
    String flinkVersion;
    @JsonProperty("createdAt")
    long createdAt;
    @JsonProperty("tables")
    List<TableEntity> tables;
    @JsonProperty("views")
    List<ViewEntity> views;
    @JsonProperty("udfs")
    List<UdfEntity> udfs;

    /**
     * 空快照 — 当文件不存在或为空时使用。
     */
    public static CatalogEntity empty() {
        return EMPTY;
    }
}