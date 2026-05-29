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
package com.lanting.flink.sql.bootstrap.util;

import org.apache.flink.api.dag.Transformation;

import java.util.*;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

/**
 * Flink Transformation DAG 的 ASCII 艺术打印工具。
 * 将 {@link Transformation} 拓扑图渲染为终端友好的竖向树形图，
 * 自动处理节点定位、单/多输入连线、Union 汇聚等布局。
 *
 * @author wangzhao
 * @since 2026-05-29
 */
public class PrintUtils {

    private static final int SLOT = 30;  // 每个节点的横向槽宽（字符数），越长节点标签越不容易重叠
    private static final int ROW_H = 5;   // 每层占用的行数（节点名 1 行 + 连线 4 行）
    private static final int PAD = 1;   // 画布上下各留的空白行

    /**
     * 打印一行文本到控制台
     */
    public static void print(String s) {
        System.out.println(s);
    }

    /**
     * 打印 CLI 帮助信息
     */
    public static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        String cmdLineSyntax = "flink run [flink-options] <jar> [--script-file <file> | --script <sql>] "
                + "[--resource-file <file> | --resource <json>] "
                + "[--catalog-file <file> | --catalog <json>] "
                + "[--dependency <jar1>,<jar2>,...]";
        String header = "Flink SQL 算子资源调优工具 — 在 SQL 提交前按算子注入 CPU/内存/并行度配置。\n\n"
                + "用法示例:\n"
                + "  flink run target/flink-sql-bootstrap.jar \\\n"
                + "    --script-file /path/to/job.sql \\\n"
                + "    --resource-file /path/to/resource-hint.json \\\n"
                + "    --dependency /path/to/udf.jar\n\n"
                + "注意: yarn-application / k8s-application 模式下不能传本地绝对路径，\n"
                + "      请使用 --script/--resource/--catalog 内联传内容，或将文件 ship 到容器内。\n";
        String footer = "";
        formatter.printHelp(cmdLineSyntax, header, options, footer, true);
    }

    /**
     * 竖向 ASCII DAG 打印器，风格对齐 Flink 官方文档示意图。
     * <p>
     * 输出示例：
     * <pre>
     *      Source          Source
     *        +               +
     *        |               |
     *        v               v
     *    Rebalance      HashPartition
     *        +               +
     *        |               |
     *        +------&gt;Union&lt;--+
     *                  +
     *                  |
     *                  v
     *                Split
     * </pre>
     */
    public static void printDAG(List<Transformation<?>> sinks) {
        DAGPrinter.print(sinks);
    }
}
