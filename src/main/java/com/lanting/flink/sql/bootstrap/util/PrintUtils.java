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
    /**
     * 打印一行文本到控制台
     */
    public static void println(String s) {
        System.out.println(s);
    }

    /**
     * 打印 CLI 帮助信息
     */
    public static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        String cmdLineSyntax = "flink run [flink-options] <jar> "
                + "[--script-file <file> | --script <sql>] "
                + "[--resource-file <file> | --resource <json>] "
                + "[--catalog-file <file> | --catalog <json>] "
                + "[--dependency <jar> ...]";
        String header = "Flink SQL Bootstrap — custom Catalog snapshots, Multi-Statement SQL Script deployment and fine-grained resource tuning.\n\n"
                + "Examples:\n"
                + "  flink run target/flink-sql-bootstrap.jar \\\n"
                + "    --script-file /path/to/job.sql \\\n"
                + "    --resource-file /path/to/resource-hint.json \\\n"
                + "    --dependency /path/to/udf.jar\n\n"
                + "Note: In Application Mode, absolute local paths are unavailable.\n"
                + "      Use --script / --resource / --catalog for inline values\n"
                + "      or ship files into the container.\n\n";
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
    @Experimental
    public static void printDAG(List<Transformation<?>> sinks) {
        DAGPrinter.print(sinks);
    }
}
