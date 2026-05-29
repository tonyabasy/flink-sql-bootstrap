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
package com.lanting.flink.sql.bootstrap;

import static com.lanting.flink.sql.bootstrap.util.PrintUtils.*;

import com.lanting.flink.sql.bootstrap.catalog.CatalogEntityFactory;
import com.lanting.flink.sql.bootstrap.executor.StreamingScriptExecutor;
import com.lanting.flink.sql.bootstrap.flink.UriSafeSessionContext;
import com.lanting.flink.sql.bootstrap.resource.ResourceEntity;
import com.lanting.flink.sql.bootstrap.util.ClassUtils;
import com.lanting.flink.sql.bootstrap.util.JSON;

import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.delegation.InternalPlan;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;
import org.apache.flink.table.gateway.service.application.Printer;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.gateway.service.context.SessionContext;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.Executors;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;

/**
 * Flink SQL 作业的 CLI 入口，支持在执行前对算子进行细粒度资源注入。
 *
 * <h3>功能</h3>
 * <ul>
 *   <li>通过 {@code --script} / {@code --scriptUri} 传入 SQL 脚本</li>
 *   <li>通过 {@code --resource} / {@code --resourceUri} 传入算子资源配置 JSON</li>
 *   <li>通过 {@code --catalog} / {@code --catalogUri} 传入 Catalog 快照 JSON（可选）</li>
 * </ul>
 *
 * <h3>执行流程</h3>
 * <ol>
 *   <li>解析 CLI 参数，读取 SQL / 资源 / Catalog 内容</li>
 *   <li>构建 {@link SessionContext}（含 Flink Configuration 和 UDF 依赖）</li>
 *   <li>注册 Catalog（如有）</li>
 *   <li>委托 {@link StreamingScriptExecutor} 执行 SQL，在执行前注入算子资源</li>
 * </ol>
 *
 * @author wangzhao
 * @see StreamingScriptExecutor
 * @since 2026-05-14
 */
public class SqlEntryPoint {
    private static final ClassLoader cl = ClassUtils.getDefaultClassLoader();

    // 每个资源类型有两组选项：
    //   1. --xxx      直接传值（字符串路径或内容）
    //   2. --xxxUri   传 URI（支持 file://、http://、hdfs:// 等）
    // 两组互斥，不能同时指定。

    public static final Option OPTION_HELP =
            Option.builder()
                    .option("h")
                    .longOpt("help")
                    .numberOfArgs(0)
                    .desc("Display help documentation.")
                    .build();

    public static final Option OPTION_SQL_FILE =
            Option.builder()
                    .option("sf")
                    .longOpt("script-file")
                    .numberOfArgs(1)
                    .desc("SQL script file URI. It supports to fetch files from the DFS or HTTP.")
                    .build();

    public static final Option OPTION_SQL_SCRIPT =
            Option.builder()
                    .option("s")
                    .longOpt("script")
                    .numberOfArgs(1)
                    .desc("Script content.")
                    .build();

    public static final Option OPTION_RESOURCE_CONF =
            Option.builder()
                    .option("r")
                    .longOpt("resource")
                    .numberOfArgs(1)
                    .desc("Path to the resource configuration JSON file for per-operator tuning (parallelism, CPU, memory, chaining strategy). Use --init-resource to generate a template.")
                    .build();

    public static final Option OPTION_RESOURCE_CONF_FILE =
            Option.builder()
                    .option("rf")
                    .longOpt("resource-file")
                    .numberOfArgs(1)
                    .desc("Resource configuration file URI. It supports to fetch files from the DFS or HTTP.")
                    .build();

    public static final Option OPTION_CATALOG_CONF =
            Option.builder()
                    .option("c")
                    .longOpt("catalog")
                    .numberOfArgs(1)
                    .desc("Path to the catalog config JSON file containing pre-captured table/view/UDF definitions. Optional — if omitted, DDL statements in the SQL script are used instead.")
                    .build();

    public static final Option OPTION_CATALOG_CONF_FILE =
            Option.builder()
                    .option("cf")
                    .longOpt("catalog-file")
                    .numberOfArgs(1)
                    .desc("Catalog configuration file URI. It supports to fetch files from the DFS or HTTP.")
                    .build();

    public static final Option OPTION_DEPENDENCIES =
            Option.builder()
                    .option("d")
                    .longOpt("dependency")
                    .numberOfArgs(Option.UNLIMITED_VALUES)
                    .desc("Local path to a dependency JAR file (e.g. UDF jars). Can be specified multiple times. JARs are appended to pipeline.classpaths and distributed to TaskManagers via BlobServer. Equivalent to 'flink run -C'.")
                    .build();

    public static final Option OPTION_SCRIPT_VALIDATE =
            Option.builder()
                    .longOpt("compile")
                    .numberOfArgs(0)
                    .desc("Parse, validate and compile the SQL script without submitting any job. Recommend use 'local' target.")
                    .build();

    public static final Option OPTION_INIT_RESOURCE =
            Option.builder()
                    .longOpt("init-resource")
                    .numberOfArgs(0)
                    .desc("Init resource configuration with current SQL script. Recommend use 'local' target.")
                    .build();

    public static void main(String[] args) throws Exception {
        ArgsContent argsContent = parseOptions(args);
        if (argsContent.isHelp) {
            printHelp(getCliOptions());
            return;
        }

        // 强制开启 UID 生成，确保每个 Transformation 都有稳定的 UID，
        // 这是后续通过 JSON 配置精确匹配算子的前提。
        Configuration innerConfig = new Configuration();
        innerConfig.set(ExecutionConfigOptions.TABLE_EXEC_UID_GENERATION,
                ExecutionConfigOptions.UidGeneration.ALWAYS);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(innerConfig);

        // 合并依赖并写入 Configuration，通过 Pipeline.classpath -> BlobServer -> TM Download & load classpath 链路加载到各个 TM 节点
        Configuration configuration = (Configuration) env.getConfiguration();
        List<URI> mergedDependencies = mergeDependencies(ConfigUtils.decodeListFromConfig(configuration, PipelineOptions.CLASSPATHS, URI::create), argsContent.dependencies);
        ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.CLASSPATHS, mergedDependencies, URI::toString);

        // 从配置 pipeline.jars 中获取依赖 Jar（如：UDFs Jar），如果使用 flink run 启动该配置是 -C 参数指定的 classpath
        DefaultContext defaultContext = new DefaultContext(
                (Configuration) env.getConfiguration(), mergedDependencies);

        // Catalog 快照是可选的 — 不传则由 SQL 中的 DDL 自行建表
        SessionEnvironment.Builder builder = SessionEnvironment.newBuilder()
                .setSessionEndpointVersion(SqlGatewayRestAPIVersion.getDefaultVersion());
        if (argsContent.catalog != null) {
            // 从 JSON 快照恢复 Catalog（免网络依赖）
            GenericInMemoryCatalog catalog = CatalogEntityFactory.from(argsContent.catalog);
            builder.registerCatalog(catalog.getName(), catalog)
                    .setDefaultCatalog(catalog.getName());
        }
        SessionEnvironment ssEnv = builder.build();

        SessionHandle sessionHandle = new SessionHandle(UUID.randomUUID());
        SessionContext sessionContext = UriSafeSessionContext.create(
                defaultContext, sessionHandle, ssEnv, Executors.newDirectExecutorService());

        // 委托 StreamingScriptExecutor：切分 SQL → translate → 注入资源 → 提交
        try (AutoCloseable ignore = sessionContext::close) {
            StreamingScriptExecutor executor = new StreamingScriptExecutor(
                    sessionContext, argsContent.script, argsContent.resource,
                    new Printer(System.out));

            if (argsContent.isValidate) {
                // 仅校验，不执行
                InternalPlan compiledPlan = executor.compile(argsContent.script);
                print("\n" + compiledPlan.asJsonString() + "\n");
                print("Validate the SQL script successfully.");
            } else if (argsContent.isInitResource) {
                // 仅生成 Resource 初始化配置，不执行
                ResourceEntity res = executor.generateResultSpec();
                print(JSON.toJSONString(res, true));
            } else {
                // 执行 SQL Script
                TableResult execute = executor.execute();
                // FIXME 这里的 Result 都有哪些内容？哪些是用户需要关心的？只 print 够吗？
                execute.print();
            }
        }
    }

    /**
     * 将来自 classpath 的依赖和来自入参的依赖合并
     */
    private static List<URI> mergeDependencies(List<URI> depsFromClasspath, List<URI> depsFromArgs) {
        Set<URI> dependencies = new HashSet<>();
        if (depsFromClasspath != null) {
            dependencies.addAll(depsFromClasspath);
        }
        if (depsFromArgs != null) {
            dependencies.addAll(depsFromArgs);
        }
        return new ArrayList<>(dependencies);
    }

    static Options getCliOptions() {
        Options options = new Options();
        options.addOption(OPTION_HELP);
        options.addOption(OPTION_SQL_FILE);
        options.addOption(OPTION_SQL_SCRIPT);
        options.addOption(OPTION_RESOURCE_CONF);
        options.addOption(OPTION_RESOURCE_CONF_FILE);
        options.addOption(OPTION_CATALOG_CONF);
        options.addOption(OPTION_CATALOG_CONF_FILE);
        options.addOption(OPTION_DEPENDENCIES);
        options.addOption(OPTION_INIT_RESOURCE);
        options.addOption(OPTION_SCRIPT_VALIDATE);
        return options;
    }

    /**
     * 解析命令行参数，返回结构化的 {@link ArgsContent}。
     *
     * <p>对每个资源类型（script / catalog / resource），{@code --xxx} 和 {@code --xxxUri}
     * 两组互斥 — 前者直接传值，后者传 URI（支持 file://、http://、hdfs:// 等），
     * 通过 {@link #getContent(String)} 统一读取。
     */
    static ArgsContent parseOptions(String[] args) {
        try {
            DefaultParser parser = new DefaultParser();
            CommandLine line = parser.parse(getCliOptions(), args);

            // --help
            if (line.hasOption(OPTION_HELP)) {
                return ArgsContent.help();
            }

            String script = getContent(line.getOptionValue(OPTION_SQL_FILE.getLongOpt()));
            if (script == null) {
                script = Preconditions.checkNotNull(
                        line.getOptionValue(OPTION_SQL_SCRIPT.getLongOpt()),
                        "Please use \"--script\" or \"--scriptUri\" to specify script either.");
            } else {
                Preconditions.checkArgument(
                        line.getOptionValue(OPTION_SQL_SCRIPT.getLongOpt()) == null,
                        "Don't set \"--script\" or \"--scriptUri\" together.");
            }

            String catalog = getContent(line.getOptionValue(OPTION_CATALOG_CONF_FILE.getLongOpt()));
            if (catalog == null) {
                catalog = line.getOptionValue(OPTION_CATALOG_CONF.getLongOpt());
            } else {
                Preconditions.checkArgument(
                        line.getOptionValue(OPTION_CATALOG_CONF.getLongOpt()) == null,
                        "Don't set \"--catalog\" or \"--catalogUri\" together.");
            }

            String resource = getContent(line.getOptionValue(OPTION_RESOURCE_CONF_FILE.getLongOpt()));
            if (resource == null) {
                resource = line.getOptionValue(OPTION_RESOURCE_CONF.getLongOpt());
            } else {
                Preconditions.checkArgument(
                        line.getOptionValue(OPTION_RESOURCE_CONF.getLongOpt()) == null,
                        "Don't set \"--resource\" or \"--resourceUri\" together.");
            }

            String[] dependencies = line.getOptionValues(OPTION_DEPENDENCIES.getLongOpt());

            boolean isValidate = line.hasOption(OPTION_SCRIPT_VALIDATE.getLongOpt());
            boolean isInitResource = line.hasOption(OPTION_INIT_RESOURCE.getLongOpt());
            if (isValidate && isInitResource) {
                throw new IllegalArgumentException("Don't set \"--validate\" or \"--init-resource\" together.");
            }

            return new ArgsContent(script, catalog, resource, dependencies, isValidate, isInitResource);

        } catch (ParseException | URISyntaxException e) {
            throw new IllegalArgumentException("Failed to parse args. It should never happens.", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("Can not read files to execute.", e);
        }
    }

    private static @Nullable String getContent(@Nullable String filePath)
            throws IOException, URISyntaxException {
        if (filePath == null) {
            return null;
        }

        URI resolvedUri = resolveURI(filePath);
        if (resolvedUri.getScheme().equals("http") || resolvedUri.getScheme().equals("https")) {
            return readFromHttp(resolvedUri);
        } else if (resolvedUri.getScheme().equals("classpath")) {
            return readFromClasspathUtf8(resolvedUri);
        } else {
            return readFileUtf8(resolvedUri);
        }
    }

    private static URI resolveURI(String path) {
        final URI uri = URI.create(path);
        if (uri.getScheme() != null) {
            return uri;
        }
        return new File(path).getAbsoluteFile().toURI();
    }

    private static String readFromHttp(URI uri) throws IOException {
        URL url = uri.toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestMethod("GET");

        try (InputStream inputStream = conn.getInputStream();
             ByteArrayOutputStream targetFile = new ByteArrayOutputStream()) {
            IOUtils.copy(inputStream, targetFile);
            return targetFile.toString(StandardCharsets.UTF_8);
        }
    }

    private static String readFileUtf8(URI uri) throws IOException {
        org.apache.flink.core.fs.Path path = new org.apache.flink.core.fs.Path(uri);
        FileSystem fs = path.getFileSystem();
        try (FSDataInputStream inputStream = fs.open(path)) {
            return new String(
                    FileUtils.read(inputStream, (int) fs.getFileStatus(path).getLen()),
                    StandardCharsets.UTF_8);
        }
    }

    private static String readFromClasspathUtf8(URI uri) throws IOException {
        String classpath = uri.getSchemeSpecificPart();
        try (InputStream is = Objects.requireNonNull(cl.getResourceAsStream(classpath), classpath)) {
            return IOUtils.toString(is, StandardCharsets.UTF_8);
        }
    }

    /**
     * 命令行解析结果，封装 SQL 脚本、资源配置、Catalog 快照及运行模式标志。
     *
     * <p>所有资源内容均为解析后的原始字符串（JSON 文本或 SQL 文本），
     * 由调用方负责反序列化和校验。
     */
    static class ArgsContent {
        /**
         * SQL 脚本内容（必选）。
         */
        final String script;
        /**
         * Catalog 快照 JSON 内容，null 表示使用 SQL 中的 DDL 建表。
         */
        final String catalog;
        /**
         * 算子资源配置 JSON 内容，null 表示不做资源注入。
         */
        final String resource;
        /**
         * 依赖 JARs 路径
         */
        final List<URI> dependencies;

        boolean isHelp;
        boolean isValidate;
        boolean isInitResource;

        ArgsContent() {
            this.script = null;
            this.catalog = null;
            this.resource = null;
            this.dependencies = null;
        }

        ArgsContent(String script, String catalog, String resource, String[] dependencies, boolean isValidate, boolean isInitResource) {
            this.script = script;
            this.catalog = catalog;
            this.resource = resource;
            this.dependencies = toURIs(dependencies);
            this.isValidate = isValidate;
            this.isInitResource = isInitResource;
        }

        static ArgsContent help() {
            ArgsContent obj = new ArgsContent();
            obj.isHelp = true;
            return obj;
        }
    }

    static List<URI> toURIs(String[] deps) {
        if (deps == null || deps.length == 0) {
            return Collections.emptyList();
        }
        return Arrays.stream(deps).map(SqlEntryPoint::resolveURI).collect(Collectors.toList());
    }
}
