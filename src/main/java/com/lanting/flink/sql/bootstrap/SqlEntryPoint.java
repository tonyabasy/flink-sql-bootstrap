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

import static com.lanting.flink.sql.bootstrap.util.PrintUtils.printHelp;
import static com.lanting.flink.sql.bootstrap.util.PrintUtils.println;

import com.lanting.flink.sql.bootstrap.catalog.CatalogEntityFactory;
import com.lanting.flink.sql.bootstrap.executor.Printer;
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
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.gateway.service.context.SessionContext;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.Executors;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
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
 *   <li>通过 {@code --script} / {@code --script-file} 传入 SQL 脚本</li>
 *   <li>通过 {@code --resource} / {@code --resource-file} 传入算子资源配置 JSON</li>
 *   <li>通过 {@code --catalog} / {@code --catalog-file} 传入 Catalog 快照 JSON（可选）</li>
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
    //   2. --xxx-file  传 URI（支持 file://、http://、hdfs:// 等）
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

    public static final Option OPTION_SCRIPT_COMPILE =
            Option.builder()
                    .longOpt("compile")
                    .numberOfArgs(0)
                    .desc("Parse, validate and compile the SQL script without submitting any job. Recommend use 'local' target.")
                    .build();

    public static final Option OPTION_SCRIPT_VALIDATE =
            Option.builder()
                    .longOpt("validate")
                    .numberOfArgs(0)
                    .desc("Parse and validate the SQL script syntax without compiling. Recommend use 'local' target.")
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
        // 将 classpath: 协议的依赖 JAR 解压到临时目录，转为 file: URI，
        // 否则 UriSafeSessionContext.toURL() 会因 Java 不认识 classpath scheme 而抛异常
        mergedDependencies = resolveClasspathDependencies(mergedDependencies);
        ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.CLASSPATHS, mergedDependencies, URI::toString);

        // 从配置 pipeline.classpaths 中获取依赖 Jar（如：UDFs Jar），如果使用 flink run 启动该配置是 -C 参数指定的 classpath
        // Flink 1.20.x 兼容：因 Flink 2.x 构造器 2-arg 从 URL -> URI 导致和 Flink 1.20.x 彻底无法兼容，这里将 deps 在
        // UriSafeSessionContext.create 传入用于创建对应的 ClassLoader（deps 只在创建 MutableURLClassLoader 时 使用）
        DefaultContext defaultContext = new DefaultContext(configuration, null);

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
                defaultContext, mergedDependencies, sessionHandle, ssEnv, Executors.newDirectExecutorService());

        // 委托 StreamingScriptExecutor：切分 SQL → translate → 注入资源 → 提交
        try (AutoCloseable ignore = sessionContext::close) {
            StreamingScriptExecutor executor = new StreamingScriptExecutor(
                    sessionContext, argsContent.script, argsContent.resource,
                    new Printer(System.out));

            if (argsContent.isValidate) {
                // 仅校验 SQL 语法，不编译不执行
                executor.validate(argsContent.script);
                println("Validate the SQL script successfully.");
            } else if (argsContent.isCompile) {
                // 校验 + 编译，不执行
                InternalPlan compiledPlan = executor.compile(argsContent.script);
                println("\n" + compiledPlan.asJsonString() + "\n");
                println("Compile the SQL script successfully!");
            } else if (argsContent.isInitResource) {
                // 仅生成 Resource 初始化配置，不执行
                ResourceEntity res = executor.generateResultSpec();
                println(JSON.toJSONString(res, true));
            } else {
                // 执行 SQL Script
                TableResult execute = executor.execute();
                execute.print();
            }
        }
    }

    /**
     * 将 classpath: 协议的依赖 JAR 解压到临时目录，转为 file: URI。
     * Java 的 URLClassLoader 不识别 classpath scheme，需要提前转换。
     *
     * @param dependencies 依赖 URI 列表（可含 classpath: 协议）
     * @return 全转为 file: 协议的 URI 列表
     * @throws SecurityException     resourceName 含路径逃逸（如 ".."）时抛出
     * @throws IOException           临时文件读写失败时抛出
     * @throws IllegalArgumentException classpath 资源在 classpath 中不存在时抛出
     */
    public static List<URI> resolveClasspathDependencies(List<URI> dependencies) throws IOException {
        File tempDir = new File(System.getProperty("java.io.tmpdir"), "flink-sql-bootstrap-deps");
        tempDir.mkdirs();
        List<URI> resolved = new ArrayList<>();
        for (URI dep : dependencies) {
            if ("classpath".equals(dep.getScheme())) {
                String resourceName = dep.getSchemeSpecificPart();
                Path resourcePath = tempDir.toPath().resolve(resourceName).normalize();
                if (!resourcePath.startsWith(tempDir.toPath())) {
                    throw new SecurityException("Invalid classpath resource path: " + resourceName);
                }
                File tempJar = resourcePath.toFile();
                try (InputStream is = cl.getResourceAsStream(resourceName)) {
                    if (is == null) {
                        throw new IllegalArgumentException("Classpath resource not found: " + resourceName);
                    }
                    Files.copy(is, tempJar.toPath(), StandardCopyOption.REPLACE_EXISTING);
                }
                resolved.add(tempJar.toURI());
            } else {
                resolved.add(dep);
            }
        }
        return resolved;
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
        options.addOption(OPTION_SCRIPT_COMPILE);
        options.addOption(OPTION_SCRIPT_VALIDATE);
        return options;
    }

    /**
     * 解析命令行参数，返回结构化的 {@link ArgsContent}。
     *
     * <p>对每个资源类型（script / catalog / resource），{@code --xxx} 和 {@code --xxx-file}
     * 两组互斥 — 前者直接传值，后者传文件路径或 URI（支持 file://、http://、hdfs:// 等），
     * 通过 {@link #getContent(String)} 统一读取。
     *
     * @throws IOException        文件 / HTTP 读取失败时抛出
     * @throws URISyntaxException path 非合法 URI 时抛出
     * @throws ParseException     CLI 参数解析失败时抛出
     */
    static ArgsContent parseOptions(String[] args) throws IOException, ParseException {
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
                    "Please use \"--script\" or \"--script-file\" to specify script either.");
        } else {
            Preconditions.checkArgument(
                    line.getOptionValue(OPTION_SQL_SCRIPT.getLongOpt()) == null,
                    "Don't set \"--script\" or \"--script-file\" together.");
        }

        String catalog = getContent(line.getOptionValue(OPTION_CATALOG_CONF_FILE.getLongOpt()));
        if (catalog == null) {
            catalog = line.getOptionValue(OPTION_CATALOG_CONF.getLongOpt());
        } else {
            Preconditions.checkArgument(
                    line.getOptionValue(OPTION_CATALOG_CONF.getLongOpt()) == null,
                    "Don't set \"--catalog\" or \"--catalog-file\" together.");
        }

        String resource = getContent(line.getOptionValue(OPTION_RESOURCE_CONF_FILE.getLongOpt()));
        if (resource == null) {
            resource = line.getOptionValue(OPTION_RESOURCE_CONF.getLongOpt());
        } else {
            Preconditions.checkArgument(
                    line.getOptionValue(OPTION_RESOURCE_CONF.getLongOpt()) == null,
                    "Don't set \"--resource\" or \"--resource-file\" together.");
        }

        String[] dependencies = line.getOptionValues(OPTION_DEPENDENCIES.getLongOpt());

        boolean isValidate = line.hasOption(OPTION_SCRIPT_VALIDATE.getLongOpt());
        boolean isCompile = line.hasOption(OPTION_SCRIPT_COMPILE.getLongOpt());
        boolean isInitResource = line.hasOption(OPTION_INIT_RESOURCE.getLongOpt());
        if (countTrue(isValidate, isCompile, isInitResource) > 1) {
            throw new IllegalArgumentException(
                    "Don't set \"--validate\", \"--compile\", or \"--init-resource\" together.");
        }

        return new ArgsContent(script, catalog, resource, dependencies, isValidate, isCompile, isInitResource);
    }

    /**
     * 根据路径读取文件内容，支持 file://、http(s)://、classpath: 及无协议（本地文件）四种格式。
     *
     * @param filePath 文件路径或 URI 字符串，为 null 时返回 null
     * @return 文件内容，UTF-8 编码
     * @throws IOException 文件 / HTTP 读取失败时抛出
     */
    private static @Nullable String getContent(@Nullable String filePath)
            throws IOException {
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

    /**
     * 将路径字符串解析为 URI。无 scheme 的路径自动转为 file: 绝对路径 URI。
     *
     * @param path 路径字符串，可为本地相对/绝对路径或带 scheme 的 URI
     * @return 解析后的 URI
     * @throws IllegalArgumentException path 非法时抛出（如含非法字符）
     */
    private static URI resolveURI(String path) {
        final URI uri = URI.create(path);
        if (uri.getScheme() != null) {
            return uri;
        }
        return new File(path).getAbsoluteFile().toURI();
    }

    public static String readFromHttp(URI uri) throws IOException {
        URL url = uri.toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestMethod("GET");

        try (InputStream inputStream = conn.getInputStream();
             ByteArrayOutputStream targetFile = new ByteArrayOutputStream()) {
            IOUtils.copy(inputStream, targetFile);
            return targetFile.toString(StandardCharsets.UTF_8);
        }
    }

    public static String readFileUtf8(URI uri) throws IOException {
        org.apache.flink.core.fs.Path path = new org.apache.flink.core.fs.Path(uri);
        FileSystem fs = path.getFileSystem();
        try (FSDataInputStream inputStream = fs.open(path)) {
            return new String(
                    IOUtils.toByteArray(inputStream),
                    StandardCharsets.UTF_8);
        }
    }

    public static String readFromClasspathUtf8(URI uri) throws IOException {
        String classpath = uri.getSchemeSpecificPart();
        String normalized = Paths.get(classpath).normalize().toString();
        if (normalized.startsWith("..") || normalized.startsWith("/")) {
            throw new SecurityException("Invalid classpath resource path: " + classpath);
        }
        InputStream is = cl.getResourceAsStream(classpath);
        if (is == null) {
            throw new FileNotFoundException("Classpath resource not found: " + classpath);
        }
        try (is) {
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
        boolean isCompile;
        boolean isInitResource;

        ArgsContent() {
            this.script = null;
            this.catalog = null;
            this.resource = null;
            this.dependencies = null;
        }

        ArgsContent(String script, String catalog, String resource, String[] dependencies,
                    boolean isValidate, boolean isCompile, boolean isInitResource) {
            this.script = script;
            this.catalog = catalog;
            this.resource = resource;
            this.dependencies = toURIs(dependencies);
            this.isValidate = isValidate;
            this.isCompile = isCompile;
            this.isInitResource = isInitResource;
        }

        static ArgsContent help() {
            ArgsContent obj = new ArgsContent();
            obj.isHelp = true;
            return obj;
        }
    }

    private static int countTrue(boolean... flags) {
        int count = 0;
        for (boolean f : flags) {
            if (f) {
                count++;
            }
        }
        return count;
    }

    static List<URI> toURIs(String[] deps) {
        if (deps == null || deps.length == 0) {
            return Collections.emptyList();
        }
        return Arrays.stream(deps).map(SqlEntryPoint::resolveURI).collect(Collectors.toList());
    }
}
