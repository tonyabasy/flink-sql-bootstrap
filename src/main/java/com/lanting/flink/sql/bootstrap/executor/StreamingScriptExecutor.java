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
package com.lanting.flink.sql.bootstrap.executor;

import com.lanting.flink.sql.bootstrap.flink.ApplicationOperationExecutor;
import com.lanting.flink.sql.bootstrap.resource.OperatorResourceSpec;
import com.lanting.flink.sql.bootstrap.resource.OperatorSpec;
import com.lanting.flink.sql.bootstrap.resource.ResourceEntity;
import com.lanting.flink.sql.bootstrap.util.JSON;

import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.table.api.SqlParserEOFException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.delegation.InternalPlan;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.context.SessionContext;
import org.apache.flink.table.gateway.service.result.ResultFetcher;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.StatementSetOperation;
import org.apache.flink.util.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import lombok.Getter;
import lombok.Setter;

/**
 * 流模式下的 SQL Script 执行器，支持对 DML 算子做细粒度资源注入。
 *
 * <h3>改造背景</h3>
 *
 * <p>Flink 2.x 内置的 ScriptExecutor 采用「边切边执行」的模式：SQL 按 {@code ;} 切分后立即通
 * 过 {@link ApplicationOperationExecutor} 执行。但 {@link ApplicationOperationExecutor#executeStatement} 对
 * DML 是原子调用——内部 {@code translate()} → {@code executeAsync()} 一步完成，没有机会在中
 * 间注入算子级别的资源（并行度、CPU 核数、堆内存等）。
 *
 * <p>如果直接修改 {@code OperationExecutor} 的内部逻辑（比如重写私有方法或用 JDK Proxy），
 * 侵入性太强且不够直观。因此本类选择在「Operation 分发层」下手：
 *
 * <ol>
 *   <li><b>复用 SQL 切分逻辑</b>：从 {@code ScriptExecutor.ResultIterator} 复制状态机，
 *       确保注释、引号、分号的处理与原版一致。</li>
 *   <li><b>预解析 Operation</b>：切分过程中同时执行 {@code parse()}，将 SQL 转换为
 *       {@link Operation}，并提前判断是否属于 DML（{@link ModifyOperation} / {@link StatementSetOperation}）。</li>
 *   <li><b>延迟 DML 执行</b>：非 DML（DDL、SET、CALL 等）仍用 {@link ApplicationOperationExecutor#executeStatement}
 *       立即执行，保证 catalog 状态及时更新。DML 暂存到 {@link Result#modifyOperations}，留到
 *       {@link #compile(List)}、{@link #transform(InternalPlan)} 和 {@link #execute()} 阶段统一处理。</li>
 *   <li><b>自定义 DML 路径</b>：{@code compile()} → {@code planner.compilePlan()} 编译、
 *       {@code transform()} → {@code planner.translatePlan()} 翻译、
 *       再通过 {@link #injectResourceSpec(List, String)} 注入资源，最后在 {@code execute()} 中
 *       反射调用 {@code TableEnvironmentImpl} 私有方法提交 Pipeline。</li>
 * </ol>
 *
 * <p>当前仅支持 {@code STREAMING} 模式。
 *
 * @author wangzhao
 * @see ApplicationOperationExecutor
 * @since 2026-05-18
 */
@SuppressWarnings("unused")
public class StreamingScriptExecutor {
    @Getter
    final SessionContext context;
    @Getter
    final Printer printer;
    @Getter
    final String script;

    @Getter
    @Setter
    String resourceProfile;

    @Getter
    List<Result> parsedStatements;
    @Getter
    private InternalPlan compiledPlan;
    @Getter
    List<Transformation<?>> transformations;

    transient TableEnvironmentImpl tableEnv;

    private static final Method EXECUTE_INTERNAL_METHOD;

    static {
        try {
            EXECUTE_INTERNAL_METHOD = TableEnvironmentImpl.class.getDeclaredMethod(
                    "executeInternal", List.class, List.class);
            EXECUTE_INTERNAL_METHOD.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public StreamingScriptExecutor(SessionContext context, String script) {
        this(context, script, null, new Printer());
    }

    public StreamingScriptExecutor(SessionContext context, String script, String resourceProfile) {
        this(context, script, resourceProfile, new Printer());
    }

    /**
     * 带着“细粒度资源配置”构造 Script Executor
     *
     * @param context         Session 上下文
     * @param script          SQL脚本
     * @param resourceProfile 细粒度资源配置
     * @param printer         SQL执行的输出
     */
    public StreamingScriptExecutor(SessionContext context, String script, String resourceProfile, Printer printer) {
        this.context = context;
        this.script = script;
        this.resourceProfile = resourceProfile;
        this.printer = printer;
    }

    public static StreamingScriptExecutor of(SessionContext context, String script) {
        return new StreamingScriptExecutor(context, script);
    }

    /**
     * 解析 &amp; 验证 SQL Script。
     *
     * <p>遍历 SQL 脚本，逐条切分并 parse。非 DML 语句（DDL、SET、CALL）会在此阶段立即执行，
     * 因为后续语句的 parse 可能依赖当前语句对 catalog 的修改。
     * DML 语句只解析不执行，留到 {@link #compile(List)}、{@link #transform(InternalPlan)} 和 {@link #execute()} 中做资源注入。
     */
    public List<Result> validate(String script) {
        ResultIterator iterator = new ResultIterator(script);
        List<Result> parsedStatements = new ArrayList<>();
        try {
            while (iterator.hasNext()) {
                Result result = iterator.next();
                if (result.error != null) {
                    throw result.error;
                } else if (result.fetcher != null) {
                    printer.print(result.statement);
                    printer.print(result.fetcher);
                }

                if (tableEnv == null) {
                    tableEnv = (TableEnvironmentImpl) result.executor.getTableEnvironment();
                }

                parsedStatements.add(result);
            }
        } catch (Throwable t) {
            printer.print(t);
            throw new SqlGatewayException("Failed to execute the script.", t);
        }

        return parsedStatements;
    }

    public InternalPlan compile(List<Result> results) {
        List<Result> modifyOperationResult = new ArrayList<>();
        for (Result stmt : results) {
            if (stmt.isModifyOperation()) {
                modifyOperationResult.add(stmt);
            }
        }
        if (modifyOperationResult.isEmpty()) {
            throw new SqlGatewayException(
                    "No DML statement found in the script. "
                            + "At least one INSERT, EXECUTE STATEMENT SET, or other write operation is required.");
        }
        if (modifyOperationResult.size() > 1) {
            throw new SqlGatewayException(
                    "Multiple DML statements found in the script. Only one INSERT or EXECUTE STATEMENT SET is allowed per execution.");
        }
        Result result = modifyOperationResult.get(0);

        return tableEnv.getPlanner().compilePlan(result.modifyOperations);
    }

    /**
     * 将编译好的 {@link InternalPlan} 翻译为 {@link Transformation} DAG。
     *
     * @param plan {@link #compile(List)} 的输出
     * @return translate 后的 Transformation 列表（尚未注入资源）
     */
    public List<Transformation<?>> transform(InternalPlan plan) {
        return tableEnv.getPlanner().translatePlan(plan);
    }

    /**
     * 编译 SQL Script
     */
    public InternalPlan compile(String script) {
        if (parsedStatements == null) {
            parsedStatements = validate(script);
        }
        if (compiledPlan == null) {
            compiledPlan = compile(parsedStatements);
        }
        return compiledPlan;
    }

    /**
     * 补齐 validate → compile → transform 步骤（幂等）。
     */
    private void prepare() {
        if (parsedStatements == null) {
            parsedStatements = validate(script);
        }
        if (compiledPlan == null) {
            compiledPlan = compile(parsedStatements);
        }

        if (transformations == null) {
            transformations = transform(compiledPlan);
        }
    }

    /**
     * 将 {@code resourceSpecStr}（JSON 字符串）解析为匹配规则，遍历 Transformations 并注入资源。
     * <p>通过 UID 精确匹配（{@link OperatorSpec#getUid()} == {@link Transformation#getUid()}），
     * 注入的资源包括：并行度、CPU 核数、Task Heap Memory、Managed Memory、Chain 策略。
     */
    public List<Transformation<?>> injectResourceSpec(List<Transformation<?>> transformations, String resourceSpecStr) {
        ResourceEntity resourceSpec = parseResourceSpec(resourceSpecStr);
        if (resourceSpec == null) {
            return transformations;
        }

        for (Transformation<?> t : allTransformations(transformations)) {
            // 跳过虚拟 Transformation（Partition、Union、SideOutput 等），
            // 它们不创建 StreamNode，不消耗运行时资源，无需配置并行度或资源
            if (!(t instanceof PhysicalTransformation)) {
                continue;
            }

            OperatorSpec op = resourceSpec.findByUid(t.getUid());
            if (op == null) {
                op = resourceSpec.findByName(t.getName());
            }
            if (op == null) {
                throw new IllegalArgumentException("Inject operator resource failed: Operation " + t.getUid() + "[" + t.getName() + "]" + " not found.");
            }

            // 用户指定了稳定 UID 时覆盖 Flink 自动生成的（用于 savepoint 对齐）
            // 当按名称匹配时，op.getUid() 与 t.getUid() 不同，覆盖生效
            if (op.getUid() != null) {
                t.setUid(op.getUid());
            }

            // 并行度：算子配置优先，其次全局默认值，否则保持 Transformation
            if (op.getParallelism() > 0) {
                t.setParallelism(op.getParallelism());
            } else if (resourceSpec.getDefaultParallelism() > 0) {
                t.setParallelism(resourceSpec.getDefaultParallelism());
            } else if (tableEnv.getConfig().get(CoreOptions.DEFAULT_PARALLELISM) != null) {
                t.setParallelism(tableEnv.getConfig().get(CoreOptions.DEFAULT_PARALLELISM));
            }

            if (op.getResource() != null) {
                // 解析 profile → 具体值（如 "small" → 0.25 CPU + 512 MB heap）
                OperatorResourceSpec r = op.getResource().resolve();

                // 使用 SlotSharingGroup 传递资源，而不是 setResources()。
                // setResources() 会导致 JobGraph.isPartialResourceConfigured() 检查失败，
                // 因为 Flink 当前禁止部分顶点配置资源、部分不配。
                // SlotSharingGroup 走的是另一条路径 ——
                // StreamGraphGenerator 提取 ResourceSpec 后存入 slotSharingGroupResources，
                // 最终落到 JobVertex.setSlotSharingGroup()，不触发 minResources 检查。
                //
                // SSG 名称使用 resourceSignature()，而非算子 UID。
                // 这样相同资源配置的算子自动归入同一组，operator chain 得以保持。
                SlotSharingGroup.Builder ssgBuilder =
                        SlotSharingGroup.newBuilder(r.getProfile())
                                .setCpuCores(r.getCpuCores())
                                .setTaskHeapMemory(MemorySize.parse(r.getHeapMemory()))
                                .setTaskOffHeapMemory(
                                        r.getOffHeapMemory() != null
                                                ? MemorySize.parse(r.getOffHeapMemory())
                                                : MemorySize.ZERO)
                                .setManagedMemory(
                                        r.getManagedMemory() != null
                                                ? MemorySize.parse(r.getManagedMemory())
                                                : MemorySize.ZERO);

                // Extra Resource
                if (r.getExternalResources() != null) {
                    r.getExternalResources().forEach(ssgBuilder::setExternalResource);
                }

                t.setSlotSharingGroup(ssgBuilder.build());
            }

            if (op.getChainStrategy() != null) {
                ((PhysicalTransformation<?>) t).setChainingStrategy(
                        ChainingStrategy.valueOf(op.getChainStrategy().toUpperCase()));
            }
        }

        return transformations;
    }

    /**
     * 执行 SQL Script。
     *
     * <p>如果尚未调用 {@link #validate(String)}、{@link #compile(List)} 或 {@link #transform(InternalPlan)}，
     * 会由 {@link #prepare()} 自动补齐。
     * 非 DML 语句在 validate 阶段已由 {@link ApplicationOperationExecutor} 执行完毕。
     *
     * <p>DML 执行路径：
     * <ol>
     *   <li>{@link #compile(List)} → {@code planner.compilePlan()} 编译为 InternalPlan</li>
     *   <li>{@link #transform(InternalPlan)} → {@code planner.translatePlan()} 生成 Transformation DAG</li>
     *   <li>{@link #injectResourceSpec(List, String)} 根据 {@code resourceHint} 注入算子资源</li>
     *   <li>{@link #executeInternal(List, TableEnvironmentImpl)} → 反射提交 Pipeline</li>
     * </ol>
     */
    public TableResult execute() throws InvocationTargetException, IllegalAccessException {
        prepare();

        List<Transformation<?>> injectedTransformations = transformations;
        if (resourceProfile != null) {
            injectedTransformations = injectResourceSpec(transformations, resourceProfile);
        }

        return executeInternal(injectedTransformations, tableEnv);
    }

    /**
     * 干运行（只 translate，不提交），输出资源配置 JSON。
     *
     * <p>与 {@link #execute()} 使用相同的 validate → transform → injectResourceSpec 路径，
     * 但不实际提交作业。输出的 JSON 可直接作为 {@link #setResourceProfile(String)} 的输入模板。
     *
     * <p><b>必须在独立 JVM 进程中调用</b>，原因同 {@link #execute()}。
     */
    public ResourceEntity generateResultSpec() {
        prepare();

        List<Transformation<?>> injected = transformations;
        if (resourceProfile != null) {
            injected = injectResourceSpec(transformations, resourceProfile);
        }

        ResourceEntity resource = resourceProfile != null ? parseResourceSpec(resourceProfile) : null;

        List<OperatorSpec> operators = new ArrayList<>();
        for (Transformation<?> t : allTransformations(injected)) {
            if (!(t instanceof PhysicalTransformation)) {
                continue;
            }

            OperatorSpec ors = new OperatorSpec();
            ors.setUid(t.getUid());
            ors.setName(t.getName());
            ors.setParallelism(t.getParallelism());

            ChainingStrategy cs = getChainStrategy(t);
            ors.setChainStrategy(cs != null ? cs.name() : null);

            ors.setResource(getOperatorResource(t));

            operators.add(ors);
        }
        // 还原为 DAG 自底向上的顺序（source 在前）
        Collections.reverse(operators);

        ResourceEntity root = new ResourceEntity();
        root.setVersion(resource != null ? resource.getVersion() : 1);
        root.setDefaultParallelism(
                resource != null ? resource.getDefaultParallelism() : 0);
        root.setOperators(operators);

        return root;
    }

    /**
     * 从 Transformation 中读取资源配置，优先从 SlotSharingGroup 读取，其次从 minResources。
     *
     * <p>资源可能通过两种方式注入：
     * <ol>
     *   <li>{@link Transformation#setSlotSharingGroup(SlotSharingGroup)} — 推荐方式，不触发
     *       partial resource 检查</li>
     *   <li>{@link Transformation#setResources(org.apache.flink.api.common.operators.ResourceSpec, org.apache.flink.api.common.operators.ResourceSpec)} — 传统方式，
     *       需要覆盖全部算子</li>
     * </ol>
     */
    private static OperatorResourceSpec getOperatorResource(Transformation<?> t) {
        // 优先从 SlotSharingGroup 读取
        if (t.getSlotSharingGroup().isPresent()) {
            SlotSharingGroup ssg = t.getSlotSharingGroup().get();
            if (ssg.getCpuCores().isPresent() || ssg.getTaskHeapMemory().isPresent()) {
                OperatorResourceSpec r;
                // 优先从 Standard 中识别
                String groupName = ssg.getName();
                r = OperatorResourceSpec.findInStandardSpec(groupName);
                if (r == null) {
                    r = new OperatorResourceSpec(
                            ssg.getCpuCores().orElse(0d),
                            ssg.getTaskHeapMemory().orElse(MemorySize.ZERO).toString(),
                            ssg.getTaskOffHeapMemory().orElse(MemorySize.ZERO).toString(),
                            ssg.getManagedMemory().orElse(MemorySize.ZERO).toString(),
                            ssg.getExternalResources()
                    );
                }

                return r;
            }
        }
        // 回退到 minResources
        return toOperatorResource(t.getMinResources());
    }

    /**
     * 将 Flink 的 {@link org.apache.flink.api.common.operators.ResourceSpec} 转换为 {@link OperatorResourceSpec}。
     *
     * @return 如果 ResourceSpec 为 UNKNOWN（未配置），返回 null
     */
    private static OperatorResourceSpec toOperatorResource(org.apache.flink.api.common.operators.ResourceSpec spec) {
        if (spec == null || spec.equals(org.apache.flink.api.common.operators.ResourceSpec.UNKNOWN)) {
            return null;
        }
        return new OperatorResourceSpec(
                spec.getCpuCores().getValue().doubleValue(),
                spec.getTaskHeapMemory().toString(),
                spec.getTaskOffHeapMemory().toString(),
                spec.getManagedMemory().toString(),
                spec.getExtendedResources().entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> e.getValue().getValue().doubleValue()))
        );
    }

    private ChainingStrategy getChainStrategy(Transformation<?> t) {
        if (t instanceof PhysicalTransformation) {
            try {
                // 先尝试 Transformation 自身有没有 getChainingStrategy（部分子类有）
                Method direct = t.getClass().getMethod("getChainingStrategy");
                return (ChainingStrategy) direct.invoke(t);
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException ignored) {
                // 没有直接方法，走 OperatorFactory 路径
            }

            try {
                // getOperatorFactory() 在 OneInputTransformation 等子类上存在
                Method getFactory = t.getClass().getMethod("getOperatorFactory");
                Object factory = getFactory.invoke(t);
                if (factory != null) {
                    Method getStrategy = factory.getClass().getMethod("getChainingStrategy");
                    return (ChainingStrategy) getStrategy.invoke(factory);
                }
            } catch (Exception ignored) {
            }
        }

        return null;
    }

    /**
     * BFS 遍历 DAG，返回所有 Transformation 的去重有序列表（根节点优先）。
     */
    public List<Transformation<?>> allTransformations(List<Transformation<?>> roots) {
        Set<Integer> visited = new HashSet<>();
        Deque<Transformation<?>> queue = new ArrayDeque<>(roots);
        List<Transformation<?>> result = new ArrayList<>();
        while (!queue.isEmpty()) {
            Transformation<?> t = queue.poll();
            if (!visited.add(t.getId())) {
                continue;
            }
            queue.addAll(t.getInputs());
            result.add(t);
        }
        return result;
    }

    private ResourceEntity parseResourceSpec(String spec) {
        if (StringUtils.isNullOrWhitespaceOnly(spec)) {
            return null;
        }
        try {
            return JSON.parseObject(spec, ResourceEntity.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse resource spec JSON", e);
        }
    }

    /**
     * 用反射调用 {@link TableEnvironmentImpl} 的私有方法提交 Pipeline。
     */
    private TableResultInternal executeInternal(List<Transformation<?>> transformations, TableEnvironmentImpl tableEnv) throws InvocationTargetException, IllegalAccessException {
        List<String> sinkNames = Collections.nCopies(transformations.size(), "sink");
        return (TableResultInternal) EXECUTE_INTERNAL_METHOD.invoke(tableEnv, transformations, sinkNames);
    }

    /**
     * SQL 切分迭代器，内部维护状态机逐字符扫描，支持引号、注释、分号跳转。
     *
     * <p>每次 {@link #hasNext()} 都会重建 {@link ApplicationOperationExecutor}，因为上一条 DDL 可能改变了
     * planner 的状态（如注册临时函数）。
     *
     * <p>与 Flink 原版 {@code ScriptExecutor.ResultIterator} 行为一致。
     */
    class ResultIterator implements Iterator<Result> {

        // 这 3 个 StringBuilder 用于将切分后的 SQL 填充回原始的行号和列号
        StringBuilder previousPaddingSqlBuilder = new StringBuilder();
        StringBuilder currentPaddingSqlBuilder = new StringBuilder();
        StringBuilder currentPaddingLineBuilder = new StringBuilder();

        private final String script;
        private int position;
        /**
         * 标记脚本中是否已包含 DML 语句，用于限制一个脚本中只允许一条 DML。
         */
        private boolean hasModifyOperation;

        private String statement;
        private Operation operation;
        private Throwable throwable;
        private ApplicationOperationExecutor executor;

        public ResultIterator(String script) {
            this.script = script;
            this.position = 0;
        }

        /**
         * 扫描并切分下一条 SQL 语句。
         *
         * <p>逐字符扫描脚本，维护引号、注释上下文状态机，遇到 {@code ;} 时执行
         * {@link #prefetch} 吃掉连续的分号和空白，然后尝试 {@link #parse}。
         * 如果 parse 遇到 {@link SqlParserEOFException} 且未到脚本末尾，继续吞字符重试
         * （处理 SQL 中换行分隔的场景）。
         *
         * @return 存在下一条 SQL 语句
         */
        @Override
        public boolean hasNext() {
            State state = State.NORMAL;
            StringBuilder currentSqlBuilder = new StringBuilder();
            char currentChar = 0;

            boolean hasNext = false;
            // 兼容说明：Flink 1.x 和 2.x 的 OperationExecutor 构造器不同：
            //   2.x 有两个构造器 — 2-arg (测试用，固定 StreamExecutionEnvironment::new) 和
            //   3-arg (生产用，接收 BiFunction<Configuration, ClassLoader, StreamExecutionEnvironment>)。
            //   1.x 仅有 2-arg 构造器。当前使用 2-arg 构造器，在两者上均可工作。
            // 每次重建 executor，因为上一条 DDL 可能改变了 planner 状态（如注册临时函数）
            executor = new ApplicationOperationExecutor(context, new Configuration());
            for (int i = position; i < script.length(); i++) {
                char lastChar = currentChar;
                currentChar = script.charAt(i);

                currentSqlBuilder.append(currentChar);
                currentPaddingLineBuilder.append(" ");

                switch (currentChar) {
                    case '\'':
                        if (state == State.SINGLE_QUOTE) {
                            state = State.NORMAL;
                        } else if (state == State.NORMAL) {
                            state = State.SINGLE_QUOTE;
                        }
                        break;
                    case '"':
                        if (state == State.DOUBLE_QUOTE) {
                            state = State.NORMAL;
                        } else if (state == State.NORMAL) {
                            state = State.DOUBLE_QUOTE;
                        }
                        break;
                    case '`':
                        if (state == State.BACK_QUOTE) {
                            state = State.NORMAL;
                        } else if (state == State.NORMAL) {
                            state = State.BACK_QUOTE;
                        }
                        break;
                    case '-':
                        if (lastChar == '-' && state == State.NORMAL) {
                            state = State.SINGLE_COMMENT;
                        }
                        break;
                    case '\n':
                        if (state == State.SINGLE_COMMENT) {
                            state = State.NORMAL;
                        }
                        currentPaddingLineBuilder.setLength(0);
                        currentPaddingSqlBuilder.append("\n");
                        break;
                    case '*':
                        if (lastChar == '/' && state == State.NORMAL) {
                            state = State.MULTI_LINE_COMMENT;
                        }
                        break;
                    case '/':
                        if (lastChar == '*' && state == State.MULTI_LINE_COMMENT) {
                            state = State.NORMAL;
                        }
                        break;
                    case ';':
                        if (state == State.NORMAL) {
                            i = prefetch(
                                    i + 1,
                                    currentSqlBuilder,
                                    currentPaddingSqlBuilder,
                                    currentPaddingLineBuilder);
                            statement = currentSqlBuilder.toString();
                            try {
                                position = i + 1;
                                operation = parse(previousPaddingSqlBuilder + statement);
                            } catch (SqlParserEOFException e) {
                                if (i == script.length() - 1) {
                                    throwable = e;
                                } else {
                                    // keep reading
                                    continue;
                                }
                            } catch (Throwable t) {
                                throwable = t;
                            }

                            hasNext = true;
                            previousPaddingSqlBuilder.append(currentPaddingSqlBuilder);
                            previousPaddingSqlBuilder.append(currentPaddingLineBuilder);
                            currentPaddingSqlBuilder.setLength(0);
                            currentPaddingLineBuilder.setLength(0);
                        }
                        break;
                    default:
                        break;
                }

                if (hasNext) {
                    return true;
                }
            }
            position = script.length();

            statement = currentSqlBuilder.toString();
            if (!StringUtils.isNullOrWhitespaceOnly(statement)) {
                operation = parse(previousPaddingSqlBuilder + statement);
                return true;
            } else {
                return false;
            }
        }

        /**
         * 返回下一条 SQL 语句的处理结果。
         *
         * <p>DML（{@link ModifyOperation} / {@link StatementSetOperation}）只解析不执行，
         * 将 {@link ModifyOperation} 暂存到 {@link Result#modifyOperations} 中，留待
         * 外层 {@link #compile(List)}、{@link #transform(InternalPlan)} 和 {@link #execute()} 做资源注入后统一提交。
         * 非 DML 语句立即通过 {@link ApplicationOperationExecutor#executeStatement} 执行。
         */
        @Override
        public Result next() {
            if (throwable != null) {
                // clear the exception
                Throwable t = throwable;
                throwable = null;
                return new Result(statement, operation, executor, t);
            }
            try {
                List<ModifyOperation> modifyOperations = null;
                ResultFetcher fetcher = null;

                // DML 语句直接暂时保存到 Result 中不立即执行，后面还需要注入细粒度资源配置
                if (operation instanceof ModifyOperation) {
                    modifyOperations = Collections.singletonList((ModifyOperation) operation);
                } else if (operation instanceof StatementSetOperation) {
                    modifyOperations = ((StatementSetOperation) operation).getOperations();
                } else {
                    // 如果是非 DML 语句立即执行，因为后面的 Statement 可能依赖当前 Statement 的执行结果
                    fetcher = executor.executeStatement(OperationHandle.create(), statement);
                }

                if (modifyOperations != null) {
                    return new Result(statement, operation, executor, modifyOperations);
                } else {
                    return new Result(statement, operation, executor, fetcher);
                }

            } catch (Throwable t) {
                return new Result(statement, operation, executor, t);
            }
        }

        /**
         * SQL 切分中的多分号/空白预读。
         *
         * <p>扫描 {@code ;} 之后连续的分号和空白字符，一并附加到当前 SQL 片段中。
         * 保持与原版 {@code ScriptExecutor.ResultIterator.prefetch} 行为一致。
         *
         * @param begin                     预读起始位置
         * @param currentSqlBuilder         当前正在构建的 SQL
         * @param currentPaddingSqlBuilder  当前填充脚本
         * @param currentPaddingLineBuilder 当前填充行
         * @return 预读结束位置
         */
        private int prefetch(
                int begin,
                StringBuilder currentSqlBuilder,
                StringBuilder currentPaddingSqlBuilder,
                StringBuilder currentPaddingLineBuilder) {
            State state = State.NORMAL;
            char currentChar;
            for (int i = begin; i < script.length(); i++) {
                currentChar = script.charAt(i);
                char nextChar = i + 1 < script.length() ? script.charAt(i + 1) : currentChar;

                switch (currentChar) {
                    case '-':
                        if (nextChar == '-' && state == State.NORMAL) {
                            state = State.SINGLE_COMMENT;
                        }
                        break;
                    case '\n':
                        if (state == State.SINGLE_COMMENT) {
                            state = State.NORMAL;
                        }
                        break;
                    case '*':
                        if (nextChar == '/' && state == State.MULTI_LINE_COMMENT) {
                            state = State.NORMAL;
                            currentSqlBuilder.append("*/");
                            currentPaddingLineBuilder.append("  ");
                            i = i + 1;
                            continue;
                        }
                        break;
                    case '/':
                        if (nextChar == '*' && state == State.NORMAL) {
                            state = State.MULTI_LINE_COMMENT;
                        }
                        break;
                }

                if (state == State.NORMAL
                        && currentChar != ';'
                        && !Character.isWhitespace(currentChar)) {
                    return i - 1;
                }

                currentSqlBuilder.append(currentChar);
                if (currentChar == '\n') {
                    currentPaddingLineBuilder.setLength(0);
                    currentPaddingSqlBuilder.append("\n");
                } else {
                    currentPaddingLineBuilder.append(" ");
                }
            }
            return script.length() - 1;
        }

        /**
         * 将 SQL 语句解析为 {@link Operation}。
         *
         * <p>限制一个脚本中最多只允许一条 DML（{@link ModifyOperation} 或
         * {@link StatementSetOperation}），由 {@link #hasModifyOperation} 标记控制。
         */
        private Operation parse(String statement) {
            List<Operation> operations =
                    executor.getTableEnvironment().getParser().parse(statement);
            if (operations.size() != 1) {
                throw new SqlGatewayException(
                        "Unsupported SQL query! Only one operation can be parsed.");
            }

            Operation op = operations.get(0);
            if (isDMLStatement(op)) {
                if (hasModifyOperation) {
                    // 一个 Script 中最多只允许一条 Modify Operator
                    throw new SqlGatewayException("Unsupported SQL query! Only one modify operation or statement-set operation can be parsed.");
                }
                hasModifyOperation = true;
            }

            return op;
        }

        /**
         * 判断 Operation 是否为 DML（数据修改语句）。
         *
         * <p>DML 包括 {@link ModifyOperation}（单条 INSERT）和
         * {@link StatementSetOperation}（BEGIN STATEMENT SET ... END）。
         */
        private boolean isDMLStatement(Operation op) {
            return (op instanceof ModifyOperation) || (op instanceof StatementSetOperation);
        }
    }

    /**
     * SQL 语句切分结果。
     *
     * <p>包含切分后的 SQL 原文、解析后的 {@link Operation}、对应的 {@link ApplicationOperationExecutor}，
     * 以及执行结果（DML 暂存 {@link #modifyOperations}，非 DML 暂存 {@link #fetcher}）。
     */
    @Getter
    public static class Result {

        final String statement;
        final Operation operation;
        final ApplicationOperationExecutor executor;
        final List<ModifyOperation> modifyOperations;
        final @Nullable ResultFetcher fetcher;
        final @Nullable Throwable error;

        public Result(String statement, Operation operation, ApplicationOperationExecutor executor, ResultFetcher fetcher) {
            this(statement, operation, executor, null, fetcher, null);
        }

        public Result(String statement, Operation operation, ApplicationOperationExecutor executor, List<ModifyOperation> modifyOperations) {
            this(statement, operation, executor, modifyOperations, null, null);
        }

        public Result(String statement, Operation operation, ApplicationOperationExecutor executor, Throwable error) {
            this(statement, operation, executor, null, null, error);
        }

        private Result(
                String statement, Operation operation, ApplicationOperationExecutor executor, @Nullable List<ModifyOperation> modifyOperations, @Nullable ResultFetcher fetcher, @Nullable Throwable error) {
            this.statement = statement;
            this.operation = operation;
            this.executor = executor;
            this.modifyOperations = modifyOperations;
            this.fetcher = fetcher;
            this.error = error;
        }

        /**
         * DML 操作的结果包含 {@link ModifyOperation} 列表。
         *
         * @return true 表示该语句是 DML（INSERT / STATEMENT SET），需要延迟执行并注入资源
         */
        public boolean isModifyOperation() {
            return modifyOperations != null;
        }
    }

    /**
     * SQL 扫描状态。
     *
     * <p>逐字符扫描 SQL 脚本时维护当前所在的上下文状态，用于正确处理引号、注释内的
     * 特殊字符（如分号、注释起始标记等）。
     */
    enum State {
        SINGLE_QUOTE, // 单引号字符串内

        DOUBLE_QUOTE, // 双引号字符串内

        BACK_QUOTE, // 反引号（MySQL 标识符）内

        SINGLE_COMMENT, // 单行注释 -- 内

        MULTI_LINE_COMMENT, /* 多行注释内 */

        NORMAL // 普通 SQL 代码
    }

}
