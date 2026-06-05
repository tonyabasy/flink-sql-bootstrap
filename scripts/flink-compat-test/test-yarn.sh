#!/usr/bin/env bash
# =============================================================================
# test-yarn.sh - 在 YARN Application 和 Session 模式下测试 Flink SQL 应用
# 前置条件：Docker + docker-compose 已启动 (./docker/yarn/)
#
# 用法：
#   ./scripts/test-yarn.sh                          # 所有版本，两种模式
#   ./scripts/test-yarn.sh --version 2.2.0          # 指定版本
#   ./scripts/test-yarn.sh --mode yarn-application  # 仅测试指定模式
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

YARN_COMPOSE="${PROJECT_ROOT}/docker/yarn/docker-compose.yml"
YARN_RM_HOST="localhost"
YARN_RM_PORT="8088"
YARN_CONTAINER="flink-yarn-hadoop"

# ── 参数解析 ─────────────────────────────────────────────────────────────────
TARGET_VERSION=""
TARGET_MODE=""       # yarn-application | yarn-session | (空 = 两者都测)
while [[ $# -gt 0 ]]; do
  case "$1" in
    --version) TARGET_VERSION="$2"; shift 2 ;;
    --mode)    TARGET_MODE="$2";    shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

# ── YARN 集群管理 ────────────────────────────────────────────────────────────
start_yarn_cluster() {
  log_info "Starting YARN cluster via Docker Compose ..."
  # 清理旧容器和数据，保证幂等运行
  docker compose -f "${YARN_COMPOSE}" down 2>/dev/null || true
  docker compose -f "${YARN_COMPOSE}" up -d
  log_info "Waiting for ResourceManager to be ready ..."
  # 等待 Docker 健康检查通过
  log_info "Waiting for container health check to pass ..."
  local retries=60
  while ((retries-- > 0)); do
    local status
    status=$(docker inspect --format='{{.State.Health.Status}}' "${YARN_CONTAINER}" 2>/dev/null)
    if [[ "${status}" == "healthy" ]]; then
      break
    fi
    sleep 5
  done
  if [[ "${status}" != "healthy" ]]; then
    log_error "Container health check timed out"
    return 1
  fi
  # 再确认 RM API 可访问
  retries=10
  while ((retries-- > 0)); do
    if curl -sf "http://${YARN_RM_HOST}:${YARN_RM_PORT}/ws/v1/cluster/info" >/dev/null 2>&1; then
      log_success "YARN ResourceManager is ready"
      return 0
    fi
    sleep 2
  done
  log_error "YARN ResourceManager start timed out"
  return 1
}

stop_yarn_cluster() {
  log_info "Stopping YARN cluster ..."
  docker compose -f "${YARN_COMPOSE}" down 2>/dev/null || true
}

# 将 JAR 和 Flink 分发包复制到 YARN 容器中，使 flink CLI 能够访问 HDFS
prepare_yarn_env() {
  local version="$1"
  local flink_home="$2"

  log_info "Uploading Flink distribution to YARN container ..."
  docker cp "${flink_home}" "${YARN_CONTAINER}:/opt/flink-${version}"
  docker cp "${APP_JAR}"    "${YARN_CONTAINER}:/opt/app.jar"

  log_info "Uploading Flink libs to HDFS ..."

  # 等待 HDFS 退出安全模式（NameNode 启动后 DataNode 需要报告块信息）
  log_info "Waiting for HDFS to leave safe mode ..."
  local safemode_retries=30
  while ((safemode_retries-- > 0)); do
    if docker exec "${YARN_CONTAINER}" hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; then
      break
    fi
    sleep 2
  done

  docker exec "${YARN_CONTAINER}" hdfs dfs -mkdir -p "/flink-dist/flink-${version}/lib"
  docker exec "${YARN_CONTAINER}" hdfs dfs -put -f \
    "/opt/flink-${version}/lib/"* \
    "/flink-dist/flink-${version}/lib/"
}

# ── YARN 专用工具 ────────────────────────────────────────────────────────────

# 拼接 Flink REST API 代理 URL
_flink_job_rest_url() {
  local app_id="$1"
  echo "http://${YARN_RM_HOST}:${YARN_RM_PORT}/proxy/${app_id}/jobs/overview"
}

# 查询 YARN Application 信息（一次请求返回 state 和 finalStatus）
# stdout: "RUNNING SUCCEEDED" 格式（空格分隔）
_yarn_app_info() {
  local app_id="$1"
  curl -sf "http://${YARN_RM_HOST}:${YARN_RM_PORT}/ws/v1/cluster/apps/${app_id}" 2>/dev/null \
    | python3 -c "import sys,json; a=json.load(sys.stdin)['app']; print(a['state'], a['finalStatus'])" 2>/dev/null
}

# 通过 YARN proxy 查询 Flink job 状态
_flink_job_state() {
  local app_id="$1"
  local rest_url
  rest_url=$(_flink_job_rest_url "${app_id}")
  curl -sf "${rest_url}" 2>/dev/null \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['jobs'][0]['state'])" 2>/dev/null
}

# 从日志提取 YARN Application ID
_yarn_extract_app_id() {
  local log_file="$1"
  if grep -q "YARN application has been deployed successfully" "${log_file}" 2>/dev/null; then
    grep -oE 'application_[0-9]+_[0-9]+' "${log_file}" | tail -1
  fi
}

# 在 YARN 容器内执行命令
_docker_yarn_exec() {
  docker exec "${YARN_CONTAINER}" "$@"
}

# 获取 hadoop classpath（带空值检查）
_yarn_get_hadoop_classpath() {
  local cp
  cp=$(_docker_yarn_exec hadoop classpath 2>/dev/null | tail -1)
  if [[ -z "${cp}" ]]; then
    log_error "Failed to get hadoop classpath from ${YARN_CONTAINER}"
    return 1
  fi
  echo "${cp}"
}

# 轮询 Flink REST API 直到 job 进入 RUNNING 状态
# 参数: app_id, start_ts, flink_pid
_poll_flink_rest_api() {
  local app_id="$1"
  local start_ts="$2"
  local flink_pid="$3"

  local elapsed=0
  while ((elapsed < JOB_TIMEOUT)); do
    # 检查 YARN 应用是否还活着（一次请求拿两个字段）
    local app_info
    app_info=$(_yarn_app_info "${app_id}")
    local yarn_state="${app_info%% *}"
    local final_status="${app_info##* }"

    if [[ "${yarn_state}" == "FINISHED" || "${yarn_state}" == "FAILED" ]]; then
      if [[ "${yarn_state}" == "FINISHED" && "${final_status}" == "SUCCEEDED" ]]; then
        _set_duration "${start_ts}"
        LAST_ERROR=""
        log_success "YARN app finished successfully (detected in ${LAST_DURATION}s)"
        wait "$flink_pid" 2>/dev/null
        return 0
      fi
      wait "$flink_pid" 2>/dev/null
      _set_duration "${start_ts}"
      LAST_ERROR="YARN app ${app_id} finished with status ${final_status}"
      log_error "${LAST_ERROR}"
      return 1
    fi

    # 轮询 Flink REST API
    local job_state
    job_state=$(_flink_job_state "${app_id}")

    case "${job_state}" in
      RUNNING)
        # 有界流仍需等待 FINISHED，继续轮询
        ;;
      FINISHED)
        _set_duration "${start_ts}"
        LAST_ERROR=""
        log_success "Job completed successfully (FINISHED in ${LAST_DURATION}s)"
        return 0
        ;;
      FAILED|FAILING)
        _set_duration "${start_ts}"
        LAST_ERROR="Flink job state: ${job_state}"
        log_error "${LAST_ERROR}"
        _wait_kill "$flink_pid"
        return 1
        ;;
    esac

    sleep 3
    elapsed=$((elapsed + 3))
  done

  # 超时——终止并标记失败
  _wait_kill "$flink_pid"
  _set_duration "${start_ts}"
  LAST_ERROR="Job did not enter RUNNING state within ${JOB_TIMEOUT}s"
  log_error "${LAST_ERROR}"
  return 1
}

# 在容器内运行 flink job（YARN 模式使用 REST API 探测状态）
# 用法：run_flink_job_in_container <version> <mode> [app_id] [extra_args...]
#   若 app_id 以 "application_" 开头，则跳过部署阶段直接轮询
run_flink_job_in_container() {
  local version="$1"
  local mode="$2"
  shift 2

  # 检测是否传入了已有 app_id（跳过 Phase 1）
  local app_id=""
  if [[ "${1:-}" =~ ^application_ ]]; then
    app_id="$1"
    shift
    log_info "Using existing YARN app: ${app_id}"
  fi

  local extra_args=("$@")

  local log_dir="${RESULTS_RAW}/logs"
  mkdir -p "${log_dir}"
  local log_file="${log_dir}/${version}_${mode}_$(date +%Y%m%d_%H%M%S).log"
  local test_script="${TEST_SCRIPT:-classpath:example-word-count.sql}"

  local cmd=(
    "/opt/flink-${version}/bin/flink" "$(_flink_subcmd "${version}" "${mode}")"
    --target "${mode}"
    -Dpipeline.name="compat-test-${version}-${mode}"
    -Dexecution.attached=true
    ${extra_args[@]+"${extra_args[@]}"}
    "/opt/app.jar"
    --script-file "${test_script}"
    ${APP_ARGS[@]+"${APP_ARGS[@]}"}
  )
  local cmd_str="${cmd[*]}"
  LAST_CMD="docker exec -e HADOOP_CLASSPATH=\$(hadoop classpath) ${YARN_CONTAINER} ${cmd_str}"
  log_info "Running: ${LAST_CMD}"

  local start_ts
  start_ts=$(date +%s)

  local hadoop_classpath
  hadoop_classpath=$(_yarn_get_hadoop_classpath) || return 1
  docker exec -e HADOOP_CLASSPATH="${hadoop_classpath}" "${YARN_CONTAINER}" "${cmd[@]}" > "${log_file}" 2>&1 &
  local flink_pid=$!

  if [[ -z "${app_id}" ]]; then
    local elapsed=0
    local deploy_timeout=60

    # ── Phase 1: 等待 YARN 应用部署 ──
    while ((elapsed < deploy_timeout)); do
      app_id=$(_yarn_extract_app_id "${log_file}")
      if [[ -n "${app_id}" ]]; then
        log_info "YARN app deployed: ${app_id}"
        break
      fi

      if ! _is_alive "$flink_pid"; then
        # 进程退出了最后试一次
        app_id=$(_yarn_extract_app_id "${log_file}")
        if [[ -n "${app_id}" ]]; then
          log_info "YARN app deployed: ${app_id}"
          break
        fi
        wait "$flink_pid" 2>/dev/null
        _set_duration "${start_ts}"
        LAST_ERROR=$(_extract_errors "${log_file}")
        [[ -z "${LAST_ERROR}" ]] && LAST_ERROR="Flink client exited before deployment"
        log_error "Client exited during deployment"
        return 1
      fi

      sleep 2
      elapsed=$((elapsed + 2))
    done

    if [[ -z "${app_id}" ]]; then
      _wait_kill "$flink_pid"
      _set_duration "${start_ts}"
      LAST_ERROR="YARN deployment timed out after ${deploy_timeout}s"
      log_error "${LAST_ERROR}"
      return 1
    fi
  fi

  log_info "Polling Flink REST API: http://${YARN_RM_HOST}:${YARN_RM_PORT}/proxy/${app_id}/jobs/overview"

  # ── Phase 2: 轮询 REST API 直到 job 进入 RUNNING ──
  _poll_flink_rest_api "${app_id}" "${start_ts}" "${flink_pid}"

  local rc=$?
  # 清理：如果是成功退出不杀进程，让它自己结束
  if [[ ${rc} -ne 0 ]]; then
    _wait_kill "$flink_pid"
  fi
  LAST_APP_ID="${app_id}"
  return ${rc}
}

# ── 测试：yarn-application ──────────────────────────────────────────────────
test_yarn_application() {
  local version="$1"
  local flink_home="$2"

  local extra_args=(
    "-Dyarn.application.name=compat-test-${version}"
    "-Dyarn.provided.lib.dirs=hdfs:///flink-dist/flink-${version}/lib"
    "-Djobmanager.memory.process.size=1g"
    "-Dtaskmanager.memory.process.size=2g"
  )

  local result=0
  if run_flink_job_in_container "${version}" "yarn-application" "${extra_args[@]}"; then
    record_result "${version}" "yarn-application" "PASS" "" "${LAST_DURATION}" "${LAST_CMD}"
  else
    record_result "${version}" "yarn-application" "FAIL" "${LAST_ERROR}" "${LAST_DURATION}" "${LAST_CMD}"
    result=1
  fi

  # 结束后 kill 掉 YARN app（如果还存活）
  if [[ -n "${LAST_APP_ID:-}" ]]; then
    _kill_yarn_app "${LAST_APP_ID}"
  fi
  return ${result}
}

# 在容器内通过 yarn CLI 关闭应用（统一入口）
_kill_yarn_app() {
  local app_id="$1"
  log_info "Force-killing YARN app ${app_id} ..."
  docker exec "${YARN_CONTAINER}" yarn application -kill "${app_id}" >/dev/null 2>&1 || true
}

# ── 测试：yarn-session ───────────────────────────────────────────────────────
test_yarn_session() {
  local version="$1"
  local flink_home="$2"

  log_info "Starting YARN Session cluster for flink-${version} ..."

  # 启动 Session 集群
  local session_log=$(_log_path "${version}" "yarn-session_cluster")
  mkdir -p "${RESULTS_RAW}/logs"

  "${flink_home}/bin/yarn-session.sh" \
    -jm 1024m -tm 2048m -s 2 \
    -Dyarn.application.name="compat-session-${version}" \
    -d > "${session_log}" 2>&1 || {
    local err
    err=$(_extract_errors "${session_log}" 3)
    record_result "${version}" "yarn-session" "FAIL" "Session start failed: ${err}" 0
    return 1
  }

  # 提取 Application ID
  local app_id
  app_id=$(grep -oE 'application_[0-9]+_[0-9]+' "${session_log}" | head -1)
  if [[ -z "${app_id}" ]]; then
    record_result "${version}" "yarn-session" "FAIL" "Could not parse YARN application ID" 0
    return 1
  fi
  log_info "Session cluster started: ${app_id}"

  # trap 确保异常退出时也能清理
  trap 'log_info "Cleaning up session ${app_id}"; _kill_yarn_app "${app_id}"' EXIT

  # 向 Session 提交任务（容器内执行，网络可达 JM）
  local extra_args=("-Dyarn.application.id=${app_id}")
  local result=0
  if run_flink_job_in_container "${version}" "yarn-session" "${app_id}" "${extra_args[@]}"; then
    record_result "${version}" "yarn-session" "PASS" "" "${LAST_DURATION}" "${LAST_CMD}"
  else
    record_result "${version}" "yarn-session" "FAIL" "${LAST_ERROR}" "${LAST_DURATION}" "${LAST_CMD}"
    result=1
  fi

  # 关闭 Session 集群
  _kill_yarn_app "${app_id}"

  # 清理 yarn-session.sh 在宿主机留下的 YARN properties 文件
  # 否则下次跑 K8s 测试时 Flink CLI 读到它会往配置注入 YARN 属性导致 ClassCastException
  find "${TMPDIR:-/tmp}" -maxdepth 1 -name '.yarn-properties-*' -delete 2>/dev/null || true

  # 恢复外层 on_exit trap（不能 trap - EXIT 否则把 main 的 trap 也清了）
  trap 'on_exit $?' EXIT
  return ${result}
}

# ── 主流程 ───────────────────────────────────────────────────────────────────
# 退出时关闭 YARN 集群：全部通过才自动关闭，失败则保留以便排查
on_exit() {
  local exit_code=$1
  if [[ ${exit_code} -eq 0 ]]; then
    stop_yarn_cluster
  else
    log_info "Test failed — keeping YARN cluster running for investigation"
    log_info "Stop it manually: docker compose -f ${YARN_COMPOSE} down -v"
    log_info "YARN web UI: http://localhost:8088"
  fi
}

main() {
  trap 'on_exit $?' EXIT
  check_jar_exists
  check_python3

  local versions
  if [[ -n "${TARGET_VERSION}" ]]; then
    versions="${TARGET_VERSION}"
  else
    versions=$(get_all_versions)
  fi

  local modes=("yarn-application" "yarn-session")
  if [[ -n "${TARGET_MODE}" ]]; then
    modes=("${TARGET_MODE}")
  fi

  start_yarn_cluster

  local pass=0 fail=0

  for version in ${versions}; do
    log_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log_info "Preparing flink-${version} for YARN ..."

    local flink_home
    flink_home=$(ensure_flink "${version}") || {
      for mode in "${modes[@]}"; do
        record_result "${version}" "${mode}" "FAIL" "Failed to download flink-${version}" 0
        ((fail++))
      done
      continue
    }

    prepare_yarn_env "${version}" "${flink_home}"

    for mode in "${modes[@]}"; do
      log_info "Testing flink-${version} × ${mode}"
      case "${mode}" in
        yarn-application)
          if test_yarn_application "${version}" "${flink_home}"; then ((pass++)); else ((fail++)); fi ;;
        yarn-session)
          if test_yarn_session "${version}" "${flink_home}"; then ((pass++)); else ((fail++)); fi ;;
      esac
    done
  done

  echo ""
  log_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log_info "YARN summary: ${pass} passed, ${fail} failed"
  [[ ${fail} -eq 0 ]]
}

main "$@"
