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
  docker compose -f "${YARN_COMPOSE}" up -d
  log_info "Waiting for ResourceManager to be ready ..."
  local retries=30
  while ((retries-- > 0)); do
    if curl -sf "http://${YARN_RM_HOST}:${YARN_RM_PORT}/ws/v1/cluster/info" >/dev/null 2>&1; then
      log_success "YARN ResourceManager is ready"
      return 0
    fi
    sleep 3
  done
  log_error "YARN ResourceManager start timed out"
  return 1
}

stop_yarn_cluster() {
  log_info "Stopping YARN cluster ..."
  docker compose -f "${YARN_COMPOSE}" down
}

# 将 JAR 和 Flink 分发包复制到 YARN 容器中，使 flink CLI 能够访问 HDFS
prepare_yarn_env() {
  local version="$1"
  local flink_home="$2"

  log_info "Uploading Flink distribution to YARN container ..."
  docker cp "${flink_home}" "${YARN_CONTAINER}:/opt/flink-${version}"
  docker cp "${APP_JAR}"    "${YARN_CONTAINER}:/opt/app.jar"
}

# ── 测试：yarn-application ──────────────────────────────────────────────────
test_yarn_application() {
  local version="$1"
  local flink_home="$2"

  # yarn-application 完全在 YARN 内运行，JAR 需放在 HDFS 或本地路径上
  local extra_args=(
    "-Dyarn.application.name=compat-test-${version}"
    "-Dyarn.provided.lib.dirs=hdfs:///flink-dist/flink-${version}/lib"
    "-Djobmanager.memory.process.size=1024m"
    "-Dtaskmanager.memory.process.size=1024m"
  )

  if run_flink_job "${version}" "yarn-application" "${flink_home}" "${extra_args[@]}"; then
    record_result "${version}" "yarn-application" "PASS" "" "${LAST_DURATION}"
    return 0
  else
    record_result "${version}" "yarn-application" "FAIL" "${LAST_ERROR}" "${LAST_DURATION}"
    return 1
  fi
}

# ── 测试：yarn-session ───────────────────────────────────────────────────────
test_yarn_session() {
  local version="$1"
  local flink_home="$2"

  log_info "Starting YARN Session cluster for flink-${version} ..."

  # 启动 Session 集群
  local session_log="${RESULTS_RAW}/logs/${version}_yarn-session_cluster.log"
  mkdir -p "${RESULTS_RAW}/logs"

  "${flink_home}/bin/yarn-session.sh" \
    -jm 1024m -tm 1024m \
    -Dyarn.application.name="compat-session-${version}" \
    -d > "${session_log}" 2>&1 || {
    local err
    err=$(grep -E "Exception|Error" "${session_log}" | head -3 | tr '\n' ' ')
    record_result "${version}" "yarn-session" "FAIL" "Session start failed: ${err}" 0
    return 1
  }

  # 提取 Application ID
  local app_id
  app_id=$(grep -oP 'application_\d+_\d+' "${session_log}" | head -1)
  if [[ -z "${app_id}" ]]; then
    record_result "${version}" "yarn-session" "FAIL" "Could not parse YARN application ID" 0
    return 1
  fi
  log_info "Session cluster started: ${app_id}"

  # 向 Session 提交任务
  local extra_args=("-Dyarn.application.id=${app_id}")
  local result=0
  if run_flink_job "${version}" "yarn-session" "${flink_home}" "${extra_args[@]}"; then
    record_result "${version}" "yarn-session" "PASS" "" "${LAST_DURATION}"
  else
    record_result "${version}" "yarn-session" "FAIL" "${LAST_ERROR}" "${LAST_DURATION}"
    result=1
  fi

  # 关闭 Session 集群
  log_info "Stopping YARN Session cluster ${app_id} ..."
  "${flink_home}/bin/yarn-session.sh" \
    -Dyarn.application.id="${app_id}" \
    --stop >/dev/null 2>&1 || true

  return ${result}
}

# ── 主流程 ───────────────────────────────────────────────────────────────────
main() {
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
  trap stop_yarn_cluster EXIT

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
          test_yarn_application "${version}" "${flink_home}" && ((pass++)) || ((fail++)) ;;
        yarn-session)
          test_yarn_session "${version}" "${flink_home}" && ((pass++)) || ((fail++)) ;;
      esac
    done
  done

  echo ""
  log_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log_info "YARN summary: ${pass} passed, ${fail} failed"
  [[ ${fail} -eq 0 ]]
}

main "$@"
