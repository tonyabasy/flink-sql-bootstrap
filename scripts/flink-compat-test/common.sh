#!/usr/bin/env bash
# =============================================================================
# common.sh - 所有测试脚本的共享工具函数
# =============================================================================
set -euo pipefail

# ── 配置 ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CONFIG_YAML="${SCRIPT_DIR}/config.yaml"

# ── 1. 从 config.yaml 加载默认值 ─────────────────────────────────────────────
# 读取 YAML 中的简单（非嵌套）键值
# 用法：get_config "app_jar"   →   "target/flink-sql-bootstrap-1.0-SNAPSHOT.jar"
get_config() {
  local key="$1"
  grep "^${key}:" "${CONFIG_YAML}" \
    | head -1 \
    | sed 's/^[^:]*:[[:space:]]*//; s/^"//; s/"$//; s/[[:space:]]*$//'
}

resolve_path() {
  local val="$1"
  val="${val/#\~/$HOME}"
  echo "${val}"
}

# 解析 YAML 配置值
RESULTS_RAW="$(resolve_path "$(get_config results_dir)")/raw"
[[ "${RESULTS_RAW}" != /* ]] && RESULTS_RAW="${PROJECT_ROOT}/${RESULTS_RAW}"

FLINK_DIST_CACHE="$(resolve_path "$(get_config flink_cache_dir)")"

APP_JAR="$(get_config app_jar)"
[[ "${APP_JAR}" != /* && "${APP_JAR}" != ~* ]] && APP_JAR="${PROJECT_ROOT}/${APP_JAR}"

JOB_TIMEOUT="$(get_config job_timeout)"
[[ -z "${JOB_TIMEOUT}" ]] && JOB_TIMEOUT=30

TEST_SCRIPT="$(get_config test_script)"
[[ -z "${TEST_SCRIPT}" ]] && TEST_SCRIPT="classpath:example-word-count.sql"

# ── 可选的应用参数 ──────────────────────────────────────────────────────────
RUN_MODE="$(get_config run_mode)"       # "" | validate | compile | init-resource

RESOURCE_FILE="$(get_config resource_file)"
CATALOG_FILE="$(get_config catalog_file)"

# 从 config.yaml 读取依赖列表
get_config_list() {
  local key="$1"
  python3 -c "
import re, sys
with open('${CONFIG_YAML}') as f:
    content = f.read()
m = re.search(r'^${key}:\n', content, re.MULTILINE)
if not m:
    sys.exit(0)
rest = content[m.end():]
for line in rest.split('\n'):
    line = line.rstrip()
    if not line.startswith('  - '):
        break
    val = line[4:].strip().strip('\"')
    if val:
        print(val)
"
}

# 根据可选配置构建 APP_ARGS 数组
APP_ARGS=()
if [[ -n "${RESOURCE_FILE}" ]]; then
  APP_ARGS+=(--resource-file "${RESOURCE_FILE}")
fi
if [[ -n "${CATALOG_FILE}" ]]; then
  APP_ARGS+=(--catalog-file "${CATALOG_FILE}")
fi
while IFS= read -r dep; do
  APP_ARGS+=(--dependency "${dep}")
done <<DEPS
$(get_config_list "dependencies")
DEPS
if [[ -n "${RUN_MODE}" ]]; then
  APP_ARGS+=(--"${RUN_MODE}")
fi

# 从 config.yaml 读取模式列表，返回 "key:label" 键值对
# 用法：get_modes  →  "local:Local yarn-application:YARN-App ..."
get_modes() {
  python3 -c "
import re, sys
with open('${CONFIG_YAML}') as f:
    content = f.read()
m = re.search(r'^modes:\n', content, re.MULTILINE)
if not m:
    sys.exit(0)
rest = content[m.end():]
for line in rest.split('\n'):
    line = line.rstrip()
    if not line.startswith('  - '):
        break
    key = re.search(r'key:\s*(\S+)', line)
    label = re.search(r'label:\s*(\S+)', line)
    if key and label:
        print(f'{key.group(1)}:{label.group(1)}')
"
}

# ── 颜色 ─────────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# 所有日志输出到 stderr（>&2），避免被命令替换 $(...) 捕获
log_info()    { printf '%b[INFO]%b  %s\n' "${BLUE}" "${NC}" "$*" >&2; }
log_success() { printf '%b[PASS]%b  %s\n' "${GREEN}" "${NC}" "$*" >&2; }
log_warn()    { printf '%b[WARN]%b  %s\n' "${YELLOW}" "${NC}" "$*" >&2; }
log_error()   { printf '%b[FAIL]%b  %s\n' "${RED}" "${NC}" "$*" >&2; }

# ── 解析 versions.yaml ──────────────────────────────────────────────────────
# 返回：空格分隔的版本号列表
get_all_versions() {
  grep '^\s*- version:' "${SCRIPT_DIR}/versions.yaml" \
    | sed 's/.*version: "\(.*\)"/\1/'
}

# 获取指定版本在 versions.yaml 中的字段值（纯 bash 实现）
# 用法：get_version_field "1.20.4" "java"
# 返回：字段值，或空字符串
get_version_field() {
  local target_version="$1"
  local field="$2"
  grep -A 10 "^  - version: \"${target_version}\"" "${SCRIPT_DIR}/versions.yaml" \
    | grep "^    ${field}:" \
    | head -1 \
    | sed 's/^    '"${field}"': "//; s/"$//'
}

# ── Flink 分发包管理 ─────────────────────────────────────────────────────────
# 下载（若未缓存则下载）并返回 FLINK_HOME 路径
ensure_flink() {
  local version="$1"
  local flink_home="${FLINK_DIST_CACHE}/flink-${version}"

  if [[ -d "${flink_home}" ]]; then
    log_info "Cache hit: flink-${version}"
    echo "${flink_home}"
    return 0
  fi

  local download_url base_url
  base_url=$(grep "^download_base_url:" "${SCRIPT_DIR}/versions.yaml" | head -1 | sed 's/^download_base_url:[[:space:]]*//; s/^"//; s/"$//')
  download_url=$(get_version_field "${version}" "download_url")
  download_url="${base_url}${download_url}"

  if [[ -z "${download_url}" ]]; then
    log_error "No download_url found for version ${version} in versions.yaml"
    return 1
  fi

  log_info "Downloading flink-${version} ..."
  mkdir -p "${FLINK_DIST_CACHE}"
  local tmp_tgz="${FLINK_DIST_CACHE}/flink-${version}.tgz"

  curl -fsSL --retry 3 --retry-delay 5 \
    -o "${tmp_tgz}" "${download_url}"

  tar -xzf "${tmp_tgz}" -C "${FLINK_DIST_CACHE}"
  # Apache 压缩包解压到 flink-X.Y.Z/
  mv "${FLINK_DIST_CACHE}/flink-${version}" "${flink_home}" 2>/dev/null || true
  rm -f "${tmp_tgz}"

  log_info "Extracted to ${flink_home}"
  echo "${flink_home}"
}

# ── 结果记录 ─────────────────────────────────────────────────────────────────
# 用法：record_result <version> <mode> <status> <error_msg> <duration_s> [cmd]
#   status: PASS | FAIL | SKIP
record_result() {
  local version="$1"
  local mode="$2"
  local status="$3"
  local error_msg="${4:-}"
  local duration="${5:-0}"
  local cmd="${6:-}"

  mkdir -p "${RESULTS_RAW}"
  local result_file="${RESULTS_RAW}/${version}_${mode}.json"
  local timestamp
  timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

  local cmd_escaped
  cmd_escaped=$(echo "${cmd}" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))' 2>/dev/null || echo '""')

  if [[ ! -f "${result_file}" ]]; then
    echo '{"history":[]}' > "${result_file}"
  fi

  # 通过管道（文件描述符 0）传入 error_msg，避免内容中的特殊字符
  # 破坏 Python 字符串边界
  printf '%s' "${error_msg}" | python3 -c "
import json, sys

error_msg = sys.stdin.read()

path = '${result_file}'
with open(path) as f:
    data = json.load(f)

entry = {
    'timestamp': '${timestamp}',
    'status': '${status}',
    'duration_s': ${duration},
    'error': error_msg.strip(),
    'cmd': ${cmd_escaped}
}
data['history'].append(entry)
data['history'] = data['history'][-20:]
data['latest'] = entry

with open(path, 'w') as f:
    json.dump(data, f, indent=2)
" 2>/dev/null || true

  case "${status}" in
    PASS) log_success "flink-${version} × ${mode} → PASS (${duration}s)" ;;
    FAIL) log_error   "flink-${version} × ${mode} → FAIL: ${error_msg}" ;;
    SKIP) log_warn    "flink-${version} × ${mode} → SKIP: ${error_msg}" ;;
  esac
}

# ── 超时执行与输出捕获 ───────────────────────────────────────────────────────
# 用法：run_flink_job <version> <mode> <flink_home> [extra_args...]
# 返回：0=PASS, 1=FAIL
# 设置：LAST_ERROR, LAST_DURATION, LAST_CMD
LAST_ERROR=""
LAST_DURATION=0
LAST_CMD=""

run_flink_job() {
  local version="$1"
  local mode="$2"
  local flink_home="$3"
  shift 3
  local extra_args=("$@")

  local log_dir="${RESULTS_RAW}/logs"
  mkdir -p "${log_dir}"
  local log_file="${log_dir}/${version}_${mode}_$(date +%Y%m%d_%H%M%S).log"
  local test_script="${TEST_SCRIPT:-classpath:example-word-count.sql}"

  # 构建命令（流式任务不会自行终止）
  local cmd=(
    "${flink_home}/bin/flink" run
    --target "${mode}"
    -Dpipeline.name="compat-test-${version}-${mode}"
    -Dexecution.attached=true
    ${extra_args[@]+"${extra_args[@]}"}
    "${APP_JAR}"
    --script-file "${test_script}"
    ${APP_ARGS[@]+"${APP_ARGS[@]}"}
  )
  LAST_CMD="${cmd[*]}"

  log_info "Running: ${LAST_CMD}"

  local start_ts
  start_ts=$(date +%s)

  if [[ -n "${RUN_MODE}" ]]; then
    # 干跑模式（validate/compile/init-resource）——进程自行退出
    "${cmd[@]}" > "${log_file}" 2>&1
    local exit_code=$?
    local end_ts
    end_ts=$(date +%s)
    LAST_DURATION=$((end_ts - start_ts))
    if [[ ${exit_code} -eq 0 ]]; then
      LAST_ERROR=""
      log_success "${RUN_MODE} completed"
      return 0
    else
      LAST_ERROR=$(grep -E "Exception|Error|FATAL|Caused by" "${log_file}" \
        | head -5 | sed 's/"/\\"/g')
      log_error "${RUN_MODE} failed (exit ${exit_code})"
      return 1
    fi
  fi

  "${cmd[@]}" > "${log_file}" 2>&1 &
  local flink_pid=$!

  local elapsed=0
  while ((elapsed < JOB_TIMEOUT)); do
    if ! kill -0 "$flink_pid" 2>/dev/null; then
      # 进程提前退出——一般是失败
      wait "$flink_pid" 2>/dev/null
      local end_ts
      end_ts=$(date +%s)
      LAST_DURATION=$((end_ts - start_ts))
      LAST_ERROR=$(grep -E "Exception|Error|FATAL|Caused by" "${log_file}" \
        | head -5 | sed 's/"/\\"/g')
      log_error "Job exited during init"
      return 1
    fi

    # 出现打印输出 +I[...] 表示管道已成功运行
    if grep -qF '+I[' "${log_file}" 2>/dev/null; then
      local end_ts
      end_ts=$(date +%s)
      LAST_DURATION=$((end_ts - start_ts))
      LAST_ERROR=""
      kill "$flink_pid" 2>/dev/null
      wait "$flink_pid" 2>/dev/null || true
      log_success "Print output detected, job cancelled"
      return 0
    fi

    sleep 2
    ((elapsed += 2))
  done

  # 超时——终止并标记失败
  kill "$flink_pid" 2>/dev/null
  wait "$flink_pid" 2>/dev/null || true
  local end_ts
  end_ts=$(date +%s)
  LAST_DURATION=$((end_ts - start_ts))
  LAST_ERROR=$(grep -E "Exception|Error|FATAL|Caused by" "${log_file}" \
    | head -5 | tr '\n' ' ' | sed 's/"/\\"/g')
  [[ -z "${LAST_ERROR}" ]] && LAST_ERROR="No print output within ${JOB_TIMEOUT}s"
  log_error "No print output within ${JOB_TIMEOUT}s"
  return 1
}

# ── 前置检查 ─────────────────────────────────────────────────────────────────
check_jar_exists() {
  if [[ ! -f "${APP_JAR}" ]]; then
    log_error "App JAR not found: ${APP_JAR}"
    log_error "Run 'mvn package -DskipTests' first in project root"
    exit 1
  fi
}

check_python3() {
  if ! command -v python3 &>/dev/null; then
    log_error "python3 is required for result recording and report generation"
    exit 1
  fi
}
