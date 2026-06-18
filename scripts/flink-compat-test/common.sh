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
# 用法：get_config "app_jar"   →   "target/flink-sql-bootstrap-${version}.jar"
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
  grep -A 10 "version: \"${target_version}\"" "${SCRIPT_DIR}/versions.yaml" \
    | grep "${field}:" \
    | head -1 \
    | sed 's/.*'"${field}"':[[:space:]]*"//; s/"[[:space:]]*$//'
}

# ── Flink 分发包管理 ─────────────────────────────────────────────────────────
# 下载（若未缓存则下载）并返回 FLINK_HOME 路径
ensure_flink() {
  local version="$1"
  local flink_home="${FLINK_DIST_CACHE}/flink-${version}"

  if [[ -d "${flink_home}" ]]; then
    log_info "Cache hit: flink-${version}"
  else
    local download_url base_url
    base_url=$(grep "^download_base_url:" "${SCRIPT_DIR}/versions.yaml" | head -1 | sed 's/^download_base_url:[[:space:]]*//; s/^"//; s/"$//')
    download_url=$(get_version_field "${version}" "download_url")
    download_url="${base_url}${download_url}"

    if [[ -z "${download_url}" ]]; then
      log_error "No download_url found for version ${version} in versions.yaml"
      return 1
    fi

    log_info "Downloading flink-${version} from ${download_url} ..."
    mkdir -p "${FLINK_DIST_CACHE}"
    local tmp_tgz="${FLINK_DIST_CACHE}/flink-${version}.tgz"

    curl -fSL --progress-bar -C - \
      --retry 3 --retry-delay 5 \
      -o "${tmp_tgz}" "${download_url}" || {
      log_error "Download failed for flink-${version}"
      rm -f "${tmp_tgz}"
      return 1
    }

    tar -xzf "${tmp_tgz}" -C "${FLINK_DIST_CACHE}" || {
      log_error "Extraction failed for flink-${version}"
      rm -f "${tmp_tgz}"
      return 1
    }
    mv "${FLINK_DIST_CACHE}/flink-${version}" "${flink_home}" 2>/dev/null || true
    rm -f "${tmp_tgz}"

    log_info "Extracted to ${flink_home}"
  fi

  # 将 flink-sql-gateway 从 opt/ 复制到 lib/（如果不在 lib/ 中）
  if ! find "${flink_home}/lib/" -maxdepth 1 -name 'flink-sql-gateway-*.jar' | grep -q .; then
    local gateway_jar
    gateway_jar=$(find "${flink_home}/opt/" -maxdepth 1 -name 'flink-sql-gateway-*.jar' 2>/dev/null | head -1)
    if [[ -n "${gateway_jar}" ]]; then
      log_info "Copying $(basename "${gateway_jar}") from opt/ to lib/ ..."
      cp "${gateway_jar}" "${flink_home}/lib/"
    else
      log_warn "flink-sql-gateway JAR not found in ${flink_home}/opt/"
    fi
  fi

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
  local datetime
  datetime=$(date +"%Y-%m-%dT%H:%M:%S%z")

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
    'datetime': '${datetime}',
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

# ── 通用工具 ─────────────────────────────────────────────────────────────────

# 检查进程是否存活
_is_alive() {
  local pid="$1"
  kill -0 "${pid}" 2>/dev/null
}

# 终止进程并等待其退出
_wait_kill() {
  local pid="$1"
  kill "${pid}" 2>/dev/null
  wait "${pid}" 2>/dev/null || true
}

# 记录当前时间与入参的差值到 LAST_DURATION
_set_duration() {
  local start_ts="$1"
  local end_ts
  end_ts=$(date +%s)
  LAST_DURATION=$((end_ts - start_ts))
}

# 从日志文件提取错误信息（保留换行，供 HTML <pre> 展示）
# 用法：_extract_errors <log_file> [max_lines]
_extract_errors() {
  local log_file="$1"
  local max_lines="${2:-50}"
  grep -E "Exception|Error|FATAL|Caused by" "${log_file}" 2>/dev/null \
    | head -"${max_lines}" | sed 's/"/\\"/g'
}

# 生成带时间戳的日志文件路径
_log_path() {
  echo "${RESULTS_RAW}/logs/${1}_${2}_$(date +%Y%m%d_%H%M%S).log"
}

# Flink 1.x Application 模式须用 run-application 子命令（YARN & K8s）
_flink_subcmd() {
  local version="$1" mode="$2"
  if [[ "${mode}" =~ (yarn|kubernetes)-application && "${version%%.*}" == "1" ]]; then
    echo "run-application"
  else
    echo "run"
  fi
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

  local cmd=(
    "${flink_home}/bin/flink" "$(_flink_subcmd "${version}" "${mode}")"
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
    _set_duration "${start_ts}"
    if [[ ${exit_code} -eq 0 ]]; then
      LAST_ERROR=""
      log_success "${RUN_MODE} completed"
      return 0
    else
      LAST_ERROR=$(_extract_errors "${log_file}")
      log_error "${RUN_MODE} failed (exit ${exit_code})"
      return 1
    fi
  fi

  "${cmd[@]}" > "${log_file}" 2>&1 &
  local flink_pid=$!

  local elapsed=0
  while ((elapsed < JOB_TIMEOUT)); do
    # 先检查输出再检查进程——有界流（number-of-rows）会在输出后立即退出
    if grep -qF '+I[' "${log_file}" 2>/dev/null; then
      _set_duration "${start_ts}"
      LAST_ERROR=""
      _wait_kill "$flink_pid"
      log_success "Print output detected, job completed"
      return 0
    fi

    if ! _is_alive "$flink_pid"; then
      # 进程退出了——先检查是否有输出（有界流正常完成）
      if grep -qF '+I[' "${log_file}" 2>/dev/null; then
        wait "$flink_pid" 2>/dev/null
        _set_duration "${start_ts}"
        LAST_ERROR=""
        log_success "Print output detected, job completed"
        return 0
      fi
      wait "$flink_pid" 2>/dev/null
      _set_duration "${start_ts}"
      LAST_ERROR=$(_extract_errors "${log_file}")
      log_error "Job exited during init"
      return 1
    fi

    sleep 2
    ((elapsed += 2))
  done

  # 超时——终止并标记失败
  _wait_kill "$flink_pid"
  _set_duration "${start_ts}"
  LAST_ERROR=$(_extract_errors "${log_file}")
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
