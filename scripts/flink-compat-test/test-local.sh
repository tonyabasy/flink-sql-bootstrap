#!/usr/bin/env bash
# =============================================================================
# test-local.sh - 在 Local 部署模式下测试 Flink SQL 应用
# 用法：
#   ./scripts/test-local.sh                    # 测试所有版本
#   ./scripts/test-local.sh --version 2.2.0    # 测试指定版本
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

# ── 参数解析 ─────────────────────────────────────────────────────────────────
TARGET_VERSION=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --version) TARGET_VERSION="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

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

  local pass=0 fail=0

  for version in ${versions}; do
    log_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log_info "Testing flink-${version} × local"

    local flink_home
    flink_home=$(ensure_flink "${version}") || {
      record_result "${version}" "local" "FAIL" "Failed to download flink-${version}" 0
      ((fail++))
      continue
    }

    if run_flink_job "${version}" "local" "${flink_home}"; then
      record_result "${version}" "local" "PASS" "" "${LAST_DURATION}" "${LAST_CMD}"
      ((pass++))
    else
      record_result "${version}" "local" "FAIL" "${LAST_ERROR}" "${LAST_DURATION}" "${LAST_CMD}"
      ((fail++))
    fi
  done

  echo ""
  log_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log_info "Local mode summary: ${pass} passed, ${fail} failed"
  [[ ${fail} -eq 0 ]]
}

main "$@"
