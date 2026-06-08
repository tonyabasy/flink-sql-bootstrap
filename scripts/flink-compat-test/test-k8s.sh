#!/usr/bin/env bash
# =============================================================================
# test-k8s.sh - 在 Kubernetes Session 和 Application 模式下测试 Flink SQL 应用
# 前置条件：
#   - Docker 已运行
#   - kind 已安装 (brew install kind)
#   - kubectl 已安装 (brew install kubectl)
#   - helm 已安装 (brew install helm)  [可选，用于 Flink Operator]
#
# 用法：
#   ./scripts/test-k8s.sh                              # 所有版本，两种模式
#   ./scripts/test-k8s.sh --version 2.2.0              # 指定版本
#   ./scripts/test-k8s.sh --mode kubernetes-session    # 仅测试指定模式
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

KIND_CLUSTER="flink-compat-test"
KIND_CONFIG="${PROJECT_ROOT}/docker/kind/kind-cluster.yaml"
K8S_NAMESPACE="flink-test"
FLINK_IMAGE_BASE="flink"

# ── 参数解析 ─────────────────────────────────────────────────────────────────
TARGET_VERSION=""
TARGET_MODE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --version) TARGET_VERSION="$2"; shift 2 ;;
    --mode)    TARGET_MODE="$2";    shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

# ── Kind 集群管理 ────────────────────────────────────────────────────────────
start_kind_cluster() {
  # 集群 + namespace 已存在则跳过重建
  if kind get clusters 2>/dev/null | grep -qx "${KIND_CLUSTER}" \
    && kubectl get ns "${K8S_NAMESPACE}" --context "kind-${KIND_CLUSTER}" >/dev/null 2>&1; then
    kubectl config use-context "kind-${KIND_CLUSTER}"
    log_info "Kind cluster '${KIND_CLUSTER}' + namespace '${K8S_NAMESPACE}' already ready, skipping creation"
    return 0
  fi

  delete_kind_cluster
  log_info "Creating Kind cluster '${KIND_CLUSTER}' ..."
  kind create cluster \
    --name "${KIND_CLUSTER}" \
    --config "${KIND_CONFIG}" \
    --wait 60s
  docker update --memory 4g --memory-swap 4g "${KIND_CLUSTER}-control-plane" 2>/dev/null || true
  kubectl config use-context "kind-${KIND_CLUSTER}"
  kubectl create namespace "${K8S_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
  _apply_flink_rbac
}

# 授予 Flink JobManager 在命名空间内管理 Pod/Service/ConfigMap 的权限
_apply_flink_rbac() {
  log_info "Applying Flink RBAC permissions ..."
  sed "s/__NAMESPACE__/${K8S_NAMESPACE}/g" "${PROJECT_ROOT}/docker/kind/flink-rbac.yaml" | kubectl apply -f -
  log_info "RBAC applied"
}

delete_kind_cluster() {
  log_info "Deleting Kind cluster '${KIND_CLUSTER}' ..."
  kind delete cluster --name "${KIND_CLUSTER}" 2>/dev/null || true
}

# 构建自定义 Docker 镜像，将应用 JAR 打包进去（Docker layer cache 自动跳过未变更的文件）
build_and_load_image() {
  local version="$1"
  local flink_home="$2"
  local image_tag="flink-compat-app:${version}"

  local java_version
  java_version=$(get_version_field "${version}" "java")
  local base_image="${FLINK_IMAGE_BASE}:${version}-scala_2.12-java${java_version}"

  local build_dir
  build_dir=$(mktemp -d)
  cp "${APP_JAR}" "${build_dir}/app.jar"
  cp "${PROJECT_ROOT}/src/main/resources/example-udf-reverse.jar"    "${build_dir}/"
  cp "${PROJECT_ROOT}/src/main/resources/example-udf-substring.jar"  "${build_dir}/"

  log_info "Building Docker image ${image_tag} ..."
  docker build -t "${image_tag}" \
    --build-arg BASE_IMAGE="${base_image}" \
    -f "${PROJECT_ROOT}/docker/kind/Dockerfile" \
    "${build_dir}"
  rm -rf "${build_dir}"

  kind load docker-image "${image_tag}" --name "${KIND_CLUSTER}"
  echo "${image_tag}"
}

# ── K8s Session 测试辅助方法 ────────────────────────────────────────────

# 启动 Session 集群 + 等待 JobManager 就绪
_start_session_cluster() {
  local version="$1" flink_home="$2" image_tag="$3" cluster_id="$4"
  local session_log=$(_log_path "${version}" "kubernetes-session_cluster")

  log_info "Starting K8s Session cluster ${cluster_id} ..."
  local s_cmd="${flink_home}/bin/kubernetes-session.sh -Dkubernetes.cluster-id=${cluster_id} -Dkubernetes.namespace=${K8S_NAMESPACE} -Dkubernetes.container.image=${image_tag} -Dkubernetes.jobmanager.cpu=0.5 -Dtaskmanager.numberOfTaskSlots=2 -Djobmanager.memory.process.size=1g -Dtaskmanager.memory.process.size=3g -Dexecution.target=kubernetes-session --detached"
  "${flink_home}/bin/kubernetes-session.sh" \
    -Dkubernetes.cluster-id="${cluster_id}" \
    -Dkubernetes.namespace="${K8S_NAMESPACE}" \
    -Dkubernetes.container.image="${image_tag}" \
    -Dkubernetes.container.image-pull-policy=IfNotPresent \
    -Dkubernetes.jobmanager.cpu=0.5 \
    -Dtaskmanager.numberOfTaskSlots=2 \
    -Djobmanager.memory.process.size=1g \
    -Dtaskmanager.memory.process.size=3g \
    -Dexecution.target=kubernetes-session \
    --detached > "${session_log}" 2>&1 || {
    local err; err=$(_extract_errors "${session_log}" 3)
    record_result "${version}" "kubernetes-session" "FAIL" "Session start failed: ${err}" 0 "${s_cmd}"
    return 1
  }

  log_info "Waiting for JobManager Pod to be ready ..."
  kubectl wait pod \
    -l "app=${cluster_id},component=jobmanager" \
    -n "${K8S_NAMESPACE}" \
    --for=condition=Ready \
    --timeout=120s >/dev/null 2>&1 || {
    record_result "${version}" "kubernetes-session" "FAIL" "JobManager pod not ready within 120s" 0 "${s_cmd}"
    kubectl delete deployment "${cluster_id}" -n "${K8S_NAMESPACE}" 2>/dev/null || true
    return 1
  }
}

# 清理集群资源（Deployment + Service）
_cleanup_cluster() {
  local cluster_id="$1" submit_pod="$2"
  kubectl delete pod "${submit_pod}" -n "${K8S_NAMESPACE}" --force --grace-period=0 2>/dev/null || true
  kubectl delete deployment "${cluster_id}" -n "${K8S_NAMESPACE}" 2>/dev/null || true
  kubectl delete service "${cluster_id}" -n "${K8S_NAMESPACE}" 2>/dev/null || true
  kubectl delete service "${cluster_id}-rest" -n "${K8S_NAMESPACE}" 2>/dev/null || true
}

# ── 测试：kubernetes-session ─────────────────────────────────────────────────
test_k8s_session() {
  local version="$1" flink_home="$2" image_tag="$3"

  local cluster_id="compat-session-${version//./-}"
  local submit_pod="compat-submit-${cluster_id}"
  local job_log=$(_log_path "${version}" "kubernetes-session")
  local tm_log=$(_log_path "${version}" "kubernetes-session_tm")
  mkdir -p "${RESULTS_RAW}/logs"

  # 1. 启动 Session 集群
  _start_session_cluster "${version}" "${flink_home}" "${image_tag}" "${cluster_id}" || return 1

  # 2. 后台提交任务（flink run --attached 阻塞到任务结束）
  log_info "Submitting job via pod ${submit_pod} ..."
  local run_cmd=(
    kubectl run "${submit_pod}"
    --namespace="${K8S_NAMESPACE}"
    --image="${image_tag}"
    --image-pull-policy=IfNotPresent
    --restart=Never
    --attach --rm
    --command --
    /opt/flink/bin/flink run
    --target kubernetes-session
    -Dkubernetes.cluster-id="${cluster_id}"
    -Dkubernetes.namespace="${K8S_NAMESPACE}"
    -Dpipeline.name="compat-test-${version}-kubernetes-session"
    -Dexecution.attached=true
    -C file:///opt/flink/usrlib/example-udf-reverse.jar
    -C file:///opt/flink/usrlib/example-udf-substring.jar
    /opt/flink/usrlib/app.jar
    --script-file "${TEST_SCRIPT}"
    ${APP_ARGS[@]+"${APP_ARGS[@]}"}
  )
  log_info "${run_cmd[*]}"
  LAST_CMD="${run_cmd[*]}"

  local start_ts=$(date +%s)

  # 后台执行提交命令
  "${run_cmd[@]}" > "${job_log}" 2>&1 &
  local submit_pid=$!

  # 3. 后台抓 TM 日志（等待 TM Pod 出现）
  (
    local _tm="" _e=0
    while [[ -z "${_tm}" && ${_e} -lt 60 ]]; do
      _tm=$(kubectl get pods -n "${K8S_NAMESPACE}" \
        -l "app=${cluster_id},component=taskmanager" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
      [[ -n "${_tm}" ]] && break
      sleep 1; ((_e++))
    done
    [[ -n "${_tm}" ]] && while true; do
      kubectl logs -n "${K8S_NAMESPACE}" "${_tm}" -f 2>/dev/null; sleep 1
    done
  ) > "${tm_log}" 2>&1 &
  local tm_log_pid=$!

  # 4. 轮询 TM 日志中的 +I[ 输出
  local result=0
  local elapsed=0
  while ((elapsed < JOB_TIMEOUT)); do
    if grep -qF '+I[' "${tm_log}" 2>/dev/null; then
      LAST_DURATION=$(( $(date +%s) - start_ts ))
      record_result "${version}" "kubernetes-session" "PASS" "" "${LAST_DURATION}" "${LAST_CMD}"
      # 终止提交进程和 TM 日志抓取
      kill "${submit_pid}" 2>/dev/null || true
      kill "${tm_log_pid}" 2>/dev/null || true
      wait "${submit_pid}" 2>/dev/null || true
      wait "${tm_log_pid}" 2>/dev/null || true
      _cleanup_cluster "${cluster_id}" "${submit_pod}"
      return 0
    fi
    # 如果提交进程已退出且无 +I[，判定失败
    if ! kill -0 "${submit_pid}" 2>/dev/null; then
      wait "${submit_pid}" 2>/dev/null || true
      LAST_DURATION=$(( $(date +%s) - start_ts ))
      # 提交进程退出后给 TM 一点时间输出
      sleep 5
      if grep -qF '+I[' "${tm_log}" 2>/dev/null; then
        record_result "${version}" "kubernetes-session" "PASS" "" "${LAST_DURATION}" "${LAST_CMD}"
        kill "${tm_log_pid}" 2>/dev/null || true
        wait "${tm_log_pid}" 2>/dev/null || true
        _cleanup_cluster "${cluster_id}" "${submit_pod}"
        return 0
      fi
      LAST_ERROR=$(_extract_errors "${job_log}")
      [[ -z "${LAST_ERROR}" ]] && LAST_ERROR="No print output from job"
      record_result "${version}" "kubernetes-session" "FAIL" "${LAST_ERROR}" "${LAST_DURATION}" "${LAST_CMD}"
      kill "${tm_log_pid}" 2>/dev/null || true
      wait "${tm_log_pid}" 2>/dev/null || true
      result=1
      log_info "Test failed — keeping K8s Session cluster for investigation"
      log_info "  Clean up manually: kubectl delete deployment ${cluster_id} -n ${K8S_NAMESPACE}"
      return ${result}
    fi
    sleep 2; ((elapsed += 2))
  done

  # 超时
  kill "${submit_pid}" 2>/dev/null || true
  kill "${tm_log_pid}" 2>/dev/null || true
  wait "${submit_pid}" 2>/dev/null || true
  wait "${tm_log_pid}" 2>/dev/null || true
  LAST_DURATION=$(( $(date +%s) - start_ts ))
  LAST_ERROR=$(_extract_errors "${job_log}")
  [[ -z "${LAST_ERROR}" ]] && LAST_ERROR="No print output within ${JOB_TIMEOUT}s"
  record_result "${version}" "kubernetes-session" "FAIL" "${LAST_ERROR}" "${LAST_DURATION}" "${LAST_CMD}"
  result=1
  log_info "Test failed — keeping K8s Session cluster for investigation"
  log_info "  Clean up manually: kubectl delete deployment ${cluster_id} -n ${K8S_NAMESPACE}"
  return ${result}
}

# ── 测试：kubernetes-application ─────────────────────────────────────────────
# Application 模式无法从客户端拿 +I[，改为后台抓 TM 日志 grep 判定。
test_k8s_application() {
  local version="$1" flink_home="$2" image_tag="$3"
  local cluster_id="compat-app-${version//./-}"
  local submit_pod="compat-submit-${cluster_id}"
  local tm_log=$(_log_path "${version}" "kubernetes-application_tm")
  mkdir -p "$(dirname "${tm_log}")"

  local start_ts=$(date +%s)
  local job_log=$(_log_path "${version}" "kubernetes-application")

  # 1. 提交
  local run_cmd=(
    kubectl run "${submit_pod}" --namespace="${K8S_NAMESPACE}"
    --image="${image_tag}" --image-pull-policy=IfNotPresent
    --restart=Never --attach --rm --command --
    /opt/flink/bin/flink $(_flink_subcmd "${version}" "kubernetes-application") --target kubernetes-application
    -Dkubernetes.cluster-id="${cluster_id}"
    -Dkubernetes.namespace="${K8S_NAMESPACE}"
    -Dkubernetes.container.image="${image_tag}"
    -Dkubernetes.jobmanager.cpu=0.5
    -Dtaskmanager.numberOfTaskSlots=2
    -Djobmanager.memory.process.size=1g
    -Dtaskmanager.memory.process.size=2g
    -Dpipeline.name="compat-test-${version}-kubernetes-application"
    -C file:///opt/flink/usrlib/example-udf-reverse.jar
    -C file:///opt/flink/usrlib/example-udf-substring.jar
    /opt/flink/usrlib/app.jar
    --script-file "${TEST_SCRIPT}"
    ${APP_ARGS[*]}
  )
  LAST_CMD="${run_cmd[*]}"
  log_info "${LAST_CMD}"

  "${run_cmd[@]}" > "${job_log}" 2>&1 || {
      LAST_DURATION=$(( $(date +%s) - start_ts ))
      LAST_ERROR=$(_extract_errors "${job_log}")
      record_result "${version}" "kubernetes-application" "FAIL" \
        "${LAST_ERROR:-Submission failed}" "${LAST_DURATION}" "${LAST_CMD}"
      return 1
    }

  # 2. 后台抓 TM 日志（重试到 ContainerRunning）
  (
    local _tm="" _e=0
    while [[ -z "${_tm}" && ${_e} -lt 60 ]]; do
      _tm=$(kubectl get pods -n "${K8S_NAMESPACE}" \
        -l "component=taskmanager" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
      [[ -n "${_tm}" ]] && break
      sleep 1; ((_e++))
    done
    [[ -n "${_tm}" ]] && while true; do
      kubectl logs -n "${K8S_NAMESPACE}" "${_tm}" -f 2>/dev/null; sleep 1
    done
  ) > "${tm_log}" 2>&1 &

  # 3. 轮询 +I[
  local elapsed=0
  while ((elapsed < JOB_TIMEOUT)); do
    if grep -qF '+I[' "${tm_log}" 2>/dev/null; then
      LAST_DURATION=$(( $(date +%s) - start_ts ))
      record_result "${version}" "kubernetes-application" "PASS" "" "${LAST_DURATION}" "${LAST_CMD}"
      _cleanup_cluster "${cluster_id}" "${submit_pod}"
      return 0
    fi
    sleep 2; ((elapsed += 2))
  done

  LAST_DURATION=$(( $(date +%s) - start_ts ))
  record_result "${version}" "kubernetes-application" "FAIL" \
    "No +I[ output within ${JOB_TIMEOUT}s" "${LAST_DURATION}" "${LAST_CMD}"
  return 1
}

# ── 主流程 ───────────────────────────────────────────────────────────────────
main() {
  trap 'on_exit $?' EXIT
  check_jar_exists
  check_python3

  # 前置条件检查
  for cmd in kind kubectl docker; do
    command -v "${cmd}" >/dev/null || {
      log_error "${cmd} not found. Install with: brew install ${cmd}"
      exit 1
    }
  done

  local versions
  if [[ -n "${TARGET_VERSION}" ]]; then
    versions="${TARGET_VERSION}"
  else
    versions=$(get_all_versions)
  fi

  local modes=("kubernetes-session" "kubernetes-application")
  if [[ -n "${TARGET_MODE}" ]]; then
    modes=("${TARGET_MODE}")
  fi

  start_kind_cluster

  local pass=0 fail=0

  for version in ${versions}; do
    log_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log_info "Preparing flink-${version} for K8s ..."

    local flink_home
    flink_home=$(ensure_flink "${version}") || {
      for mode in "${modes[@]}"; do
        record_result "${version}" "${mode}" "FAIL" "Failed to download flink-${version}" 0
        ((fail++))
      done
      continue
    }

    local image_tag
    image_tag=$(build_and_load_image "${version}" "${flink_home}")

    for mode in "${modes[@]}"; do
      log_info "Testing flink-${version} × ${mode}"
      case "${mode}" in
        kubernetes-session)
          if test_k8s_session      "${version}" "${flink_home}" "${image_tag}"; then ((pass++)); else ((fail++)); fi ;;
        kubernetes-application)
          if test_k8s_application  "${version}" "${flink_home}" "${image_tag}"; then ((pass++)); else ((fail++)); fi ;;
      esac
    done
  done

  echo ""
  log_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log_info "K8s summary: ${pass} passed, ${fail} failed"
  [[ ${fail} -eq 0 ]]
}

on_exit() {
  local exit_code=$1
  if [[ ${exit_code} -eq 0 ]]; then
    delete_kind_cluster
  else
    log_info "Test failed — keeping Kind cluster for investigation"
    log_info "Clean up manually: kind delete cluster --name ${KIND_CLUSTER}"
  fi
}

main "$@"
