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
FLINK_IMAGE_BASE="apache/flink"

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
ensure_kind_cluster() {
  if kind get clusters 2>/dev/null | grep -q "^${KIND_CLUSTER}$"; then
    log_info "Kind cluster '${KIND_CLUSTER}' already exists"
  else
    log_info "Creating Kind cluster '${KIND_CLUSTER}' ..."
    kind create cluster \
      --name "${KIND_CLUSTER}" \
      --config "${KIND_CONFIG}" \
      --wait 60s
  fi
  kubectl config use-context "kind-${KIND_CLUSTER}"
  kubectl create namespace "${K8S_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
}

delete_kind_cluster() {
  log_info "Deleting Kind cluster '${KIND_CLUSTER}' ..."
  kind delete cluster --name "${KIND_CLUSTER}" 2>/dev/null || true
}

# 构建自定义 Docker 镜像，将应用 JAR 打包进去
build_and_load_image() {
  local version="$1"
  local flink_home="$2"
  local image_tag="flink-compat-app:${version}"

  log_info "Building Docker image ${image_tag} ..."

  # 从配置中获取 Java 版本
  local java_version
  java_version=$(get_version_field "${version}" "java")
  local base_image="${FLINK_IMAGE_BASE}:${version}-java${java_version}"

  # 编写最小 Dockerfile
  local build_dir
  build_dir=$(mktemp -d)
  cp "${APP_JAR}" "${build_dir}/app.jar"

  cat > "${build_dir}/Dockerfile" <<DOCKERFILE
FROM ${base_image}
COPY app.jar /opt/flink/usrlib/app.jar
DOCKERFILE

  docker build --platform linux/amd64 -t "${image_tag}" "${build_dir}"
  kind load docker-image "${image_tag}" --name "${KIND_CLUSTER}"
  rm -rf "${build_dir}"
  echo "${image_tag}"
}

# ── 测试：kubernetes-session ─────────────────────────────────────────────────
test_k8s_session() {
  local version="$1"
  local flink_home="$2"
  local image_tag="$3"
  local java_version
  java_version=$(get_version_field "${version}" "java")

  local cluster_id="compat-session-${version//./-}"
  local session_log="${RESULTS_RAW}/logs/${version}_kubernetes-session_cluster.log"
  mkdir -p "${RESULTS_RAW}/logs"

  log_info "Starting K8s Session cluster ${cluster_id} ..."

  # 启动 Session 集群
  "${flink_home}/bin/kubernetes-session.sh" \
    -Dkubernetes.cluster-id="${cluster_id}" \
    -Dkubernetes.namespace="${K8S_NAMESPACE}" \
    -Dkubernetes.container.image="${image_tag}" \
    -Dkubernetes.container.image.pull-policy=Never \
    -Dkubernetes.jobmanager.cpu=0.5 \
    -Dtaskmanager.numberOfTaskSlots=2 \
    -Djobmanager.memory.process.size=1024m \
    -Dtaskmanager.memory.process.size=1024m \
    -Dexecution.target=kubernetes-session \
    -Dkubernetes.rest-service.exposed.type=NodePort \
    --detached > "${session_log}" 2>&1 || {
    local err
    err=$(grep -E "Exception|Error" "${session_log}" | head -3 | tr '\n' ' ')
    record_result "${version}" "kubernetes-session" "FAIL" "Session start failed: ${err}" 0
    return 1
  }

  # 等待 JobManager 就绪
  log_info "Waiting for JobManager Pod to be ready ..."
  kubectl wait pod \
    -l "app=${cluster_id},component=jobmanager" \
    -n "${K8S_NAMESPACE}" \
    --for=condition=Ready \
    --timeout=120s >/dev/null 2>&1 || {
    record_result "${version}" "kubernetes-session" "FAIL" "JobManager pod not ready within 120s" 0
    kubectl delete deployment "${cluster_id}" -n "${K8S_NAMESPACE}" 2>/dev/null || true
    return 1
  }

  # 提交任务
  local extra_args=(
    "-Dkubernetes.cluster-id=${cluster_id}"
    "-Dkubernetes.namespace=${K8S_NAMESPACE}"
  )
  local result=0
  if run_flink_job "${version}" "kubernetes-session" "${flink_home}" "${extra_args[@]}"; then
    record_result "${version}" "kubernetes-session" "PASS" "" "${LAST_DURATION}"
  else
    record_result "${version}" "kubernetes-session" "FAIL" "${LAST_ERROR}" "${LAST_DURATION}"
    result=1
  fi

  # 清理 Session 集群
  kubectl delete deployment "${cluster_id}" -n "${K8S_NAMESPACE}" 2>/dev/null || true
  kubectl delete service "${cluster_id}" -n "${K8S_NAMESPACE}" 2>/dev/null || true
  kubectl delete service "${cluster_id}-rest" -n "${K8S_NAMESPACE}" 2>/dev/null || true

  return ${result}
}

# ── 测试：kubernetes-application ─────────────────────────────────────────────
test_k8s_application() {
  local version="$1"
  local flink_home="$2"
  local image_tag="$3"

  local cluster_id="compat-app-${version//./-}"

  local extra_args=(
    "-Dkubernetes.cluster-id=${cluster_id}"
    "-Dkubernetes.namespace=${K8S_NAMESPACE}"
    "-Dkubernetes.container.image=${image_tag}"
    "-Dkubernetes.container.image.pull-policy=Never"
    "-Dkubernetes.jobmanager.cpu=0.5"
    "-Dtaskmanager.numberOfTaskSlots=2"
    "-Djobmanager.memory.process.size=1024m"
    "-Dtaskmanager.memory.process.size=1024m"
  )

  local result=0
  if run_flink_job "${version}" "kubernetes-application" "${flink_home}" "${extra_args[@]}"; then
    record_result "${version}" "kubernetes-application" "PASS" "" "${LAST_DURATION}"
  else
    record_result "${version}" "kubernetes-application" "FAIL" "${LAST_ERROR}" "${LAST_DURATION}"
    result=1
  fi

  # 清理资源
  kubectl delete deployment "${cluster_id}" -n "${K8S_NAMESPACE}" 2>/dev/null || true
  kubectl delete service "${cluster_id}" -n "${K8S_NAMESPACE}" 2>/dev/null || true
  kubectl delete service "${cluster_id}-rest" -n "${K8S_NAMESPACE}" 2>/dev/null || true

  return ${result}
}

# ── 主流程 ───────────────────────────────────────────────────────────────────
main() {
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

  ensure_kind_cluster
  # 不自动删除集群——跨多次运行复用可以加快速度
  # 手动清理：kind delete cluster --name flink-compat-test

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
          test_k8s_session      "${version}" "${flink_home}" "${image_tag}" && ((pass++)) || ((fail++)) ;;
        kubernetes-application)
          test_k8s_application  "${version}" "${flink_home}" "${image_tag}" && ((pass++)) || ((fail++)) ;;
      esac
    done
  done

  echo ""
  log_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log_info "K8s summary: ${pass} passed, ${fail} failed"
  [[ ${fail} -eq 0 ]]
}

main "$@"
