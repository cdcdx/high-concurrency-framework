#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── 颜色 ──
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
log_ok()   { echo -e "${GREEN}[OK]${NC}    $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_err()  { echo -e "${RED}[ERROR]${NC} $*"; }

# ── 配置 ──
BINARY_NAME="server"
KUBECTL_VERSION="v1.34.0"
KIND_VERSION="v0.22.0"
CLUSTER_NAME="dev"

# ── 帮助 ──
show_usage() {
    cat <<EOF
用法: ./build.sh <command> [options]

Commands:
  init        初始化开发环境 (安装 kubectl + kind, 创建本地 K8s 集群)
  build       编译并启动服务 (默认)
  docker      构建 Docker 镜像并部署到 K8s
  test        运行测试 (含 race detector)
  lint        代码检查 (vet + fmt)
  clean       清理构建产物
  deps        安装/更新 Go 依赖
  swagger     生成 Swagger 文档

示例:
  ./build.sh                # 编译并启动
  ./build.sh init           # 初始化环境
  ./build.sh docker         # Docker 构建+部署
  ./build.sh test           # 运行测试

EOF
}

# ── 工具检查 ──
require_cmd() {
    local cmd="$1"
    if ! command -v "$cmd" &>/dev/null; then
        log_err "缺少命令: $cmd, 请先安装"
        exit 1
    fi
}

# ── init: 初始化开发环境 ──
cmd_init() {
    log_info "初始化开发环境..."

    apt install -y apache2-utils

    # kubectl
    if command -v kubectl &>/dev/null; then
        log_ok "kubectl 已安装: $(kubectl version --client --short 2>/dev/null || kubectl version --client 2>/dev/null | head -1)"
    else
        log_info "安装 kubectl ${KUBECTL_VERSION}..."
        curl -fsSLO "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl"
        chmod +x kubectl
        sudo mv kubectl /usr/local/bin/kubectl
        log_ok "kubectl 安装完成: $(kubectl version --client 2>/dev/null | head -1)"
    fi

    # kind
    if command -v kind &>/dev/null; then
        log_ok "kind 已安装: $(kind version 2>/dev/null | head -1)"
    else
        log_info "安装 kind ${KIND_VERSION}..."
        curl -fsSLo /usr/local/bin/kind "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-amd64"
        chmod +x /usr/local/bin/kind
        log_ok "kind 安装完成: $(kind version 2>/dev/null | head -1)"
    fi

    # Docker 检查
    if ! docker info &>/dev/null; then
        log_err "Docker 未运行, 请先启动 Docker"
        exit 1
    fi

    # 创建 K8s 集群
    if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
        log_ok "K8s 集群 '${CLUSTER_NAME}' 已存在"
    else
        log_info "创建 K8s 集群 '${CLUSTER_NAME}'..."
        kind create cluster --name "$CLUSTER_NAME"
        log_ok "集群创建完成"
    fi

    log_info "初始化完成, 执行 make deploy 部署应用"
}

# ── docker: 构建镜像并部署 ──
cmd_docker() {
    require_cmd docker
    require_cmd kubectl

    log_info "构建 Docker 镜像..."
    make docker-build
    log_ok "镜像构建完成"

    log_info "部署到 Kubernetes..."
    # 将镜像加载到 kind 集群 (本地集群)
    if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
        kind load docker-image server:1.0.0 --name "$CLUSTER_NAME" 2>/dev/null || true
    fi
    make deploy
    log_ok "部署完成"

    # 等待就绪
    log_info "等待 Pod 就绪..."
    kubectl rollout status deployment/server --timeout=120s 2>/dev/null || log_warn "等待超时, 请手动检查"
    kubectl get pods -l app=server -o wide 2>/dev/null || true
}

# ── build: 编译并启动 ──
cmd_build() {
    # Swagger 文档 (可选)
    if command -v swag &>/dev/null; then
        log_info "生成 Swagger 文档..."
        swag init -g cmd/server/main.go -o swagger/ --parseDependency --parseInternal 2>&1 | grep -vE "warning: (failed to get package name|failed to evaluate const)" || true
        log_ok "Swagger 文档已更新"
    else
        log_warn "swag 未安装, 跳过文档生成 (install: go install github.com/swaggo/swag/cmd/swag@latest)"
    fi

    log_info "编译 ${BINARY_NAME}..."
    make build
    log_ok "编译完成"

    log_info "启动服务..."
    exec ./bin/"$BINARY_NAME"
}

# ── test: 运行测试 ──
cmd_test() {
    log_info "运行测试 (race detector)..."
    make test
}

# ── lint: 代码检查 ──
cmd_lint() {
    log_info "代码检查..."
    make lint
    log_ok "检查完成"
}

# ── clean: 清理 ──
cmd_clean() {
    log_info "清理构建产物..."
    make clean
    log_ok "清理完成"
}

# ── deps: 依赖管理 ──
cmd_deps() {
    log_info "更新 Go 依赖..."
    go mod download
    go mod tidy
    log_ok "依赖更新完成"
}

# ── swagger: 仅生成文档 ──
cmd_swagger() {
    require_cmd swag
    log_info "生成 Swagger 文档..."
    swag init -g cmd/server/main.go -o swagger/ --parseDependency --parseInternal 2>&1 | grep -vE "warning: (failed to get package name|failed to evaluate const)" || true
    log_ok "Swagger 文档已生成: swagger/"
}

# ── 入口 ──
CMD="${1:-build}"
shift || true

case "$CMD" in
    init)    cmd_init    "$@" ;;
    docker)  cmd_docker  "$@" ;;
    build)   cmd_build   "$@" ;;
    test)    cmd_test    "$@" ;;
    lint)    cmd_lint    "$@" ;;
    clean)   cmd_clean   "$@" ;;
    deps)    cmd_deps    "$@" ;;
    swagger) cmd_swagger "$@" ;;
    -h|--help|help) show_usage ;;
    *)
        log_err "未知命令: $CMD"
        show_usage
        exit 1
        ;;
esac
