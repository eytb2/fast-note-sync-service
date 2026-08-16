#!/usr/bin/env bash
# Docker 化的 Go 构建/测试入口（不在宿主机安装 Go 工具链）
# 用法:
#   ./scripts/docker-dev.sh build   # 编译到 build/linux_amd64/
#   ./scripts/docker-dev.sh test    # 跑全部单测
#   ./scripts/docker-dev.sh test ./internal/dao/...  # 跑指定包
#   ./scripts/docker-dev.sh fmt / vet / tidy
#   ./scripts/docker-dev.sh run ...  # 任意 go 命令
set -euo pipefail

cd "$(dirname "$0")/.."

GO_IMAGE=${GO_IMAGE:-golang:1.26-alpine}
CACHE_VOL=fns-go-mod-cache

# 模块缓存卷：首次创建后复用，避免重复下载依赖
docker volume inspect "$CACHE_VOL" >/dev/null 2>&1 || docker volume create "$CACHE_VOL" >/dev/null

cmd=${1:-build}; shift || true

CGO=0
case "$cmd" in
  build)
    set -- go build -ldflags '-s -w' -o build/linux_amd64/fast-note-sync-service . ;;
  test)
    # 测试里的 gorm.io/driver/sqlite 依赖 CGO，用带 gcc 的 debian 镜像
    GO_IMAGE=${GO_IMAGE_OVERRIDE:-golang:1.26}
    CGO=1
    set -- go test -count=1 "$@" ;;
  fmt)  set -- gofmt -l -w . ;;
  vet)  set -- go vet ./... ;;
  tidy) set -- go mod tidy ;;
  run)  ;;
  *)    echo "unknown cmd: $cmd" >&2; exit 1 ;;
esac

# 透传 FNS_* 环境变量（如 FNS_FUZZ_SEEDS）到容器
env_pass=()
while IFS= read -r var; do
  env_pass+=(-e "$var")
done < <(env | cut -d= -f1 | grep '^FNS_' || true)

exec docker run --rm \
  -v "$PWD":/src -w /src \
  -v "$CACHE_VOL":/go/pkg/mod \
  -e CGO_ENABLED=$CGO \
  -e GOFLAGS=-buildvcs=false \
  -e GOPROXY=${GOPROXY:-https://goproxy.cn,direct} \
  -e GOTOOLCHAIN=local \
  "${env_pass[@]}" \
  "$GO_IMAGE" "$@"
