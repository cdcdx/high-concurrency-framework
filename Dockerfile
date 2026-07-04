# ---- 构建阶段 ----
# Go 版本需与 go.mod 中的 go 指令一致
FROM golang:1.25-alpine AS builder

# 使用国内 Go 代理加速依赖下载
ENV GOPROXY=https://goproxy.cn,https://proxy.golang.org,direct

RUN apk add --no-cache git ca-certificates

WORKDIR /build

# 先复制依赖文件, 利用Docker缓存层
COPY go.mod go.sum ./
RUN go mod download

# 复制源码并编译
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -X main.version=1.0.0" \
    -o /build/server \
    ./cmd/server/

# ---- 运行阶段 ----
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata wget && \
    cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "Asia/Shanghai" > /etc/timezone

WORKDIR /app

COPY --from=builder /build/server .
COPY --from=builder /build/config.yaml .
COPY --from=builder /build/sql ./sql

# 健康检查
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health/liveness || exit 1

EXPOSE 8080

USER nobody

ENTRYPOINT ["./server"]
