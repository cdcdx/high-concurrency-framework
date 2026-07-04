.PHONY: build run test lint clean docker-build docker-push deploy perf-auth perf-write perf-read

# 变量
BINARY_NAME=server
IMAGE_NAME=server
VERSION=1.0.0
PORT=8080

# 编译
build:
	@echo "Building $(BINARY_NAME)..."
	CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=$(VERSION)" -o bin/$(BINARY_NAME) ./cmd/server/

# 本地运行
run: build
	@echo "Starting server on :$(PORT)..."
	./bin/$(BINARY_NAME)

# 测试
test:
	@echo "Running tests..."
	go test -v -race -cover ./...

# 高可用集成测试
ha-test:
	@echo "Running HA integration tests..."
	go test -v -count=1 -timeout 120s ./tests/ -run "TestRead|TestWrite|TestHigh|TestMon|TestDegrad|TestCircuit|TestLiveness|TestStart|TestReadiness|TestTrace|TestHot|TestInval|TestGrace|TestConcur|TestMulti|TestBusiness|TestMonitor|TestJSON"

# 基准测试
bench:
	@echo "Running benchmarks..."
	go test -bench=. -benchmem -timeout 60s ./tests/...

# 代码检查
lint:
	@echo "Running linters..."
	go vet ./...
	gofmt -s -w .

# 清理
clean:
	@echo "Cleaning..."
	rm -rf bin/

# Docker构建
docker-build:
	@echo "Building docker image $(IMAGE_NAME):$(VERSION)..."
	docker build -t $(IMAGE_NAME):$(VERSION) .

# K8s部署
deploy:
	@echo "Deploying to Kubernetes..."
	kubectl apply -f k8s-deployment.yaml

# 扩容测试
scale-up:
	kubectl scale deployment server --replicas=10

# 压测 (需要ab或wrk, jq)
# TOKEN_FILE 用于跨 target 共享 token
TOKEN_FILE := /tmp/bench_token_$(PORT).txt

# 获取认证Token (先登录，失败则注册后再登录)
perf-auth:
	@echo "=== Obtaining auth token ==="
	@TOKEN=$$(curl -s -X POST http://localhost:$(PORT)/api/v1/auth/login \
		-H 'Content-Type: application/json' \
		-d '{"username":"testuser","password":"123456"}' | jq -r '.data.access_token'); \
	if [ "$$TOKEN" = "null" ] || [ -z "$$TOKEN" ]; then \
		echo "Login failed, trying register..."; \
		curl -s -X POST http://localhost:$(PORT)/api/v1/auth/register \
			-H 'Content-Type: application/json' \
			-d '{"username":"testuser","password":"123456","email":"test@example.com"}' > /dev/null; \
		TOKEN=$$(curl -s -X POST http://localhost:$(PORT)/api/v1/auth/login \
			-H 'Content-Type: application/json' \
			-d '{"username":"testuser","password":"123456"}' | jq -r '.data.access_token'); \
	fi; \
	echo "$$TOKEN" > $(TOKEN_FILE); \
	echo "Token: $${TOKEN:0:20}..."

perf-write: perf-auth
	@echo "=== Write API Benchmark ==="
	@TOKEN=$$(cat $(TOKEN_FILE)); \
	ab -n 100000 -c 1000 -p tests/order.json -T application/json \
		-H "Authorization: Bearer $$TOKEN" \
		http://localhost:$(PORT)/api/v1/orders

perf-read: perf-auth
	@echo "=== Read API Benchmark ==="
	@TOKEN=$$(cat $(TOKEN_FILE)); \
	ab -n 100000 -c 1000 \
		-H "Authorization: Bearer $$TOKEN" \
		http://localhost:$(PORT)/api/v1/users/10001/profile

# 安装依赖
deps:
	go mod download
	go mod tidy

# 生成mock (需要mockgen)
mock:
	mockgen -source=internal/service/order_service.go -destination=internal/service/mocks/order_service_mock.go

# 完整CI流程
ci: lint test build
