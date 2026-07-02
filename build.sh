clear
make clean

echo "build swagger ------------------>"
# 生成 Swagger 文档 (在线文档)
export PATH=$PATH:$(go env GOPATH)/bin
swag init -g cmd/server/main.go -o docs/ --parseDependency --parseInternal 2>&1 | grep -vE "warning: (failed to get package name|failed to evaluate const)"

echo "build server ------------------>"
make build

echo "start server ------------------>"
./bin/server
