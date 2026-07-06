#!/bin/bash
# API 测试脚本 (含 JWT 认证)
BASE="http://127.0.0.1:8080"


ab -n 10000 -c 100 -k -r -s 30 \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3ODMxMzcyNzEsImlhdCI6MTc4MzEzMDA3MSwidXNlcl9pZCI6MSwidXNlcm5hbWUiOiJwYW5ndG91In0.UgfBAPTq5Qs1Yd5hkh5-gOy_4DgyG12B2bJ0p8rLsOQ" \
  http://localhost:8080/api/v1/users/10001/profile

ab -n 10000 -c 100 -k -r -s 30 \
  -p tests/order.json -T application/json \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3ODMxMzcyNzEsImlhdCI6MTc4MzEzMDA3MSwidXNlcl9pZCI6MSwidXNlcm5hbWUiOiJwYW5ndG91In0.UgfBAPTq5Qs1Yd5hkh5-gOy_4DgyG12B2bJ0p8rLsOQ" \
  http://localhost:8080/api/v1/orders/sync

# ---------------------------------------------------------- 认证 (先注册+登录获取Token)
echo "=== 1. 注册用户 ==="
curl -s -X POST "$BASE/api/v1/auth/register" \
  -H 'Content-Type: application/json' \
  -d '{"username":"testuser","password":"123456","email":"test@example.com"}' | jq .

echo ""
echo "=== 2. 登录获取 Token ==="
LOGIN_RESP=$(curl -s -X POST "$BASE/api/v1/auth/login" \
  -H 'Content-Type: application/json' \
  -d '{"username":"testuser","password":"123456"}')
echo "$LOGIN_RESP" | jq .

# 提取 access_token
TOKEN=$(echo "$LOGIN_RESP" | jq -r '.data.access_token')
if [ "$TOKEN" = "null" ] || [ -z "$TOKEN" ]; then
  echo "ERROR: 获取 Token 失败, 退出"
  exit 1
fi

echo "Token: ${TOKEN:0:20}..."
echo ""

# ---------------------------------------------------------- 用户 (需认证)
echo "=== 3. 创建用户资料 ==="
curl -s -X POST "$BASE/api/v1/users/profile" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"user_id":10001,"nickname":"Alice","email":"alice@example.com","phone":"13800000001","bio":"Hello","tags":["vip","active"]}' | jq .

curl -s -X POST "$BASE/api/v1/users/profile" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"user_id":10002,"nickname":"Bob","email":"bob@example.com","phone":"13800000002","bio":"Hello","tags":["vip"]}' | jq .

curl -s -X POST "$BASE/api/v1/users/profile" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"user_id":10003,"nickname":"Carol","email":"carol@example.com","phone":"13800000003","bio":"Hello","tags":["active"]}' | jq .

echo ""
echo "=== 4. 查询用户资料 ==="
curl -s "$BASE/api/v1/users/10001/profile" -H "Authorization: Bearer $TOKEN" | jq .
curl -s "$BASE/api/v1/users/10002/profile" -H "Authorization: Bearer $TOKEN" | jq .
curl -s "$BASE/api/v1/users/10003/profile" -H "Authorization: Bearer $TOKEN" | jq .

echo ""
echo "=== 5. 搜索用户 (模板) ==="
# 替换 {keyword} 为实际搜索词
echo "curl -s \"$BASE/api/v1/users/search?q={keyword}&page=1&size=20\" -H \"Authorization: Bearer $TOKEN\" | jq ."

# ---------------------------------------------------------- 订单 (需认证)
echo ""
echo "=== 6. 创建订单 ==="
curl -s -X POST "$BASE/api/v1/orders" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"user_id":10001,"amount":99.91}' | jq .

curl -s -X POST "$BASE/api/v1/orders" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"user_id":10002,"amount":99.92}' | jq .

curl -s -X POST "$BASE/api/v1/orders" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"user_id":10003,"amount":99.93}' | jq .

echo ""
echo "=== 7. 查询订单 (模板) ==="
echo "curl -s \"$BASE/api/v1/orders/{orderNo}\" -H \"Authorization: Bearer $TOKEN\" | jq ."
echo "curl -s \"$BASE/api/v1/orders/search?q={keyword}&page=1&size=20\" -H \"Authorization: Bearer $TOKEN\" | jq ."
# ---------------------------------------------------------- 分析
curl -s "http://127.0.0.1:8080/api/v1/analytics/daily?from=2026-06-28&to=2026-07-01" | jq .
curl -s "http://127.0.0.1:8080/api/v1/analytics/behaviors?type=order_create" | jq .
# ---------------------------------------------------------- 监控
curl -s "http://127.0.0.1:8080/api/v1/monitor/metrics" | jq .
curl -s "http://127.0.0.1:8080/api/v1/monitor/circuit-breaker" | jq .
# ---------------------------------------------------------- 探针
curl -s "http://127.0.0.1:8080/health/liveness" | jq .
curl -s "http://127.0.0.1:8080/health/readiness" | jq .
curl -s "http://127.0.0.1:8080/health/startup" | jq .
# ----------------------------------------------------------

docker exec mongodb mongosh --quiet --eval '
use demo;
db.createUser({
  user: "demo",
  pwd: "123456",
  roles: [{ role: "readWrite", db: "demo" }]
});
print("---");
use demo;
db.getUsers();
' 2>&1

# 创建 admin 用户
docker exec mongodb mongosh "mongodb://127.0.0.1:27017/admin" --quiet --eval '
  db.createUser({
    user: "demo",
    pwd: "123456",
    roles: [{ role: "readWrite", db: "demo" }]
  })
'

docker run -d --name kafka-map -p 8888:8080 \
    --restart always \
    --network 1panel-network \
    -e DEFAULT_USERNAME=webx \
    -e DEFAULT_PASSWORD=webx123 \
    -v /opt/data/kafka-map:/usr/local/kafka-map/data \
    -d dushixiang/kafka-map:latest
