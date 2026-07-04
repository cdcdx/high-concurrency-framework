#!/bin/bash
# Elasticsearch 索引初始化
# 用法: ES_URL=http://localhost:9200 bash sql/elasticsearch_init.sh
# 修改索引配置请只改此文件，程序启动时会通过 EnsureSearchIndexes() 自动执行

ES_URL="${ES_URL:-http://localhost:9200}"
AUTH="${ES_AUTH:-}"  # eg: "elastic:password"

AUTH_FLAG=""
[ -n "$AUTH" ] && AUTH_FLAG="-u $AUTH"

echo "==> Elasticsearch 索引初始化 (target: $ES_URL)"

# ──────────────────────────────────────────
# 1. 订单搜索索引
# ──────────────────────────────────────────
echo ""
echo "--- [orders_search] ---"

curl -s -o /dev/null -w "%{http_code}" $AUTH_FLAG -X PUT "$ES_URL/orders_search" \
  -H 'Content-Type: application/json' -d '{
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 1
  },
  "mappings": {
    "properties": {
      "order_no":   { "type": "keyword" },
      "user_id":    { "type": "long" },
      "amount":     { "type": "float" },
      "status":     { "type": "keyword" },
      "created_at": { "type": "date" },
      "updated_at": { "type": "date" }
    }
  }
}'
echo "  orders_search created"

# # 插入测试数据
# curl -s -o /dev/null -w "%{http_code}" $AUTH_FLAG -X POST "$ES_URL/_bulk" \
#   -H 'Content-Type: application/x-ndjson' --data-binary '
#   { "index": { "_index": "orders_search", "_id": "ORD20260628001" } }
#   { "order_no": "ORD20260628001", "user_id": 1, "amount": 299.50, "status": "paid",      "created_at": "2026-06-28T10:00:00", "updated_at": "2026-06-28T10:00:00" }
#   { "index": { "_index": "orders_search", "_id": "ORD20260628002" } }
#   { "order_no": "ORD20260628002", "user_id": 2, "amount": 158.00, "status": "shipped",   "created_at": "2026-06-28T12:30:00", "updated_at": "2026-06-29T08:00:00" }
#   { "index": { "_index": "orders_search", "_id": "ORD20260629001" } }
#   { "order_no": "ORD20260629001", "user_id": 1, "amount": 520.00, "status": "paid",      "created_at": "2026-06-29T09:00:00", "updated_at": "2026-06-29T09:00:00" }
#   { "index": { "_index": "orders_search", "_id": "ORD20260629002" } }
#   { "order_no": "ORD20260629002", "user_id": 3, "amount": 89.90,  "status": "cancelled", "created_at": "2026-06-29T15:00:00", "updated_at": "2026-06-29T16:00:00" }
#   { "index": { "_index": "orders_search", "_id": "ORD20260630001" } }
#   { "order_no": "ORD20260630001", "user_id": 2, "amount": 1280.00,"status": "paid",      "created_at": "2026-06-30T08:00:00", "updated_at": "2026-06-30T08:00:00" }
#   { "index": { "_index": "orders_search", "_id": "ORD20260701001" } }
#   { "order_no": "ORD20260701001", "user_id": 1, "amount": 66.00,  "status": "pending",   "created_at": "2026-07-01T18:00:00", "updated_at": "2026-07-01T18:00:00" }
#   '
# echo "  test data inserted"



# ──────────────────────────────────────────
# 2. 用户搜索索引
# ──────────────────────────────────────────
echo ""
echo "--- [users_search] ---"

curl -s -o /dev/null -w "%{http_code}" $AUTH_FLAG -X PUT "$ES_URL/users_search" \
  -H 'Content-Type: application/json' -d '{
    "settings": {
      "number_of_shards": 3,
      "number_of_replicas": 1
    },
    "mappings": {
      "properties": {
        "user_id":    { "type": "long" },
        "nickname":   { "type": "text", "analyzer": "standard" },
        "email":      { "type": "text" },
        "phone":      { "type": "keyword" },
        "created_at": { "type": "date" }
      }
    }
  }'
echo "  users_search created"

# # 插入测试数据
# curl -s -o /dev/null -w "%{http_code}" $AUTH_FLAG -X POST "$ES_URL/_bulk" \
#   -H 'Content-Type: application/x-ndjson' --data-binary '
#   { "index": { "_index": "users_search", "_id": "1" } }
#   { "user_id": 1, "nickname": "张三",   "email": "zhangsan@example.com", "phone": "13800000001", "created_at": "2026-06-01T08:00:00" }
#   { "index": { "_index": "users_search", "_id": "2" } }
#   { "user_id": 2, "nickname": "李四",   "email": "lisi@example.com",     "phone": "13800000002", "created_at": "2026-06-10T10:00:00" }
#   { "index": { "_index": "users_search", "_id": "3" } }
#   { "user_id": 3, "nickname": "王五",   "email": "wangwu@example.com",   "phone": "13800000003", "created_at": "2026-06-15T14:00:00" }
#   { "index": { "_index": "users_search", "_id": "4" } }
#   { "user_id": 4, "nickname": "赵六",   "email": "zhaoliu@example.com",  "phone": "13800000004", "created_at": "2026-06-20T09:00:00" }
#   { "index": { "_index": "users_search", "_id": "5" } }
#   { "user_id": 5, "nickname": "孙七",   "email": "sunqi@example.com",    "phone": "13800000005", "created_at": "2026-06-25T16:00:00" }
# '
# echo "  test data inserted"

echo ""
echo "==> Elasticsearch 索引初始化完成"
