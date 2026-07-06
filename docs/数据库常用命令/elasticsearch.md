# Elasticsearch 常用命令

> **场景**：高并发业务系统 — 千万级用户、百万级在线，全文检索与实时分析


curl -X POST "http://localhost:9200/orders_search/_search" -H 'Content-Type: application/json' -d '{ "from": 9900, "size": 100, "query": { "match_all": {} } }' | jq .

curl -X POST "http://localhost:9200/orders_search/_search?scroll=2m" -H 'Content-Type: application/json' -d '{ "from": 29900, "size": 100, "query": { "match_all": {} } }' | jq .

curl -X POST "http://localhost:9200/orders_search/_search" -H 'Content-Type: application/json' -d '{ "size": 100, "query": { "match_all": {} }, "search_after": ["30000"] }' | jq .


---

```bash
# 1. 先用当前的 elastic 重置密码登录
docker exec -it elasticsearch elasticsearch-reset-password -u elastic -i

# 2. 创建 demo 用户
curl -u elastic:123456 -X POST "http://localhost:9200/_security/user/demo" \
  -H 'Content-Type: application/json' -d '{
  "password": "123456",
  "roles": ["superuser"]
}'

# 3. 验证新用户
curl -u demo:123456 http://localhost:9200



# 1. 不知道当前密码？先用 -b 获取一个，然后用它认证
docker exec -it elasticsearch elasticsearch-reset-password -u elastic -b

# 2. 修改elastic密码
curl -u elastic:上一步输出的密码 -X POST "http://localhost:9200/_security/user/elastic/_password" \
  -H 'Content-Type: application/json' -d '{
  "password": "123456"
}'

# 3. 验证新用户
curl -u elastic:123456 http://localhost:9200

```

## 一、连接与集群管理

```bash
# 健康检查
curl http://localhost:9200
curl http://localhost:9200/_cat/health?v                      # 集群健康 (green/yellow/red)

# 带认证连接
curl -u elastic:password http://localhost:9200

# 查看节点
curl http://localhost:9200/_cat/nodes?v                       # 所有节点
curl http://localhost:9200/_cat/master?v                      # 主节点

# 查看索引
curl http://localhost:9200/_cat/indices?v                     # 所有索引 (含文档数/大小)
curl http://localhost:9200/_cat/indices?v&s=store.size:desc   # 按存储大小排序

# 查看分片
curl http://localhost:9200/_cat/shards?v                      # 分片分布
curl http://localhost:9200/_cat/allocation?v                  # 磁盘分配

# 集群统计
curl http://localhost:9200/_cluster/stats?pretty
curl http://localhost:9200/_cluster/settings?pretty           # 集群配置
```

---

## 二、索引与映射设计

```bash
# 索引操作
curl -X DELETE "http://localhost:9200/old_index"              # 删索引
curl -X POST  "http://localhost:9200/users/_close"            # 关闭索引 (禁止读写)
curl -X POST  "http://localhost:9200/users/_open"             # 开启索引
curl -X POST  "http://localhost:9200/old_index/_rename/new_index"  # 重命名 (7.4+)
curl -X GET   "http://localhost:9200/users/_stats?pretty"     # 索引统计
```

### 建索引示例 — 用户搜索索引与订单搜索索引

```bash
# ============ 用户搜索索引 ============
curl -X PUT "http://localhost:9200/users_search" -H 'Content-Type: application/json' -d '{
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 1,
    "refresh_interval": "5s"
  },
  "mappings": {
    "properties": {
      "user_id":    { "type": "long" },
      "username":   { "type": "keyword" },
      "nickname":   { "type": "text", "analyzer": "standard" },
      "email":      { "type": "text" },
      "phone":      { "type": "keyword" },
      "avatar":     { "type": "keyword", "index": false },
      "bio":        { "type": "text", "analyzer": "ik_max_word" },
      "tags":       { "type": "keyword" },
      "settings":   { "type": "object", "enabled": false },
      "status":     { "type": "integer" },
      "created_at": { "type": "date" },
      "updated_at": { "type": "date" }
    }
  }
}'

# ============ 订单搜索索引 ============
curl -X PUT "http://localhost:9200/orders_search" -H 'Content-Type: application/json' -d '{
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 1
  },
  "mappings": {
    "properties": {
      "order_no":   { "type": "keyword" },
      "user_id":    { "type": "long" },
      "amount":     { "type": "scaled_float", "scaling_factor": 100 },
      "status":     { "type": "keyword" },
      "created_at": { "type": "date" },
      "updated_at": { "type": "date" }
    }
  }
}'
```

### 常用字段类型速查

| ES 类型 | 说明 | 等价 MySQL |
|---------|------|-----------|
| `keyword` | 精确匹配，不分词 (订单号/手机号/状态) | `VARCHAR` + 等值查询 |
| `text` | 全文检索，分词 (昵称/简介/文章) | `FULLTEXT INDEX` |
| `long` / `integer` | 整数 | `BIGINT` / `INT` |
| `float` / `double` | 浮点数 | `FLOAT` / `DOUBLE` |
| `scaled_float` | 定点小数 (金额) | `DECIMAL` |
| `date` | 日期时间 | `DATETIME` |
| `boolean` | 布尔 | `TINYINT` |
| `object` | 嵌套 JSON | `JSON` |
| `nested` | 独立索引的嵌套对象数组 | — |

---

## 三、增删改查 (CRUD)

### 3.1 插入/索引文档

```bash
# 单条插入 (指定ID)
curl -X PUT "http://localhost:9200/users_search/_doc/1" -H 'Content-Type: application/json' -d '{
  "user_id": 1,
  "username": "zhangsan",
  "nickname": "张三",
  "email": "zhangsan@example.com",
  "phone": "13800000001",
  "bio": "资深后端工程师，专注于高并发系统架构",
  "tags": ["go", "backend", "distributed"],
  "settings": { "theme": "dark", "lang": "zh" },
  "status": 1,
  "created_at": "2026-07-02T00:00:00",
  "updated_at": "2026-07-02T00:00:00"
}'

# 自动生成ID
curl -X POST "http://localhost:9200/users_search/_doc" -H 'Content-Type: application/json' -d '{ ... }'

# 批量插入 (_bulk)
curl -X POST "http://localhost:9200/_bulk" -H 'Content-Type: application/x-ndjson' -d '
{ "index": { "_index": "users_search", "_id": "2" } }
{ "user_id": 2, "username": "lisi", "nickname": "李四", "email": "lisi@example.com", "status": 1, "created_at": "2026-07-02T00:00:00" }
{ "index": { "_index": "users_search", "_id": "3" } }
{ "user_id": 3, "username": "wangwu", "nickname": "王五", "email": "wangwu@example.com", "status": 1, "created_at": "2026-07-02T00:00:00" }
'

# 插入订单
curl -X PUT "http://localhost:9200/orders_search/_doc/ORD20260702001" -H 'Content-Type: application/json' -d '{
  "order_no": "ORD20260702001",
  "user_id": 1,
  "amount": 199.99,
  "status": "paid",
  "created_at": "2026-07-02T10:30:00",
  "updated_at": "2026-07-02T10:30:00"
}'
```

### 3.2 查询

```bash
# ───── 基础查询 ─────

# 查询全部
curl -X GET "http://localhost:9200/users_search/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": { "match_all": {} }
}'

# 按ID查询
curl -X GET "http://localhost:9200/users_search/_doc/1?pretty"

# 等值查询 (keyword 字段用 term)
curl -X GET "http://localhost:9200/users_search/_search" -H 'Content-Type: application/json' -d '{
  "query": { "term": { "username": "zhangsan" } }
}'

# ───── 全文搜索 ─────

# 单字段全文搜索 (text 字段用 match)
curl -X GET "http://localhost:9200/users_search/_search" -H 'Content-Type: application/json' -d '{
  "query": { "match": { "bio": "高并发 工程师" } }
}'

# 多字段全文搜索 (multi_match)
curl -X GET "http://localhost:9200/users_search/_search" -H 'Content-Type: application/json' -d '{
  "query": {
    "multi_match": {
      "query": "张三",
      "fields": ["nickname^3", "email^2", "phone"]
    }
  }
}'

# 模糊搜索 (fuzzy, 容错1个字符)
curl -X GET "http://localhost:9200/users_search/_search" -H 'Content-Type: application/json' -d '{
  "query": { "fuzzy": { "nickname": { "value": "张三", "fuzziness": "AUTO" } } }
}'

# 短语精确匹配 (match_phrase)
curl -X GET "http://localhost:9200/users_search/_search" -H 'Content-Type: application/json' -d '{
  "query": { "match_phrase": { "bio": "高并发系统架构" } }
}'

# ───── 范围查询 ─────

# 金额范围 (gt/gte/lt/lte)
curl -X GET "http://localhost:9200/orders_search/_search" -H 'Content-Type: application/json' -d '{
  "query": { "range": { "amount": { "gte": 100, "lte": 500 } } }
}'

# 时间范围
curl -X GET "http://localhost:9200/orders_search/_search" -H 'Content-Type: application/json' -d '{
  "query": {
    "range": {
      "created_at": {
        "gte": "2026-06-25",
        "lte": "2026-07-02"
      }
    }
  }
}'

# ───── 布尔组合查询 ─────

# must(AND) + filter(无评分) + should(OR) + must_not(NOT)
curl -X GET "http://localhost:9200/orders_search/_search" -H 'Content-Type: application/json' -d '{
  "query": {
    "bool": {
      "must": [
        { "term": { "user_id": 1 } }
      ],
      "filter": [
        { "term": { "status": "paid" } },
        { "range": { "amount": { "gte": 50 } } }
      ],
      "must_not": [
        { "term": { "status": "cancelled" } }
      ]
    }
  }
}'

# IN 查询 (terms)
curl -X GET "http://localhost:9200/orders_search/_search" -H 'Content-Type: application/json' -d '{
  "query": { "terms": { "status": ["paid", "shipped"] } }
}'

# ───── 数组/嵌套查询 ─────

# tags 精确包含某个值
curl -X GET "http://localhost:9200/users_search/_search" -H 'Content-Type: application/json' -d '{
  "query": { "term": { "tags": "go" } }
}'

# tags 同时包含多个值
curl -X GET "http://localhost:9200/users_search/_search" -H 'Content-Type: application/json' -d '{
  "query": {
    "bool": {
      "must": [
        { "term": { "tags": "go" } },
        { "term": { "tags": "backend" } }
      ]
    }
  }
}'

# ───── 分页 + 排序 ─────

# 分页 (from + size)
curl -X GET "http://localhost:9200/users_search/_search" -H 'Content-Type: application/json' -d '{
  "from": 0,
  "size": 10,
  "query": { "match_all": {} },
  "sort": [
    { "created_at": { "order": "desc" } },
    { "_id": { "order": "asc" } }
  ]
}'

# 深度分页用 search_after (性能优于 from/size)
curl -X GET "http://localhost:9200/orders_search/_search" -H 'Content-Type: application/json' -d '{
  "size": 100,
  "query": { "match_all": {} },
  "sort": [
    { "created_at": "desc" },
    { "order_no": "asc" }
  ],
  "search_after": ["2026-06-25T00:00:00", "ORD20260625001"]
}'

# 只返回指定字段 (_source)
curl -X GET "http://localhost:9200/users_search/_search" -H 'Content-Type: application/json' -d '{
  "_source": ["user_id", "nickname", "email"],
  "query": { "match_all": {} }
}'
```

### 3.3 更新

```bash
# 更新部分字段 (_update)
curl -X POST "http://localhost:9200/users_search/_update/1" -H 'Content-Type: application/json' -d '{
  "doc": {
    "nickname": "张三 (已更新)",
    "updated_at": "2026-07-02T12:00:00"
  }
}'

# 脚本更新 — 原子增加 (类似 $inc)
curl -X POST "http://localhost:9200/orders_search/_update/ORD20260702001" -H 'Content-Type: application/json' -d '{
  "script": {
    "source": "ctx._source.amount += params.increment",
    "params": { "increment": 10 }
  }
}'

# Upsert (有则更新，无则插入)
curl -X POST "http://localhost:9200/users_search/_update/99" -H 'Content-Type: application/json' -d '{
  "doc": {
    "nickname": "新用户",
    "updated_at": "2026-07-02T12:00:00"
  },
  "doc_as_upsert": true
}'

# 批量更新 (_update_by_query)
curl -X POST "http://localhost:9200/users_search/_update_by_query" -H 'Content-Type: application/json' -d '{
  "script": { "source": "ctx._source.status = 0" },
  "query": { "term": { "status": 1 } }
}'
```

### 3.4 删除

```bash
# 按ID删除
curl -X DELETE "http://localhost:9200/users_search/_doc/1"

# 按条件删除 (_delete_by_query)
curl -X POST "http://localhost:9200/orders_search/_delete_by_query" -H 'Content-Type: application/json' -d '{
  "query": { "term": { "status": "cancelled" } }
}'

# 删除整个索引
curl -X DELETE "http://localhost:9200/users_search"
```

---

## 四、聚合分析

```bash
# ───── 桶聚合 (Bucket) ─────

# 按状态分组计数 (GROUP BY status)
curl -X GET "http://localhost:9200/orders_search/_search" -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": {
    "by_status": {
      "terms": { "field": "status" }
    }
  }
}'

# 按用户分组，看谁订单最多 (TOP N)
curl -X GET "http://localhost:9200/orders_search/_search" -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": {
    "by_user": {
      "terms": { "field": "user_id", "size": 10, "order": { "_count": "desc" } }
    }
  }
}'

# 日期直方图 — 按天统计订单量 (GROUP BY DATE)
curl -X GET "http://localhost:9200/orders_search/_search" -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": {
    "orders_per_day": {
      "date_histogram": {
        "field": "created_at",
        "calendar_interval": "day",
        "format": "yyyy-MM-dd"
      }
    }
  }
}'

# ───── 指标聚合 (Metric) ─────

# 统计计数/平均值/最大值/最小值/总和
curl -X GET "http://localhost:9200/orders_search/_search" -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": {
    "stats_amount": {
      "stats": { "field": "amount" }
    }
  }
}'

# 去重计数 (COUNT DISTINCT)
curl -X GET "http://localhost:9200/orders_search/_search" -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": {
    "unique_users": {
      "cardinality": { "field": "user_id" }
    }
  }
}'

# ───── 嵌套聚合 ─────

# 先按日期分组，再在每组内按状态分组 (两层嵌套)
curl -X GET "http://localhost:9200/orders_search/_search" -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": {
    "by_date": {
      "date_histogram": {
        "field": "created_at",
        "calendar_interval": "day"
      },
      "aggs": {
        "by_status": {
          "terms": { "field": "status" }
        }
      }
    }
  }
}'

# 按用户分组后计算人均订单金额
curl -X GET "http://localhost:9200/orders_search/_search" -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": {
    "by_user": {
      "terms": { "field": "user_id", "size": 100 },
      "aggs": {
        "avg_amount": { "avg": { "field": "amount" } },
        "total_amount": { "sum": { "field": "amount" } },
        "order_count": { "value_count": { "field": "order_no" } }
      }
    }
  }
}'

# ───── 管道聚合 ─────

# 在日期聚合的基础上，求每日订单量的移动平均
curl -X GET "http://localhost:9200/orders_search/_search" -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": {
    "orders_per_day": {
      "date_histogram": {
        "field": "created_at",
        "calendar_interval": "day"
      },
      "aggs": {
        "moving_avg": {
          "moving_fn": {
            "script": "MovingFunctions.unweightedAvg(values)",
            "window": 7
          }
        }
      }
    }
  }
}'

# ───── 聚合速查 ─────
# terms           → GROUP BY
# date_histogram  → GROUP BY DATE
# histogram       → 数值区间分组
# range           → 自定义范围分组
# avg / sum / min / max / stats  → 聚合函数
# cardinality     → COUNT DISTINCT
# value_count     → COUNT
# top_hits        → 每组取 TOP N 文档
# filter          → WHERE 后聚合
```

---

## 五、索引管理与优化

```bash
# 查看映射
curl -X GET "http://localhost:9200/users_search/_mapping?pretty"

# 查看索引配置
curl -X GET "http://localhost:9200/users_search/_settings?pretty"

# 添加字段 (动态映射已存在的索引)
curl -X PUT "http://localhost:9200/users_search/_mapping" -H 'Content-Type: application/json' -d '{
  "properties": {
    "new_field": { "type": "keyword" }
  }
}'

# 查看索引别名
curl -X GET "http://localhost:9200/_cat/aliases?v"

# 索引别名 (零停机切换)
curl -X POST "http://localhost:9200/_aliases" -H 'Content-Type: application/json' -d '{
  "actions": [
    { "remove": { "index": "users_search_v1", "alias": "users_search" } },
    { "add":    { "index": "users_search_v2", "alias": "users_search" } }
  ]
}'

# ───── 分片管理 ─────

# 修改副本数
curl -X PUT "http://localhost:9200/users_search/_settings" -H 'Content-Type: application/json' -d '{
  "index": { "number_of_replicas": 2 }
}'

# 修改刷新间隔 (降低写入负载)
curl -X PUT "http://localhost:9200/users_search/_settings" -H 'Content-Type: application/json' -d '{
  "index": { "refresh_interval": "30s" }
}'

# 手动刷新 (使最近写入立即可搜索)
curl -X POST "http://localhost:9200/users_search/_refresh"

# 强制合并段 (减少段数量，提升查询性能)
curl -X POST "http://localhost:9200/users_search/_forcemerge?max_num_segments=1"

# ───── 查询分析 ─────

# 查看查询执行计划 (不实际执行)
curl -X GET "http://localhost:9200/users_search/_search" -H 'Content-Type: application/json' -d '{
  "profile": true,
  "query": { "match": { "nickname": "张三" } }
}'

# 分析分词结果
curl -X POST "http://localhost:9200/_analyze" -H 'Content-Type: application/json' -d '{
  "analyzer": "standard",
  "text": "高并发系统架构"
}'

# IK 中文分词
curl -X POST "http://localhost:9200/_analyze" -H 'Content-Type: application/json' -d '{
  "analyzer": "ik_max_word",
  "text": "高并发系统架构"
}'
```

---

## 六、性能与运维

```bash
# ───── 集群监控 ─────

# 集群健康详情
curl http://localhost:9200/_cluster/health?pretty

# 热点线程 (类似 MySQL SHOW PROCESSLIST)
curl http://localhost:9200/_nodes/hot_threads?pretty

# 节点统计
curl http://localhost:9200/_nodes/stats?pretty

# 任务管理 (查看/取消长时间运行的任务)
curl http://localhost:9200/_tasks?actions=*search*&pretty
curl -X POST "http://localhost:9200/_tasks/oTUltX4IQMOUUVeiohTt8A:12345/_cancel"

# ───── 慢查询 ─────

# 查看慢查询阈值配置
curl http://localhost:9200/users_search/_settings?pretty

# 设置慢查询阈值 (查询 > 1s, 写入 > 500ms 记录)
curl -X PUT "http://localhost:9200/users_search/_settings" -H 'Content-Type: application/json' -d '{
  "index": {
    "search.slowlog.threshold.query.warn": "1s",
    "search.slowlog.threshold.query.info": "500ms",
    "indexing.slowlog.threshold.index.warn": "500ms"
  }
}'

# ───── 段与缓存 ─────

# 查看段信息
curl http://localhost:9200/_cat/segments/users_search?v

# 清除缓存
curl -X POST "http://localhost:9200/users_search/_cache/clear"

# 查看字段数据缓存使用量
curl http://localhost:9200/_nodes/stats/indices/fielddata?pretty
```

---

## 七、实用命令速查

```bash
# 文档计数
curl http://localhost:9200/users_search/_count                  # 全部文档数
curl -X GET "http://localhost:9200/users_search/_count" -H 'Content-Type: application/json' -d '{
  "query": { "term": { "status": 1 } }
}'                                                               # 条件计数

# 文档是否存在
curl -I "http://localhost:9200/users_search/_doc/1"             # 200=存在 404=不存在

# 批量获取
curl -X GET "http://localhost:9200/_mget" -H 'Content-Type: application/json' -d '{
  "docs": [
    { "_index": "users_search", "_id": "1" },
    { "_index": "users_search", "_id": "2" }
  ]
}'

# Reindex (跨索引复制数据到新索引)
curl -X POST "http://localhost:9200/_reindex" -H 'Content-Type: application/json' -d '{
  "source": { "index": "users_search" },
  "dest":   { "index": "users_search_v2" }
}'

# 滚动查询 (Scroll, 全量导出数据)
curl -X GET "http://localhost:9200/users_search/_search?scroll=1m" -H 'Content-Type: application/json' -d '{
  "size": 1000,
  "query": { "match_all": {} }
}'
# 后续滚动
curl -X GET "http://localhost:9200/_search/scroll" -H 'Content-Type: application/json' -d '{
  "scroll": "1m",
  "scroll_id": "DXF1ZXJ5QW5kRmV0Y2gB..."
}'

# ES 版本
curl http://localhost:9200
```

---

## 八、Demo — 统一业务场景

> 场景：用户管理模块，包含 `users_search`（用户搜索） 和 `orders_search`（订单搜索） 两个索引。
> 以下为同一套业务逻辑在 Elasticsearch 中的实现。

```bash
# ────────── 基础查询 ──────────

# 1. 查前 10 条用户
curl -X GET "http://localhost:9200/users_search/_search" -H 'Content-Type: application/json' -d '{
  "size": 10, "query": { "match_all": {} }
}'

# 2. 按 ID 查用户
curl -X GET "http://localhost:9200/users_search/_doc/1"

# 3. 按姓名模糊搜索 (全文)
curl -X GET "http://localhost:9200/users_search/_search" -H 'Content-Type: application/json' -d '{
  "query": { "match": { "nickname": "张三" } }
}'

# 4. 按邮箱后缀模糊
curl -X GET "http://localhost:9200/users_search/_search" -H 'Content-Type: application/json' -d '{
  "query": { "wildcard": { "email": "*@example.com" } }
}'

# ────────── 多字段搜索 ──────────

# 5. 按用户ID查订单
curl -X GET "http://localhost:9200/orders_search/_search" -H 'Content-Type: application/json' -d '{
  "query": { "term": { "user_id": 1 } }
}'

# 6. 查找同时具备 "go" 和 "backend" 标签的用户
curl -X GET "http://localhost:9200/users_search/_search" -H 'Content-Type: application/json' -d '{
  "query": {
    "bool": {
      "must": [
        { "term": { "tags": "go" } },
        { "term": { "tags": "backend" } }
      ]
    }
  }
}'

# 7. 查找 bio 中包含 "工程师" 的用户
curl -X GET "http://localhost:9200/users_search/_search" -H 'Content-Type: application/json' -d '{
  "query": { "match": { "bio": "工程师" } }
}'

# ────────── 分页 ──────────

# 8. 分页第1页 (每页10条，按创建时间倒序)
curl -X GET "http://localhost:9200/users_search/_search" -H 'Content-Type: application/json' -d '{
  "from": 0, "size": 10,
  "query": { "match_all": {} },
  "sort": [ { "created_at": "desc" } ]
}'

# 9. 分页第2页
curl -X GET "http://localhost:9200/users_search/_search" -H 'Content-Type: application/json' -d '{
  "from": 10, "size": 10,
  "query": { "match_all": {} },
  "sort": [ { "created_at": "desc" } ]
}'

# ────────── 聚合统计 ──────────

# 10. 用户总数
curl http://localhost:9200/users_search/_count

# 11. 按状态分组统计订单
curl -X GET "http://localhost:9200/orders_search/_search" -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": {
    "by_status": { "terms": { "field": "status" } }
  }
}'

# 12. 每日订单量统计 (近7天)
curl -X GET "http://localhost:9200/orders_search/_search" -H 'Content-Type: application/json' -d '{
  "size": 0,
  "query": {
    "range": { "created_at": { "gte": "now-7d/d", "lte": "now/d" } }
  },
  "aggs": {
    "daily_orders": {
      "date_histogram": {
        "field": "created_at",
        "calendar_interval": "day"
      }
    }
  }
}'

# 13. 标签分布统计
curl -X GET "http://localhost:9200/users_search/_search" -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": {
    "by_tag": { "terms": { "field": "tags", "size": 20 } }
  }
}'

# ────────── 组合查询 ──────────

# 14. 高价值已支付订单 (金额>100 且状态=paid)，按金额降序
curl -X GET "http://localhost:9200/orders_search/_search" -H 'Content-Type: application/json' -d '{
  "size": 10,
  "query": {
    "bool": {
      "must": [
        { "term": { "status": "paid" } },
        { "range": { "amount": { "gt": 100 } } }
      ]
    }
  },
  "sort": [ { "amount": "desc" } ]
}'
```

---

## 九、Elasticsearch vs MongoDB vs MySQL 对照速查

| 概念 | Elasticsearch | MongoDB | MySQL |
|------|--------------|---------|-------|
| 数据库 | Cluster | Database | Database |
| 表 | Index | Collection | TABLE |
| 行 | Document (JSON) | Document (BSON) | Row |
| 主键 | `_id` (字符串) | `_id` (ObjectId) | `PRIMARY KEY` (自增) |
| 全文搜索 | `match` / `multi_match` (原生) | `$text` (有限) | `LIKE` / FULLTEXT |
| 分组 | `terms` / `date_histogram` 聚合 | `$group` 聚合 | `GROUP BY` |
| JOIN | 不支持 (需应用层或嵌套) | `$lookup` | `JOIN ... ON` |
| 排序 | `sort` | `$sort` | `ORDER BY` |
| 分页 | `from`+`size` / `search_after` | `skip`+`limit` | `LIMIT`+`OFFSET` |
| 事务 | 不支持多索引 ACID | Session.startTransaction() | START TRANSACTION |
| 典型场景 | **全文检索**、日志分析 | 文档存储、灵活 Schema | OLTP 事务、强一致性 |
| 写入一致性 | 近实时 (refresh_interval) | 可配置 Write Concern | ACID 严格一致 |
| 水平扩展 | 原生分片 (Shard) | 原生分片 (Sharding) | 分库分表中间件 |
