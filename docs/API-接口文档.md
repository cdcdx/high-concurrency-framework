# 高并发业务框架 - API 接口文档

**Base URL:** `http://localhost:8080`

---

## 目录

- [通用说明](#通用说明)
  - [统一响应格式](#统一响应格式)
  - [公共请求头](#公共请求头)
  - [错误码](#错误码)
- [业务接口（订单）](#业务接口订单)
  - [1. 创建订单（双通道分流）](#1-创建订单双通道分流)
  - [2. 创建订单（强制同步）](#2-创建订单强制同步)
  - [3. 查询订单](#3-查询订单)
  - [4. 搜索订单（ES 全文检索）](#4-搜索订单es-全文检索)
- [业务接口（用户）](#业务接口用户)
  - [5. 获取用户资料](#5-获取用户资料)
  - [6. 创建/更新用户资料](#6-创建更新用户资料)
  - [7. 搜索用户（ES 全文检索）](#7-搜索用户es-全文检索)
- [分析接口（PostgreSQL）](#分析接口postgresql)
  - [8. 日度订单统计](#8-日度订单统计)
  - [9. 行为事件汇总](#9-行为事件汇总)
- [监控接口](#监控接口)
  - [10. 系统指标](#10-系统指标)
  - [11. 熔断器状态](#11-熔断器状态)
- [健康探针](#健康探针)
  - [12. 探活检测](#12-探活检测)
  - [13. 就绪检测](#13-就绪检测)
  - [14. 启动检测](#14-启动检测)
- [架构说明](#架构说明)
  - [写链路：双通道分流](#写链路双通道分流)
  - [读链路：三级缓存](#读链路三级缓存)
  - [高可用机制](#高可用机制)

---

## 通用说明

### 统一响应格式

所有接口返回统一的 JSON 结构：

```json
{
  "code": 200,
  "message": "success",
  "data": {},
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `code` | int | 状态码（200/400/404/500/503） |
| `message` | string | 状态描述 |
| `data` | object | 业务数据（成功时返回） |
| `trace_id` | string | 全链路追踪 ID，贯穿 HTTP → Service → Kafka → Consumer |

### 公共请求头

| Header | 类型 | 必填 | 说明 |
|--------|------|------|------|
| `Content-Type` | string | 是 | `application/json`（POST 接口） |
| `X-Trace-Id` | string | 否 | 透传 trace_id，不传则自动生成 UUID |

### 错误码

| code | 含义 | 触发条件 |
|------|------|----------|
| 200 | 成功 | 请求正常处理 |
| 400 | 参数校验失败 | JSON 格式错误 / 字段缺失 / validator 校验不通过 |
| 404 | 资源不存在 | 订单号或用户 ID 未找到 |
| 500 | 服务内部错误 | 数据库写入失败等非降级类错误 |
| 503 | 服务降级 | MongoDB / Elasticsearch 不可用，返回友好降级消息 |

---

## 业务接口（订单）

### 1. 创建订单（双通道分流）

```http
POST /api/v1/orders
Content-Type: application/json
```

**描述：** 创建订单，根据令牌桶限流自动分流：

- **同步通道 (sync)：** 令牌桶有可用令牌时，分布式锁 → 直接写 MySQL → 异步发 Kafka 事件
- **异步通道 (async)：** 令牌耗尽时，仅投递 Kafka（由消费者限速入库），< 10ms 响应

**请求参数：**

| 参数 | 类型 | 必填 | 校验规则 | 说明 |
|------|------|------|----------|------|
| `user_id` | uint64 | 是 | `min=1` | 用户 ID |
| `amount` | float64 | 是 | `gt=0` | 订单金额 |

**请求示例：**

```json
{
  "user_id": 10001,
  "amount": 299.99
}
```

**成功响应：**

```json
{
  "code": 200,
  "message": "order accepted",
  "data": {
    "order_no": "ORD10001042",
    "channel": "sync"
  },
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

| 字段 | 说明 |
|------|------|
| `order_no` | 生成的订单号，格式 `ORD{userID}{nano%1000}` |
| `channel` | `"sync"`（同步入库）或 `"async"`（Kafka 异步削峰） |

**错误响应：**

```json
{
  "code": 400,
  "message": "validation failed: Key: 'Order.Amount' Error:Field validation for 'Amount' failed on the 'gt' tag",
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

**追踪：** 同步通道写入 MySQL 后异步发送 `ORDER_CREATE` 事件到 Kafka `order-create` topic，消息 Header 携带 `trace_id`。

---

### 2. 创建订单（强制同步）

```http
POST /api/v1/orders/sync
Content-Type: application/json
```

**描述：** 创建订单，强制执行同步写入通道（跳过令牌桶分流，不经过 Kafka）。

> 请求参数、响应格式与 [创建订单](#1-创建订单双通道分流) 完全一致，仅 `message` 为 `"order created"`。

---

### 3. 查询订单

```http
GET /api/v1/orders/{orderNo}
```

**描述：** 根据订单号查询订单详情，走三级缓存读链路（L1 Ristretto → L2 Redis → L3 MySQL）。

**路径参数：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `orderNo` | string | 订单号，如 `ORD10001042` |

**成功响应：**

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "id": 1,
    "user_id": 10001,
    "order_no": "ORD10001042",
    "amount": 299.99,
    "status": "pending",
    "created_at": "2026-07-01T07:18:35Z",
    "updated_at": "2026-07-01T07:18:35Z"
  },
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

**订单对象字段：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | uint64 | 数据库主键 |
| `user_id` | uint64 | 用户 ID |
| `order_no` | string | 订单号 |
| `amount` | float64 | 订单金额 |
| `status` | string | 状态：`pending` / `paid` / `shipped` / `cancelled` |
| `created_at` | datetime | 创建时间 |
| `updated_at` | datetime | 更新时间 |

**错误响应：**

```json
{
  "code": 404,
  "message": "order not found: ORD99999001",
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

---

### 4. 搜索订单（ES 全文检索）

```http
GET /api/v1/orders/search?q={keyword}&page=1&size=20
```

**描述：** 通过 Elasticsearch 全文搜索订单。

**查询参数：**

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `q` | string | 是 | — | 搜索关键词 |
| `page` | int | 否 | `1` | 页码 |
| `size` | int | 否 | `20` | 每页条数（最大 100） |

**成功响应：**

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "orders": [
      {
        "id": 1,
        "user_id": 10001,
        "order_no": "ORD10001042",
        "amount": 299.99,
        "status": "pending",
        "created_at": "2026-07-01T07:18:35Z",
        "updated_at": "2026-07-01T07:18:35Z"
      }
    ],
    "total": 1,
    "page": 1,
    "size": 20
  },
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `orders` | array | 订单列表 |
| `total` | int64 | 匹配总数 |
| `page` | int | 当前页码 |
| `size` | int | 每页条数 |

**错误响应：**

```json
{
  "code": 400,
  "message": "query parameter 'q' is required",
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

ES 不可用时返回 503：
```json
{
  "code": 503,
  "message": "order search temporarily unavailable, service degraded",
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

---

## 业务接口（用户）

### 5. 获取用户资料

```http
GET /api/v1/users/{userID}/profile
```

**描述：** 根据用户 ID 获取用户资料，走三级缓存读链路（L1 Ristretto → L2 Redis → L3 MongoDB）。

**路径参数：**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `userID` | uint64 | 是 | 用户 ID |

**成功响应：**

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "id": 1,
    "user_id": 10001,
    "nickname": "Bob",
    "avatar": "https://cdn.example.com/avatar/10001.jpg",
    "email": "bob@example.com",
    "phone": "13800138000",
    "bio": "Hello from high-concurrency framework",
    "tags": ["vip", "active"],
    "created_at": "2026-07-01T07:18:35Z",
    "updated_at": "2026-07-01T07:18:35Z"
  },
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

**用户资料字段：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | uint64 | 主键 |
| `user_id` | uint64 | 用户 ID |
| `nickname` | string | 昵称 |
| `avatar` | string | 头像 URL |
| `email` | string | 邮箱 |
| `phone` | string | 手机号 |
| `bio` | string | 个人简介（可选） |
| `tags` | []string | 标签列表（可选） |
| `created_at` | datetime | 创建时间 |
| `updated_at` | datetime | 更新时间 |

**错误响应：**

```json
{
  "code": 400,
  "message": "invalid user_id",
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

```json
{
  "code": 404,
  "message": "user not found: 99999",
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

MongoDB 不可用时返回 503：
```json
{
  "code": 503,
  "message": "profile lookup temporarily unavailable, service degraded",
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

---

### 6. 创建/更新用户资料

```http
POST /api/v1/users/profile
Content-Type: application/json
```

**描述：** 创建或更新用户资料（MongoDB upsert）。已存在则更新，不存在则插入。

**请求参数：**

| 参数 | 类型 | 必填 | 校验规则 | 说明 |
|------|------|------|----------|------|
| `user_id` | uint64 | 是 | `required` | 用户 ID |
| `nickname` | string | 否 | — | 昵称 |
| `avatar` | string | 否 | — | 头像 URL |
| `email` | string | 否 | — | 邮箱 |
| `phone` | string | 否 | — | 手机号 |
| `bio` | string | 否 | — | 个人简介 |
| `tags` | []string | 否 | — | 标签列表 |

**请求示例：**

```json
{
  "user_id": 10002,
  "nickname": "Bob",
  "email": "bob@example.com",
  "phone": "13800000002",
  "bio": "Hello from high-concurrency framework",
  "tags": ["vip", "active"]
}
```

**成功响应：**

```json
{
  "code": 200,
  "message": "profile saved",
  "data": {
    "user_id": 10002,
    "nickname": "Bob",
    "email": "bob@example.com",
    "phone": "13800000002",
    "bio": "Hello from high-concurrency framework",
    "tags": ["vip", "active"]
  },
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

**错误响应：**

```json
{
  "code": 400,
  "message": "validation failed: Key: 'UserProfile.UserID' Error:Field validation for 'UserID' failed on the 'required' tag",
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

MongoDB 不可用时返回 503：
```json
{
  "code": 503,
  "message": "profile upsert temporarily unavailable, service degraded",
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

---

### 7. 搜索用户（ES 全文检索）

```http
GET /api/v1/users/search?q={keyword}&page=1&size=20
```

**描述：** 通过 Elasticsearch 全文搜索用户。

**查询参数：**

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `q` | string | 是 | — | 搜索关键词 |
| `page` | int | 否 | `1` | 页码 |
| `size` | int | 否 | `20` | 每页条数（最大 100） |

**成功响应：**

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "users": [
      {
        "id": 1,
        "user_id": 10001,
        "nickname": "Bob",
        "avatar": "https://cdn.example.com/avatar/10001.jpg",
        "email": "bob@example.com",
        "phone": "13800138000",
        "created_at": "2026-07-01T07:18:35Z",
        "updated_at": "2026-07-01T07:18:35Z"
      }
    ],
    "total": 1,
    "page": 1,
    "size": 20
  },
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `users` | array | 用户资料列表 |
| `total` | int64 | 匹配总数 |
| `page` | int | 当前页码 |
| `size` | int | 每页条数 |

---

## 分析接口（PostgreSQL）

> 分析接口基于 PostgreSQL 时序数据，默认查询近 7 天。

### 8. 日度订单统计

```http
GET /api/v1/analytics/daily?from=2026-06-28&to=2026-07-01
```

**描述：** 查询指定日期范围内的每日订单统计。

**查询参数：**

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `from` | string | 否 | 7 天前 | 起始日期，格式 `YYYY-MM-DD` |
| `to` | string | 否 | 当天 | 截止日期，格式 `YYYY-MM-DD` |

**成功响应：**

```json
{
  "code": 200,
  "message": "success",
  "data": [
    {
      "date": "2026-06-28",
      "order_count": 1250,
      "total_amount": 498750.50
    },
    {
      "date": "2026-06-29",
      "order_count": 1320,
      "total_amount": 523100.00
    }
  ],
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `date` | string | 统计日期 |
| `order_count` | int64 | 当日订单数 |
| `total_amount` | float64 | 当日订单总金额 |

**错误响应：**

```json
{
  "code": 400,
  "message": "invalid 'from' date, use YYYY-MM-DD",
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

---

### 9. 行为事件汇总

```http
GET /api/v1/analytics/behaviors?type={eventType}
```

**描述：** 查询近 7 天行为事件的类型分布汇总。

**查询参数：**

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `type` | string | 否 | 空（全部） | 事件类型，如 `order_create` |

**成功响应：**

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "event_type": "order_create",
    "summary": {
      "total": 8750,
      "today": 1250
    }
  },
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `event_type` | string | 筛选的事件类型（空表示全部） |
| `summary` | map | 按维度聚合的统计结果 |

---

## 监控接口

### 10. 系统指标

```http
GET /api/v1/monitor/metrics
```

**描述：** 获取系统实时运行指标，包括多级缓存、熔断器、限流器状态。

**成功响应：**

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "cache": {
      "total": 12500,
      "l1_hit": 11200,
      "l1_miss": 1300,
      "l2_hit": 980,
      "l2_miss": 320,
      "l1_hit_rate": 0.896,
      "combo_hit_rate": 0.9744
    },
    "circuit_breaker": {
      "state": "CLOSED",
      "success_count": 5000,
      "failure_count": 0,
      "slow_call_count": 3,
      "failure_rate": 0.0
    },
    "rate_limiter": {
      "tokens": 4872
    }
  },
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

**缓存指标字段：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `total` | uint64 | 总访问次数 |
| `l1_hit` | uint64 | L1 本地缓存命中次数 |
| `l1_miss` | uint64 | L1 未命中次数 |
| `l2_hit` | uint64 | L2 Redis 命中次数 |
| `l2_miss` | uint64 | L2 未命中（穿透至 L3）次数 |
| `l1_hit_rate` | float64 | L1 命中率 |
| `combo_hit_rate` | float64 | 组合命中率 (L1+L2) / Total |

**熔断器指标字段：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `state` | string | 当前状态：`CLOSED` / `OPEN` / `HALF_OPEN` |
| `success_count` | int64 | 统计窗口内成功次数 |
| `failure_count` | int64 | 统计窗口内失败次数 |
| `slow_call_count` | int64 | 统计窗口内慢调用次数 |
| `failure_rate` | float64 | 失败率 |

**限流器字段：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `tokens` | int64 | 当前令牌桶剩余令牌数（容量：5000，补充 5000/s） |

---

### 11. 熔断器状态

```http
GET /api/v1/monitor/circuit-breaker
```

**描述：** 获取熔断器当前状态及详细指标，用于故障排查。

**成功响应：**

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "state": "CLOSED",
    "metrics": {
      "state": "CLOSED",
      "success_count": 5000,
      "failure_count": 2,
      "slow_call_count": 5,
      "failure_rate": 0.0004
    }
  },
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

**熔断器状态说明：**

| 状态 | 说明 |
|------|------|
| `CLOSED` | 正常，请求全量通过 |
| `OPEN` | 熔断开启，直接拒绝请求（30 秒后自动切换至 HALF_OPEN） |
| `HALF_OPEN` | 半开探测，仅允许 5 个请求通过，成功则恢复 CLOSED |

---

## 健康探针

> K8s 标准健康检查端点，供 Pod 探针配置使用。

### 12. 探活检测

```http
GET /health/liveness
```

**K8s 配置建议：** `livenessProbe`，失败则重启 Pod

**成功响应：**

```json
{
  "status": "UP",
  "uptime": "2h30m15s"
}
```

---

### 13. 就绪检测

```http
GET /health/readiness
```

**K8s 配置建议：** `readinessProbe`，失败则摘除流量

**检查项：** Redis 连通性 / 熔断器状态 / 缓存命中率 / Kafka 连通性

**成功响应（200）：**

```json
{
  "status": "READY",
  "details": {
    "redis": "connected",
    "circuit_breaker": "CLOSED",
    "l1_hit_rate": 0.89,
    "combo_hit_rate": 0.97,
    "kafka": "connected"
  }
}
```

**失败响应（503）：**

```json
{
  "status": "NOT_READY",
  "details": {
    "redis": "disconnected: dial tcp 127.0.0.1:7000: connect: connection refused",
    "circuit_breaker": "OPEN",
    "l1_hit_rate": 0.0,
    "combo_hit_rate": 0.0,
    "kafka": "connected"
  }
}
```

---

### 14. 启动检测

```http
GET /health/startup
```

**K8s 配置建议：** `startupProbe`，初始延迟较长，给予缓存预热时间

**成功响应（200）：**

```json
{
  "status": "STARTED",
  "uptime": "15s"
}
```

**预热中响应（503）：**

```json
{
  "status": "STARTING",
  "uptime": "3s"
}
```

> 超 60 秒未完成预热将强制标记为 `STARTED`。

---

## 架构说明

### 写链路：双通道分流

```
POST /api/v1/orders
        │
        ▼
  ┌─────────────┐
  │  参数校验    │ ← validator v10
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │  令牌桶限流  │ ← 容量 5000, 补充 5000/s
  └──┬──────┬───┘
     │      │
  [通过]  [溢出]
     │      │
     ▼      ▼
 ┌──────┐ ┌────────────┐
 │sync  │ │  async     │
 │同步  │ │  异步      │
 └──┬───┘ └─────┬──────┘
    │            │
    ▼            ▼
┌───────┐   ┌─────────┐
│分布式锁│   │Kafka投递 │
│写MySQL│   │trace_id  │
│缓存双删│  │透传Header│
└───┬───┘   └────┬────┘
    │             │
    ▼             ▼
┌───────┐  ┌───────────────┐
│Kafka  │  │Kafka Consumer │
│异步事件│  │  (并发消费x8)  │
└───────┘  └───────┬───────┘
                    │
                    ▼
              ┌──────────┐
              │  DB 落库  │
              │+DLQ死信  │
              │+缓存回填  │
              └──────────┘
```

### 读链路：三级缓存

```
GET /api/v1/users/:userID/profile
        │
        ▼
  ┌──────────┐
  │ L1 本地   │ ← Ristretto
  │ 100万条目  │   命中延迟：~154ns
  └───┬──────┘
      │ [miss]
      ▼
  ┌──────────┐
  │ L2 分布式 │ ← Redis
  │ 命中：回填L1│   命中延迟：~1ms
  └───┬──────┘
      │ [miss]
      ▼
  ┌──────────┐
  │ L3 数据库  │ ← MySQL / MongoDB
  │ 命中：回填L2│   回填 L2 → L1
  │ 穿透：空标记│   空标记 TTL 60s
  └──────────┘
```

**缓存防护策略：**

| 策略 | 机制 | 配置 |
|------|------|------|
| **穿透防护** | 空标记缓存 | TTL 60s，下次命中直接返回 |
| **击穿防护** | L1 `GetOrSet` 单飞 | 仅一个请求穿透至 L3 |
| **雪崩防护** | TTL 随机抖动 ±20% | `jitter_percent: 0.2` |
| **热点防护** | 滑动窗口检测 + 本地缓存 | 10s 窗口，阈值 100 次 |

### 高可用机制

| 组件 | 机制 | 配置 |
|------|------|------|
| **熔断器** | CLOSED → OPEN → HALF_OPEN 三态 | 失败率 50%、30s 超时、5 探针 |
| **限流器** | 令牌桶 | 容量 5000，补充 5000/s |
| **分布式锁** | Redis SETNX | 5s 过期，200ms 重试 |
| **Kafka 幂等** | `RequiredAcks=All` + 消息去重 | `MaxAttempts: 3` |
| **死信队列** | 消费者 3 次重试失败 → DLQ | `order-create.dlq` |
| **延迟双删** | 写后缓存失效（先删 → 写 DB → 延迟再删） | goroutine 异步执行 |
| **全链路追踪** | 每个请求生成 `trace_id`，贯穿 HTTP → Kafka Header → 消费者日志 | `X-Trace-Id` Header |
| **优雅关闭** | SIGTERM 信号触发 | 30s 等待进行中请求完成 |

### 全链路 trace_id 追踪

```
HTTP 请求                       Kafka 消息                      消费者
──────────────────────────────────────────────────────────────────────
middleware.TraceID()            producer.Send()
  ├─ gin.Context                   │
  ├─ context.Context ──────────────┼─ kafka.Header["trace_id"]
  │   (trace.NewContext)          │     ↓
  │                               └─── consumer.consumeLoop()
  │                                      │
  │                                 trace.NewContext(ctx, traceID)
  │                                      │
  │                                 HandleOrderEvent(ctx, ...)
  │                                      │
  │                            s.logger.Infow("order from kafka",
  │                                "traceId", event.TraceID)
  │
handler → service:
  trace.FromContext(ctx)
    → event.TraceID = traceID
```

**追踪命令：**

```bash
# 1. 从 API 响应拿 trace_id
curl -s -X POST http://localhost:8080/api/v1/orders \
  -d '{"user_id":10001,"amount":99.9}' | jq .trace_id

# 2. 日志中 grep 全链路
grep "a1b2c3d4" app.log

# 3. Kafka 消息中查看 trace_id
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic order-create --from-beginning \
  --property print.headers=true
```

---

## 性能基准

| 操作 | 延迟 | QPS |
|------|------|-----|
| L1 缓存命中 | **154 ns** | 890 万/s |
| 令牌桶限流 | **55 ns** | 2100 万/s |
| 全链路读（Profile） | 9.1 µs | 17 万/s |
| 全链路写（Order） | 15.6 µs | 6.6 万/s |
| 混合压测（375 读 + 125 写） | — | 4.2 万 req/s |

---

## K8s 探针配置建议

```yaml
livenessProbe:
  httpGet:
    path: /health/liveness
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 10
readinessProbe:
  httpGet:
    path: /health/readiness
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 5
startupProbe:
  httpGet:
    path: /health/startup
    port: 8080
  initialDelaySeconds: 0
  periodSeconds: 3
  failureThreshold: 20
```

---

## 接口速查表

| # | 方法 | 路径 | 说明 | 存储 |
|---|------|------|------|------|
| 1 | POST | `/api/v1/orders` | 创建订单（双通道分流） | MySQL + Kafka |
| 2 | POST | `/api/v1/orders/sync` | 创建订单（强制同步） | MySQL |
| 3 | GET | `/api/v1/orders/:orderNo` | 查询订单 | L1→L2→MySQL |
| 4 | GET | `/api/v1/orders/search?q=` | 搜索订单 | Elasticsearch |
| 5 | GET | `/api/v1/users/:userID/profile` | 获取用户资料 | L1→L2→MongoDB |
| 6 | POST | `/api/v1/users/profile` | 创建/更新用户资料 | MongoDB |
| 7 | GET | `/api/v1/users/search?q=` | 搜索用户 | Elasticsearch |
| 8 | GET | `/api/v1/analytics/daily?from=&to=` | 日度订单统计 | PostgreSQL |
| 9 | GET | `/api/v1/analytics/behaviors?type=` | 行为事件汇总 | PostgreSQL |
| 10 | GET | `/api/v1/monitor/metrics` | 系统运行指标 | 内存 |
| 11 | GET | `/api/v1/monitor/circuit-breaker` | 熔断器状态 | 内存 |
| 12 | GET | `/health/liveness` | K8s 探活 | — |
| 13 | GET | `/health/readiness` | K8s 就绪 | Redis+Kafka+DB |
| 14 | GET | `/health/startup` | K8s 启动 | 缓存预热 |
