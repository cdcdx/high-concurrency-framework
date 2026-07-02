# CDCDX 高并发业务框架 (Go 实现)

面向千万级用户、百万级在线的 **高并发读写框架**, 使用 Go 语言实现。

## 架构概览

```
请求 → L7 Nginx → API Gateway
                    │
        ┌───────────┼───────────┐
        ▼           ▼           ▼
   [同步通路]     [限流控制]   [异步通路]
        │           │           │
   L1 Ristretto  RateLimiter  Kafka Topic
      (<1ms)        │           │
        │       ┌───┴───┐    Consumer(限速2000/s)
   L2 Redis    同步DB  异步DB     │
     (1-5ms)                    DB
        │                    DLQ(死信)
   L3 Database
        │
   熔断器 Sentinel
```

## 核心能力对照

| 能力 | Java 版 | Go 版 |
|------|---------|-------|
| HTTP 框架 | Spring Boot Web | Gin |
| L1 本地缓存 | Caffeine | Ristretto |
| L2 分布式缓存 | Redis Cluster (Lettuce) | Redis (go-redis) |
| L3 数据库 | JPA + HikariCP | database/sql |
| 消息队列 | Spring Kafka | segmentio/kafka-go |
| 熔断器 | Resilience4j | sony/gobreaker (自研) |
| 限流器 | Resilience4j RateLimiter | 令牌桶 (自研) |
| 分布式锁 | Redisson | Redis SETNX (自研) |
| 参数校验 | @Valid + Hibernate Validator | go-playground/validator |
| 链路追踪 | Micrometer + OpenTelemetry | otel |
| 健康检查 | Actuator | 自研 HTTP 端点 |
| 结构化日志 | Logback + MDC | zap |

## 特性

### 双通道分流
```
写请求 → WriteRateLimiter.TryAcquire()
           ├── true  → 同步写DB + 分布式锁防并发
           └── false → Kafka异步 (<10ms) → Consumer限速 → DB
```

### 三级缓存防护矩阵
- **穿透防护**: DB返回nil → 缓存空标记(60s)
- **击穿防护**: 热点Key过期 → Redis SETNX互斥锁
- **雪崩防护**: TTL ±20% 随机偏移
- **一致性**: 延迟双删 (写后500ms再删)

### 水平扩展
- 无状态设计: 业务Pod不存本地状态
- K8s HPA: CPU>60%自动扩容 4→20 Pod
- Pod反亲和: 分散部署
- 三种探针: Liveness / Readiness / Startup

## 快速开始

```bash
# 编译
make build

# 本地运行 (需要修改config.yaml中的Redis/Kafka地址)
make run

# 基础测试 (13项)
make test

# 高可用集成测试 (18项, 读写全链路)
make ha-test

# 性能基准测试
make bench

# 压测
make perf-read
make perf-write
```

## 接口列表

| ## | 方法 | 路径 | 说明 | 存储 |
|----|------|------|------|------|
| 01 | POST | `/api/v1/orders` | 创建订单（双通道分流） | MySQL + Kafka |
| 02 | POST | `/api/v1/orders/sync` | 创建订单（强制同步） | MySQL |
| 03 | GET | `/api/v1/orders/:orderNo` | 查询订单 | L1→L2→MySQL |
| 04 | GET | `/api/v1/orders/search?q=` | 搜索订单 | Elasticsearch |
| 05 | POST | `/api/v1/users/profile` | 创建/更新用户资料 | MongoDB |
| 06 | GET | `/api/v1/users/:userID/profile` | 获取用户资料 | L1→L2→MongoDB |
| 07 | GET | `/api/v1/users/search?q=` | 搜索用户 | Elasticsearch |
| 08 | GET | `/api/v1/analytics/daily?from=&to=` | 日度订单统计 | PostgreSQL |
| 09 | GET | `/api/v1/analytics/behaviors?type=` | 行为事件汇总 | PostgreSQL |
| 10 | GET | `/api/v1/monitor/metrics` | 系统运行指标 | 内存 |
| 11 | GET | `/api/v1/monitor/circuit-breaker` | 熔断器状态 | 内存 |
| 12 | GET | `/api/v1/monitor/cache-stats` | 缓存命中率统计 | 内存 |
| 13 | GET | `/health/liveness` | K8s 探活 | — |
| 14 | GET | `/health/readiness` | K8s 就绪 | Redis+Kafka+DB |
| 15 | GET | `/health/startup` | K8s 启动 | 缓存预热 |

## 项目结构

```
high-concurrency-framework-go/
├── cmd/server/main.go               # 入口 + 依赖注入 + 优雅启停
├── internal/
│   ├── cache/
│   │   ├── multi_level_cache.go     # L1(Ristretto)→L2(Redis)→L3(DB) + 三维防护
│   │   └── hot_key_detector.go      # 滑动窗口热点Key检测
│   ├── mq/
│   │   ├── producer.go              # Kafka生产者(幂等/补偿/DLQ)
│   │   ├── consumer.go              # Kafka消费者(重试/DLQ/限速)
│   │   └── mock_producer.go         # 内存Mock生产者 (测试用)
│   ├── resilience/
│   │   ├── circuit_breaker.go       # 熔断管理器(三态模型 + 优雅停止)
│   │   └── rate_limiter.go          # 令牌桶写限流器
│   ├── lock/
│   │   └── distributed_lock.go      # Redis分布式锁(SETNX+Lua原子解锁)
│   ├── model/models.go              # 数据模型+验证标签
│   ├── database/
│   │   ├── rwdb.go                  # 读写分离数据库(Master写/Replica读)
│   │   ├── manager.go               # 多数据库管理器(MySQL/PG/Mongo/ES)
│   │   ├── mongo.go                 # MongoDB用户资料仓库
│   │   ├── postgres.go              # PostgreSQL分析仓库
│   │   ├── elasticsearch.go         # ES全文搜索客户端
│   │   └── schema.go                # 统一DDL初始化(sql/*)
│   ├── service/
│   │   ├── order_service.go         # 订单服务(读写分流+削峰)
│   │   ├── user_profile_service.go  # 用户服务(纯读多级缓存)
│   │   └── analytics_service.go     # 分析服务(PostgreSQL时序)
│   ├── handler/
│   │   ├── business_handler.go      # 业务API接口
│   │   ├── monitor_handler.go       # 监控API接口
│   │   └── analytics_handler.go     # 分析API接口
│   ├── middleware/
│   │   ├── traceid.go               # 全链路TraceID过滤器 + ZapLogger
│   │   ├── ratelimit.go             # 限流中间件(写操作)
│   │   └── recovery.go              # Panic恢复+统一ErrorHandler
│   ├── trace/trace.go               # Context trace_id 注入/提取
│   ├── health/health.go             # 三种K8s探针(Liveness/Readiness/Startup)
│   └── config/config.go             # YAML配置加载+默认值
├── sql/
│   ├── mysql_init.sql               # MySQL 订单表 DDL
│   ├── postgresql_init.sql          # PostgreSQL 分析表 + 种子数据
│   └── mongo_init.json              # MongoDB 集合与索引定义
├── tests/
│   ├── integration_test.go          # 14个基础集成测试+基准
│   ├── ha_integration_test.go       # 18个高可用全链路测试
│   └── order.json                   # 订单压测JSON
├── config.yaml                      # 配置文件
├── Dockerfile                       # 多阶段构建(~10MB镜像)
├── k8s-deployment.yaml              # K8s部署+HPA自动扩缩
├── Makefile                         # 构建/测试/部署命令
└── go.mod                           # Go模块依赖
```

## 配置

编辑 `config.yaml`:

```yaml
cache:
  l1:
    max_entries: 1000000    # L1最大100万条目
    ttl_seconds: 30         # L1过期时间
  l2:
    addresses:              # Redis Cluster节点
      - "redis:6379"
  jitter_percent: 0.2       # TTL ±20%防雪崩
  null_ttl_seconds: 60      # 空标记60秒防穿透

kafka:
  brokers: ["kafka:9092"]
  consumer:
    rate_limit: 2000        # 每秒消费2000条
    concurrency: 8          # 8个消费协程
    dlq_topic: "order-create.dlq"  # 死信队列
```

## 与 Java 版对比

| 维度 | Java | Go |
|------|------|-----|
| 镜像大小 | ~200MB | ~10MB |
| 启动时间 | 3-10s | <100ms |
| 内存占用 | 512MB-2GB | 50-200MB |
| 并发模型 | 虚拟线程(Java 21+) | Goroutine(原生) |
| 编译产物 | JAR (需JRE) | 单一二进制 |
| GC | ZGC/Shenandoah | 并发三色标记 |

## 高可用测试覆盖

`tests/ha_integration_test.go` 提供 **18 项**高可用全链路测试, 覆盖所有核心设计要点:

### 读接口 (GET /api/v1/users/:userID/profile)
| 测试项 | 验证能力 |
|--------|---------|
| `TestReadL1CacheHit` | L1命中 <1ms, 数据一致性 |
| `TestReadCachePenetrationProtection` | 空标记防穿透, 404语义 |
| `TestReadHotKeyBreakdownProtection` | 热点Key互斥锁防击穿, 50并发读 |
| `TestReadCacheAvalancheProtection` | TTL ±20% 随机偏移, 50样本验证 |
| `TestReadMultiUserConcurrent` | 200并发, 20用户, 验证吞吐 |

### 写接口 (POST /api/v1/orders)
| 测试项 | 验证能力 |
|--------|---------|
| `TestWriteRateLimitSyncAsyncSplit` | 令牌桶分流: 同步≤桶容量, 超限转异步 |
| `TestWriteAsyncChannelFallback` | 令牌耗尽异步降级 |
| `TestWriteCircuitBreakerFallback` | 熔断打开全量异步降级 |
| `TestWriteCacheInvalidation` | 写后延迟双删 (500ms) |
| `TestWriteDistributedLockConcurrency` | 50并发写, 限流器正确拦截 |
| `TestWriteValidation` | 5种参数校验 (正常/零值/负数/缺失/格式) |

### 读写联动
| 测试项 | 验证能力 |
|--------|---------|
| `TestReadAfterWriteConsistency` | 写前预热→写后延迟双删→读回源 |

### 压力测试
| 测试项 | 验证能力 |
|--------|---------|
| `TestHighConcurrencyReadWriteMix` | 500混合请求 (75%读/25%写), 错误率<10% |

### 监控与退化
| 测试项 | 验证能力 |
|--------|---------|
| `TestMonitorMetrics` | 缓存/熔断/限流指标完整性 |
| `TestCircuitBreakerMonitor` | 熔断器状态实时查询 |
| `TestDegradedModeRead` | 无Redis退化 (L1→L3直连) |
| `TestDegradedModeWrite` | 无Kafka退化 (异步安全降级) |

### 运行
```bash
# 运行所有HA集成测试
go test -v -count=1 -timeout 120s ./tests/ -run "TestRead|TestWrite|TestHigh|TestMon|TestDegrad|TestCircuit"

# 运行单个
go test -v -count=1 -timeout 30s ./tests/ -run TestReadL1CacheHit
```
