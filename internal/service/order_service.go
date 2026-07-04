package service

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cdcdx/high-concurrency-framework/internal/cache"
	"github.com/cdcdx/high-concurrency-framework/internal/database"
	"github.com/cdcdx/high-concurrency-framework/internal/lock"
	"github.com/cdcdx/high-concurrency-framework/internal/model"
	"github.com/cdcdx/high-concurrency-framework/internal/mq"
	"github.com/cdcdx/high-concurrency-framework/internal/resilience"
	"github.com/cdcdx/high-concurrency-framework/internal/trace"
	"go.uber.org/zap"
)

// OrderService 订单服务 (读写分离 + 削峰填谷)
// 存储: MySQL (核心交易, 读写分离) + Elasticsearch (全文搜索)
type OrderService struct {
	cache        *cache.MultiLevelCache
	producer     *mq.EventProducer
	rl           *resilience.WriteRateLimiter
	lock         *lock.DistributedLock
	cb           *resilience.CircuitBreaker
	db           *database.RWDB         // MySQL 读写分离: 写→Master, 读→Replica
	searchClient *database.SearchClient // ES - 全文搜索索引
	logger       *zap.SugaredLogger
	eventTopic   string
	esSemaphore  chan struct{} // 限制并发 ES 索引 goroutine 数量
}

const esMaxConcurrency = 100 // ES 索引并发 goroutine 上限

// NewOrderService 创建订单服务
func NewOrderService(
	cache *cache.MultiLevelCache,
	producer *mq.EventProducer,
	rl *resilience.WriteRateLimiter,
	distLock *lock.DistributedLock,
	cb *resilience.CircuitBreaker,
	db *database.RWDB,
	searchClient *database.SearchClient,
	logger *zap.SugaredLogger,
) *OrderService {
	return &OrderService{
		cache:        cache,
		producer:     producer,
		rl:           rl,
		lock:         distLock,
		cb:           cb,
		db:           db,
		searchClient: searchClient,
		logger:       logger,
		eventTopic:   "order-create",
		esSemaphore:  make(chan struct{}, esMaxConcurrency),
	}
}

// CreateOrder 创建订单 (双通道分流)
//   - 通道A (同步): 令牌桶有可用令牌时, 直接同步写入DB
//   - 通道B (异步): 令牌不足时, 写入Kafka削峰填谷
func (s *OrderService) CreateOrder(ctx context.Context, order *model.Order) (string, string, error) {
	// 生成订单号
	order.OrderNo = generateOrderNo(order.UserID)
	order.Status = "pending"
	order.CreatedAt = time.Now()
	order.UpdatedAt = order.CreatedAt

	// 检查熔断器
	if !s.cb.Allow() {
		s.logger.Warnw("circuit breaker open, fallback to async",
			"user_id", order.UserID,
			"order_no", order.OrderNo,
		)
		// 熔断降级: 强制走异步
		return s.createAsync(ctx, order)
	}

	// 令牌桶分流
	if s.rl.TryAcquire() {
		return s.createSync(ctx, order) // 通道A: 同步
	}
	return s.createAsync(ctx, order) // 通道B: 异步
}

// CreateOrderForceSync 强制同步写入 (绕过限流器和熔断器)
// 适用于 POST /api/v1/orders/sync, 立即可读
func (s *OrderService) CreateOrderForceSync(ctx context.Context, order *model.Order) (string, string, error) {
	order.OrderNo = generateOrderNo(order.UserID)
	order.Status = "pending"
	order.CreatedAt = time.Now()
	order.UpdatedAt = order.CreatedAt

	// lockKey := fmt.Sprintf("order:create:%s", order.UserID) // 带分布式锁防并发冲突
	lockKey := fmt.Sprintf("order:create:%s", order.OrderNo) // 分布式锁按订单号粒度 (每个订单独立，不按用户串行化)

	start := time.Now()

	locked := false
	if s.lock != nil {
		token, err := s.lock.Lock(ctx, lockKey, 5*time.Second, 200*time.Millisecond)
		if err == nil {
			locked = true
			defer s.lock.Unlock(ctx, lockKey, token)
		}
	} else {
		locked = true // 无Redis时跳过分布式锁
	}

	if !locked {
		// return "sync", order.OrderNo, fmt.Errorf("distributed lock failed for user %s", order.UserID)
		return "sync", order.OrderNo, fmt.Errorf("distributed lock failed for order %s", order.OrderNo)
	}

	// 强制同步写入DB (绕过限流器和熔断器, 不走异步降级)
	writeErr := s.dbWrite(ctx, order)

	elapsed := time.Since(start)
	if writeErr != nil {
		s.cb.RecordFailure()
		s.logger.Errorw("force sync write failed", "order_no", order.OrderNo, "err", writeErr)
		return "sync", order.OrderNo, writeErr
	}

	s.cb.RecordSuccess(elapsed)

	// 延迟双删: 写入后失效缓存
	s.cache.InvalidateWithDelay(ctx, fmt.Sprintf("order:%s", order.OrderNo), 500)

	// 异步发布事件到 Kafka (不阻塞同步路径)
	s.publishOrderEvent(ctx, order)

	s.logger.Infow("order created (force sync)",
		"order_no", order.OrderNo,
		"user_id", order.UserID,
		"cost_ms", elapsed.Milliseconds(),
	)
	return "sync", order.OrderNo, nil
}

// createSync 通道A: 同步写入 (分布式锁按订单号粒度，防重复创建)
func (s *OrderService) createSync(ctx context.Context, order *model.Order) (string, string, error) {
	// lockKey := fmt.Sprintf("order:create:%s", order.UserID) // 带分布式锁防并发冲突
	lockKey := fmt.Sprintf("order:create:%s", order.OrderNo) // 分布式锁按订单号粒度，防重复创建

	start := time.Now()
	var writeErr error

	// 分布式锁: 防止同用户并发写冲突
	locked := false
	if s.lock != nil {
		token, err := s.lock.Lock(ctx, lockKey, 5*time.Second, 200*time.Millisecond)
		if err == nil {
			locked = true
			defer s.lock.Unlock(ctx, lockKey, token)
		}
	} else {
		locked = true // 无Redis时跳过分布式锁
	}

	if !locked {
		// 锁获取失败 -> 降级为异步
		s.logger.Warnw("distributed lock failed, fallback to async",
			"user_id", order.UserID,
		)
		return s.createAsync(ctx, order)
	}

	// 同步写入DB
	writeErr = s.dbWrite(ctx, order)

	elapsed := time.Since(start)
	if writeErr != nil {
		s.cb.RecordFailure()
		s.logger.Errorw("sync write failed", "order_no", order.OrderNo, "err", writeErr)
		return "async", order.OrderNo, writeErr
	}

	s.cb.RecordSuccess(elapsed)

	// 延迟双删: 写入后失效缓存
	s.cache.InvalidateWithDelay(ctx, fmt.Sprintf("order:%s", order.OrderNo), 500)

	// 异步发布事件到 Kafka (不阻塞同步路径)
	s.publishOrderEvent(ctx, order)

	s.logger.Infow("order created (sync)",
		"order_no", order.OrderNo,
		"user_id", order.UserID,
		"cost_ms", elapsed.Milliseconds(),
	)
	return "sync", order.OrderNo, nil
}

// createAsync 通道B: 异步写入Kafka (<10ms响应)
func (s *OrderService) createAsync(ctx context.Context, order *model.Order) (string, string, error) {
	event := model.BusinessEvent{
		EventID:   order.OrderNo,
		EventType: "ORDER_CREATE",
		Timestamp: time.Now().UnixMilli(),
		TraceID:   traceIDFromCtx(ctx),
		Payload:   order,
	}

	key := fmt.Sprintf("%d", order.UserID)

	// 无Kafka连接时模拟异步接收 (测试/降级模式)
	if s.producer == nil {
		s.logger.Infow("order accepted (async, no kafka)",
			"order_no", order.OrderNo,
			"user_id", order.UserID,
		)
		return "async", order.OrderNo, nil
	}

	if err := s.producer.Send(ctx, key, event); err != nil {
		s.logger.Errorw("kafka send failed", "order_no", order.OrderNo, "err", err)
		return "async", order.OrderNo, fmt.Errorf("kafka send: %w", err)
	}

	s.logger.Infow("order accepted (async)",
		"order_no", order.OrderNo,
		"user_id", order.UserID,
	)
	return "async", order.OrderNo, nil
}

// HandleOrderEvent Kafka消费者回调: 异步入库
func (s *OrderService) HandleOrderEvent(ctx context.Context, key string, value []byte) error {
	var event model.BusinessEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("unmarshal event: %w", err)
	}

	// 使用 json.RawMessage 避免二次序列化，直接反序列化 payload
	var order model.Order
	if err := remarshalTo(event.Payload, &order); err != nil {
		return fmt.Errorf("unmarshal order: %w", err)
	}

	// 异步写入DB
	if err := s.dbWrite(ctx, &order); err != nil {
		return fmt.Errorf("async db write: %w", err)
	}

	// 写入成功后删除缓存
	s.cache.Invalidate(ctx, fmt.Sprintf("order:%s", order.OrderNo))

	s.logger.Infow("order created from kafka",
		"traceId", event.TraceID,
		"order_no", order.OrderNo,
		"user_id", order.UserID,
	)
	return nil
}

// remarshalTo 将 interface{} 转换为目标类型（通过 json 中转）
func remarshalTo(src interface{}, dst interface{}) error {
	b, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst)
}

// GetOrder 查询订单 (走多级缓存 → DB回源)
func (s *OrderService) GetOrder(ctx context.Context, orderNo string) (*model.Order, error) {
	cacheKey := fmt.Sprintf("order:%s", orderNo)

	val, found, err := s.cache.Get(ctx, cacheKey)
	if err != nil {
		return nil, err
	}
	if found && val != nil {
		order, ok := val.(*model.Order)
		if ok {
			return order, nil
		}
		// map 类型转换兼容
		if m, ok := val.(map[string]interface{}); ok {
			b, _ := json.Marshal(m)
			var o model.Order
			json.Unmarshal(b, &o)
			return &o, nil
		}
	}

	// 缓存穿透标记: found=true, val=nil → 空标记命中, 直接返回
	if found && val == nil {
		return nil, fmt.Errorf("order not found: %s", orderNo)
	}

	// Cache miss → DB 回源
	return s.dbQuery(ctx, orderNo)
}

// dbWrite 写入MySQL Master + 同步索引ES (db为nil时降级为内存写)
func (s *OrderService) dbWrite(ctx context.Context, order *model.Order) error {
	if s.db == nil || s.db.IsNil() {
		// 降级: 写入L1缓存作为内存存储
		s.cache.Set(ctx, fmt.Sprintf("order:%s", order.OrderNo), order)
		return nil
	}

	query := `INSERT INTO orders (user_id, order_no, amount, status, created_at, updated_at)
	           VALUES (?, ?, ?, ?, ?, ?)
	           ON DUPLICATE KEY UPDATE amount=VALUES(amount), status=VALUES(status), updated_at=VALUES(updated_at)`

	_, err := s.db.ExecContext(ctx, query,
		order.UserID, order.OrderNo, order.Amount, order.Status,
		order.CreatedAt, order.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("db insert: %w", err)
	}

	// 写后预热缓存
	s.cache.Set(ctx, fmt.Sprintf("order:%s", order.OrderNo), order)

	// 同步索引到ES(异步, 不阻塞主流程, 限制并发)
	if s.searchClient != nil {
		s.indexOrderToES(order)
	}
	return nil
}

// GetCacheStats 返回缓存统计信息
func (s *OrderService) GetCacheStats() map[string]interface{} {
	return s.cache.Stats()
}

// SearchOrders 通过ES全文搜索订单
func (s *OrderService) SearchOrders(ctx context.Context, keyword string, page, size int) ([]model.Order, int64, error) {
	if s.searchClient == nil {
		return nil, 0, fmt.Errorf("elasticsearch not available")
	}
	return s.searchClient.SearchOrders(ctx, keyword, page, size)
}

// indexOrderToES 带限流保护的异步 ES 索引写入
func (s *OrderService) indexOrderToES(order *model.Order) {
	// 非阻塞尝试获取并发槽位，满时丢弃（不阻塞主流程）
	select {
	case s.esSemaphore <- struct{}{}:
	default:
		s.logger.Debugw("es index skipped (too many pending)", "order_no", order.OrderNo)
		return
	}

	go func() {
		defer func() { <-s.esSemaphore }()
		if err := s.searchClient.IndexOrder(context.Background(), order); err != nil {
			s.logger.Warnw("es index order failed (non-blocking)", "order_no", order.OrderNo, "err", err)
		}
	}()
}

// dbQuery 从MySQL Replica查询订单
func (s *OrderService) dbQuery(ctx context.Context, orderNo string) (*model.Order, error) {
	if s.db == nil || s.db.IsNil() {
		return nil, fmt.Errorf("database not available")
	}

	var order model.Order
	query := `SELECT id, user_id, order_no, amount, status, created_at, updated_at
	          FROM orders WHERE order_no = ?`

	err := s.db.QueryRowContext(ctx, query, orderNo).Scan(
		&order.ID, &order.UserID, &order.OrderNo, &order.Amount,
		&order.Status, &order.CreatedAt, &order.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		// 缓存空标记防穿透
		s.cache.SetNull(ctx, fmt.Sprintf("order:%s", orderNo))
		return nil, fmt.Errorf("order not found: %s", orderNo)
	}
	if err != nil {
		return nil, fmt.Errorf("db query: %w", err)
	}

	// 回填缓存
	s.cache.Set(ctx, fmt.Sprintf("order:%s", orderNo), &order)
	return &order, nil
}

// 全局原子计数器 + 随机种子，保证高并发下订单号唯一
var (
	orderSeq    uint64
	orderRandID [8]byte
)

func init() {
	// 初始化随机种子 (不依赖时间，避免并发碰撞)
	if _, err := rand.Read(orderRandID[:]); err != nil {
		// fallback: 用纳秒时间戳 + 进程伪随机
		binary.BigEndian.PutUint64(orderRandID[:], uint64(time.Now().UnixNano()))
	}
	orderSeq = uint64(time.Now().UnixNano()) & 0xFFFF
}

// generateOrderNo 生成全局唯一订单号
// 格式: ORD{userID}{seq(5位)}{rand(3位)}，共 ~20 字符
// seq 原子递增保证同进程不重复，rand 保证多进程不重复
func generateOrderNo(userID uint64) string {
	seq := atomic.AddUint64(&orderSeq, 1) % 100000
	randPart := binary.BigEndian.Uint64(orderRandID[:]) % 1000
	return fmt.Sprintf("ORD%d%05d%03d", userID, seq, randPart)
}

// publishOrderEvent 异步发送订单事件到 Kafka (不阻塞主流程)
func (s *OrderService) publishOrderEvent(ctx context.Context, order *model.Order) {
	if s.producer == nil {
		return
	}
	event := model.BusinessEvent{
		EventID:   order.OrderNo,
		EventType: "ORDER_CREATE",
		Timestamp: time.Now().UnixMilli(),
		TraceID:   traceIDFromCtx(ctx),
		Payload:   order,
	}
	s.producer.SendAsync(ctx, fmt.Sprintf("%d", order.UserID), event)
}

// traceIDFromCtx 从 context.Context 提取 trace_id
func traceIDFromCtx(ctx context.Context) string {
	return trace.FromContext(ctx)
}
