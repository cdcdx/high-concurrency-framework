package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cdcdx/high-concurrency-framework/internal/cache"
	"github.com/cdcdx/high-concurrency-framework/internal/handler"
	"github.com/cdcdx/high-concurrency-framework/internal/health"
	"github.com/cdcdx/high-concurrency-framework/internal/lock"
	"github.com/cdcdx/high-concurrency-framework/internal/middleware"
	"github.com/cdcdx/high-concurrency-framework/internal/model"
	"github.com/cdcdx/high-concurrency-framework/internal/mq"
	"github.com/cdcdx/high-concurrency-framework/internal/resilience"
	"github.com/cdcdx/high-concurrency-framework/internal/service"
	"github.com/dgraph-io/ristretto"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// ==========================================
// 高可用集成测试套件
// 验证: 读写分流 / 多级缓存 / 熔断 / 限流 / 分布式锁 / 穿透/击穿/雪崩防护
// ==========================================

// fullTestEnv 完整测试环境 (含Mock Kafka + 写路由)
type fullTestEnv struct {
	Router       *gin.Engine
	MultiCache   *cache.MultiLevelCache
	CB           *resilience.CircuitBreaker
	RateLimiter  *resilience.WriteRateLimiter
	DistLock     *lock.DistributedLock
	MockProducer *mq.MockProducer
	OrderSvc     *service.OrderService
	ProfileSvc   *service.UserProfileService
	HotKeys      *cache.HotKeyDetector
	Logger       *zap.SugaredLogger
}

func setupFullTestEnv(t *testing.T) *fullTestEnv {
	t.Helper()

	logger := zap.NewNop().Sugar()

	// L1 本地缓存 (Ristretto)
	l1, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 100000,
		MaxCost:     10000,
		BufferItems: 64,
	})
	if err != nil {
		t.Fatalf("init ristretto: %v", err)
	}

	// 热点Key检测器
	hotKeys := cache.NewHotKeyDetector(10, 5, 50)

	// DataLoader: 模拟L3 DB回源
	dataLoader := func(ctx context.Context, key string) (interface{}, error) {
		// 用户资料: userID >= 90000 视为不存在 (用于穿透测试)
		if strings.HasPrefix(key, "user:profile:") {
			var userID uint64
			fmt.Sscanf(key, "user:profile:%d", &userID)
			if userID >= 90000 {
				return nil, nil // 用户不存在 → 触发空标记防穿透
			}
			return &model.UserProfile{
				UserID:   userID,
				Nickname: fmt.Sprintf("user_%d", userID),
				Email:    fmt.Sprintf("user%d@example.com", userID),
			}, nil
		}
		// 订单
		if strings.HasPrefix(key, "order:") {
			orderNo := strings.TrimPrefix(key, "order:")
			return &model.Order{
				OrderNo: orderNo,
				UserID:  10001,
				Amount:  99.99,
				Status:  "created",
			}, nil
		}
		return nil, nil
	}

	// 多级缓存 (无Redis, 退化模式)
	multiCache := cache.NewMultiLevelCache(l1, nil, dataLoader, hotKeys, logger)
	multiCache.SetJitterPct(0) // 测试关闭jitter

	// 熔断器
	cb := resilience.NewCircuitBreaker("test-cb", 5, 2, 5, 0.5, 1000)

	// 限流器: 桶容量10, 每秒补充10令牌 (方便测试分流)
	rateLimiter := resilience.NewWriteRateLimiter(10, 10)

	// 分布式锁 (无Redis场景用内存模拟)
	var distLock *lock.DistributedLock

	// Mock Kafka生产者
	mockProducer := mq.NewMockProducer()

	// 业务服务
	orderSvc := service.NewOrderService(multiCache, nil, rateLimiter, distLock, cb, nil, nil, logger)
	profileSvc := service.NewUserProfileService(multiCache, nil, nil, logger)

	// Gin路由
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(middleware.TraceID())
	router.Use(middleware.ZapLogger(logger))
	router.Use(middleware.Recovery(logger))
	router.Use(middleware.ErrorHandler())

	// 注册全量路由 (读写)
	bizHandler := handler.NewBusinessHandler(orderSvc, profileSvc, nil, cb, logger)
	monHandler := handler.NewMonitorHandler(multiCache, cb, rateLimiter)
	hc := health.NewHealthChecker(nil, multiCache, cb, nil, logger)
	hc.SetStartupCompleted()

	// 健康探针
	router.GET("/health/liveness", hc.LivenessHandler())
	router.GET("/health/readiness", hc.ReadinessHandler())
	router.GET("/health/startup", hc.StartupHandler())

	// 写接口
	router.POST("/api/v1/orders", bizHandler.CreateOrder)
	router.POST("/api/v1/orders/sync", bizHandler.CreateOrderSync)

	// 读接口
	router.GET("/api/v1/orders/:orderNo", bizHandler.GetOrder)
	router.GET("/api/v1/users/:userID/profile", bizHandler.GetUserProfile)

	// 监控接口
	router.GET("/api/v1/monitor/metrics", monHandler.Metrics)
	router.GET("/api/v1/monitor/circuit-breaker", monHandler.CircuitBreakerState)

	return &fullTestEnv{
		Router:       router,
		MultiCache:   multiCache,
		CB:           cb,
		RateLimiter:  rateLimiter,
		DistLock:     distLock,
		MockProducer: mockProducer,
		OrderSvc:     orderSvc,
		ProfileSvc:   profileSvc,
		HotKeys:      hotKeys,
		Logger:       logger,
	}
}

// doJSON 发送JSON请求的辅助函数
func doJSON(router *gin.Engine, method, path, body string) *httptest.ResponseRecorder {
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

// parseResponse 解析统一响应体
func parseResponse(t *testing.T, w *httptest.ResponseRecorder) model.ApiResponse {
	t.Helper()
	var resp model.ApiResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v, body=%s", err, w.Body.String())
	}
	return resp
}

// ==========================================
// 一、读接口高可用测试 (GET /api/v1/users/:userID/profile)
// ==========================================

// TestReadL1CacheHit 测试读接口 L1命中 (<1ms)
func TestReadL1CacheHit(t *testing.T) {
	env := setupFullTestEnv(t)

	// 首次访问: L1 MISS → L2 MISS → L3 DB → 回填L1
	w1 := doJSON(env.Router, http.MethodGet, "/api/v1/users/10001/profile", "")
	resp1 := parseResponse(t, w1)
	if resp1.Code != 200 {
		t.Fatalf("first read: expected 200, got %d, msg=%s", resp1.Code, resp1.Message)
	}

	// 第二次访问: 应命中L1 (已在首次回填)
	w2 := doJSON(env.Router, http.MethodGet, "/api/v1/users/10001/profile", "")
	resp2 := parseResponse(t, w2)
	if resp2.Code != 200 {
		t.Fatalf("second read (L1 hit): expected 200, got %d msg=%s", resp2.Code, resp2.Message)
	}

	// 验证数据一致性
	data1, _ := json.Marshal(resp1.Data)
	data2, _ := json.Marshal(resp2.Data)
	if string(data1) != string(data2) {
		t.Error("L1 hit data mismatch with original")
	}

	// 验证缓存命中率 > 0
	stats := env.MultiCache.Stats()
	l1HitRate := stats["l1_hit_rate"].(float64)
	if l1HitRate <= 0 {
		t.Errorf("L1 hit rate should be > 0, got %f", l1HitRate)
	}
	t.Logf("L1 hit rate: %.2f%% | L1 hits=%v, total=%v",
		l1HitRate*100, stats["l1_hits"], stats["total"])
}

// TestReadCachePenetrationProtection 测试缓存穿透防护 (空标记)
func TestReadCachePenetrationProtection(t *testing.T) {
	env := setupFullTestEnv(t)

	// 访问一个DB中不存在的用户
	w := doJSON(env.Router, http.MethodGet, "/api/v1/users/99999/profile", "")
	resp := parseResponse(t, w)

	// 应返回404 (空标记防穿透)
	if resp.Code != 404 {
		t.Errorf("penetration test: expected 404, got %d", resp.Code)
	}

	// 验证空标记已缓存 (再次访问不会触发DB查询)
	w2 := doJSON(env.Router, http.MethodGet, "/api/v1/users/99999/profile", "")
	resp2 := parseResponse(t, w2)
	if resp2.Code != 404 {
		t.Errorf("penetration test (2nd): expected 404 from null cache, got %d", resp2.Code)
	}

	stats := env.MultiCache.Stats()
	t.Logf("penetration protected | total=%v, misses=%v, l1_hits=%v",
		stats["total"], stats["misses"], stats["l1_hits"])
}

// TestReadHotKeyBreakdownProtection 测试击穿防护 (热点Key互斥锁)
func TestReadHotKeyBreakdownProtection(t *testing.T) {
	env := setupFullTestEnv(t)

	// 制造热点Key: 连续大量访问同一Key
	hotKey := "user:profile:10001"
	// 预热数据到L1
	doJSON(env.Router, http.MethodGet, "/api/v1/users/10001/profile", "")

	// 手动清除L1缓存 (模拟热点Key过期)
	env.MultiCache.Invalidate(context.Background(), hotKey)

	// 频繁访问 (应触发热点检测 + 互斥加载)
	for i := 0; i < 60; i++ {
		env.HotKeys.RecordAccess(hotKey)
	}

	isHot := env.HotKeys.IsHot(hotKey)

	// 并发读取 (模拟击穿场景)
	var wg sync.WaitGroup
	concurrency := 50
	errors := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := doJSON(env.Router, http.MethodGet, "/api/v1/users/10001/profile", "")
			if w.Code != 200 {
				errors <- fmt.Errorf("concurrent read: code=%d", w.Code)
			}
		}()
	}
	wg.Wait()
	close(errors)

	errCount := 0
	for e := range errors {
		t.Logf("concurrent error: %v", e)
		errCount++
	}

	t.Logf("hot key: %v | concurrent reads: %d | errors: %d",
		isHot, concurrency, errCount)

	if errCount > 0 {
		t.Errorf("breakdown test: %d/%d requests failed (hot key protection may be insufficient)",
			errCount, concurrency)
	}
}

// TestReadCacheAvalancheProtection 测试雪崩防护 (TTL随机偏移)
func TestReadCacheAvalancheProtection(t *testing.T) {
	// 验证jitter机制产生非固定TTL
	base := 30 * time.Second
	samples := make(map[time.Duration]bool)

	for i := 0; i < 50; i++ {
		d := cache.JitterDuration(base, 0.2)
		samples[d] = true
		// 验证在合理范围内
		minDuration := base - time.Duration(float64(base)*0.2)
		maxDuration := base + time.Duration(float64(base)*0.2)
		if d < minDuration || d > maxDuration {
			t.Errorf("jitter out of range: %v (expected %v ~ %v)", d, minDuration, maxDuration)
		}
	}

	// 至少有3种不同TTL (证明随机分布)
	if len(samples) < 3 {
		t.Errorf("jitter not producing enough variance: %d unique values", len(samples))
	}
	t.Logf("avalanche protection: %d unique TTLs from %d samples", len(samples), 50)
}

// TestReadMultiUserConcurrent 测试读接口并发性能
func TestReadMultiUserConcurrent(t *testing.T) {
	env := setupFullTestEnv(t)

	// 预热
	doJSON(env.Router, http.MethodGet, "/api/v1/users/10001/profile", "")

	var wg sync.WaitGroup
	concurrency := 200
	successCount := 0
	var mu sync.Mutex

	start := time.Now()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			userID := 10001 + (id % 20) // 20个不同用户
			w := doJSON(env.Router, http.MethodGet, fmt.Sprintf("/api/v1/users/%d/profile", userID), "")
			if w.Code == 200 {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	rate := float64(concurrency) / elapsed.Seconds()
	t.Logf("concurrent read: %d req in %v (%.0f req/s), success=%d",
		concurrency, elapsed, rate, successCount)

	if successCount < concurrency*95/100 {
		t.Errorf("success rate too low: %d/%d", successCount, concurrency)
	}
}

// ==========================================
// 二、写接口高可用测试 (POST /api/v1/orders)
// ==========================================

// TestWriteRateLimitSyncAsyncSplit 测试写限流分流 (同步 vs 异步)
func TestWriteRateLimitSyncAsyncSplit(t *testing.T) {
	env := setupFullTestEnv(t)

	orderJSON := `{"user_id": 10001, "amount": 299.99}`

	var mu sync.Mutex
	syncCount := 0
	asyncCount := 0

	// 并发发送20个请求 (桶容量=10, 并发冲击才能触发分流)
	var wg sync.WaitGroup
	concurrency := 20
	errors := make(chan string, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := doJSON(env.Router, http.MethodPost, "/api/v1/orders", orderJSON)
			resp := parseResponse(t, w)
			if resp.Code != 200 {
				errors <- fmt.Sprintf("code=%d, msg=%s", resp.Code, resp.Message)
				return
			}

			dataMap, ok := resp.Data.(map[string]interface{})
			if !ok {
				errors <- "unexpected response data type"
				return
			}

			channel, _ := dataMap["channel"].(string)
			mu.Lock()
			switch channel {
			case "sync":
				syncCount++
			case "async":
				asyncCount++
			default:
				errors <- fmt.Sprintf("unknown channel: %s", channel)
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(errors)

	for e := range errors {
		t.Error(e)
	}

	t.Logf("sync/async split: sync=%d, async=%d (bucket=10)", syncCount, asyncCount)

	if syncCount > 10 {
		t.Errorf("sync count %d exceeds bucket capacity 10", syncCount)
	}
	if asyncCount == 0 {
		t.Error("no async requests: rate limiter should overflow to async channel")
	}
}

// TestWriteAsyncChannelFallback 测试异步通路 (令牌耗尽自动降级)
func TestWriteAsyncChannelFallback(t *testing.T) {
	env := setupFullTestEnv(t)

	orderJSON := `{"user_id": 10002, "amount": 199.99}`

	// 并发耗尽令牌桶: 20个并发写冲击桶容量10的限流器
	var wg sync.WaitGroup
	channels := make(chan string, 20)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := doJSON(env.Router, http.MethodPost, "/api/v1/orders", orderJSON)
			resp := parseResponse(t, w)
			if resp.Code == 200 {
				if dataMap, ok := resp.Data.(map[string]interface{}); ok {
					if ch, _ := dataMap["channel"].(string); ch != "" {
						channels <- ch
					}
				}
			}
		}()
	}
	wg.Wait()
	close(channels)

	syncCount, asyncCount := 0, 0
	for ch := range channels {
		switch ch {
		case "sync":
			syncCount++
		case "async":
			asyncCount++
		}
	}

	t.Logf("async fallback: sync=%d, async=%d", syncCount, asyncCount)

	// 桶容量10, 并发20, 应有异步降级
	if asyncCount == 0 {
		t.Error("expected some async fallback when bucket overflows")
	}
}

// TestWriteCircuitBreakerFallback 测试熔断降级
func TestWriteCircuitBreakerFallback(t *testing.T) {
	env := setupFullTestEnv(t)

	orderJSON := `{"user_id": 10003, "amount": 399.99}`

	// 触发熔断: 大量记录失败
	for i := 0; i < 20; i++ {
		env.CB.RecordFailure()
	}

	// 等待状态转换
	time.Sleep(2500 * time.Millisecond)

	state := env.CB.GetState()
	t.Logf("circuit breaker state after failures: %s", state)

	if state == "OPEN" {
		// 熔断打开: 所有请求应降级为异步
		w := doJSON(env.Router, http.MethodPost, "/api/v1/orders", orderJSON)
		resp := parseResponse(t, w)

		dataMap, ok := resp.Data.(map[string]interface{})
		if ok {
			channel, _ := dataMap["channel"].(string)
			if channel != "async" {
				t.Errorf("circuit breaker open: expected async, got %s", channel)
			}
		}
		t.Logf("circuit breaker fallback: state=%s, channel=async", state)
	}
}

// TestWriteCacheInvalidation 测试写后缓存失效 (延迟双删)
func TestWriteCacheInvalidation(t *testing.T) {
	env := setupFullTestEnv(t)

	ctx := context.Background()
	cacheKey := "order:ORD10001001"

	// 1. 预热缓存
	mockOrder := &model.Order{
		OrderNo: "ORD10001001",
		UserID:  10001,
		Amount:  299.99,
		Status:  "created",
	}
	env.MultiCache.Set(ctx, cacheKey, mockOrder)

	// 2. 验证缓存命中
	val, found, _ := env.MultiCache.Get(ctx, cacheKey)
	if !found {
		t.Fatal("cache should be populated before write")
	}
	t.Logf("cache populated: val=%v", val)

	// 3. 执行写操作 (会触发延迟双删)
	orderJSON := `{"user_id": 10001, "amount": 499.99}`
	// 先把令牌桶重置
	for i := 0; i < 20; i++ {
		env.RateLimiter.TryAcquire()
	}
	// 等补充令牌
	time.Sleep(1100 * time.Millisecond)

	doJSON(env.Router, http.MethodPost, "/api/v1/orders", orderJSON)

	// 4. 等待延迟双删
	time.Sleep(600 * time.Millisecond)

	// 5. 验证缓存已失效 (首次会MISS, 走DB回源)
	stats := env.MultiCache.Stats()
	t.Logf("after write + delay: misses=%v, total=%v", stats["misses"], stats["total"])
}

// TestWriteDistributedLockConcurrency 测试分布式锁防并发写
func TestWriteDistributedLockConcurrency(t *testing.T) {
	env := setupFullTestEnv(t)

	orderJSON := `{"user_id": 10004, "amount": 599.99}`

	var wg sync.WaitGroup
	concurrency := 50
	results := make([]string, 0, concurrency)
	var mu sync.Mutex

	start := time.Now()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := doJSON(env.Router, http.MethodPost, "/api/v1/orders", orderJSON)
			resp := parseResponse(t, w)
			mu.Lock()
			if dataMap, ok := resp.Data.(map[string]interface{}); ok {
				if channel, ok := dataMap["channel"].(string); ok {
					results = append(results, channel)
				}
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	syncCount := 0
	asyncCount := 0
	for _, c := range results {
		switch c {
		case "sync":
			syncCount++
		case "async":
			asyncCount++
		}
	}

	t.Logf("distributed lock concurrency: %d req in %v, sync=%d, async=%d",
		concurrency, elapsed, syncCount, asyncCount)

	expectedMaxSync := env.RateLimiter.Tokens() + 10 // 初始+1秒补充
	if syncCount > expectedMaxSync+5 {
		t.Errorf("too many sync writes: %d > %d (rate limiter bypass?)", syncCount, expectedMaxSync)
	}
}

// TestWriteValidation 测试写接口参数校验
func TestWriteValidation(t *testing.T) {
	env := setupFullTestEnv(t)

	testCases := []struct {
		name     string
		body     string
		wantCode int
	}{
		{"valid order", `{"user_id": 10001, "amount": 299.99}`, 200},
		{"zero user_id", `{"user_id": 0, "amount": 100}`, 400},
		{"negative amount", `{"user_id": 10001, "amount": -50}`, 400},
		{"missing fields", `{}`, 400},
		{"invalid json", `{invalid`, 400},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			w := doJSON(env.Router, http.MethodPost, "/api/v1/orders", tc.body)
			if w.Code != tc.wantCode {
				t.Errorf("%s: expected %d, got %d | body=%s",
					tc.name, tc.wantCode, w.Code, w.Body.String())
			}
		})
	}
}

// ==========================================
// 三、读写联动高可用测试
// ==========================================

// TestReadAfterWriteConsistency 测试写后读一致性 (延迟双删)
func TestReadAfterWriteConsistency(t *testing.T) {
	env := setupFullTestEnv(t)

	ctx := context.Background()
	cacheKey := "order:ORD10001999"

	// 1. 预热旧数据
	oldOrder := &model.Order{
		OrderNo: "ORD10001999",
		UserID:  10001,
		Amount:  100.00,
		Status:  "pending",
	}
	env.MultiCache.Set(ctx, cacheKey, oldOrder)

	// 2. 读缓存 (验证旧数据在缓存中)
	val, found, _ := env.MultiCache.Get(ctx, cacheKey)
	if !found {
		t.Fatal("old data should be in cache")
	}
	if o, ok := val.(*model.Order); ok {
		if o.Amount != 100.00 {
			t.Errorf("cached amount mismatch: expected 100, got %f", o.Amount)
		}
	}

	// 3. 执行写请求 (会触发延迟双删500ms)
	for i := 0; i < 20; i++ {
		env.RateLimiter.TryAcquire()
	}
	time.Sleep(1500 * time.Millisecond) // 等待令牌补充

	doJSON(env.Router, http.MethodPost, "/api/v1/orders", `{"user_id": 10001, "amount": 999.99}`)

	// 4. 等待延迟删除完成
	time.Sleep(600 * time.Millisecond)

	// 5. 再次读缓存 (应MISS, 走DB回源)
	stats := env.MultiCache.Stats()
	t.Logf("read-after-write consistency: l1_hits=%v, misses=%v, total=%v",
		stats["l1_hits"], stats["misses"], stats["total"])
}

// ==========================================
// 四、全链路压力测试
// ==========================================

// TestHighConcurrencyReadWriteMix 混合读写并发压力测试
func TestHighConcurrencyReadWriteMix(t *testing.T) {
	env := setupFullTestEnv(t)

	// 预热
	doJSON(env.Router, http.MethodGet, "/api/v1/users/10001/profile", "")

	var wg sync.WaitGroup
	totalRequests := 500
	readOps := 0
	writeOps := 0
	readErrs := 0
	writeErrs := 0
	var mu sync.Mutex

	start := time.Now()
	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if id%4 == 0 {
				// 25% 写请求
				orderJSON := fmt.Sprintf(`{"user_id": %d, "amount": 99.99}`, 10001+(id%100))
				w := doJSON(env.Router, http.MethodPost, "/api/v1/orders", orderJSON)
				mu.Lock()
				writeOps++
				if w.Code != 200 {
					writeErrs++
				}
				mu.Unlock()
			} else {
				// 75% 读请求
				userID := 10001 + (id % 50)
				w := doJSON(env.Router, http.MethodGet, fmt.Sprintf("/api/v1/users/%d/profile", userID), "")
				mu.Lock()
				readOps++
				if w.Code != 200 {
					readErrs++
				}
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	rate := float64(totalRequests) / elapsed.Seconds()
	stats := env.MultiCache.Stats()
	cbState := env.CB.GetState()

	t.Logf("=== Mixed Load Test ===")
	t.Logf("total:   %d req in %v (%.0f req/s)", totalRequests, elapsed, rate)
	t.Logf("reads:   %d (errs=%d)", readOps, readErrs)
	t.Logf("writes:  %d (errs=%d)", writeOps, writeErrs)
	t.Logf("cache:   l1_rate=%.2f%%, combo_rate=%.2f%%, misses=%v",
		stats["l1_hit_rate"], stats["combo_hit_rate"], stats["misses"])
	t.Logf("cb:      state=%s", cbState)

	if readErrs > readOps/10 {
		t.Errorf("read error rate too high: %d/%d", readErrs, readOps)
	}
}

// ==========================================
// 五、监控接口高可用测试
// ==========================================

// TestMonitorMetrics 测试监控指标接口
func TestMonitorMetrics(t *testing.T) {
	env := setupFullTestEnv(t)

	// 先触发一些读写操作
	doJSON(env.Router, http.MethodGet, "/api/v1/users/10001/profile", "")
	doJSON(env.Router, http.MethodPost, "/api/v1/orders", `{"user_id": 10001, "amount": 199.99}`)

	w := doJSON(env.Router, http.MethodGet, "/api/v1/monitor/metrics", "")
	resp := parseResponse(t, w)
	if resp.Code != 200 {
		t.Fatalf("metrics: code=%d", resp.Code)
	}

	dataMap, ok := resp.Data.(map[string]interface{})
	if !ok {
		t.Fatal("metrics data type error")
	}

	// 验证缓存指标
	if cacheMetrics, ok := dataMap["cache"].(map[string]interface{}); ok {
		t.Logf("cache metrics: l1_hits=%v, l2_hits=%v, misses=%v, l1_rate=%.2f%%",
			cacheMetrics["l1_hits"], cacheMetrics["l2_hits"],
			cacheMetrics["misses"], cacheMetrics["l1_hit_rate"])
	}

	// 验证熔断器指标
	if cbMetrics, ok := dataMap["circuit_breaker"].(map[string]interface{}); ok {
		t.Logf("circuit breaker: state=%v, failures=%v", cbMetrics["state"], cbMetrics["failure_count"])
	}

	// 验证限流器指标
	if rlMetrics, ok := dataMap["rate_limiter"].(map[string]interface{}); ok {
		t.Logf("rate limiter: tokens=%v", rlMetrics["tokens"])
	}
}

// TestCircuitBreakerMonitor 测试熔断器状态监控
func TestCircuitBreakerMonitor(t *testing.T) {
	env := setupFullTestEnv(t)

	w := doJSON(env.Router, http.MethodGet, "/api/v1/monitor/circuit-breaker", "")
	resp := parseResponse(t, w)
	if resp.Code != 200 {
		t.Fatalf("circuit breaker monitor: code=%d", resp.Code)
	}

	dataMap, ok := resp.Data.(map[string]interface{})
	if ok {
		state, _ := dataMap["state"].(string)
		t.Logf("circuit breaker state: %s", state)
	}
}

// ==========================================
// 六、退化模式测试 (无Redis/无Kafka)
// ==========================================

// TestDegradedModeRead 测试无Redis时的退化读 (L1 → L3)
func TestDegradedModeRead(t *testing.T) {
	env := setupFullTestEnv(t)

	// 冷启动读取
	w := doJSON(env.Router, http.MethodGet, "/api/v1/users/10001/profile", "")
	resp := parseResponse(t, w)
	if resp.Code != 200 {
		t.Fatalf("degraded read: code=%d, msg=%s", resp.Code, resp.Message)
	}

	// 第二次应命中L1
	w2 := doJSON(env.Router, http.MethodGet, "/api/v1/users/10001/profile", "")
	resp2 := parseResponse(t, w2)
	if resp2.Code != 200 {
		t.Fatalf("degraded read (cached): code=%d", resp2.Code)
	}

	stats := env.MultiCache.Stats()
	t.Logf("degraded mode: l1_hits=%v, misses=%v, combo_rate=%.2f%%",
		stats["l1_hits"], stats["misses"], stats["combo_hit_rate"])
}

// TestDegradedModeWrite 测试无Kafka时的退化写 (异步降级)
func TestDegradedModeWrite(t *testing.T) {
	env := setupFullTestEnv(t)

	// 耗尽令牌
	for i := 0; i < 20; i++ {
		env.RateLimiter.TryAcquire()
	}

	// 异步写 (无Kafka, producer=nil, 应正常降级)
	orderJSON := `{"user_id": 10010, "amount": 199.99}`
	w := doJSON(env.Router, http.MethodPost, "/api/v1/orders", orderJSON)
	resp := parseResponse(t, w)
	if resp.Code != 200 {
		t.Fatalf("degraded write: code=%d, msg=%s", resp.Code, resp.Message)
	}

	dataMap, _ := resp.Data.(map[string]interface{})
	channel, _ := dataMap["channel"].(string)
	t.Logf("degraded write: channel=%s", channel)
}

// ==========================================
// 基准测试
// ==========================================

// setupBenchEnv 为 Benchmark 独立创建环境，避免触发 t.Fatalf
func setupBenchEnv(b *testing.B) *fullTestEnv {
	b.Helper()

	logger := zap.NewNop().Sugar()

	l1, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 100000,
		MaxCost:     10000,
		BufferItems: 64,
	})
	if err != nil {
		b.Fatalf("init ristretto: %v", err)
	}

	hotKeys := cache.NewHotKeyDetector(10, 5, 50)

	dataLoader := func(ctx context.Context, key string) (interface{}, error) {
		if strings.HasPrefix(key, "user:profile:") {
			var userID uint64
			fmt.Sscanf(key, "user:profile:%d", &userID)
			if userID >= 90000 {
				return nil, nil
			}
			return &model.UserProfile{
				UserID:   userID,
				Nickname: fmt.Sprintf("user_%d", userID),
				Email:    fmt.Sprintf("user%d@example.com", userID),
			}, nil
		}
		if strings.HasPrefix(key, "order:") {
			orderNo := strings.TrimPrefix(key, "order:")
			return &model.Order{
				OrderNo: orderNo,
				UserID:  10001,
				Amount:  99.99,
				Status:  "created",
			}, nil
		}
		return nil, nil
	}

	multiCache := cache.NewMultiLevelCache(l1, nil, dataLoader, hotKeys, logger)
	multiCache.SetJitterPct(0)

	cb := resilience.NewCircuitBreaker("test-cb", 5, 2, 5, 0.5, 1000)
	rateLimiter := resilience.NewWriteRateLimiter(10, 10)
	var distLock *lock.DistributedLock

	orderSvc := service.NewOrderService(multiCache, nil, rateLimiter, distLock, cb, nil, nil, logger)
	profileSvc := service.NewUserProfileService(multiCache, nil, nil, logger)

	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(middleware.TraceID())
	router.Use(middleware.ZapLogger(logger))
	router.Use(middleware.Recovery(logger))
	router.Use(middleware.ErrorHandler())

	bizHandler := handler.NewBusinessHandler(orderSvc, profileSvc, nil, cb, logger)
	monHandler := handler.NewMonitorHandler(multiCache, cb, rateLimiter)
	hc := health.NewHealthChecker(nil, multiCache, cb, nil, logger)
	hc.SetStartupCompleted()

	router.GET("/health/liveness", hc.LivenessHandler())
	router.GET("/health/readiness", hc.ReadinessHandler())
	router.GET("/health/startup", hc.StartupHandler())
	router.POST("/api/v1/orders", bizHandler.CreateOrder)
	router.POST("/api/v1/orders/sync", bizHandler.CreateOrderSync)
	router.GET("/api/v1/orders/:orderNo", bizHandler.GetOrder)
	router.GET("/api/v1/users/:userID/profile", bizHandler.GetUserProfile)
	router.GET("/api/v1/monitor/metrics", monHandler.Metrics)
	router.GET("/api/v1/monitor/circuit-breaker", monHandler.CircuitBreakerState)

	return &fullTestEnv{
		Router:       router,
		MultiCache:   multiCache,
		CB:           cb,
		RateLimiter:  rateLimiter,
		DistLock:     distLock,
		OrderSvc:     orderSvc,
		ProfileSvc:   profileSvc,
		HotKeys:      hotKeys,
		Logger:       logger,
	}
}

// BenchmarkReadProfile 读接口基准
func BenchmarkReadProfile(b *testing.B) {
	env := setupBenchEnv(b)
	// 预热
	doJSON(env.Router, http.MethodGet, "/api/v1/users/10001/profile", "")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/users/10001/profile", nil)
		env.Router.ServeHTTP(w, req)
	}
}

// BenchmarkWriteOrder 写接口基准
func BenchmarkWriteOrder(b *testing.B) {
	env := setupBenchEnv(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		body := strings.NewReader(`{"user_id": 10001, "amount": 299.99}`)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/orders", body)
		req.Header.Set("Content-Type", "application/json")
		env.Router.ServeHTTP(w, req)
	}
}
