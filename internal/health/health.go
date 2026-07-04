package health

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/cdcdx/high-concurrency-framework/internal/cache"
	"github.com/cdcdx/high-concurrency-framework/internal/resilience"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// KafkaChecker Kafka连通性检查函数
type KafkaChecker func(ctx context.Context) error

// HealthChecker 健康检查器
type HealthChecker struct {
	redis        *redis.Client
	cache        *cache.MultiLevelCache
	cb           *resilience.CircuitBreaker
	logger       *zap.SugaredLogger
	kafkaChecker KafkaChecker

	startupCompleted atomic.Bool
	startupTime      time.Time
}

// NewHealthChecker 创建健康检查器
func NewHealthChecker(
	redis *redis.Client,
	cache *cache.MultiLevelCache,
	cb *resilience.CircuitBreaker,
	kafkaChecker KafkaChecker,
	logger *zap.SugaredLogger,
) *HealthChecker {
	return &HealthChecker{
		redis:        redis,
		cache:        cache,
		cb:           cb,
		logger:       logger,
		kafkaChecker: kafkaChecker,
		startupTime:  time.Now(),
	}
}

// SetStartupCompleted 标记启动完成（线程安全）
func (h *HealthChecker) SetStartupCompleted() {
	h.startupCompleted.Store(true)
}

// IsStarted 返回是否已启动完成
func (h *HealthChecker) IsStarted() bool {
	return h.startupCompleted.Load()
}

// LivenessHandler 探活检测 (/health/liveness)
// 失败则K8s重启Pod
func (h *HealthChecker) LivenessHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "UP",
			"uptime": time.Since(h.startupTime).String(),
		})
	}
}

// ReadinessHandler 就绪检测 (/health/readiness)
// 检查所有依赖: Redis + Kafka + DB + 熔断器状态
// 失败则K8s摘除流量
func (h *HealthChecker) ReadinessHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		details := make(map[string]interface{})

		// 1. 检查Redis连通性
		redisOK := true
		if h.redis != nil {
			ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
			defer cancel()
			if _, err := h.redis.Ping(ctx).Result(); err != nil {
				redisOK = false
				details["redis"] = "disconnected: " + err.Error()
			} else {
				details["redis"] = "connected"
			}
		} else {
			redisOK = false
			details["redis"] = "not configured"
		}

		// 2. 检查熔断器状态
		cbState := "NONE"
		if h.cb != nil {
			cbState = h.cb.GetState()
		}
		details["circuit_breaker"] = cbState

		// 3. 缓存命中率
		if h.cache != nil {
			stats := h.cache.Stats()
			details["l1_hit_rate"] = stats["l1_hit_rate"]
			details["combo_hit_rate"] = stats["combo_hit_rate"]
		}

		// 4. Kafka连通性检查
		kafkaOK := true
		if h.kafkaChecker != nil {
			ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
			defer cancel()
			if err := h.kafkaChecker(ctx); err != nil {
				kafkaOK = false
				details["kafka"] = "disconnected: " + err.Error()
			} else {
				details["kafka"] = "connected"
			}
		} else {
			kafkaOK = false
			details["kafka"] = "not configured"
		}

		status := http.StatusOK
		if !redisOK || !kafkaOK || cbState == "OPEN" {
			status = http.StatusServiceUnavailable
		}

		c.JSON(status, gin.H{
			"status":  map[bool]string{true: "READY", false: "NOT_READY"}[status == http.StatusOK],
			"details": details,
		})
	}
}

// StartupHandler 启动检测 (/health/startup)
// 仅在启动阶段检查, 给予足够预热时间
func (h *HealthChecker) StartupHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !h.startupCompleted.Load() {
			// 检查缓存预热是否完成
			if h.cache != nil {
				stats := h.cache.Stats()
				if total, ok := stats["total"].(uint64); ok && total > 0 {
					h.startupCompleted.Store(true)
				}
			}
			if !h.startupCompleted.Load() && time.Since(h.startupTime) > 60*time.Second {
				// 超60秒强制标记完成
				h.startupCompleted.Store(true)
			}
		}

		status := http.StatusOK
		msg := "STARTED"
		if !h.startupCompleted.Load() {
			status = http.StatusServiceUnavailable
			msg = "STARTING"
		}

		c.JSON(status, gin.H{
			"status": msg,
			"uptime": time.Since(h.startupTime).String(),
		})
	}
}
