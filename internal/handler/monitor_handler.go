package handler

import (
	"net/http"

	"github.com/cdcdx/high-concurrency-framework/internal/cache"
	"github.com/cdcdx/high-concurrency-framework/internal/middleware"
	"github.com/cdcdx/high-concurrency-framework/internal/model"
	"github.com/cdcdx/high-concurrency-framework/internal/resilience"
	"github.com/gin-gonic/gin"
)

// MonitorHandler 监控接口处理器
type MonitorHandler struct {
	cache *cache.MultiLevelCache
	cb    *resilience.CircuitBreaker
	rl    *resilience.WriteRateLimiter
}

// NewMonitorHandler 创建监控处理器
func NewMonitorHandler(
	cache *cache.MultiLevelCache,
	cb *resilience.CircuitBreaker,
	rl *resilience.WriteRateLimiter,
) *MonitorHandler {
	return &MonitorHandler{
		cache: cache,
		cb:    cb,
		rl:    rl,
	}
}

// Metrics GET /api/v1/monitor/metrics
// @Summary      服务指标
// @Description  获取缓存命中率、QPS、熔断器状态等实时运行指标
// @Tags         监控
// @Produce      json
// @Success      200  {object}  model.ApiResponse
// @Router       /api/v1/monitor/metrics [get]
func (h *MonitorHandler) Metrics(c *gin.Context) {
	cacheStats := h.cache.Stats()
	cbMetrics := h.cb.Metrics()

	c.JSON(http.StatusOK, model.ApiResponse{
		Code:    200,
		Message: "success",
		Data: map[string]interface{}{
			"cache":           cacheStats,
			"circuit_breaker": cbMetrics,
			"rate_limiter": map[string]interface{}{
				"tokens": h.rl.Tokens(),
			},
		},
		TraceID: middleware.GetTraceID(c),
	})
}

// CircuitBreakerState GET /api/v1/monitor/circuit-breaker
// @Summary      熔断器状态
// @Description  查看当前熔断器状态(CLOSED/OPEN/HALF_OPEN)及详细指标
// @Tags         监控
// @Produce      json
// @Success      200  {object}  model.ApiResponse
// @Router       /api/v1/monitor/circuit-breaker [get]
func (h *MonitorHandler) CircuitBreakerState(c *gin.Context) {
	c.JSON(http.StatusOK, model.ApiResponse{
		Code:    200,
		Message: "success",
		Data: map[string]interface{}{
			"state":   h.cb.GetState(),
			"metrics": h.cb.Metrics(),
		},
		TraceID: middleware.GetTraceID(c),
	})
}
