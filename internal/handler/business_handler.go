package handler

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/cdcdx/high-concurrency-framework/internal/middleware"
	"github.com/cdcdx/high-concurrency-framework/internal/model"
	"github.com/cdcdx/high-concurrency-framework/internal/resilience"
	"github.com/cdcdx/high-concurrency-framework/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"
)

var validate = validator.New()

// BusinessHandler 业务接口处理器
type BusinessHandler struct {
	orderSvc     *service.OrderService
	profileSvc   *service.UserProfileService
	analyticsSvc *service.AnalyticsService
	cb           *resilience.CircuitBreaker
	logger       *zap.SugaredLogger
}

// NewBusinessHandler 创建业务处理器
func NewBusinessHandler(
	orderSvc *service.OrderService,
	profileSvc *service.UserProfileService,
	analyticsSvc *service.AnalyticsService,
	cb *resilience.CircuitBreaker,
	logger *zap.SugaredLogger,
) *BusinessHandler {
	return &BusinessHandler{
		orderSvc:     orderSvc,
		profileSvc:   profileSvc,
		analyticsSvc: analyticsSvc,
		cb:           cb,
		logger:       logger,
	}
}

// CreateOrder POST /api/v1/orders
// @Summary      创建订单 (异步)
// @Description  双通道分流：同步(限流内) | 异步(超限→Kafka)，适用于高并发写场景
// @Tags         订单
// @Accept       json
// @Produce      json
// @Param        body  body      model.Order  true  "订单信息"
// @Success      200   {object}  model.ApiResponse{data=object{order_no=string,channel=string}}
// @Failure      400   {object}  model.ApiResponse
// @Failure      500   {object}  model.ApiResponse
// @Security     Bearer
// @Router       /api/v1/orders [post]
func (h *BusinessHandler) CreateOrder(c *gin.Context) {
	h.handleCreateOrder(c, "order accepted")
}

// CreateOrderSync POST /api/v1/orders/sync
// @Summary      创建订单 (同步)
// @Description  强制同步写入MySQL，不走Kafka，立即可读
// @Tags         订单
// @Accept       json
// @Produce      json
// @Param        body  body      model.Order  true  "订单信息"
// @Success      200   {object}  model.ApiResponse{data=object{order_no=string,channel=string}}
// @Failure      400   {object}  model.ApiResponse
// @Failure      500   {object}  model.ApiResponse
// @Security     Bearer
// @Router       /api/v1/orders/sync [post]
func (h *BusinessHandler) CreateOrderSync(c *gin.Context) {
	h.handleCreateOrder(c, "order created")
}

// handleCreateOrder 统一处理订单创建 (消除重复代码)
func (h *BusinessHandler) handleCreateOrder(c *gin.Context, successMsg string) {
	traceID := middleware.GetTraceID(c)

	var order model.Order
	if err := c.ShouldBindJSON(&order); err != nil {
		c.JSON(http.StatusBadRequest, model.ApiResponse{
			Code:    400,
			Message: "invalid request body: " + err.Error(),
			TraceID: traceID,
		})
		return
	}

	// 参数校验
	if err := validate.Struct(&order); err != nil {
		c.JSON(http.StatusBadRequest, model.ApiResponse{
			Code:    400,
			Message: "validation failed: " + err.Error(),
			TraceID: traceID,
		})
		return
	}

	channel, orderNo, err := h.orderSvc.CreateOrder(c.Request.Context(), &order)
	if err != nil {
		h.logger.Errorw("create order failed",
			"traceId", traceID,
			"user_id", order.UserID,
			"err", err,
		)
		c.JSON(http.StatusInternalServerError, model.ApiResponse{
			Code:    500,
			Message: err.Error(),
			TraceID: traceID,
		})
		return
	}

	// 异步写入PostgreSQL行为日志
	if h.analyticsSvc != nil {
		h.analyticsSvc.LogOrderCreated(c.Request.Context(), order.UserID, orderNo)
	}

	c.JSON(http.StatusOK, model.ApiResponse{
		Code:    200,
		Message: successMsg,
		Data: map[string]interface{}{
			"order_no": orderNo,
			"channel":  channel,
		},
		TraceID: traceID,
	})
}

// GetOrder GET /api/v1/orders/:orderNo
// @Summary      查询订单
// @Description  通过订单号查询，走L1→L2→MySQL三级缓存
// @Tags         订单
// @Produce      json
// @Param        orderNo  path      string  true  "订单号"
// @Success      200      {object}  model.ApiResponse{data=model.Order}
// @Failure      404      {object}  model.ApiResponse
// @Security     Bearer
// @Router       /api/v1/orders/{orderNo} [get]
func (h *BusinessHandler) GetOrder(c *gin.Context) {
	traceID := middleware.GetTraceID(c)
	orderNo := c.Param("orderNo")

	order, err := h.orderSvc.GetOrder(c.Request.Context(), orderNo)
	if err != nil {
		c.JSON(http.StatusNotFound, model.ApiResponse{
			Code:    404,
			Message: err.Error(),
			TraceID: traceID,
		})
		return
	}

	c.JSON(http.StatusOK, model.ApiResponse{
		Code:    200,
		Message: "success",
		Data:    order,
		TraceID: traceID,
	})
}

// GetUserProfile GET /api/v1/users/:userID/profile
// @Summary      查询用户资料
// @Description  走多级缓存: L1→L2→L3(MongoDB)
// @Tags         用户
// @Produce      json
// @Param        userID  path      int  true  "用户ID"
// @Success      200     {object}  model.ApiResponse{data=model.UserProfile}
// @Failure      400     {object}  model.ApiResponse
// @Failure      404     {object}  model.ApiResponse
// @Security     Bearer
// @Router       /api/v1/users/{userID}/profile [get]
func (h *BusinessHandler) GetUserProfile(c *gin.Context) {
	traceID := middleware.GetTraceID(c)
	userIDStr := c.Param("userID")
	userID, err := strconv.ParseUint(userIDStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, model.ApiResponse{
			Code:    400,
			Message: "invalid user_id",
			TraceID: traceID,
		})
		return
	}

	profile, err := h.profileSvc.GetProfile(c.Request.Context(), userID)
	if err != nil {
		statusCode, message := h.handleServiceError(err, "profile lookup")
		if statusCode == http.StatusServiceUnavailable {
			c.JSON(statusCode, model.ApiResponse{
				Code:    statusCode,
				Message: message,
				TraceID: traceID,
			})
			return
		}
		c.JSON(http.StatusNotFound, model.ApiResponse{
			Code:    404,
			Message: err.Error(),
			TraceID: traceID,
		})
		return
	}

	c.JSON(http.StatusOK, model.ApiResponse{
		Code:    200,
		Message: "success",
		Data:    profile,
		TraceID: traceID,
	})
}

// GetCacheStats GET /api/v1/monitor/cache-stats
// @Summary      缓存统计
// @Description  返回L1本地缓存命中率和多级缓存统计信息
// @Tags         监控
// @Produce      json
// @Success      200  {object}  model.ApiResponse
// @Router       /api/v1/monitor/cache-stats [get]
func (h *BusinessHandler) GetCacheStats(c *gin.Context) {
	c.JSON(http.StatusOK, model.ApiResponse{
		Code:    200,
		Message: "success",
		Data:    h.orderSvc.GetCacheStats(), // 委托到业务层获取统计
		TraceID: middleware.GetTraceID(c),
	})
}

// parseSearchParams 提取搜索公共参数 (keyword/page/size), 返回 (keyword, page, size, ok)
// ok=false 时 caller 已写入错误响应
func (h *BusinessHandler) parseSearchParams(c *gin.Context) (string, int, int, bool) {
	traceID := middleware.GetTraceID(c)
	keyword := c.DefaultQuery("q", "")
	if keyword == "" {
		c.JSON(http.StatusBadRequest, model.ApiResponse{
			Code:    400,
			Message: "query parameter 'q' is required",
			TraceID: traceID,
		})
		return "", 0, 0, false
	}

	page := 1
	size := 20
	if p, err := strconv.Atoi(c.DefaultQuery("page", "1")); err == nil && p > 0 {
		page = p
	}
	if s, err := strconv.Atoi(c.DefaultQuery("size", "20")); err == nil && s > 0 && s <= 100 {
		size = s
	}
	return keyword, page, size, true
}

// SearchOrders GET /api/v1/orders/search?q=keyword&page=1&size=20
// @Summary      搜索订单
// @Description  通过Elasticsearch全文搜索订单
// @Tags         订单
// @Produce      json
// @Param        q      query     string  true   "搜索关键词"
// @Param        page   query     int     false  "页码"  default(1)
// @Param        size   query     int     false  "每页条数"  default(20)
// @Success      200    {object}  model.ApiResponse
// @Failure      400    {object}  model.ApiResponse
// @Security     Bearer
// @Router       /api/v1/orders/search [get]
func (h *BusinessHandler) SearchOrders(c *gin.Context) {
	traceID := middleware.GetTraceID(c)
	keyword, page, size, ok := h.parseSearchParams(c)
	if !ok {
		return
	}

	orders, total, err := h.orderSvc.SearchOrders(c.Request.Context(), keyword, page, size)
	if err != nil {
		statusCode, message := h.handleServiceError(err, "order search")
		c.JSON(statusCode, model.ApiResponse{
			Code:    statusCode,
			Message: message,
			TraceID: traceID,
		})
		return
	}

	c.JSON(http.StatusOK, model.ApiResponse{
		Code:    200,
		Message: "success",
		Data: map[string]interface{}{
			"orders": orders,
			"total":  total,
			"page":   page,
			"size":   size,
		},
		TraceID: traceID,
	})
}

// SearchUsers GET /api/v1/users/search?q=keyword&page=1&size=20
// @Summary      搜索用户
// @Description  通过Elasticsearch全文搜索用户
// @Tags         用户
// @Produce      json
// @Param        q      query     string  true   "搜索关键词"
// @Param        page   query     int     false  "页码"  default(1)
// @Param        size   query     int     false  "每页条数"  default(20)
// @Success      200    {object}  model.ApiResponse
// @Failure      400    {object}  model.ApiResponse
// @Security     Bearer
// @Router       /api/v1/users/search [get]
func (h *BusinessHandler) SearchUsers(c *gin.Context) {
	traceID := middleware.GetTraceID(c)
	keyword, page, size, ok := h.parseSearchParams(c)
	if !ok {
		return
	}

	profiles, total, err := h.profileSvc.SearchUsers(c.Request.Context(), keyword, page, size)
	if err != nil {
		statusCode, message := h.handleServiceError(err, "user search")
		c.JSON(statusCode, model.ApiResponse{
			Code:    statusCode,
			Message: message,
			TraceID: traceID,
		})
		return
	}

	c.JSON(http.StatusOK, model.ApiResponse{
		Code:    200,
		Message: "success",
		Data: map[string]interface{}{
			"users": profiles,
			"total": total,
			"page":  page,
			"size":  size,
		},
		TraceID: traceID,
	})
}

// UpsertUserProfile POST /api/v1/users/profile
// @Summary      创建/更新用户资料
// @Description  MongoDB upsert 写入，同时更新缓存
// @Tags         用户
// @Accept       json
// @Produce      json
// @Param        body  body      model.UserProfile  true  "用户资料"
// @Success      200   {object}  model.ApiResponse{data=model.UserProfile}
// @Failure      400   {object}  model.ApiResponse
// @Failure      500   {object}  model.ApiResponse
// @Security     Bearer
// @Router       /api/v1/users/profile [post]
func (h *BusinessHandler) UpsertUserProfile(c *gin.Context) {
	traceID := middleware.GetTraceID(c)

	var profile model.UserProfile
	if err := c.ShouldBindJSON(&profile); err != nil {
		c.JSON(http.StatusBadRequest, model.ApiResponse{
			Code:    400,
			Message: "invalid request body: " + err.Error(),
			TraceID: traceID,
		})
		return
	}

	if err := validate.Struct(&profile); err != nil {
		c.JSON(http.StatusBadRequest, model.ApiResponse{
			Code:    400,
			Message: "validation failed: " + err.Error(),
			TraceID: traceID,
		})
		return
	}

	if err := h.profileSvc.UpsertProfile(c.Request.Context(), &profile); err != nil {
		statusCode, message := h.handleServiceError(err, "profile upsert")
		h.logger.Warnw("upsert profile failed", "traceId", traceID, "user_id", profile.UserID, "err", err)
		c.JSON(statusCode, model.ApiResponse{
			Code:    statusCode,
			Message: message,
			TraceID: traceID,
		})
		return
	}

	c.JSON(http.StatusOK, model.ApiResponse{
		Code:    200,
		Message: "profile saved",
		Data:    profile,
		TraceID: traceID,
	})
}

// handleServiceError 将服务层错误映射为合适的 HTTP 状态码和用户消息
// 区分"服务不可用(降级)"和"内部错误"
func (h *BusinessHandler) handleServiceError(err error, operation string) (int, string) {
	errMsg := err.Error()

	// 数据库/搜索服务不可用 → 503 Service Unavailable
	if strings.Contains(errMsg, "not available") ||
		strings.Contains(errMsg, "mongodb not available") ||
		strings.Contains(errMsg, "elasticsearch not available") {
		return http.StatusServiceUnavailable,
			operation + " temporarily unavailable, service degraded"
	}

	// 其他 → 500 Internal Server Error
	return http.StatusInternalServerError,
		operation + " failed: " + errMsg
}
