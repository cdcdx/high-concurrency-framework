package handler

import (
	"net/http"
	"time"

	"github.com/cdcdx/high-concurrency-framework/internal/middleware"
	"github.com/cdcdx/high-concurrency-framework/internal/model"
	"github.com/cdcdx/high-concurrency-framework/internal/service"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// AnalyticsHandler 分析接口处理器 (PostgreSQL)
type AnalyticsHandler struct {
	analyticsSvc *service.AnalyticsService
	logger       *zap.SugaredLogger
}

// NewAnalyticsHandler 创建分析处理器
func NewAnalyticsHandler(analyticsSvc *service.AnalyticsService, logger *zap.SugaredLogger) *AnalyticsHandler {
	return &AnalyticsHandler{analyticsSvc: analyticsSvc, logger: logger}
}

// GetDailyStats GET /api/v1/analytics/daily?from=2026-06-28&to=2026-07-01
// @Summary      每日统计
// @Description  查询PostgreSQL中的每日统计数据，支持日期范围筛选
// @Tags         分析
// @Produce      json
// @Param        from  query     string  false  "起始日期(YYYY-MM-DD)"  default(__7DAYS_AGO__)
// @Param        to    query     string  false  "结束日期(YYYY-MM-DD)"  default(__TODAY__)
// @Success      200   {object}  model.ApiResponse
// @Failure      400   {object}  model.ApiResponse
// @Failure      500   {object}  model.ApiResponse
// @Router       /api/v1/analytics/daily [get]
func (h *AnalyticsHandler) GetDailyStats(c *gin.Context) {
	traceID := middleware.GetTraceID(c)

	fromStr := c.DefaultQuery("from", time.Now().AddDate(0, 0, -7).Format("2006-01-02"))
	toStr := c.DefaultQuery("to", time.Now().Format("2006-01-02"))

	from, err := time.Parse("2006-01-02", fromStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, model.ApiResponse{Code: 400, Message: "invalid 'from' date, use YYYY-MM-DD", TraceID: traceID})
		return
	}
	to, err := time.Parse("2006-01-02", toStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, model.ApiResponse{Code: 400, Message: "invalid 'to' date, use YYYY-MM-DD", TraceID: traceID})
		return
	}

	// 参数校验：from 不能晚于 to（在调整 to 时间之前比较日期部分）
	if from.After(to) {
		c.JSON(http.StatusBadRequest, model.ApiResponse{Code: 400, Message: "'from' date must be before or equal to 'to' date", TraceID: traceID})
		return
	}

	// 将 to 设置为当天结束（包含整天数据）
	to = to.Add(24*time.Hour - time.Second)

	stats, err := h.analyticsSvc.GetDailyStats(c.Request.Context(), from, to)
	if err != nil {
		h.logger.Errorw("get daily stats failed", "traceId", traceID, "err", err)
		c.JSON(http.StatusInternalServerError, model.ApiResponse{Code: 500, Message: err.Error(), TraceID: traceID})
		return
	}

	c.JSON(http.StatusOK, model.ApiResponse{
		Code:    200,
		Message: "success",
		Data:    stats,
		TraceID: traceID,
	})
}

// GetBehaviorSummary GET /api/v1/analytics/behaviors?type=order_create
// @Summary      行为摘要
// @Description  查询用户行为事件汇总数据
// @Tags         分析
// @Produce      json
// @Param        type  query     string  false  "事件类型(如order_create)"
// @Success      200   {object}  model.ApiResponse
// @Failure      500   {object}  model.ApiResponse
// @Router       /api/v1/analytics/behaviors [get]
func (h *AnalyticsHandler) GetBehaviorSummary(c *gin.Context) {
	traceID := middleware.GetTraceID(c)
	eventType := c.DefaultQuery("type", "")

	summary, err := h.analyticsSvc.GetBehaviorSummary(c.Request.Context(), eventType)
	if err != nil {
		h.logger.Errorw("get behavior summary failed", "traceId", traceID, "err", err)
		c.JSON(http.StatusInternalServerError, model.ApiResponse{Code: 500, Message: err.Error(), TraceID: traceID})
		return
	}

	c.JSON(http.StatusOK, model.ApiResponse{
		Code:    200,
		Message: "success",
		Data: map[string]interface{}{
			"event_type": eventType,
			"summary":    summary,
		},
		TraceID: traceID,
	})
}
