package middleware

import (
	"context"

	"github.com/cdcdx/high-concurrency-framework/internal/trace"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

const (
	HeaderTraceID = "X-Trace-Id"
	CtxTraceID    = "trace_id"
)

// TraceIDFromContext 从 context.Context 中提取 traceID（供 Service 层和 Kafka 消费者使用）
// Deprecated: 请使用 trace.FromContext(ctx) 代替
func TraceIDFromContext(ctx context.Context) string {
	return trace.FromContext(ctx)
}

// ContextWithTraceID 将 traceID 注入 context.Context（供 Kafka 消费者还原上下文）
// Deprecated: 请使用 trace.NewContext(ctx, traceID) 代替
func ContextWithTraceID(ctx context.Context, traceID string) context.Context {
	return trace.NewContext(ctx, traceID)
}

// TraceID 全链路追踪中间件
// 自动分配/透传 TraceID, 注入到 Gin Context、context.Context 和响应头
func TraceID() gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := c.GetHeader(HeaderTraceID)
		if traceID == "" {
			traceID = uuid.New().String()
		}

		// 写入 Gin Context (供后续 Handler 使用)
		c.Set(CtxTraceID, traceID)

		// 写入 context.Context (供 Service 层、Kafka 消息传递)
		c.Request = c.Request.WithContext(
			trace.NewContext(c.Request.Context(), traceID),
		)

		// 透传到响应头
		c.Header(HeaderTraceID, traceID)

		c.Next()
	}
}

// GetTraceID 从Gin Context提取TraceID
func GetTraceID(c *gin.Context) string {
	if val, exists := c.Get(CtxTraceID); exists {
		if tid, ok := val.(string); ok {
			return tid
		}
	}
	return ""
}

// ZapLogger 将TraceID注入到zap日志的中间件
func ZapLogger(logger *zap.SugaredLogger) gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := GetTraceID(c)
		// 设置request-scoped logger
		requestLogger := logger.With("traceId", traceID)
		c.Set("logger", requestLogger)
		c.Next()
	}
}

// GetLogger 从Context获取request-scoped logger
func GetLogger(c *gin.Context) *zap.SugaredLogger {
	if val, exists := c.Get("logger"); exists {
		if logger, ok := val.(*zap.SugaredLogger); ok {
			return logger
		}
	}
	return nil
}
