package middleware

import (
	"net/http"

	"github.com/cdcdx/high-concurrency-framework/internal/resilience"
	"github.com/gin-gonic/gin"
)

// RateLimit 全局限流中间件
func RateLimit(rl *resilience.WriteRateLimiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 仅对写请求限流
		if isWriteMethod(c.Request.Method) {
			if !rl.TryAcquire() {
				c.JSON(http.StatusTooManyRequests, gin.H{
					"code":     429,
					"message":  "too many requests, use async channel",
					"trace_id": GetTraceID(c),
				})
				c.Abort()
				return
			}
		}
		c.Next()
	}
}

func isWriteMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}
