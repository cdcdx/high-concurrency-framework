package middleware

import (
	"net/http"
	"runtime/debug"

	"github.com/cdcdx/high-concurrency-framework/internal/model"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// Recovery 全局异常恢复中间件 (统一错误响应)
func Recovery(logger *zap.SugaredLogger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				// 捕获panic堆栈
				stack := debug.Stack()
				logger.Errorw("panic recovered",
					"err", err,
					"path", c.Request.URL.Path,
					"method", c.Request.Method,
					"stack", string(stack),
				)

				c.AbortWithStatusJSON(http.StatusInternalServerError, model.ApiResponse{
					Code:    500,
					Message: "internal server error",
					TraceID: GetTraceID(c),
				})
			}
		}()
		c.Next()
	}
}

// ErrorHandler 统一错误处理
func ErrorHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		// 处理gin内部的错误
		if len(c.Errors) > 0 {
			err := c.Errors.Last().Err
			c.JSON(http.StatusBadRequest, model.ApiResponse{
				Code:    400,
				Message: err.Error(),
				TraceID: GetTraceID(c),
			})
		}
	}
}
