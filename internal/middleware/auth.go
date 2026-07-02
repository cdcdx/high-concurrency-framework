package middleware

import (
	"net/http"
	"strings"

	"github.com/cdcdx/high-concurrency-framework/internal/model"
	"github.com/cdcdx/high-concurrency-framework/internal/service"
	"github.com/gin-gonic/gin"
)

const (
	CtxUserID   = "auth_user_id"
	CtxUsername = "auth_username"
)

// JWTAuth JWT 认证中间件
// 从 Authorization 头解析并验证 token, 支持两种格式:
//
//	Bearer <token>    (标准格式)
//	<token>           (直接传裸 token, Swagger UI 方便)
//
// 将 userID 和 username 注入 Gin Context
func JWTAuth(authSvc *service.AuthService) gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := GetTraceID(c)

		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, model.ApiResponse{
				Code: 401, Message: "missing authorization header", TraceID: traceID,
			})
			c.Abort()
			return
		}

		// 兼容两种格式: "Bearer <token>" 或 "<token>"
		tokenString := authHeader
		if parts := strings.SplitN(authHeader, " ", 2); len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
			tokenString = parts[1]
		}

		// 解析并验证 token
		userID, username, err := authSvc.ParseToken(tokenString)
		if err != nil {
			c.JSON(http.StatusUnauthorized, model.ApiResponse{
				Code: 401, Message: "invalid or expired token", TraceID: traceID,
			})
			c.Abort()
			return
		}

		// 注入上下文
		c.Set(CtxUserID, userID)
		c.Set(CtxUsername, username)

		c.Next()
	}
}

// GetUserID 从 Gin Context 中提取已认证的用户 ID
func GetUserID(c *gin.Context) uint64 {
	if val, exists := c.Get(CtxUserID); exists {
		if id, ok := val.(uint64); ok {
			return id
		}
	}
	return 0
}

// GetUsername 从 Gin Context 中提取已认证的用户名
func GetUsername(c *gin.Context) string {
	if val, exists := c.Get(CtxUsername); exists {
		if name, ok := val.(string); ok {
			return name
		}
	}
	return ""
}
