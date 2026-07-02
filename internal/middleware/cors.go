package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// CORSConfig CORS 中间件配置
type CORSConfig struct {
	Enabled          bool     // 是否启用
	AllowedOrigins   []string // 允许的来源, ["*"] 表示允许所有
	AllowedMethods   []string // 允许的 HTTP 方法
	AllowedHeaders   []string // 允许的请求头
	ExposedHeaders   []string // 暴露给浏览器的响应头
	AllowCredentials bool     // 是否允许携带 Cookie
	MaxAge           int      // 预检请求缓存时间(秒)
}

// DefaultCORSConfig 返回宽松的默认配置 (开发环境)
func DefaultCORSConfig() CORSConfig {
	return CORSConfig{
		Enabled:          false,
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Origin", "Content-Type", "Accept", "Authorization", "X-Trace-Id", "X-Request-ID"},
		ExposedHeaders:   []string{"Content-Length", "X-Trace-Id"},
		AllowCredentials: true,
		MaxAge:           86400, // 24h
	}
}

// CORS 返回一个基于配置的 Gin CORS 中间件
func CORS(cfg CORSConfig) gin.HandlerFunc {
	if !cfg.Enabled {
		return func(c *gin.Context) { c.Next() }
	}

	// 规范化: 全部小写用于比对
	allowedOrigins := make(map[string]bool)
	allowAll := false
	for _, o := range cfg.AllowedOrigins {
		o = strings.TrimSpace(o)
		if o == "*" {
			allowAll = true
			break
		}
		allowedOrigins[strings.ToLower(o)] = true
	}

	allowedMethods := strings.Join(cfg.AllowedMethods, ", ")
	allowedHeaders := strings.Join(cfg.AllowedHeaders, ", ")
	exposedHeaders := ""
	if len(cfg.ExposedHeaders) > 0 {
		exposedHeaders = strings.Join(cfg.ExposedHeaders, ", ")
	}

	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")
		if origin == "" {
			c.Next()
			return
		}

		// 检查 Origin 是否允许
		if !allowAll && !allowedOrigins[strings.ToLower(origin)] {
			// 同源请求也放行 (Origin 与 Host 相同)
			if origin == c.Request.Host || "http://"+c.Request.Host == origin || "https://"+c.Request.Host == origin {
				// 同源, 放行
			} else {
				c.AbortWithStatus(http.StatusForbidden)
				return
			}
		}

		// 设置 CORS 响应头
		if allowAll {
			c.Header("Access-Control-Allow-Origin", "*")
		} else {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Vary", "Origin")
		}
		if cfg.AllowCredentials && !allowAll {
			c.Header("Access-Control-Allow-Credentials", "true")
		}
		if exposedHeaders != "" {
			c.Header("Access-Control-Expose-Headers", exposedHeaders)
		}

		// 预检请求 (OPTIONS)
		if c.Request.Method == http.MethodOptions {
			c.Header("Access-Control-Allow-Methods", allowedMethods)
			c.Header("Access-Control-Allow-Headers", allowedHeaders)
			if cfg.MaxAge > 0 {
				c.Header("Access-Control-Max-Age", "86400")
			}
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}
