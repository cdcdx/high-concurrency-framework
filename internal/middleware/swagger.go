package middleware

import (
	"bytes"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

// SwaggerWithRuntimeDefaults 包装标准 gin-swagger handler,
// 在返回 swagger.json 时实时替换日期占位符 (__7DAYS_AGO__, __TODAY__)
func SwaggerWithRuntimeDefaults() gin.HandlerFunc {
	base := ginSwagger.WrapHandler(swaggerFiles.Handler)

	return func(c *gin.Context) {
		path := c.Request.URL.Path

		// 只对 swagger.json (即 /swagger/doc.json) 做替换
		if strings.HasSuffix(path, "doc.json") ||
			strings.HasSuffix(path, "swagger.json") {

			// 用 ResponseWriter 包装器拦截写入
			w := &swaggerResponseWriter{
				ResponseWriter: c.Writer,
				body:           &bytes.Buffer{},
			}
			c.Writer = w
			base(c)
			c.Writer = w.ResponseWriter // 还原

			if w.body.Len() > 0 {
				today := time.Now().Format("2006-01-02")
				days7Ago := time.Now().AddDate(0, 0, -7).Format("2006-01-02")
				content := w.body.String()
				content = strings.ReplaceAll(content, "__7DAYS_AGO__", days7Ago)
				content = strings.ReplaceAll(content, "__TODAY__", today)
				// 更新 Content-Length
				c.Header("Content-Length", "")
				c.String(http.StatusOK, content)
			}
			return
		}

		base(c)
	}
}

// swaggerResponseWriter 捕获响应体
type swaggerResponseWriter struct {
	gin.ResponseWriter
	body *bytes.Buffer
}

func (w *swaggerResponseWriter) Write(b []byte) (int, error) {
	return w.body.Write(b)
}

func (w *swaggerResponseWriter) WriteString(s string) (int, error) {
	return w.body.WriteString(s)
}
