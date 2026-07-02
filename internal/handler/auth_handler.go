package handler

import (
	"net/http"

	"github.com/cdcdx/high-concurrency-framework/internal/middleware"
	"github.com/cdcdx/high-concurrency-framework/internal/model"
	"github.com/cdcdx/high-concurrency-framework/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"
)

// AuthHandler 认证接口处理器
type AuthHandler struct {
	authSvc   *service.AuthService
	validator *validator.Validate
	logger    *zap.SugaredLogger
}

// NewAuthHandler 创建认证处理器
func NewAuthHandler(authSvc *service.AuthService, logger *zap.SugaredLogger) *AuthHandler {
	return &AuthHandler{
		authSvc:   authSvc,
		validator: validator.New(),
		logger:    logger,
	}
}

// Register POST /api/v1/auth/register
// @Summary      用户注册
// @Description  创建新用户账号并返回JWT令牌
// @Tags         认证
// @Accept       json
// @Produce      json
// @Param        body  body      model.RegisterRequest  true  "注册信息"
// @Success      200   {object}  model.ApiResponse{data=model.TokenResponse}
// @Failure      400   {object}  model.ApiResponse
// @Failure      409   {object}  model.ApiResponse
// @Router       /api/v1/auth/register [post]
func (h *AuthHandler) Register(c *gin.Context) {
	traceID := middleware.GetTraceID(c)

	var req model.RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.ApiResponse{Code: 400, Message: "invalid request body", TraceID: traceID})
		return
	}

	if err := h.validator.Struct(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.ApiResponse{Code: 400, Message: err.Error(), TraceID: traceID})
		return
	}

	resp, err := h.authSvc.Register(c.Request.Context(), &req)
	if err != nil {
		switch err {
		case service.ErrUserExists:
			c.JSON(http.StatusConflict, model.ApiResponse{Code: 409, Message: "username already exists", TraceID: traceID})
		case service.ErrEmailExists:
			c.JSON(http.StatusConflict, model.ApiResponse{Code: 409, Message: "email already exists", TraceID: traceID})
		default:
			h.logger.Errorw("register failed", "traceId", traceID, "err", err)
			c.JSON(http.StatusInternalServerError, model.ApiResponse{Code: 500, Message: "internal server error", TraceID: traceID})
		}
		return
	}

	c.JSON(http.StatusOK, model.ApiResponse{Code: 200, Message: "register success", Data: resp, TraceID: traceID})
}

// Login POST /api/v1/auth/login
// @Summary      用户登录
// @Description  验证用户名密码并返回JWT令牌
// @Tags         认证
// @Accept       json
// @Produce      json
// @Param        body  body      model.LoginRequest  true  "登录信息"
// @Success      200   {object}  model.ApiResponse{data=model.TokenResponse}
// @Failure      401   {object}  model.ApiResponse
// @Router       /api/v1/auth/login [post]
func (h *AuthHandler) Login(c *gin.Context) {
	traceID := middleware.GetTraceID(c)

	var req model.LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.ApiResponse{Code: 400, Message: "invalid request body", TraceID: traceID})
		return
	}

	if err := h.validator.Struct(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.ApiResponse{Code: 400, Message: err.Error(), TraceID: traceID})
		return
	}

	resp, err := h.authSvc.Login(c.Request.Context(), &req)
	if err != nil {
		switch err {
		case service.ErrInvalidLogin:
			c.JSON(http.StatusUnauthorized, model.ApiResponse{Code: 401, Message: "invalid username or password", TraceID: traceID})
		case service.ErrUserDisabled:
			c.JSON(http.StatusForbidden, model.ApiResponse{Code: 403, Message: "user account is disabled", TraceID: traceID})
		default:
			h.logger.Errorw("login failed", "traceId", traceID, "err", err)
			c.JSON(http.StatusInternalServerError, model.ApiResponse{Code: 500, Message: "internal server error", TraceID: traceID})
		}
		return
	}

	c.JSON(http.StatusOK, model.ApiResponse{Code: 200, Message: "login success", Data: resp, TraceID: traceID})
}

// GetMe GET /api/v1/auth/me
// @Summary      获取当前用户信息
// @Description  根据JWT令牌返回当前登录用户信息 (需Authorization头)
// @Tags         认证
// @Produce      json
// @Security     Bearer
// @Success      200  {object}  model.ApiResponse{data=model.User}
// @Failure      401  {object}  model.ApiResponse
// @Router       /api/v1/auth/me [get]
func (h *AuthHandler) GetMe(c *gin.Context) {
	traceID := middleware.GetTraceID(c)
	userID := middleware.GetUserID(c)

	user, err := h.authSvc.GetUserInfo(c.Request.Context(), userID)
	if err != nil {
		if err == service.ErrUserNotFound {
			c.JSON(http.StatusNotFound, model.ApiResponse{Code: 404, Message: "user not found", TraceID: traceID})
			return
		}
		h.logger.Errorw("get user info failed", "traceId", traceID, "userID", userID, "err", err)
		c.JSON(http.StatusInternalServerError, model.ApiResponse{Code: 500, Message: "internal server error", TraceID: traceID})
		return
	}

	c.JSON(http.StatusOK, model.ApiResponse{Code: 200, Message: "success", Data: user, TraceID: traceID})
}
