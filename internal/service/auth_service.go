package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cdcdx/high-concurrency-framework/internal/database"
	"github.com/cdcdx/high-concurrency-framework/internal/model"
	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
)

// 通用错误
var (
	ErrUserExists       = errors.New("username already exists")
	ErrEmailExists      = errors.New("email already exists")
	ErrInvalidLogin     = errors.New("invalid username or password")
	ErrUserDisabled     = errors.New("user account is disabled")
	ErrUserNotFound     = errors.New("user not found")
	ErrInvalidToken     = errors.New("invalid or expired token")
)

// AuthService 用户认证服务
type AuthService struct {
	repo       *database.UserAuthRepo
	jwtSecret  []byte
	jwtExpire  time.Duration
	logger     *zap.SugaredLogger
}

// NewAuthService 创建认证服务
func NewAuthService(repo *database.UserAuthRepo, jwtSecret string, jwtExpireSeconds int, logger *zap.SugaredLogger) *AuthService {
	return &AuthService{
		repo:      repo,
		jwtSecret: []byte(jwtSecret),
		jwtExpire: time.Duration(jwtExpireSeconds) * time.Second,
		logger:    logger,
	}
}

// Register 用户注册
func (s *AuthService) Register(ctx context.Context, req *model.RegisterRequest) (*model.TokenResponse, error) {
	// 1. 检查用户名是否已存在
	existing, err := s.repo.GetByUsername(ctx, req.Username)
	if err != nil {
		return nil, fmt.Errorf("check username: %w", err)
	}
	if existing != nil {
		return nil, ErrUserExists
	}

	// 2. 如果提供了邮箱，检查邮箱唯一性
	if req.Email != "" {
		emailUser, err := s.repo.GetByEmail(ctx, req.Email)
		if err != nil {
			return nil, fmt.Errorf("check email: %w", err)
		}
		if emailUser != nil {
			return nil, ErrEmailExists
		}
	}

	// 3. bcrypt 密码哈希
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}

	// 4. 写入数据库
	user := &model.User{
		Username:     req.Username,
		PasswordHash: string(hashedPassword),
		Email:        req.Email,
		Phone:        req.Phone,
		Status:       1, // 默认正常
	}
	userID, err := s.repo.InsertUser(ctx, user)
	if err != nil {
		// 处理数据库唯一约束冲突（高并发下竞态兜底）
		if strings.Contains(err.Error(), "Duplicate") {
			return nil, ErrUserExists
		}
		return nil, fmt.Errorf("insert user: %w", err)
	}

	// 5. 生成 JWT
	token, expiresIn, err := s.generateToken(userID, req.Username)
	if err != nil {
		return nil, err
	}

	s.logger.Infow("user registered", "user_id", userID, "username", req.Username)
	return &model.TokenResponse{
		AccessToken: token,
		TokenType:   "Bearer",
		ExpiresIn:   expiresIn,
		UserID:      userID,
		Username:    req.Username,
	}, nil
}

// Login 用户登录
func (s *AuthService) Login(ctx context.Context, req *model.LoginRequest) (*model.TokenResponse, error) {
	// 1. 查询用户
	user, err := s.repo.GetByUsername(ctx, req.Username)
	if err != nil {
		return nil, fmt.Errorf("query user: %w", err)
	}
	if user == nil {
		return nil, ErrInvalidLogin
	}

	// 2. 检查账号状态
	if user.Status != 1 {
		return nil, ErrUserDisabled
	}

	// 3. 验证密码
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		return nil, ErrInvalidLogin
	}

	// 4. 生成 JWT
	token, expiresIn, err := s.generateToken(user.ID, user.Username)
	if err != nil {
		return nil, err
	}

	// 5. 异步更新最后登录时间
	go func() {
		if err := s.repo.UpdateLastLoginAt(context.Background(), user.ID); err != nil {
			s.logger.Warnw("update last login at failed", "user_id", user.ID, "err", err)
		}
	}()

	s.logger.Infow("user logged in", "user_id", user.ID, "username", req.Username)
	return &model.TokenResponse{
		AccessToken: token,
		TokenType:   "Bearer",
		ExpiresIn:   expiresIn,
		UserID:      user.ID,
		Username:    user.Username,
	}, nil
}

// GetUserInfo 根据 userID 查询用户基本信息 (不含密码)
func (s *AuthService) GetUserInfo(ctx context.Context, userID uint64) (*model.User, error) {
	user, err := s.repo.GetByID(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("query user: %w", err)
	}
	if user == nil {
		return nil, ErrUserNotFound
	}
	// 安全: 确保不返回密码哈希
	user.PasswordHash = ""
	return user, nil
}

// ParseToken 解析并验证 JWT token, 返回 userID 和 username
func (s *AuthService) ParseToken(tokenString string) (uint64, string, error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return s.jwtSecret, nil
	})
	if err != nil {
		return 0, "", ErrInvalidToken
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		return 0, "", ErrInvalidToken
	}

	// 提取 userID
	userIDFloat, ok := claims["user_id"].(float64)
	if !ok {
		return 0, "", ErrInvalidToken
	}
	userID := uint64(userIDFloat)

	username, _ := claims["username"].(string)

	return userID, username, nil
}

// generateToken 生成 JWT token
func (s *AuthService) generateToken(userID uint64, username string) (string, int64, error) {
	now := time.Now()
	expiresAt := now.Add(s.jwtExpire)

	claims := jwt.MapClaims{
		"user_id":  userID,
		"username": username,
		"iat":      now.Unix(),
		"exp":      expiresAt.Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(s.jwtSecret)
	if err != nil {
		return "", 0, fmt.Errorf("sign token: %w", err)
	}

	return tokenString, s.jwtExpire.Milliseconds() / 1000, nil
}
