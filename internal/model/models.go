package model

import (
	"encoding/json"
	"time"
)

// Order 订单模型 (MySQL 持久化 + ES 全文搜索)
type Order struct {
	ID        uint64    `json:"id" gorm:"primaryKey" bson:"id"`
	UserID    uint64    `json:"user_id" validate:"required,min=1" gorm:"column:user_id;not null" bson:"user_id"`
	OrderNo   string    `json:"order_no" gorm:"column:order_no;size:64;not null" bson:"order_no"`
	Amount    float64   `json:"amount" validate:"gt=0" gorm:"column:amount;type:decimal(12,2)" bson:"amount"`
	Status    string    `json:"status" gorm:"column:status;size:20;default:'pending'" bson:"status"`
	CreatedAt time.Time `json:"created_at" bson:"created_at"`
	UpdatedAt time.Time `json:"updated_at" bson:"updated_at"`
}

// UserProfile 用户资料模型 (MongoDB 持久化 + 缓存查询)
type UserProfile struct {
	ID        uint64    `json:"id" gorm:"primaryKey" bson:"id,omitempty"`
	UserID    uint64    `json:"user_id" validate:"required" gorm:"column:user_id;uniqueIndex;not null" bson:"user_id"`
	Nickname  string    `json:"nickname" gorm:"column:nickname;size:100" bson:"nickname"`
	Avatar    string    `json:"avatar" gorm:"column:avatar;size:500" bson:"avatar"`
	Email     string    `json:"email" gorm:"column:email;size:200" bson:"email"`
	Phone     string    `json:"phone" gorm:"column:phone;size:20" bson:"phone"`
	Bio       string    `json:"bio,omitempty" bson:"bio,omitempty"`
	Tags      []string  `json:"tags,omitempty" bson:"tags,omitempty"`
	CreatedAt time.Time `json:"created_at" bson:"created_at"`
	UpdatedAt time.Time `json:"updated_at" bson:"updated_at"`
}

// MarshalBinary 实现 encoding.BinaryMarshaler 供缓存使用
func (u *UserProfile) MarshalBinary() ([]byte, error) {
	return json.Marshal(u)
}

// UnmarshalBinary 实现 encoding.BinaryUnmarshaler
func (u *UserProfile) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, u)
}

// ApiResponse 统一API响应
type ApiResponse struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
	TraceID string      `json:"trace_id,omitempty"`
}

// BusinessEvent 业务事件 (Kafka消息体)
type BusinessEvent struct {
	EventID   string      `json:"event_id"`
	EventType string      `json:"event_type"`
	Timestamp int64       `json:"timestamp"`
	TraceID   string      `json:"trace_id"`
	Payload   interface{} `json:"payload"`
}

// ==========================================
// 用户认证模型
// ==========================================

// User 用户认证模型 (MySQL 持久化)
type User struct {
	ID           uint64    `json:"id"`
	Username     string    `json:"username"`
	PasswordHash string    `json:"-"` // 永远不在 JSON 中暴露
	Email        string    `json:"email,omitempty"`
	Phone        string    `json:"phone,omitempty"`
	Status       int       `json:"status"` // 1=正常 0=禁用
	LastLoginAt  *string   `json:"last_login_at,omitempty"`
	CreatedAt    string    `json:"created_at"`
	UpdatedAt    string    `json:"updated_at"`
}

// RegisterRequest 注册请求
type RegisterRequest struct {
	Username string `json:"username" validate:"required,min=3,max=64"`
	Password string `json:"password" validate:"required,min=6,max=128"`
	Email    string `json:"email"    validate:"omitempty,email,max=128"`
	Phone    string `json:"phone"    validate:"omitempty,max=20"`
}

// LoginRequest 登录请求
type LoginRequest struct {
	Username string `json:"username" validate:"required"`
	Password string `json:"password" validate:"required"`
}

// TokenResponse JWT 令牌响应
type TokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"` // "Bearer"
	ExpiresIn   int64  `json:"expires_in"` // 秒
	UserID      uint64 `json:"user_id"`
	Username    string `json:"username"`
}

