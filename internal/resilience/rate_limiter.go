package resilience

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// WriteRateLimiter 写操作限流器 (令牌桶算法)
// 读请求直通缓存, 写请求超阈值→转Kafka异步
type WriteRateLimiter struct {
	mu            sync.Mutex
	capacity      int       // 桶容量
	refillRate    int       // 每秒补充令牌数
	tokens        float64   // 当前令牌数
	lastRefill    time.Time
}

// NewWriteRateLimiter 创建写限流器
func NewWriteRateLimiter(capacity, refillRate int) *WriteRateLimiter {
	return &WriteRateLimiter{
		capacity:   capacity,
		refillRate: refillRate,
		tokens:     float64(capacity),
		lastRefill: time.Now(),
	}
}

// TryAcquire 尝试获取令牌
// 返回 true = 可以同步写DB, false = 需转Kafka异步
func (rl *WriteRateLimiter) TryAcquire() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.refill()
	if rl.tokens >= 1 {
		rl.tokens--
		return true
	}
	return false
}

// WaitAcquire 阻塞等待获取令牌
func (rl *WriteRateLimiter) WaitAcquire(ctx context.Context) error {
	for {
		rl.mu.Lock()
		rl.refill()
		if rl.tokens >= 1 {
			rl.tokens--
			rl.mu.Unlock()
			return nil
		}
		rl.mu.Unlock()

		select {
		case <-ctx.Done():
			return fmt.Errorf("rate limiter: %w", ctx.Err())
		case <-time.After(10 * time.Millisecond):
			// 等待后重试
		}
	}
}

// Tokens 当前可用令牌数
func (rl *WriteRateLimiter) Tokens() int {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.refill()
	return int(rl.tokens)
}

// refill 补充令牌 (调用者需要持锁)
func (rl *WriteRateLimiter) refill() {
	now := time.Now()
	elapsed := now.Sub(rl.lastRefill).Seconds()
	rl.tokens += elapsed * float64(rl.refillRate)
	if rl.tokens > float64(rl.capacity) {
		rl.tokens = float64(rl.capacity)
	}
	rl.lastRefill = now
}
