package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

// DistributedLock Redis分布式锁 (等价于 Redisson Lock)
type DistributedLock struct {
	client *redis.Client
}

// NewDistributedLock 创建分布式锁
func NewDistributedLock(client *redis.Client) *DistributedLock {
	return &DistributedLock{client: client}
}

// Lock 获取锁 (SETNX + 过期时间)
// 参数:
//   - key: 锁的Key
//   - ttl: 锁过期时间 (防止死锁)
//   - waitTimeout: 等待超时 (0=不等待)
//
// 返回: 锁令牌 (用于解锁) + 是否成功
func (d *DistributedLock) Lock(ctx context.Context, key string, ttl, waitTimeout time.Duration) (string, error) {
	token := fmt.Sprintf("%d-%s", time.Now().UnixNano(), randomHex(8))

	deadline := time.Now().Add(waitTimeout)
	for {
		ok, err := d.client.SetNX(ctx, key, token, ttl).Result()
		if err != nil {
			return "", fmt.Errorf("redis setnx: %w", err)
		}
		if ok {
			return token, nil
		}
		if waitTimeout == 0 || time.Now().After(deadline) {
			return "", fmt.Errorf("acquire lock timeout: %s", key)
		}
		time.Sleep(50 * time.Millisecond) // 等待重试
	}
}

// Unlock 释放锁 (Lua脚本保证原子性)
func (d *DistributedLock) Unlock(ctx context.Context, key, token string) error {
	// Lua脚本: 仅当value匹配时才删除
	script := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`
	_, err := d.client.Eval(ctx, script, []string{key}, token).Result()
	if err != nil {
		return fmt.Errorf("unlock: %w", err)
	}
	return nil
}

// WithLock 在锁保护下执行函数
func (d *DistributedLock) WithLock(ctx context.Context, key string, ttl, waitTimeout time.Duration, fn func() error) error {
	token, err := d.Lock(ctx, key, ttl, waitTimeout)
	if err != nil {
		return err
	}
	defer d.Unlock(ctx, key, token)
	return fn()
}

func randomHex(n int) string {
	b := make([]byte, n/2+1)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)[:n]
}
