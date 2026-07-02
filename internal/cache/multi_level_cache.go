package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// NullMarker 空标记 (防缓存穿透)
const NullMarker = "__NULL__"

// DataLoader DB回源函数: 根据Key加载原始数据
type DataLoader func(ctx context.Context, key string) (interface{}, error)

// MultiLevelCache 多级缓存 (L1 Caffeine/Ristretto → L2 Redis → L3 DB)
// 构建完整的缓存防护矩阵: 穿透/击穿/雪崩全防护
type MultiLevelCache struct {
	l1          *ristretto.Cache       // L1 本地缓存 (Ristretto, 等价于 Caffeine)
	l2          *redis.Client          // L2 分布式缓存 (Redis)
	logger      *zap.SugaredLogger
	loader      DataLoader              // L3 DB回源
	hotKeys     *HotKeyDetector         // 热点Key检测

	// 缓存 TTL 配置
	l1BaseTTL time.Duration // L1 基础 TTL (可通过 SetL1TTL 覆盖)
	l2BaseTTL time.Duration // L2 基础 TTL (可通过 SetL2TTL 覆盖)
	jitterPct float64       // TTL随机偏移比例 (防雪崩)
	nullTTL   time.Duration // 空标记过期时间 (防穿透)

	// 分布式锁防击穿
	lockKeyPrefix string
	lockTTL       time.Duration // 分布式锁过期时间
	lockRetryWait time.Duration // 锁重试等待时间

	// 统计
	l1Hits  uint64
	l2Hits  uint64
	misses  uint64
	l1Total uint64

	// L1失效广播
	evictionCh chan struct{}
}

// 默认缓存参数
const (
	DefaultL1BaseTTL  = 30 * time.Second
	DefaultL2BaseTTL  = 30 * time.Minute
	DefaultJitterPct  = 0.2
	DefaultNullTTL    = 60 * time.Second
	DefaultLockTTL    = 10 * time.Second
	DefaultLockRetry  = 50 * time.Millisecond
)

// NewMultiLevelCache 创建多级缓存
func NewMultiLevelCache(
	l1 *ristretto.Cache,
	l2 *redis.Client,
	loader DataLoader,
	hotKeys *HotKeyDetector,
	logger *zap.SugaredLogger,
) *MultiLevelCache {
	mc := &MultiLevelCache{
		l1:            l1,
		l2:            l2,
		logger:        logger,
		loader:        loader,
		hotKeys:       hotKeys,
		l1BaseTTL:     DefaultL1BaseTTL,
		l2BaseTTL:     DefaultL2BaseTTL,
		jitterPct:     DefaultJitterPct,
		nullTTL:       DefaultNullTTL,
		lockKeyPrefix: "cache:lock:",
		lockTTL:       DefaultLockTTL,
		lockRetryWait: DefaultLockRetry,
		evictionCh:    make(chan struct{}, 1024),
	}
	return mc
}

// Set 直接写入L1+L2缓存 (用于写后预热), Wait确保可见
func (mc *MultiLevelCache) Set(ctx context.Context, key string, value interface{}) {
	mc.l1.SetWithTTL(key, value, 1, mc.l1TTL())
	mc.l1.Wait() // 确保异步写入完成
	if mc.l2 != nil {
		jsonBytes, _ := json.Marshal(value)
		mc.l2.Set(ctx, key, string(jsonBytes), mc.l2TTL())
	}
}

// SetJitterPct 设置防雪崩偏移比例
func (mc *MultiLevelCache) SetJitterPct(pct float64) {
	mc.jitterPct = pct
}

// SetNullTTL 设置空标记过期时间
func (mc *MultiLevelCache) SetNullTTL(ttl time.Duration) {
	mc.nullTTL = ttl
}

// SetL1BaseTTL 设置 L1 基础 TTL (来自配置)
func (mc *MultiLevelCache) SetL1BaseTTL(base time.Duration) {
	mc.l1BaseTTL = base
}

// SetL2BaseTTL 设置 L2 基础 TTL (来自配置)
func (mc *MultiLevelCache) SetL2BaseTTL(base time.Duration) {
	mc.l2BaseTTL = base
}

// SetNull 写入空标记 (防缓存穿透)
func (mc *MultiLevelCache) SetNull(ctx context.Context, key string) {
	mc.l1.SetWithTTL(key, NullMarker, 1, mc.nullTTL)
	mc.l1.Wait()
	if mc.l2 != nil {
		mc.l2.Set(ctx, key, NullMarker, mc.nullTTL)
	}
}

// Get 从缓存中读取 (L1 → L2 → L3)
// 返回: (value, found, error)
func (mc *MultiLevelCache) Get(ctx context.Context, key string) (interface{}, bool, error) {
	atomic.AddUint64(&mc.l1Total, 1)

	// -- L1: 本地缓存 (<1ms) --
	if val, found := mc.l1.Get(key); found {
		atomic.AddUint64(&mc.l1Hits, 1)
		if s, ok := val.(string); ok && s == NullMarker {
			return nil, true, nil // 空标记命中 → found=true, val=nil (已知不存在)
		}
		return val, true, nil
	}

	// -- L2: 分布式缓存 (1-5ms) --
	if mc.l2 != nil {
		val, err := mc.l2.Get(ctx, key).Result()
		if err == nil {
			atomic.AddUint64(&mc.l2Hits, 1)
			if val == NullMarker {
				// 回填L1空标记，found=true 表示已知不存在
				mc.l1.SetWithTTL(key, NullMarker, 1, mc.nullTTL)
				mc.l1.Wait()
				return nil, true, nil
			}
			// 回填L1
			parsed := mc.parseValue(val)
			mc.l1.SetWithTTL(key, parsed, 1, mc.l1TTL())
			mc.l1.Wait()
			return parsed, true, nil
		}
		if err != redis.Nil {
			mc.logger.Warnw("L2 cache error", "key", key, "err", err)
		}
	}

	// -- L3: DB回源 (10-200ms) --
	// 缓存穿透防护: 记录access并检查热点
	isHot := false
	if mc.hotKeys != nil {
		isHot = mc.hotKeys.RecordAccess(key)
	}

	// 缓存击穿防护: 对热点Key加分布式锁
	if isHot {
		atomic.AddUint64(&mc.misses, 1)
		return mc.loadWithLock(ctx, key)
	}

	// 普通回源
	atomic.AddUint64(&mc.misses, 1)
	return mc.loadFromDB(ctx, key)
}

// loadFromDB 从DB加载数据并回填L2/L1
func (mc *MultiLevelCache) loadFromDB(ctx context.Context, key string) (interface{}, bool, error) {
	if mc.loader == nil {
		return nil, false, fmt.Errorf("no data loader configured")
	}

	data, err := mc.loader(ctx, key)
	if err != nil {
		return nil, false, err
	}

	// 缓存穿透防护: DB返回nil时缓存空标记，found=true 表示已知不存在
	if data == nil {
		if mc.l2 != nil {
			mc.l2.Set(ctx, key, NullMarker, mc.nullTTL)
		}
		mc.l1.SetWithTTL(key, NullMarker, 1, mc.nullTTL)
		return nil, true, nil
	}

	// 回填L2 + L1
	if mc.l2 != nil {
		jsonBytes, _ := json.Marshal(data)
		mc.l2.Set(ctx, key, string(jsonBytes), mc.l2TTL())
	}
	mc.l1.SetWithTTL(key, data, 1, mc.l1TTL())
	mc.l1.Wait() // 确保异步写入完成, 下次Get立即可见

	return data, true, nil
}

// loadWithLock 热点Key加分布式锁后单线程回源 (防击穿)
func (mc *MultiLevelCache) loadWithLock(ctx context.Context, key string) (interface{}, bool, error) {
	if mc.l2 == nil {
		// 无Redis时跳过锁, 直接回源
		return mc.loadFromDB(ctx, key)
	}

	lockKey := mc.lockKeyPrefix + key

	// 尝试获取分布式锁
	ok, err := mc.l2.SetNX(ctx, lockKey, "1", mc.lockTTL).Result()
	if err != nil {
		return nil, false, err
	}

	if ok {
		// 获得锁: 单线程加载DB
		defer mc.l2.Del(ctx, lockKey)
		return mc.loadFromDB(ctx, key)
	}

	// 未获得锁: 等待后重试读缓存
	time.Sleep(mc.lockRetryWait)
	// 重试L2
	val, err := mc.l2.Get(ctx, key).Result()
	if err == nil {
		if val == NullMarker {
			return nil, true, nil // 空标记命中，已知不存在
		}
		return mc.parseValue(val), true, nil
	}
	return nil, false, nil
}

// Invalidate 失效指定Key的缓存 (写后删除)
func (mc *MultiLevelCache) Invalidate(ctx context.Context, key string) {
	mc.l1.Del(key)
	if mc.l2 != nil {
		mc.l2.Del(ctx, key)
	}
}

// InvalidateWithDelay 延时双删 (防止并发读写脏数据)
// 第一次删除后, 异步延迟执行第二次删除 (不阻塞调用方)
func (mc *MultiLevelCache) InvalidateWithDelay(ctx context.Context, key string, delayMs int) {
	// 第一次删除
	if mc.l2 != nil {
		mc.l2.Del(ctx, key)
	}
	mc.l1.Del(key)
	// 异步延迟第二次删除
	go func() {
		time.Sleep(time.Duration(delayMs) * time.Millisecond)
		if mc.l2 != nil {
			mc.l2.Del(context.Background(), key)
		}
		mc.l1.Del(key)
	}()
}

// BroadcastEviction 广播缓存失效通知 (L2 → 所有Pod L1)
func (mc *MultiLevelCache) BroadcastEviction(ctx context.Context, key string) {
	mc.l1.Del(key)
	if mc.l2 != nil {
		mc.l2.Publish(ctx, "cache:eviction", key)
	}
}

// Close 清理后台资源
func (mc *MultiLevelCache) Close() {
	close(mc.evictionCh)
}

// ReceiveEviction 接收远程失效通知 (非阻塞写入)
func (mc *MultiLevelCache) ReceiveEviction(key string) {
	mc.l1.Del(key)
	// 非阻塞通知（channel 关闭后 select 会立刻返回）
	select {
	case mc.evictionCh <- struct{}{}:
	default:
	}
}

// L1HitRate L1命中率
func (mc *MultiLevelCache) L1HitRate() float64 {
	total := atomic.LoadUint64(&mc.l1Total)
	if total == 0 {
		return 0
	}
	return float64(atomic.LoadUint64(&mc.l1Hits)) / float64(total)
}

// Stats 缓存统计
func (mc *MultiLevelCache) Stats() map[string]interface{} {
	total := atomic.LoadUint64(&mc.l1Total)
	misses := atomic.LoadUint64(&mc.misses)
	comboHits := atomic.LoadUint64(&mc.l1Hits) + atomic.LoadUint64(&mc.l2Hits)
	comboRate := 0.0
	if total > 0 {
		comboRate = float64(comboHits) / float64(total)
	}
	return map[string]interface{}{
		"l1_hits":    atomic.LoadUint64(&mc.l1Hits),
		"l2_hits":    atomic.LoadUint64(&mc.l2Hits),
		"misses":     misses,
		"total":      total,
		"l1_hit_rate": mc.L1HitRate(),
		"combo_hit_rate": comboRate,
	}
}

// l1TTL L1过期时间 (带防雪崩随机偏移，使用配置的基础 TTL)
func (mc *MultiLevelCache) l1TTL() time.Duration {
	return jitterDuration(mc.l1BaseTTL, mc.jitterPct)
}

// l2TTL L2过期时间 (带防雪崩随机偏移，使用配置的基础 TTL)
func (mc *MultiLevelCache) l2TTL() time.Duration {
	return jitterDuration(mc.l2BaseTTL, mc.jitterPct)
}

func (mc *MultiLevelCache) parseValue(val string) interface{} {
	// 尝试反序列化JSON
	var result interface{}
	if err := json.Unmarshal([]byte(val), &result); err != nil {
		return val // 非JSON直接返回字符串
	}
	return result
}

// JitterDuration 在duration ± pct 范围内随机偏移 (公开, 供测试/配置使用)
func JitterDuration(d time.Duration, pct float64) time.Duration {
	if pct <= 0 {
		return d
	}
	delta := time.Duration(float64(d) * pct)
	offset := time.Duration(int64(delta) * int64(time.Now().UnixNano()%200-100) / 100)
	return d + offset
}

// jitterDuration 内部别名 (保持向后兼容)
func jitterDuration(d time.Duration, pct float64) time.Duration {
	return JitterDuration(d, pct)
}
