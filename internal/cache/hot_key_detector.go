package cache

import (
	"sync"
	"time"
)

// HotKeyDetector 热点Key检测器
// 使用滑动窗口计数，当某个Key在窗口内的访问次数超过阈值时标记为热点
type HotKeyDetector struct {
	mu          sync.RWMutex
	keyCounters map[string]*slotRing
	threshold   int64
	windowSize  time.Duration
	slots       int
	hotKeys     map[string]bool
	stopCh      chan struct{}
}

// slotRing 环形槽位计数器 (免锁时间轮)
type slotRing struct {
	counters []int64
	slotIdx  int
	lastTick time.Time
	mu       sync.Mutex
}

func newSlotRing(slots int) *slotRing {
	return &slotRing{
		counters: make([]int64, slots),
		lastTick: time.Now(),
	}
}

// advance 推进时间槽, 清理过期计数
func (sr *slotRing) advance(slotInterval time.Duration) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(sr.lastTick)
	steps := int(elapsed / slotInterval)
	if steps <= 0 {
		return
	}
	if steps > len(sr.counters) {
		steps = len(sr.counters)
	}
	for i := 0; i < steps; i++ {
		sr.slotIdx = (sr.slotIdx + 1) % len(sr.counters)
		sr.counters[sr.slotIdx] = 0
	}
	sr.lastTick = now
}

// inc 在当前槽位 +1
func (sr *slotRing) inc() {
	sr.mu.Lock()
	sr.counters[sr.slotIdx]++
	sr.mu.Unlock()
}

// sum 求和所有槽位
func (sr *slotRing) sum() int64 {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	var total int64
	for _, v := range sr.counters {
		total += v
	}
	return total
}

// NewHotKeyDetector 创建热点Key检测器
func NewHotKeyDetector(windowSeconds, slots, threshold int) *HotKeyDetector {
	hkd := &HotKeyDetector{
		keyCounters: make(map[string]*slotRing),
		threshold:   int64(threshold),
		windowSize:  time.Duration(windowSeconds) * time.Second,
		slots:       slots,
		hotKeys:     make(map[string]bool),
		stopCh:      make(chan struct{}),
	}
	// 后台定期清理过期Key
	go hkd.cleanupLoop()
	return hkd
}

// Stop 停止后台清理 goroutine
func (h *HotKeyDetector) Stop() {
	close(h.stopCh)
}

// RecordAccess 记录一次Key访问, 返回是否热点
func (h *HotKeyDetector) RecordAccess(key string) bool {
	h.mu.RLock()
	sr, exists := h.keyCounters[key]
	h.mu.RUnlock()

	slotInterval := h.windowSize / time.Duration(h.slots)

	if !exists {
		h.mu.Lock()
		// double-check
		sr, exists = h.keyCounters[key]
		if !exists {
			sr = newSlotRing(h.slots)
			h.keyCounters[key] = sr
		}
		h.mu.Unlock()
	}

	sr.advance(slotInterval)
	sr.inc()

	total := sr.sum()
	isHot := total > h.threshold

	if isHot {
		h.mu.Lock()
		h.hotKeys[key] = true
		h.mu.Unlock()
	}

	return isHot
}

// IsHot 检查Key是否为热点
func (h *HotKeyDetector) IsHot(key string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.hotKeys[key]
}

// GetHotKeys 获取所有热点Key
func (h *HotKeyDetector) GetHotKeys() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	keys := make([]string, 0, len(h.hotKeys))
	for k := range h.hotKeys {
		keys = append(keys, k)
	}
	return keys
}

// cleanupLoop 后台清理过期/低频Key
func (h *HotKeyDetector) cleanupLoop() {
	ticker := time.NewTicker(h.windowSize * 2)
	defer ticker.Stop()
	for {
		select {
		case <-h.stopCh:
			return
		case <-ticker.C:
			h.cleanup()
		}
	}
}

func (h *HotKeyDetector) cleanup() {
	h.mu.Lock()
	defer h.mu.Unlock()
	// 清除低计数Key
	for key, sr := range h.keyCounters {
		if sr.sum() <= h.threshold/10 {
			delete(h.keyCounters, key)
			delete(h.hotKeys, key)
		}
	}
}
