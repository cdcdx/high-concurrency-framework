package resilience

import (
	"sync"
	"sync/atomic"
	"time"
)

// State 熔断器状态
type State int32

const (
	StateClosed    State = iota // 正常
	StateOpen                   // 熔断
	StateHalfOpen               // 半开
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateOpen:
		return "OPEN"
	case StateHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}

// CircuitBreaker 熔断管理器 (等价于 Resilience4j CircuitBreaker)
type CircuitBreaker struct {
	mu sync.RWMutex

	state State

	// 配置
	maxRequests     int           // 半开状态允许通过的探测请求数
	interval        time.Duration // 统计窗口
	timeout         time.Duration // OPEN→HALF_OPEN 等待时间
	failureRate     float64       // 失败率阈值
	slowCallMs      int64         // 慢调用阈值

	// 滑动窗口统计 (atomic 操作)
	successCount int64
	failureCount int64
	slowCount    int64
	totalCount   int64

	// 半开状态计数
	halfOpenCount int64

	// 状态切换时间
	lastStateChange time.Time
	lastFailureTime time.Time

	// 生命周期控制
	stopCh chan struct{}
	doneCh chan struct{}

	// 名称
	name string
}

// NewCircuitBreaker 创建熔断器
func NewCircuitBreaker(name string, maxRequests, intervalSec, timeoutSec int, failureThreshold float64, slowCallMs int) *CircuitBreaker {
	cb := &CircuitBreaker{
		name:            name,
		maxRequests:     maxRequests,
		interval:        time.Duration(intervalSec) * time.Second,
		timeout:         time.Duration(timeoutSec) * time.Second,
		failureRate:     failureThreshold,
		slowCallMs:      int64(slowCallMs),
		state:           StateClosed,
		lastStateChange: time.Now(),
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}, 2), // windowResetLoop + stateTransitionLoop
	}

	// 后台定期重置滑动窗口
	go cb.windowResetLoop()
	// 后台状态转换检测
	go cb.stateTransitionLoop()

	return cb
}

// Stop 优雅停止后台 goroutine
func (cb *CircuitBreaker) Stop() {
	close(cb.stopCh)
	// 等待两个后台 goroutine 结束，最多 2 秒
	for i := 0; i < 2; i++ {
		select {
		case <-cb.doneCh:
		case <-time.After(time.Second):
		}
	}
}

// Allow 检查是否允许请求通过
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		// 检查是否到了半开尝试时间
		return time.Since(cb.lastStateChange) > cb.timeout
	case StateHalfOpen:
		return cb.halfOpenCount < int64(cb.maxRequests)
	default:
		return true
	}
}

// RecordSuccess 记录成功
func (cb *CircuitBreaker) RecordSuccess(duration time.Duration) {
	atomic.AddInt64(&cb.successCount, 1)
	atomic.AddInt64(&cb.totalCount, 1)
	if duration.Milliseconds() > cb.slowCallMs {
		atomic.AddInt64(&cb.slowCount, 1)
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == StateHalfOpen {
		cb.halfOpenCount++
		if cb.halfOpenCount >= int64(cb.maxRequests) {
			cb.transitionTo(StateClosed)
		}
	}
}

// RecordFailure 记录失败
func (cb *CircuitBreaker) RecordFailure() {
	atomic.AddInt64(&cb.failureCount, 1)
	atomic.AddInt64(&cb.totalCount, 1)
	cb.mu.Lock()
	cb.lastFailureTime = time.Now()
	cb.mu.Unlock()

	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == StateHalfOpen {
		cb.transitionTo(StateOpen)
	}
}

// GetState 获取当前状态
func (cb *CircuitBreaker) GetState() string {
	return cb.state.String()
}

// Metrics 熔断器指标
func (cb *CircuitBreaker) Metrics() map[string]interface{} {
	return map[string]interface{}{
		"name":           cb.name,
		"state":          cb.state.String(),
		"success_count":  atomic.LoadInt64(&cb.successCount),
		"failure_count":  atomic.LoadInt64(&cb.failureCount),
		"slow_count":     atomic.LoadInt64(&cb.slowCount),
		"total_count":    atomic.LoadInt64(&cb.totalCount),
		"failure_rate":   cb.currentFailureRate(),
	}
}

func (cb *CircuitBreaker) currentFailureRate() float64 {
	total := atomic.LoadInt64(&cb.totalCount)
	if total == 0 {
		return 0
	}
	return float64(atomic.LoadInt64(&cb.failureCount)) / float64(total)
}

func (cb *CircuitBreaker) transitionTo(newState State) {
	cb.state = newState
	cb.lastStateChange = time.Now()
	cb.halfOpenCount = 0
}

// windowResetLoop 定期重置统计窗口
func (cb *CircuitBreaker) windowResetLoop() {
	defer func() { cb.doneCh <- struct{}{} }()
	ticker := time.NewTicker(cb.interval)
	defer ticker.Stop()
	for {
		select {
		case <-cb.stopCh:
			return
		case <-ticker.C:
			atomic.StoreInt64(&cb.successCount, 0)
			atomic.StoreInt64(&cb.failureCount, 0)
			atomic.StoreInt64(&cb.slowCount, 0)
			atomic.StoreInt64(&cb.totalCount, 0)
		}
	}
}

// stateTransitionLoop 状态转换检测
func (cb *CircuitBreaker) stateTransitionLoop() {
	defer func() { cb.doneCh <- struct{}{} }()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-cb.stopCh:
			return
		case <-ticker.C:
			cb.mu.Lock()
			if cb.state == StateClosed {
				rate := cb.currentFailureRate()
				total := atomic.LoadInt64(&cb.totalCount)
				if total > 0 && rate >= cb.failureRate {
					cb.transitionTo(StateOpen)
				}
			} else if cb.state == StateOpen {
				if time.Since(cb.lastStateChange) > cb.timeout {
					cb.transitionTo(StateHalfOpen)
				}
			}
			// HalfOpen 由 RecordSuccess/RecordFailure 驱动状态切换
			cb.mu.Unlock()
		}
	}
}
