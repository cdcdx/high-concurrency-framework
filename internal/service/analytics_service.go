package service

import (
	"context"
	"encoding/json"
	"time"

	"github.com/cdcdx/high-concurrency-framework/internal/database"
	"go.uber.org/zap"
)

// AnalyticsService 分析服务 (PostgreSQL)
// 负责: 日度报表查询、行为日志写入、时序聚合
type AnalyticsService struct {
	pgRepo    *database.AnalyticsRepo
	logger    *zap.SugaredLogger
	semaphore chan struct{} // 限制并发写入 goroutine 数量
}

const maxConcurrentLogs = 100 // 最多 100 个并发异步写入

// NewAnalyticsService 创建分析服务
func NewAnalyticsService(pgRepo *database.AnalyticsRepo, logger *zap.SugaredLogger) *AnalyticsService {
	return &AnalyticsService{
		pgRepo:    pgRepo,
		logger:    logger,
		semaphore: make(chan struct{}, maxConcurrentLogs),
	}
}

// GetDailyStats 查询日度订单统计
func (s *AnalyticsService) GetDailyStats(ctx context.Context, from, to time.Time) ([]database.DailyOrderStats, error) {
	return s.pgRepo.GetDailyStats(ctx, from, to)
}

// GetBehaviorSummary 查询行为事件汇总
func (s *AnalyticsService) GetBehaviorSummary(ctx context.Context, eventType string) (map[string]int64, error) {
	now := time.Now()
	from := now.AddDate(0, 0, -7) // 近7天
	return s.pgRepo.GetBehaviorSummary(ctx, eventType, from, now)
}

// LogOrderCreated 记录订单创建行为 (异步写入PG时序表，不阻塞请求路径)
func (s *AnalyticsService) LogOrderCreated(ctx context.Context, userID uint64, orderNo string) {
	// 非阻塞尝试获取信号量（满时丢弃，不阻塞请求路径）
	select {
	case s.semaphore <- struct{}{}:
	default:
		s.logger.Debugw("analytics log dropped (write channel full)", "user_id", userID)
		return
	}

	go func() {
		defer func() { <-s.semaphore }()
		data, _ := json.Marshal(map[string]string{"order_no": orderNo})
		bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.pgRepo.LogBehavior(bgCtx, userID, "order_create", string(data)); err != nil {
			s.logger.Warnw("pg log behavior failed (non-blocking)", "user_id", userID, "err", err)
		}
	}()
}
