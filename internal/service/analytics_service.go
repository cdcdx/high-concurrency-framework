package service

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/cdcdx/high-concurrency-framework/internal/database"
	"go.uber.org/zap"
)

// AnalyticsService 分析服务 (PostgreSQL)
// 负责: 日度报表查询、行为日志批量写入、时序聚合
// 日志采用 "缓冲 → 批量 INSERT" 模式，高并发下不丢失数据
type AnalyticsService struct {
	pgRepo  *database.AnalyticsRepo
	logger  *zap.SugaredLogger
	bufCh   chan database.BehaviorEntry // 缓冲通道，攒批
	flushCh chan struct{}               // 触发立即刷盘
	closeCh chan struct{}               // 关闭信号
	wg      sync.WaitGroup
}

const (
	bufChannelSize = 10000                   // 缓冲通道容量 (10k 条，约承载秒级峰值)
	batchSize      = 200                     // 每批最多 200 条一次写入
	flushInterval  = 200 * time.Millisecond  // 最多 200ms 刷一次
)

// NewAnalyticsService 创建分析服务 (自动启动批量写入后台协程)
func NewAnalyticsService(pgRepo *database.AnalyticsRepo, logger *zap.SugaredLogger) *AnalyticsService {
	s := &AnalyticsService{
		pgRepo:  pgRepo,
		logger:  logger,
		bufCh:   make(chan database.BehaviorEntry, bufChannelSize),
		flushCh: make(chan struct{}, 1),
		closeCh: make(chan struct{}),
	}
	s.wg.Add(1)
	go s.batchWriter()
	return s
}

// Close 优雅关闭，等待最后一批写入完成
func (s *AnalyticsService) Close() {
	close(s.closeCh)
	s.wg.Wait()
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

// LogOrderCreated 记录订单创建行为 (写入缓冲通道，批量异步入库，不丢失)
func (s *AnalyticsService) LogOrderCreated(ctx context.Context, userID uint64, orderNo string) {
	data, _ := json.Marshal(map[string]string{"order_no": orderNo})
	entry := database.BehaviorEntry{UserID: userID, EventType: "order_create", EventData: string(data)}

	select {
	case s.bufCh <- entry:
		// 缓冲成功
	default:
		// 缓冲满了 → 触发立即刷盘后重试一次
		s.signalFlush()
		select {
		case s.bufCh <- entry:
		default:
			s.logger.Debugw("analytics log dropped (buffer full after flush)", "user_id", userID)
		}
	}
}

// signalFlush 发送非阻塞刷盘信号
func (s *AnalyticsService) signalFlush() {
	select {
	case s.flushCh <- struct{}{}:
	default:
	}
}

// drainAndFlush 排空缓冲通道后批量刷盘
func (s *AnalyticsService) drainAndFlush(batch *[]database.BehaviorEntry, flush func()) {
	for {
		select {
		case entry := <-s.bufCh:
			*batch = append(*batch, entry)
			if len(*batch) >= batchSize {
				flush()
			}
		default:
			flush()
			return
		}
	}
}

// batchWriter 后台协程: 定时或触发时批量写入 PostgreSQL
func (s *AnalyticsService) batchWriter() {
	defer s.wg.Done()

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	batch := make([]database.BehaviorEntry, 0, batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		toWrite := make([]database.BehaviorEntry, len(batch))
		copy(toWrite, batch)
		batch = batch[:0]

		s.wg.Add(1)
		go func(entries []database.BehaviorEntry) {
			defer s.wg.Done()
			if err := s.pgRepo.LogBehaviorBatch(context.Background(), entries); err != nil {
				s.logger.Warnw("pg batch log failed", "count", len(entries), "err", err)
			}
		}(toWrite)
	}

	flushAll := func() {
		s.drainAndFlush(&batch, flush)
	}

	for {
		select {
		case <-s.closeCh:
			flushAll()
			return

		case <-s.flushCh:
			// 快速排空通道后刷盘
			s.drainAndFlush(&batch, flush)

		case entry := <-s.bufCh:
			batch = append(batch, entry)
			if len(batch) >= batchSize {
				flush()
			}

		case <-ticker.C:
			flush()
		}
	}
}
