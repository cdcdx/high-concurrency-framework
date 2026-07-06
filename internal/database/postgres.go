package database

import (
	"context"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

// DailyOrderStats 订单日度统计
type DailyOrderStats struct {
	StatDate    time.Time `json:"stat_date"`
	TotalOrders int64     `json:"total_orders"`
	TotalAmount float64   `json:"total_amount"`
	AvgAmount   float64   `json:"avg_amount"`
	PaidOrders  int64     `json:"paid_orders"`
	Cancelled   int64     `json:"cancelled"`
}

// BehaviorLog 用户行为日志
type BehaviorLog struct {
	ID        int64     `json:"id"`
	UserID    int64     `json:"user_id"`
	EventType string    `json:"event_type"`
	EventData string    `json:"event_data"`
	CreatedAt time.Time `json:"created_at"`
}

// AnalyticsRepo PostgreSQL 分析数据仓库 (读写分离: 查询→Replica, 写入→Master)
type AnalyticsRepo struct {
	db *RWDB
}

// NewAnalyticsRepo 创建分析仓库
func NewAnalyticsRepo(db *RWDB) *AnalyticsRepo {
	return &AnalyticsRepo{db: db}
}

// GetDailyStats 获取日度统计（按日期范围）→ 读 Replica
func (r *AnalyticsRepo) GetDailyStats(ctx context.Context, from, to time.Time) ([]DailyOrderStats, error) {
	if r.db == nil || r.db.IsNil() {
		return nil, fmt.Errorf("postgres not available")
	}

	query := `SELECT stat_date, total_orders, total_amount, avg_amount, paid_orders, cancelled
	           FROM orders_daily_stats
	           WHERE stat_date >= $1 AND stat_date <= $2
	           ORDER BY stat_date DESC`

	rows, err := r.db.QueryContext(ctx, query, from.Format("2006-01-02"), to.Format("2006-01-02"))
	if err != nil {
		return nil, fmt.Errorf("query daily stats: %w", err)
	}
	defer rows.Close()

	var stats []DailyOrderStats
	for rows.Next() {
		var s DailyOrderStats
		if err := rows.Scan(&s.StatDate, &s.TotalOrders, &s.TotalAmount,
			&s.AvgAmount, &s.PaidOrders, &s.Cancelled); err != nil {
			return nil, fmt.Errorf("scan daily stats: %w", err)
		}
		stats = append(stats, s)
	}
	return stats, rows.Err()
}

// GetBehaviorSummary 获取用户行为汇总 → 读 Replica
func (r *AnalyticsRepo) GetBehaviorSummary(ctx context.Context, eventType string, from, to time.Time) (map[string]int64, error) {
	if r.db == nil || r.db.IsNil() {
		return nil, fmt.Errorf("postgres not available")
	}

	query := `SELECT event_type, COUNT(*) as cnt
	           FROM user_behavior_log
	           WHERE created_at >= $1 AND created_at <= $2`
	args := []interface{}{from, to}

	if eventType != "" {
		query += ` AND event_type = $3`
		args = append(args, eventType)
	}
	query += ` GROUP BY event_type ORDER BY cnt DESC`

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query behavior summary: %w", err)
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var etype string
		var cnt int64
		if err := rows.Scan(&etype, &cnt); err != nil {
			return nil, fmt.Errorf("scan behavior summary: %w", err)
		}
		result[etype] = cnt
	}
	return result, rows.Err()
}

// behaviorEntry 行为日志条目 (供批量写入)
type BehaviorEntry struct {
	UserID    uint64
	EventType string
	EventData string
}

// LogBehavior 写入用户行为日志 → 写 Master
func (r *AnalyticsRepo) LogBehavior(ctx context.Context, userID uint64, eventType, eventData string) error {
	if r.db == nil || r.db.IsNil() {
		return nil // 降级: 静默跳过
	}

	_, err := r.db.ExecContext(ctx,
		`INSERT INTO user_behavior_log (user_id, event_type, event_data, created_at)
		 VALUES ($1, $2, $3::jsonb, NOW())`,
		userID, eventType, eventData,
	)
	return err
}

// LogBehaviorBatch 批量写入用户行为日志 (单次 INSERT 多行，减少 PG 往返)
func (r *AnalyticsRepo) LogBehaviorBatch(ctx context.Context, entries []BehaviorEntry) error {
	if r.db == nil || r.db.IsNil() || len(entries) == 0 {
		return nil
	}

	// 构造多行 VALUES: ($1,$2,$3,NOW()),($4,$5,$6,NOW()),...
	// PostgreSQL 参数上限约 65535，每条 3 个参数 + 1 (NOW())
	// 安全分批: 每批最多 200 条 (已由调用方保证)
	values := make([]string, 0, len(entries))
	args := make([]interface{}, 0, len(entries)*3)
	placeholderIdx := 1

	for _, e := range entries {
		values = append(values, fmt.Sprintf("($%d,$%d,$%d::jsonb,NOW())",
			placeholderIdx, placeholderIdx+1, placeholderIdx+2))
		args = append(args, e.UserID, e.EventType, e.EventData)
		placeholderIdx += 3
	}

	query := fmt.Sprintf(`INSERT INTO user_behavior_log (user_id, event_type, event_data, created_at) VALUES %s`,
		joinStrings(values, ","))

	_, err := r.db.ExecContext(ctx, query, args...)
	return err
}

func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}
