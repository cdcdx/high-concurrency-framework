package database

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"

	"go.uber.org/zap"
)

// RWDB 读写分离数据库包装器
//
//	写操作 (ExecContext)         → Master (主库)
//	读操作 (QueryContext/RowContext) → Replica (从库, 负载均衡)
//	DDL/健康检查                  → Master
//
// 若未配置 Replica, 读写均走 Master (兼容单库部署)
type RWDB struct {
	master   *sql.DB
	replicas []*sql.DB
	nextIdx  atomic.Uint32 // 副本轮询计数器
	logger   *zap.SugaredLogger
}

// NewRWDB 创建读写分离数据库
func NewRWDB(master *sql.DB, replicas []*sql.DB, logger *zap.SugaredLogger) *RWDB {
	rw := &RWDB{
		master:   master,
		replicas: replicas,
		logger:   logger,
	}
	if master != nil {
		logger.Infow("rwdb master ready", "driver", master.Driver())
	}
	if len(replicas) > 0 {
		logger.Infow("rwdb replicas ready", "count", len(replicas))
	} else {
		logger.Infow("rwdb: no replicas configured, reads go to master")
	}
	return rw
}

// Master 返回主库连接 (用于 DDL / 事务)
func (rw *RWDB) Master() *sql.DB {
	return rw.master
}

// replica 轮询选取一个副本节点; 无副本时回退到主库
func (rw *RWDB) replica() *sql.DB {
	if len(rw.replicas) == 0 {
		return rw.master
	}
	idx := rw.nextIdx.Add(1) % uint32(len(rw.replicas))
	return rw.replicas[idx]
}

// ExecContext 写操作 → Master
func (rw *RWDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if rw.master == nil {
		return nil, fmt.Errorf("rwdb: master not available")
	}
	return rw.master.ExecContext(ctx, query, args...)
}

// QueryContext 读操作 → Replica (轮询负载均衡)
func (rw *RWDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	r := rw.replica()
	if r == nil {
		return nil, fmt.Errorf("rwdb: no db available")
	}
	return r.QueryContext(ctx, query, args...)
}

// QueryRowContext 读操作 → Replica
// 注意: 调用方必须在 Scan 时检查 error，nil receiver 会导致 panic
func (rw *RWDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	r := rw.replica()
	if r == nil {
		// 返回 master 上的 Row，比空 DB 更安全但 master 也可能为 nil
		// 调用方必须在使用前通过 IsNil 检查
		if rw.master != nil {
			return rw.master.QueryRowContext(ctx, query, args...)
		}
	}
	return r.QueryRowContext(ctx, query, args...)
}

// PingContext 健康检查 → Master
func (rw *RWDB) PingContext(ctx context.Context) error {
	if rw.master == nil {
		return fmt.Errorf("rwdb: master not available")
	}
	return rw.master.PingContext(ctx)
}

// IsNil 判断数据库是否未初始化
func (rw *RWDB) IsNil() bool {
	return rw.master == nil
}

// Close 关闭所有连接
func (rw *RWDB) Close() {
	if rw.master != nil {
		rw.master.Close()
	}
	for _, r := range rw.replicas {
		r.Close()
	}
}
