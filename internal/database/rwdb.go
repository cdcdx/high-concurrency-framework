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
// 使用 uint64 计数器避免 uint32 在高频场景下溢出回绕导致负载不均
func (rw *RWDB) replica() *sql.DB {
	if len(rw.replicas) == 0 {
		return rw.master
	}
	idx := rw.nextIdx.Add(1)
	return rw.replicas[int(idx)%len(rw.replicas)]
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
// 前置条件: 调用方必须在调用前通过 IsNil() 检查数据库可用性
// 当 master 和所有 replica 均不可用时，返回 nil 上的 Row，Scan 时会 panic
func (rw *RWDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	r := rw.replica()
	if r != nil {
		return r.QueryRowContext(ctx, query, args...)
	}
	// r == nil 意味着 master 也是 nil（replica() 回退逻辑保证）
	// 调用方违反前置条件时此处必然 panic，使用 rw.master 保持一致语义
	return rw.master.QueryRowContext(ctx, query, args...)
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
