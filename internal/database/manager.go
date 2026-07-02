package database

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cdcdx/high-concurrency-framework/internal/config"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.uber.org/zap"
)

// Manager 多数据库管理器
// 按业务功能将数据路由至最优存储引擎:
//
//	MySQL      → 核心交易数据 (订单/事务) - OLTP, ACID, 支持读写分离
//	PostgreSQL → 分析/报表/时序数据 - 窗口函数, CTE, 支持读写分离
//	MongoDB    → 非结构化数据 (用户资料/行为日志), 支持 ReadPreference
//	ES         → 全文搜索与聚合分析
type Manager struct {
	MySQL       *RWDB             // 读写分离: 写→Master, 读→Replica
	Postgres    *RWDB             // 读写分离: 写→Master, 读→Replica
	MongoDB     *mongo.Database   // ReadPreference 控制读写分离
	MongoClient *mongo.Client
	Logger      *zap.SugaredLogger
	mu          sync.RWMutex
}

// parseReadPref 解析读偏好配置
func parseReadPref(pref string) *readpref.ReadPref {
	switch pref {
	case "primary":
		return readpref.Primary()
	case "primaryPreferred":
		return readpref.PrimaryPreferred()
	case "secondary":
		return readpref.Secondary()
	case "secondaryPreferred":
		return readpref.SecondaryPreferred()
	case "nearest":
		return readpref.Nearest()
	default:
		return readpref.PrimaryPreferred() // 默认
	}
}

// ConnectMongoDB 连接MongoDB (支持读偏好配置)
func ConnectMongoDB(cfg config.MongoConfig, logger *zap.SugaredLogger) (*mongo.Client, *mongo.Database, error) {
	if !cfg.Enabled || cfg.URI == "" {
		logger.Warnw("mongodb disabled or empty uri")
		return nil, nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(cfg.ConnectTimeoutSeconds)*time.Second)
	defer cancel()

	rp := parseReadPref(cfg.ReadPreference)

	clientOpts := options.Client().
		ApplyURI(cfg.URI).
		SetMaxPoolSize(cfg.MaxPoolSize).
		SetMinPoolSize(cfg.MinPoolSize).
		SetReadPreference(rp) // 客户端级读偏好

	if cfg.Username != "" {
		clientOpts.SetAuth(options.Credential{
			Username:   cfg.Username,
			Password:   cfg.Password,
			AuthSource: cfg.Database,
		})
	}

	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return nil, nil, fmt.Errorf("mongodb connect: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, nil, fmt.Errorf("mongodb ping: %w", err)
	}

	db := client.Database(cfg.Database)
	logger.Infow("mongodb connected", "db", cfg.Database, "uri", cfg.URI, "read_preference", cfg.ReadPreference)
	return client, db, nil
}

// Close 关闭所有数据库连接
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.MySQL != nil {
		m.MySQL.Close()
		m.Logger.Infow("mysql closed")
	}
	if m.Postgres != nil {
		m.Postgres.Close()
		m.Logger.Infow("postgres closed")
	}
	if m.MongoClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.MongoClient.Disconnect(ctx)
		m.Logger.Infow("mongodb closed")
	}
}

// IsMySQLReady 检查MySQL是否可用
func (m *Manager) IsMySQLReady(ctx context.Context) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.MySQL == nil || m.MySQL.IsNil() {
		return false
	}
	return m.MySQL.PingContext(ctx) == nil
}

// IsMongoReady 检查MongoDB是否可用
func (m *Manager) IsMongoReady(ctx context.Context) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.MongoClient == nil {
		return false
	}
	return m.MongoClient.Ping(ctx, readpref.Primary()) == nil
}

// IsPostgresReady 检查PostgreSQL是否可用
func (m *Manager) IsPostgresReady(ctx context.Context) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.Postgres == nil || m.Postgres.IsNil() {
		return false
	}
	return m.Postgres.PingContext(ctx) == nil
}
