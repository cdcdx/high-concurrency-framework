package config

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 全局配置
type Config struct {
	Server         ServerConfig         `yaml:"server"`
	Cache          CacheConfig          `yaml:"cache"`
	Databases      MultiDatabaseConfig  `yaml:"databases"` // 多数据库配置
	Kafka          KafkaConfig          `yaml:"kafka"`
	CircuitBreaker CircuitBreakerConfig `yaml:"circuit_breaker"`
	RateLimiter    RateLimiterConfig    `yaml:"rate_limiter"`
	HotKey         HotKeyConfig         `yaml:"hot_key"`
	Observability  ObservabilityConfig  `yaml:"observability"`
	Logging        LoggingConfig        `yaml:"logging"`
	Swagger        SwaggerConfig        `yaml:"swagger"`
	CORS           CORSConfig           `yaml:"cors"`
	JWT            JWTConfig            `yaml:"jwt"`
}

// === 数据库连接配置（消除与 database 包的循环依赖） ===

// MongoConfig MongoDB 连接配置
type MongoConfig struct {
	Enabled               bool   `yaml:"enabled"`
	URI                   string `yaml:"uri"`
	Username              string `yaml:"username"`
	Password              string `yaml:"password"`
	Database              string `yaml:"database"`
	MaxPoolSize           uint64 `yaml:"max_pool_size"`
	MinPoolSize           uint64 `yaml:"min_pool_size"`
	ConnectTimeoutSeconds int    `yaml:"connect_timeout_seconds"`
	ReadPreference        string `yaml:"read_preference"`
	// 读偏好: "primary" | "primaryPreferred" | "secondary" | "secondaryPreferred" | "nearest"
	// secondaryPreferred = 优先从副本读, 副本不可用时回退主库 (推荐写多读少场景)
	// 为空时使用 MongoDB 驱动默认值 (primaryPreferred)
}

// ESConfig Elasticsearch 连接配置
type ESConfig struct {
	Enabled   bool     `yaml:"enabled"`
	Addresses []string `yaml:"addresses"`
	Username  string   `yaml:"username"`
	Password  string   `yaml:"password"`
}

// MultiDatabaseConfig 多数据库配置
type MultiDatabaseConfig struct {
	MySQL         DatabaseConfig `yaml:"mysql"`
	Postgres      DatabaseConfig `yaml:"postgres"`
	MongoDB       MongoConfig    `yaml:"mongodb"`
	Elasticsearch ESConfig       `yaml:"elasticsearch"`
}

type ServerConfig struct {
	Port int    `yaml:"port"`
	Mode string `yaml:"mode"`
}

type CacheConfig struct {
	L1             L1CacheConfig `yaml:"l1"`
	L2             L2CacheConfig `yaml:"l2"`
	JitterPercent  float64       `yaml:"jitter_percent"`
	NullTTLSeconds int           `yaml:"null_ttl_seconds"`
}

type L1CacheConfig struct {
	MaxEntries     int `yaml:"max_entries"`
	TTLSeconds     int `yaml:"ttl_seconds"`
	RefreshSeconds int `yaml:"refresh_seconds"` // 提前异步刷新 (TODO: 未实现, 预留字段)
}

type L2CacheConfig struct {
	Addresses     []string `yaml:"addresses"`
	Password      string   `yaml:"password"`
	PoolSize      int      `yaml:"pool_size"`
	MinIdle       int      `yaml:"min_idle"`
	DialTimeoutMs int      `yaml:"dial_timeout_ms"`
	ReadTimeoutMs int      `yaml:"read_timeout_ms"`
	TTLSeconds    int      `yaml:"ttl_seconds"` // L2 基础 TTL (秒), 默认 1800 (30分钟)
}

type DatabaseConfig struct {
	Enabled             bool     `yaml:"enabled"`
	Driver              string   `yaml:"driver"`
	DSN                 string   `yaml:"dsn"`          // 主库 (写入)
	ReplicaDSNs         []string `yaml:"replica_dsns"` // 只读副本 (读取, 负载均衡), 为空则读写均走主库
	MaxOpenConns        int      `yaml:"max_open_conns"`
	MaxIdleConns        int      `yaml:"max_idle_conns"`
	ConnMaxLifetimeSecs int      `yaml:"conn_max_lifetime_seconds"`
}

type KafkaConfig struct {
	Brokers  []string       `yaml:"brokers"`
	Producer ProducerConfig `yaml:"producer"`
	Consumer ConsumerConfig `yaml:"consumer"`
}

type ProducerConfig struct {
	Topic       string `yaml:"topic"`
	Acks        string `yaml:"acks"`
	Compression string `yaml:"compression"`
	BatchSize   int    `yaml:"batch_size"`
	LingerMs    int    `yaml:"linger_ms"`
	MaxRetries  int    `yaml:"max_retries"`
	Idempotent  bool   `yaml:"idempotent"`
}

type ConsumerConfig struct {
	GroupID        string `yaml:"group_id"`
	Topic          string `yaml:"topic"`
	DLQTopic       string `yaml:"dlq_topic"`
	MaxRetries     int    `yaml:"max_retries"`
	RateLimit      int    `yaml:"rate_limit"`
	Concurrency    int    `yaml:"concurrency"`
	MinBytes       int    `yaml:"min_bytes"`
	MaxBytes       int    `yaml:"max_bytes"`
	StartFromLatest bool  `yaml:"start_from_latest"` // true=跳过积压, false=消费全部历史 (削峰填谷)
}

type CircuitBreakerConfig struct {
	MaxRequests         int     `yaml:"max_requests"`
	IntervalSeconds     int     `yaml:"interval_seconds"`
	TimeoutSeconds      int     `yaml:"timeout_seconds"`
	FailureThreshold    float64 `yaml:"failure_threshold"`
	SlowCallThresholdMs int     `yaml:"slow_call_threshold_ms"`
}

type RateLimiterConfig struct {
	BucketCapacity int `yaml:"bucket_capacity"`
	RefillRate     int `yaml:"refill_rate"`
}

type HotKeyConfig struct {
	WindowSeconds int `yaml:"window_seconds"`
	Slots         int `yaml:"slots"`
	Threshold     int `yaml:"threshold"`
}

type ObservabilityConfig struct {
	Trace   TraceConfig   `yaml:"trace"`
	Metrics MetricsConfig `yaml:"metrics"`
}

type TraceConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Exporter string `yaml:"exporter"`
}

type MetricsConfig struct {
	Enabled bool `yaml:"enabled"`
}

type LoggingConfig struct {
	Level      string `yaml:"level"`        // debug | info | warn | error
	Format     string `yaml:"format"`       // structured | text
	Output     string `yaml:"output"`       // 日志文件路径, 为空则仅输出到 stderr
	MaxSizeMB  int    `yaml:"max_size_mb"`  // 单个日志文件最大 MB, 默认 100
	MaxBackups int    `yaml:"max_backups"`  // 保留旧日志文件数量, 默认 7
	MaxAgeDays int    `yaml:"max_age_days"` // 保留旧日志最大天数, 默认 30
	Compress   bool   `yaml:"compress"`     // 是否压缩旧日志, 默认 true
}

// SwaggerConfig Swagger 在线文档配置
type SwaggerConfig struct {
	Enabled bool `yaml:"enabled"` // 是否启用在线文档, 默认为 false (生产环境关闭)
}

// JWTConfig JWT 认证配置
type JWTConfig struct {
	Secret        string `yaml:"secret"`         // JWT 签名密钥 (生产环境使用强随机字符串)
	ExpireSeconds int    `yaml:"expire_seconds"` // Token 过期时间 (秒)
}

// CORSConfig 跨域配置
type CORSConfig struct {
	Enabled          bool     `yaml:"enabled"`           // 是否启用 CORS
	AllowedOrigins   []string `yaml:"allowed_origins"`   // 允许的来源, ["*"] 表示允许所有
	AllowedMethods   []string `yaml:"allowed_methods"`   // 允许的 HTTP 方法
	AllowedHeaders   []string `yaml:"allowed_headers"`   // 允许的请求头
	ExposedHeaders   []string `yaml:"exposed_headers"`   // 暴露给浏览器的响应头
	AllowCredentials bool     `yaml:"allow_credentials"` // 是否允许携带 Cookie
	MaxAge           int      `yaml:"max_age"`           // 预检请求缓存时间(秒)
}

// 默认配置
var defaultConfig = Config{
	Server: ServerConfig{Port: 8080, Mode: "release"},
	Cache: CacheConfig{
		L1:             L1CacheConfig{MaxEntries: 1000000, TTLSeconds: 30, RefreshSeconds: 25},
		L2:             L2CacheConfig{Addresses: []string{"127.0.0.1:6379"}, PoolSize: 100, MinIdle: 10, DialTimeoutMs: 3000, ReadTimeoutMs: 1000},
		JitterPercent:  0.2,
		NullTTLSeconds: 60,
	},
	Databases: MultiDatabaseConfig{
		MySQL:         DatabaseConfig{Enabled: true, Driver: "mysql", MaxOpenConns: 50, MaxIdleConns: 10, ConnMaxLifetimeSecs: 1800},
		Postgres:      DatabaseConfig{Enabled: true, Driver: "postgres", MaxOpenConns: 30, MaxIdleConns: 5, ConnMaxLifetimeSecs: 1800},
		MongoDB:       MongoConfig{Enabled: true, MaxPoolSize: 100, MinPoolSize: 10, ConnectTimeoutSeconds: 10},
		Elasticsearch: ESConfig{Enabled: true},
	},
	Kafka: KafkaConfig{
		Producer: ProducerConfig{Topic: "order-create", Acks: "all", Compression: "lz4", BatchSize: 16384, LingerMs: 5, MaxRetries: 3, Idempotent: true},
		Consumer: ConsumerConfig{GroupID: "order-consumer-group", Topic: "order-create", DLQTopic: "order-create.dlq", MaxRetries: 3, RateLimit: 2000, Concurrency: 8, StartFromLatest: false},
	},
	CircuitBreaker: CircuitBreakerConfig{MaxRequests: 5, IntervalSeconds: 60, TimeoutSeconds: 30, FailureThreshold: 0.5, SlowCallThresholdMs: 1000},
	RateLimiter:    RateLimiterConfig{BucketCapacity: 5000, RefillRate: 5000},
	HotKey:         HotKeyConfig{WindowSeconds: 10, Slots: 10, Threshold: 100},
	Logging:        LoggingConfig{Level: "debug", Format: "structured"},
	Swagger:        SwaggerConfig{Enabled: false},                                     // 默认关闭, 开发环境通过 config.yaml 开启
	JWT:            JWTConfig{Secret: "change-me-in-production", ExpireSeconds: 7200}, // 默认2小时
	CORS: CORSConfig{
		Enabled:          false, // 默认关闭, 开发环境通过 config.yaml 开启
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Origin", "Content-Type", "Accept", "Authorization", "X-Trace-Id", "X-Request-ID"},
		ExposedHeaders:   []string{"Content-Length", "X-Trace-Id"},
		AllowCredentials: true,
		MaxAge:           86400,
	},
}

// Load 加载配置 (YAML文件 + 默认值)
func Load(path string) (*Config, error) {
	cfg := defaultConfig

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &cfg, nil // 文件不存在时使用默认值
		}
		return nil, fmt.Errorf("read config: %w", err)
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	return &cfg, nil
}

// L1TTL 返回L1缓存TTL (带随机偏移防雪崩)
func (c *CacheConfig) L1TTL() time.Duration {
	base := time.Duration(c.L1.TTLSeconds) * time.Second
	if c.JitterPercent <= 0 {
		return base
	}
	// ±20% 随机偏移
	return jitterDuration(base, c.JitterPercent)
}

// L2TTL 返回L2缓存TTL (带随机偏移防雪崩)
func (c *CacheConfig) L2TTL() time.Duration {
	base := time.Duration(c.L2.TTLSeconds) * time.Second
	if base <= 0 {
		base = 30 * time.Minute // 默认 30 分钟
	}
	return jitterDuration(base, c.JitterPercent)
}

// NullTTL 空标记TTL
func (c *CacheConfig) NullTTL() time.Duration {
	return time.Duration(c.NullTTLSeconds) * time.Second
}

// jitterDuration 在 base * (1 ± pct) 范围内随机偏移
// 使用 crypto/rand 确保均匀分布, 防止所有 Pod 同时过期 (缓存雪崩)
func jitterDuration(base time.Duration, pct float64) time.Duration {
	if pct <= 0 {
		return base
	}
	// 使用 crypto/rand 生成 [0,1) 均匀分布的随机数
	var b [8]byte
	_, _ = rand.Read(b[:])
	n := float64(binary.LittleEndian.Uint64(b[:])>>11) / (1 << 53)
	// 偏移范围: base * (1 - pct) → base * (1 + pct)
	delta := float64(base) * pct
	result := float64(base) - delta + 2*delta*n
	return time.Duration(result)
}
