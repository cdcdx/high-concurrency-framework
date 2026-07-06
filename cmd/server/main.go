package main

// @title           高并发业务框架 API
// @version         1.0.0
// @description     高并发业务处理框架，支持多级缓存、读写分离、异步消息、全文搜索等能力
// @contact.name    cdcdx
// @host            localhost:8080
// @BasePath        /
// @securityDefinitions.apikey Bearer
// @in header
// @name Authorization
// @description 输入JWT令牌 (可带或不带 "Bearer " 前缀), 通过 POST /api/v1/auth/login 或 /api/v1/auth/register 获取

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/cdcdx/high-concurrency-framework/internal/cache"
	"github.com/cdcdx/high-concurrency-framework/internal/config"
	"github.com/cdcdx/high-concurrency-framework/internal/database"
	"github.com/cdcdx/high-concurrency-framework/internal/handler"
	"github.com/cdcdx/high-concurrency-framework/internal/health"
	"github.com/cdcdx/high-concurrency-framework/internal/lock"
	"github.com/cdcdx/high-concurrency-framework/internal/middleware"
	"github.com/cdcdx/high-concurrency-framework/internal/model"
	"github.com/cdcdx/high-concurrency-framework/internal/mq"
	"github.com/cdcdx/high-concurrency-framework/internal/resilience"
	"github.com/cdcdx/high-concurrency-framework/internal/service"
	"github.com/dgraph-io/ristretto"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"

	_ "github.com/cdcdx/high-concurrency-framework/swagger" // swagger 生成的文档包
)

func main() {
	// --- 1. 加载配置 ---
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// --- 2. 初始化日志 (结构化 + traceId) ---
	logger := initLogger(cfg.Logging)
	defer logger.Sync()

	logger.Infow("starting high-concurrency-framework",
		"version", "1.0.0",
		"port", cfg.Server.Port,
	)

	// --- 3. 初始化Redis (L2分布式缓存 + 分布式锁) ---
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.Cache.L2.Addresses[0], // Cluster暂用第一个节点
		Password: cfg.Cache.L2.Password,
		PoolSize: cfg.Cache.L2.PoolSize,
	})
	defer redisClient.Close()

	// Ping Redis验证连通性
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		logger.Warnw("redis not available, starting in degraded mode", "err", err)
	}

	// --- 4. 初始化多数据库 (按功能选最优存储引擎 + 读写分离) ---
	dbManager := &database.Manager{Logger: logger}
	var searchClient *database.SearchClient

	// 4a. MySQL: 核心交易数据 (订单/事务) → 写 Master, 读 Replica
	mysqlRW := initRWDB(cfg.Databases.MySQL, logger)
	if mysqlRW == nil {
		logger.Warnw("mysql not available, order persistence degraded")
	} else {
		dbManager.MySQL = mysqlRW
	}

	// 4b. PostgreSQL: 分析/报表/时序数据 → 写 Master, 读 Replica
	pgRW := initRWDB(cfg.Databases.Postgres, logger)
	if pgRW == nil {
		logger.Warnw("postgres not available, analytics degraded")
	} else {
		dbManager.Postgres = pgRW
	}

	// 4c. MongoDB: 非结构化数据 (用户资料/行为日志)
	mongoClient, mongoDB, mongoErr := database.ConnectMongoDB(cfg.Databases.MongoDB, logger)
	if mongoErr != nil {
		logger.Warnw("mongodb not available, user profiles degraded",
			"err", mongoErr,
			"uri", cfg.Databases.MongoDB.URI)
	} else if mongoDB != nil {
		dbManager.MongoDB = mongoDB
		dbManager.MongoClient = mongoClient
	}

	// 4d. Elasticsearch: 全文搜索与聚合分析
	esClient, esErr := database.ConnectElasticsearch(cfg.Databases.Elasticsearch, logger)
	if esErr != nil {
		logger.Warnw("elasticsearch not available, full-text search degraded",
			"err", esErr)
	} else if esClient != nil {
		searchClient = database.NewSearchClient(esClient, logger)
	}

	defer dbManager.Close()

	// --- 5. 初始化Ristretto (L1本地缓存) ---
	l1Cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: int64(cfg.Cache.L1.MaxEntries) * 10, // 10倍计数器防碰撞
		MaxCost:     int64(cfg.Cache.L1.MaxEntries),
		BufferItems: 64, // 缓冲写入
	})
	if err != nil {
		logger.Fatalw("failed to init L1 cache", "err", err)
	}

	// --- 6. 初始化热点Key检测器 ---
	hotKeyDetector := cache.NewHotKeyDetector(
		cfg.HotKey.WindowSeconds,
		cfg.HotKey.Slots,
		cfg.HotKey.Threshold,
	)

	// --- 7. 初始化核心组件 ---
	// MongoDB Repos
	userProfileRepo := database.NewUserProfileRepo(dbManager.MongoDB)

	// DataLoader: L3 DB回源函数 (多数据库路由)
	dataLoader := newDataLoader(dbManager, userProfileRepo, logger)

	// 多级缓存
	multiLevelCache := cache.NewMultiLevelCache(
		l1Cache,
		redisClient,
		dataLoader,
		hotKeyDetector,
		logger,
	)
	multiLevelCache.SetJitterPct(cfg.Cache.JitterPercent)
	multiLevelCache.SetNullTTL(cfg.Cache.NullTTL())
	multiLevelCache.SetL1BaseTTL(time.Duration(cfg.Cache.L1.TTLSeconds) * time.Second)
	if cfg.Cache.L2.TTLSeconds > 0 {
		multiLevelCache.SetL2BaseTTL(time.Duration(cfg.Cache.L2.TTLSeconds) * time.Second)
	}

	// 熔断器
	circuitBreaker := resilience.NewCircuitBreaker(
		"cdcdx-cb",
		cfg.CircuitBreaker.MaxRequests,
		cfg.CircuitBreaker.IntervalSeconds,
		cfg.CircuitBreaker.TimeoutSeconds,
		cfg.CircuitBreaker.FailureThreshold,
		cfg.CircuitBreaker.SlowCallThresholdMs,
	)

	// 写限流器
	rateLimiter := resilience.NewWriteRateLimiter(
		cfg.RateLimiter.BucketCapacity,
		cfg.RateLimiter.RefillRate,
	)

	// 分布式锁
	distLock := lock.NewDistributedLock(redisClient)

	// Kafka生产者
	kafkaProducer := mq.NewEventProducer(
		cfg.Kafka.Brokers,
		cfg.Kafka.Producer.Topic,
		cfg.Kafka.Producer.MaxRetries,
		logger,
	)
	defer kafkaProducer.Close()

	// Kafka DLQ生产者 (用于消费者写入死信)
	dlqProducer := mq.NewEventProducer(
		cfg.Kafka.Brokers,
		cfg.Kafka.Consumer.DLQTopic,
		cfg.Kafka.Producer.MaxRetries,
		logger,
	)
	defer dlqProducer.Close()

	// --- 8. 初始化业务服务 ---
	orderService := service.NewOrderService(
		multiLevelCache,
		kafkaProducer,
		rateLimiter,
		distLock,
		circuitBreaker,
		dbManager.MySQL, // MySQL - 核心交易持久化
		searchClient,    // Elasticsearch - 全文搜索索引
		logger,
	)

	userProfileService := service.NewUserProfileService(
		multiLevelCache,
		userProfileRepo, // MongoDB - 用户资料持久化
		searchClient,    // Elasticsearch - 用户搜索索引
		logger,
	)

	// 分析服务 (PostgreSQL)
	analyticsRepo := database.NewAnalyticsRepo(dbManager.Postgres)
	analyticsService := service.NewAnalyticsService(analyticsRepo, logger)

	// 认证服务 (MySQL users 表 + JWT)
	userAuthRepo := database.NewUserAuthRepo(dbManager.MySQL)
	authService := service.NewAuthService(userAuthRepo, cfg.JWT.Secret, cfg.JWT.ExpireSeconds, logger)

	// --- 9. 统一初始化所有数据库表/索引 (DDL from sql/*) ---
	go func() {
		database.EnsureAllSQLTables(context.Background(), dbManager.MySQL, dbManager.Postgres, logger)
		database.EnsureMongoCollections(context.Background(), dbManager.MongoDB, logger)
		if searchClient != nil {
			if err := searchClient.EnsureSearchIndexes(context.Background()); err != nil {
				logger.Warnw("es ensure indexes failed", "err", err)
			}
		}
	}()

	// --- 10. 启动Kafka消费者 (异步通路) ---
	kafkaConsumer := mq.NewEventConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.Consumer.Topic,
		cfg.Kafka.Consumer.GroupID,
		cfg.Kafka.Consumer.Concurrency,
		cfg.Kafka.Consumer.RateLimit,
		cfg.Kafka.Consumer.MaxRetries,
		orderService.HandleOrderEvent,
		dlqProducer,
		logger,
		cfg.Kafka.Consumer.StartFromLatest,
	)
	go kafkaConsumer.Start(context.Background(), cfg.Kafka.Consumer.Concurrency)
	defer kafkaConsumer.Stop()

	// --- 11. 设置Gin路由 ---
	gin.SetMode(cfg.Server.Mode)
	router := gin.New()

	// 全局中间件
	router.Use(middleware.TraceID())
	router.Use(middleware.ZapLogger(logger))
	router.Use(middleware.Recovery(logger))
	router.Use(middleware.ErrorHandler())

	// CORS 跨域中间件 (通过配置控制)
	corsCfg := middleware.CORSConfig{
		Enabled:          cfg.CORS.Enabled,
		AllowedOrigins:   cfg.CORS.AllowedOrigins,
		AllowedMethods:   cfg.CORS.AllowedMethods,
		AllowedHeaders:   cfg.CORS.AllowedHeaders,
		ExposedHeaders:   cfg.CORS.ExposedHeaders,
		AllowCredentials: cfg.CORS.AllowCredentials,
		MaxAge:           cfg.CORS.MaxAge,
	}
	router.Use(middleware.CORS(corsCfg))
	// 注: 限流由 service.OrderService.CreateOrder 层控制同步/异步分流
	// 不在中间件层拦截, 避免与业务限流器冲突

	// 初始化处理器
	businessHandler := handler.NewBusinessHandler(
		orderService,
		userProfileService,
		analyticsService,
		circuitBreaker,
		logger,
	)
	analyticsHandler := handler.NewAnalyticsHandler(analyticsService, logger)
	monitorHandler := handler.NewMonitorHandler(
		multiLevelCache,
		circuitBreaker,
		rateLimiter,
	)
	authHandler := handler.NewAuthHandler(authService, logger)
	// Kafka 连通性检查函数
	kafkaChecker := func(ctx context.Context) error {
		return kafkaProducer.Ping(ctx)
	}

	healthChecker := health.NewHealthChecker(
		redisClient,
		multiLevelCache,
		circuitBreaker,
		kafkaChecker,
		logger,
	)

	// === 路由注册 ===

	// 1. 认证路由 (公开, 无需JWT)
	auth := router.Group("/api/v1/auth")
	{
		auth.POST("/register", authHandler.Register)
		auth.POST("/login", authHandler.Login)
	}

	// 2. 业务API (需JWT认证)
	v1 := router.Group("/api/v1")
	v1.Use(middleware.JWTAuth(authService))
	{
		// 当前用户信息
		v1.GET("/auth/me", authHandler.GetMe)

		// 订单 (MySQL + Kafka + ES)
		v1.POST("/orders", businessHandler.CreateOrder)          // MySQL写入 + Kafka
		v1.POST("/orders/sync", businessHandler.CreateOrderSync) // MySQL写入
		v1.GET("/orders/:orderNo", businessHandler.GetOrder)     // L1→L2→MySQL
		v1.GET("/orders/search", businessHandler.SearchOrders)   // ES全文搜索

		// 用户 (MongoDB + 缓存 + ES)
		v1.POST("/users/profile", businessHandler.UpsertUserProfile)     // MongoDB写入
		v1.GET("/users/:userID/profile", businessHandler.GetUserProfile) // L1→L2→MongoDB
		v1.GET("/users/search", businessHandler.SearchUsers)             // ES全文搜索

		// 分析接口 (PostgreSQL)
		v1.GET("/analytics/daily", analyticsHandler.GetDailyStats)
		v1.GET("/analytics/behaviors", analyticsHandler.GetBehaviorSummary)
	}

	// 3. 监控接口 (公开, 无需JWT)
	monitor := router.Group("/api/v1/monitor")
	{
		monitor.GET("/metrics", monitorHandler.Metrics)
		monitor.GET("/circuit-breaker", monitorHandler.CircuitBreakerState)
		monitor.GET("/cache-stats", businessHandler.GetCacheStats)
	}

	// K8s健康探针
	router.GET("/health/liveness", healthChecker.LivenessHandler())
	router.GET("/health/readiness", healthChecker.ReadinessHandler())
	router.GET("/health/startup", healthChecker.StartupHandler())

	// Swagger 在线文档 (通过配置控制, 生产环境建议关闭)
	// 日期占位符 __7DAYS_AGO__/__TODAY__ 在运行时根据当前时间自动计算
	if cfg.Swagger.Enabled {
		router.GET("/swagger/*any", middleware.SwaggerWithRuntimeDefaults())
		logger.Infow("swagger html enabled", "url", fmt.Sprintf("http://localhost:%d/swagger/index.html", cfg.Server.Port))
	}

	// 标记启动完成
	healthChecker.SetStartupCompleted()

	// --- 12. 优雅启停 ---
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Server.Port),
		Handler: router,
	}

	// 启动服务
	go func() {
		logger.Infow("server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalw("server failed", "err", err)
		}
	}()

	// 等待退出信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Infow("shutting down server...")

	// 停止后台组件
	circuitBreaker.Stop()
	hotKeyDetector.Stop()
	multiLevelCache.Close()
	analyticsService.Close()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Errorw("server forced shutdown", "err", err)
	}

	logger.Infow("server exited gracefully")
}

// newDataLoader 创建 L3 DB 回源函数（多数据库智能路由）
func newDataLoader(dbManager *database.Manager, userProfileRepo *database.UserProfileRepo, logger *zap.SugaredLogger) cache.DataLoader {
	return func(ctx context.Context, key string) (interface{}, error) {
		switch {
		// 用户资料 → MongoDB 回源
		case len(key) > 12 && key[:12] == "user:profile:":
			if dbManager.MongoDB == nil {
				logger.Debugw("L3 load user profile (mock, mongo unavailable)", "key", key)
				return &model.UserProfile{
					UserID:   10001,
					Nickname: "test_user",
				}, nil
			}
			var userID uint64
			fmt.Sscanf(key, "user:profile:%d", &userID)
			profile, err := userProfileRepo.FindByUserID(ctx, userID)
			if err != nil {
				return nil, nil // 不存在 → 触发空标记
			}
			logger.Debugw("L3 loaded user profile from mongodb", "user_id", userID)
			return profile, nil

		// 订单 → MySQL Replica 回源
		case len(key) > 6 && key[:6] == "order:":
			if dbManager.MySQL == nil || dbManager.MySQL.IsNil() {
				return nil, nil
			}
			orderNo := key[6:]
			var order model.Order
			err := dbManager.MySQL.QueryRowContext(ctx,
				`SELECT id, user_id, order_no, amount, status, created_at, updated_at
				 FROM orders WHERE order_no = ?`, orderNo,
			).Scan(&order.ID, &order.UserID, &order.OrderNo, &order.Amount,
				&order.Status, &order.CreatedAt, &order.UpdatedAt)
			if err != nil {
				return nil, nil // 不存在 → 触发空标记
			}
			logger.Debugw("L3 loaded order from mysql", "order_no", orderNo)
			return &order, nil

		default:
			return nil, nil
		}
	}
}

// initLogger 初始化结构化日志
// 支持 stderr 输出和文件输出（含日志轮转）
func initLogger(lc config.LoggingConfig) *zap.SugaredLogger {
	var zcfg zap.Config
	if lc.Format == "structured" {
		zcfg = zap.NewProductionConfig()
	} else {
		zcfg = zap.NewDevelopmentConfig()
	}

	// 解析日志级别
	var zapLevel zapcore.Level
	switch lc.Level {
	case "debug":
		zapLevel = zapcore.DebugLevel
	case "info":
		zapLevel = zapcore.InfoLevel
	case "warn":
		zapLevel = zapcore.WarnLevel
	case "error":
		zapLevel = zapcore.ErrorLevel
	default:
		zapLevel = zapcore.InfoLevel
	}
	zcfg.Level = zap.NewAtomicLevelAt(zapLevel)

	// 自定义输出格式
	zcfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	zcfg.EncoderConfig.MessageKey = "msg"

	// 根据 format 配置选择编码器
	var encoder zapcore.Encoder
	if lc.Format == "structured" {
		encoder = zapcore.NewJSONEncoder(zcfg.EncoderConfig)
	} else {
		// text 模式：控制台友好的彩色输出
		zcfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		encoder = zapcore.NewConsoleEncoder(zcfg.EncoderConfig)
	}

	// 日志输出: stderr + 可选文件（带轮转）
	var writers []zapcore.WriteSyncer
	writers = append(writers, os.Stderr) // 始终输出到 stderr (兼容 docker/k8s)

	if lc.Output != "" {
		// 确保日志目录存在
		if dir := filepath.Dir(lc.Output); dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				log.Printf("WARN: create log dir %s: %v", dir, err)
			}
		}

		// 默认轮转参数
		maxSize := lc.MaxSizeMB
		if maxSize <= 0 {
			maxSize = 100
		}
		maxBackups := lc.MaxBackups
		if maxBackups <= 0 {
			maxBackups = 7
		}
		maxAge := lc.MaxAgeDays
		if maxAge <= 0 {
			maxAge = 30
		}

		lj := &lumberjack.Logger{
			Filename:   lc.Output,
			MaxSize:    maxSize,
			MaxBackups: maxBackups,
			MaxAge:     maxAge,
			Compress:   lc.Compress,
		}
		writers = append(writers, zapcore.AddSync(lj))
	}

	core := zapcore.NewCore(
		encoder,
		zapcore.NewMultiWriteSyncer(writers...),
		zcfg.Level,
	)

	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
	return logger.Sugar()
}

// initRWDB 初始化读写分离数据库 (Master + Replica)
func initRWDB(cfg config.DatabaseConfig, logger *zap.SugaredLogger) *database.RWDB {
	if cfg.DSN == "" || !cfg.Enabled {
		return nil
	}

	// 连接主库
	master, err := openDB(cfg.Driver, cfg.DSN, cfg)
	if err != nil {
		logger.Warnw("master connect failed", "driver", cfg.Driver, "err", err)
		return nil
	}
	logger.Infow("master connected", "driver", cfg.Driver)

	// 连接副本
	var replicas []*sql.DB
	for i, dsn := range cfg.ReplicaDSNs {
		r, err := openDB(cfg.Driver, dsn, cfg)
		if err != nil {
			logger.Warnw("replica connect failed", "driver", cfg.Driver, "index", i, "err", err)
			continue
		}
		replicas = append(replicas, r)
		logger.Infow("replica connected", "driver", cfg.Driver, "index", i)
	}

	return database.NewRWDB(master, replicas, logger)
}

// openDB 打开数据库连接并进行健康检查
func openDB(driver, dsn string, cfg config.DatabaseConfig) (*sql.DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(time.Duration(cfg.ConnMaxLifetimeSecs) * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("db ping: %w", err)
	}

	return db, nil
}
