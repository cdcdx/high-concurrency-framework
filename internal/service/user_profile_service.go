package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cdcdx/high-concurrency-framework/internal/cache"
	"github.com/cdcdx/high-concurrency-framework/internal/database"
	"github.com/cdcdx/high-concurrency-framework/internal/model"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/zap"
)

const profileESMaxConcurrency = 50 // ES 索引并发 goroutine 上限

// UserProfileService 用户资料服务
// 存储: MongoDB (主存储, schema灵活) + 多级缓存 (读加速)
type UserProfileService struct {
	cache        *cache.MultiLevelCache
	mongoRepo    *database.UserProfileRepo // MongoDB 持久化
	searchClient *database.SearchClient    // ES 搜索索引
	logger       *zap.SugaredLogger
	esSemaphore  chan struct{} // 限制并发 ES 索引 goroutine 数量
}

// NewUserProfileService 创建用户服务
func NewUserProfileService(
	cache *cache.MultiLevelCache,
	mongoRepo *database.UserProfileRepo,
	searchClient *database.SearchClient,
	logger *zap.SugaredLogger,
) *UserProfileService {
	return &UserProfileService{
		cache:        cache,
		mongoRepo:    mongoRepo,
		searchClient: searchClient,
		logger:       logger,
		esSemaphore:  make(chan struct{}, profileESMaxConcurrency),
	}
}

// GetProfile 获取用户资料 (L1→L2→L3 → MongoDB回源)
func (s *UserProfileService) GetProfile(ctx context.Context, userID uint64) (*model.UserProfile, error) {
	cacheKey := fmt.Sprintf("user:profile:%d", userID)

	val, found, err := s.cache.Get(ctx, cacheKey)
	if err != nil {
		return nil, err
	}
	if found && val != nil {
		// 缓存命中
		switch v := val.(type) {
		case *model.UserProfile:
			return v, nil
		case map[string]interface{}:
			b, _ := json.Marshal(v)
			var profile model.UserProfile
			json.Unmarshal(b, &profile)
			return &profile, nil
		default:
			return nil, fmt.Errorf("invalid cache type for user profile")
		}
	}

	// 缓存穿透标记命中
	if found && val == nil {
		return nil, fmt.Errorf("user not found: %d", userID)
	}

	// Cache miss → MongoDB回源
	return s.loadFromMongo(ctx, userID, cacheKey)
}

// loadFromMongo 从MongoDB加载用户资料并回填缓存
func (s *UserProfileService) loadFromMongo(ctx context.Context, userID uint64, cacheKey string) (*model.UserProfile, error) {
	if s.mongoRepo == nil {
		return nil, fmt.Errorf("mongodb not available")
	}

	profile, err := s.mongoRepo.FindByUserID(ctx, userID)
	if err == mongo.ErrNoDocuments {
		// 缓存空标记防穿透
		s.cache.SetNull(ctx, cacheKey)
		return nil, fmt.Errorf("user not found: %d", userID)
	}
	if err != nil {
		return nil, fmt.Errorf("mongo query user profile: %w", err)
	}

	// 回填缓存
	s.cache.Set(ctx, cacheKey, profile)
	s.logger.Debugw("user profile loaded from mongodb", "user_id", userID)

	return profile, nil
}

// UpsertProfile 创建或更新用户资料 (MongoDB upsert + 缓存失效 + ES索引)
func (s *UserProfileService) UpsertProfile(ctx context.Context, profile *model.UserProfile) error {
	if s.mongoRepo == nil {
		return fmt.Errorf("mongodb not available")
	}

	if err := s.mongoRepo.Upsert(ctx, profile); err != nil {
		return fmt.Errorf("mongo upsert: %w", err)
	}

	// 延迟双删: 使缓存失效
	cacheKey := fmt.Sprintf("user:profile:%d", profile.UserID)
	s.cache.InvalidateWithDelay(ctx, cacheKey, 500)

	// 异步索引到ES（带限流保护，不阻塞主流程）
	if s.searchClient != nil {
		s.indexProfileToES(profile)
	}

	s.logger.Infow("user profile upserted to mongodb", "user_id", profile.UserID)
	return nil
}

// SearchUsers 通过ES全文搜索用户
func (s *UserProfileService) SearchUsers(ctx context.Context, keyword string, page, size int) ([]model.UserProfile, int64, error) {
	if s.searchClient == nil {
		return nil, 0, fmt.Errorf("elasticsearch not available")
	}
	return s.searchClient.SearchUsers(ctx, keyword, page, size)
}

// indexProfileToES 带限流保护的异步 ES 索引写入
func (s *UserProfileService) indexProfileToES(profile *model.UserProfile) {
	select {
	case s.esSemaphore <- struct{}{}:
	default:
		s.logger.Debugw("es index skipped (too many pending)", "user_id", profile.UserID)
		return
	}

	go func() {
		defer func() { <-s.esSemaphore }()
		if err := s.searchClient.IndexUserProfile(context.Background(), profile); err != nil {
			s.logger.Warnw("es index profile failed (non-blocking)", "user_id", profile.UserID, "err", err)
		}
	}()
}
