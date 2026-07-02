package database

import (
	"context"
	"fmt"

	"github.com/cdcdx/high-concurrency-framework/internal/model"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// UserProfileCollection MongoDB 用户资料集合名
const UserProfileCollection = "user_profiles"

// UserProfileRepo MongoDB 用户资料数据访问
type UserProfileRepo struct {
	coll *mongo.Collection
}

// NewUserProfileRepo 创建MongoDB用户资料仓库
func NewUserProfileRepo(mongoDB *mongo.Database) *UserProfileRepo {
	if mongoDB == nil {
		return &UserProfileRepo{}
	}
	return &UserProfileRepo{coll: mongoDB.Collection(UserProfileCollection)}
}

// FindByUserID 按user_id查询用户资料
func (r *UserProfileRepo) FindByUserID(ctx context.Context, userID uint64) (*model.UserProfile, error) {
	if r.coll == nil {
		return nil, fmt.Errorf("mongodb not available")
	}

	var profile model.UserProfile
	err := r.coll.FindOne(ctx, bson.M{"user_id": userID}).Decode(&profile)
	if err == mongo.ErrNoDocuments {
		return nil, mongo.ErrNoDocuments
	}
	if err != nil {
		return nil, fmt.Errorf("mongo find user profile: %w", err)
	}
	return &profile, nil
}

// Upsert 插入或更新用户资料
func (r *UserProfileRepo) Upsert(ctx context.Context, profile *model.UserProfile) error {
	if r.coll == nil {
		return fmt.Errorf("mongodb not available")
	}

	filter := bson.M{"user_id": profile.UserID}
	update := bson.M{"$set": profile}
	opts := options.UpdateOne().SetUpsert(true)

	_, err := r.coll.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return fmt.Errorf("mongo upsert user profile: %w", err)
	}
	return nil
}

// EnsureIndexes 创建MongoDB索引（已统一由 schema.EnsureMongoCollections 管理）
// Deprecated: 索引定义请在 sql/mongo_init.json 中修改，启动时会自动执行
func (r *UserProfileRepo) EnsureIndexes(ctx context.Context) error {
	return nil // no-op, handled by schema.EnsureMongoCollections
}
