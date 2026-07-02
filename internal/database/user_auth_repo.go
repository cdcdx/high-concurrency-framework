package database

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/cdcdx/high-concurrency-framework/internal/model"
)

// UserAuthRepo 用户认证数据访问层 (MySQL)
type UserAuthRepo struct {
	db *RWDB
}

// NewUserAuthRepo 创建用户认证仓库
func NewUserAuthRepo(db *RWDB) *UserAuthRepo {
	return &UserAuthRepo{db: db}
}

// InsertUser 注册用户 (写入 Master)
func (r *UserAuthRepo) InsertUser(ctx context.Context, user *model.User) (uint64, error) {
	result, err := r.db.ExecContext(ctx,
		`INSERT INTO users (username, password_hash, email, phone, status)
		 VALUES (?, ?, ?, ?, ?)`,
		user.Username, user.PasswordHash, user.Email, user.Phone, user.Status,
	)
	if err != nil {
		return 0, fmt.Errorf("insert user: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get last insert id: %w", err)
	}
	return uint64(id), nil
}

// GetByUsername 根据用户名查询用户 (读取 Replica)
func (r *UserAuthRepo) GetByUsername(ctx context.Context, username string) (*model.User, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT id, username, password_hash, email, phone, status,
		        COALESCE(last_login_at, '') as last_login_at, created_at, updated_at
		 FROM users WHERE username = ?`, username,
	)

	var user model.User
	var lastLoginAt sql.NullString
	err := row.Scan(
		&user.ID, &user.Username, &user.PasswordHash, &user.Email, &user.Phone,
		&user.Status, &lastLoginAt, &user.CreatedAt, &user.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query user by username: %w", err)
	}
	if lastLoginAt.Valid {
		user.LastLoginAt = &lastLoginAt.String
	}
	return &user, nil
}

// GetByEmail 根据邮箱查询用户 (读取 Replica)
func (r *UserAuthRepo) GetByEmail(ctx context.Context, email string) (*model.User, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT id, username, password_hash, email, phone, status,
		        COALESCE(last_login_at, '') as last_login_at, created_at, updated_at
		 FROM users WHERE email = ?`, email,
	)

	var user model.User
	var lastLoginAt sql.NullString
	err := row.Scan(
		&user.ID, &user.Username, &user.PasswordHash, &user.Email, &user.Phone,
		&user.Status, &lastLoginAt, &user.CreatedAt, &user.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query user by email: %w", err)
	}
	if lastLoginAt.Valid {
		user.LastLoginAt = &lastLoginAt.String
	}
	return &user, nil
}

// GetByID 根据用户ID查询用户 (读取 Replica)
func (r *UserAuthRepo) GetByID(ctx context.Context, userID uint64) (*model.User, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT id, username, password_hash, email, phone, status,
		        COALESCE(last_login_at, '') as last_login_at, created_at, updated_at
		 FROM users WHERE id = ?`, userID,
	)

	var user model.User
	var lastLoginAt sql.NullString
	err := row.Scan(
		&user.ID, &user.Username, &user.PasswordHash, &user.Email, &user.Phone,
		&user.Status, &lastLoginAt, &user.CreatedAt, &user.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query user by id: %w", err)
	}
	if lastLoginAt.Valid {
		user.LastLoginAt = &lastLoginAt.String
	}
	return &user, nil
}

// UpdateLastLoginAt 更新最后登录时间 (写入 Master)
func (r *UserAuthRepo) UpdateLastLoginAt(ctx context.Context, userID uint64) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE users SET last_login_at = NOW() WHERE id = ?`, userID,
	)
	return err
}
