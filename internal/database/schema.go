package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.uber.org/zap"
)

// SQLDir SQL/NoSQL 初始化脚本目录（所有表/集合结构的唯一权威来源）
const SQLDir = "sql"

// ==========================================
// 修改数据库表结构：只需编辑 sql/ 目录下的文件
//   sql/mysql_init.sql      → MySQL `orders` 表
//   sql/postgresql_init.sql → PostgreSQL 分析表
//   sql/mongo_init.json     → MongoDB 集合与索引
// 程序启动时会自动执行建表 + 种子数据
// ==========================================

// EnsureAllSQLTables 统一初始化所有 SQL 数据库的表结构 (DDL → Master)
func EnsureAllSQLTables(ctx context.Context, mysqlDB, pgDB *RWDB, logger *zap.SugaredLogger) {
	if mysqlDB != nil && !mysqlDB.IsNil() {
		if err := execSQLFile(ctx, mysqlDB.Master(), "MySQL", filepath.Join(SQLDir, "mysql_init.sql"), logger); err != nil {
			logger.Warnw("mysql ensure tables failed", "err", err)
		} else {
			logger.Infow("mysql tables ready")
		}
	}

	if pgDB != nil && !pgDB.IsNil() {
		if err := execSQLFile(ctx, pgDB.Master(), "PostgreSQL", filepath.Join(SQLDir, "postgresql_init.sql"), logger); err != nil {
			logger.Warnw("postgres ensure tables failed", "err", err)
		} else {
			logger.Infow("postgres tables ready")
		}
	}
}

// mongoInitSchema JSON 文件中定义的 MongoDB 集合和索引结构
type mongoInitSchema struct {
	Collections map[string]struct {
		Comment string `json:"comment"`
		Indexes []struct {
			Keys    map[string]int `json:"keys"`
			Unique  bool           `json:"unique"`
			Comment string         `json:"comment"`
		} `json:"indexes"`
	} `json:"collections"`
}

// EnsureMongoCollections 统一初始化 MongoDB 集合索引（从 sql/mongo_init.json 读取）
func EnsureMongoCollections(ctx context.Context, mongoDB *mongo.Database, logger *zap.SugaredLogger) {
	if mongoDB == nil {
		return
	}

	data, err := os.ReadFile(filepath.Join(SQLDir, "mongo_init.json"))
	if err != nil {
		logger.Warnw("mongo read init file failed", "err", err)
		return
	}

	var schema mongoInitSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		logger.Warnw("mongo parse init file failed", "err", err)
		return
	}

	for collName, collDef := range schema.Collections {
		coll := mongoDB.Collection(collName)
		var indexes []mongo.IndexModel
		for _, idx := range collDef.Indexes {
			keys := bson.D{}
			for field, dir := range idx.Keys {
				keys = append(keys, bson.E{Key: field, Value: dir})
			}
			im := mongo.IndexModel{Keys: keys}
			if idx.Unique {
				im.Options = options.Index().SetUnique(true)
			}
			indexes = append(indexes, im)
		}
		if _, err := coll.Indexes().CreateMany(ctx, indexes); err != nil {
			logger.Warnw("mongo create indexes failed", "collection", collName, "err", err)
		} else {
			logger.Infow("mongo indexes ready", "collection", collName, "index_count", len(indexes))
		}
	}
}

// execSQLFile 读取 SQL 文件并逐条执行
func execSQLFile(ctx context.Context, db *sql.DB, dbName, filePath string, logger *zap.SugaredLogger) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read %s: %w", filePath, err)
	}

	statements := splitSQL(string(data))
	for i, stmt := range statements {
		trimmed := strings.TrimSpace(stmt)
		if trimmed == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, trimmed); err != nil {
			return fmt.Errorf("%s statement #%d: %w\n  SQL: %s", dbName, i+1, err, shortSQL(trimmed))
		}
	}
	return nil
}

// splitSQL 按分号拆分 SQL 语句，保留多行语句
func splitSQL(sql string) []string {
	var statements []string
	var current strings.Builder
	inString := false

	for _, ch := range sql {
		if ch == '\'' {
			inString = !inString
		}
		if ch == ';' && !inString {
			statements = append(statements, current.String())
			current.Reset()
			continue
		}
		current.WriteRune(ch)
	}
	if remaining := strings.TrimSpace(current.String()); remaining != "" {
		statements = append(statements, remaining)
	}
	return statements
}

// shortSQL 截断 SQL 用于日志展示
func shortSQL(s string) string {
	s = strings.TrimSpace(s)
	// 只取第一行，最多80字符
	if idx := strings.Index(s, "\n"); idx > 0 {
		s = s[:idx]
	}
	if len(s) > 80 {
		return s[:80] + "..."
	}
	return s
}
