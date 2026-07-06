# PostgreSQL 常用命令

> **场景**：高并发业务系统 — 千万级用户、百万级在线，用户数据存储与检索

---

## 一、连接与库管理

```bash
# 连接
psql -h localhost -U postgres -d postgres                                # 命令行连接
psql "postgres://user:pass@localhost:5432/mydb"                          # URI 方式连接

# psql 元命令
\l                                                                       # 看所有库
\l+                                                                      # 库详情（含大小、表空间）
\c mydb                                                                  # 切换库
\conninfo                                                                # 当前连接信息
\q                                                                       # 退出

# SQL 方式
CREATE DATABASE mydb ENCODING 'UTF8';                                    # 创建库
SELECT current_database();                                               # 当前库名
DROP DATABASE mydb;                                                      # 删库（需先切换至其他库）
```

---

## 二、数据表设计

```sql
-- 表操作
\dt                                                                      -- 看所有表
\dt+                                                                     -- 表详情（含大小）
\d users                                                                 -- 看表结构
\d+ users                                                                -- 表详情（含存储、注释）
DROP TABLE users;                                                        -- 删表
ALTER TABLE users RENAME TO old_users;                                   -- 重命名表
```

### 建表示例 — 用户表与用户档案表

```sql
-- ============ 用户基础表 ============
CREATE TABLE users (
    id         BIGSERIAL    PRIMARY KEY,                                  -- 自增主键
    name       VARCHAR(100) NOT NULL,
    email      VARCHAR(200) NOT NULL UNIQUE,                             -- 唯一约束
    age        INT          DEFAULT 0,
    status     SMALLINT     DEFAULT 1,                                   -- 1正常 0禁用
    created_at TIMESTAMP    DEFAULT NOW(),
    updated_at TIMESTAMP    DEFAULT NOW()
);
COMMENT ON TABLE users IS '用户基础表';
COMMENT ON COLUMN users.name IS '用户名';
COMMENT ON COLUMN users.status IS '状态: 1正常 0禁用';

-- ============ 用户档案表（1:1 关联 users）============
CREATE TABLE user_profiles (
    user_id    BIGINT       NOT NULL PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    avatar     VARCHAR(500) DEFAULT '',
    bio        TEXT,
    tags       JSONB,                                                      -- JSONB 支持索引和高效查询
    settings   JSONB,
    created_at TIMESTAMP   DEFAULT NOW(),
    updated_at TIMESTAMP   DEFAULT NOW()
);
COMMENT ON TABLE user_profiles IS '用户档案表';
```

---

## 三、增删改查 (CRUD)

### 3.1 插入

```sql
-- 单条插入 (可 ON CONFLICT 实现 upsert)
INSERT INTO users (name, email, age) VALUES ('张三', 'zhangsan@example.com', 18);

-- 批量插入
INSERT INTO users (name, email, age) VALUES
    ('李四', 'lisi@example.com', 20),
    ('王五', 'wangwu@example.com', 25);

-- 返回插入的行（RETURNING 子句）
INSERT INTO users (name, email) VALUES ('赵六', 'zhaoliu@example.com')
RETURNING id, name, created_at;

-- 插入 user_profiles
INSERT INTO user_profiles (user_id, avatar, bio, tags, settings) VALUES (
    1, '/img/1.jpg',
    '资深后端工程师，专注于高并发系统架构',
    '["go", "backend", "distributed"]'::JSONB,
    '{"theme": "dark", "lang": "zh"}'::JSONB
);

-- 从查询结果插入
INSERT INTO user_profiles (user_id, bio)
SELECT id, '新用户' FROM users
WHERE id NOT IN (SELECT user_id FROM user_profiles);
```

### 3.2 查询

```sql
-- 全查
SELECT * FROM users;
SELECT id, name, email FROM users;                                        -- 投影

-- 等值查询
SELECT * FROM users WHERE name = '张三';

-- 比较运算符
SELECT * FROM users WHERE age > 20;                                      -- 大于20
SELECT * FROM users WHERE age BETWEEN 20 AND 30;                        -- 范围

-- 集合查询
SELECT * FROM users WHERE name IN ('张三', '李四');                       -- IN
SELECT * FROM users WHERE name NOT IN ('张三');                           -- NOT IN

-- 逻辑运算
SELECT * FROM users WHERE age >= 18 AND status = 1;
SELECT * FROM users WHERE name = '张三' OR age < 20;

-- 只取一条
SELECT * FROM users WHERE name = '张三' LIMIT 1;

-- 排序 + 分页
SELECT * FROM users ORDER BY id DESC LIMIT 5;                            -- 最新5条
SELECT * FROM users ORDER BY id LIMIT 3 OFFSET 0;                        -- 分页第1页
SELECT * FROM users ORDER BY id LIMIT 3 OFFSET 3;                        -- 分页第2页

-- 模糊匹配
SELECT * FROM users WHERE name LIKE '张%';                                -- name 以"张"开头
SELECT * FROM users WHERE name LIKE '%张%';                               -- name 含"张"
SELECT * FROM users WHERE email LIKE '%@example.com';                     -- 邮箱后缀
SELECT * FROM users WHERE name ~ '^张';                                   -- 正则（以"张"开头，~ 区分大小写）
SELECT * FROM users WHERE name ~* 'zhang';                                -- 正则不区分大小写

-- JSONB 字段查询
SELECT * FROM user_profiles WHERE tags @> '"go"';                         -- tags 包含 "go"
SELECT * FROM user_profiles WHERE tags @> '["go", "backend"]';            -- 同时包含
SELECT * FROM user_profiles WHERE tags ? 'go';                            -- tags 顶层 key 包含 "go"
SELECT * FROM user_profiles WHERE settings ->> 'theme' = 'dark';          -- settings.theme = 'dark'
SELECT user_id, jsonb_array_length(tags) FROM user_profiles;              -- tags 数组长度

-- GIN 索引加速 JSONB 查询
-- CREATE INDEX idx_tags ON user_profiles USING GIN (tags);
```

### 3.3 更新

```sql
-- 更新单条（RETURNING 返回更新后的行）
UPDATE users SET age = 30, updated_at = NOW()
WHERE name = '张三'
RETURNING id, name, age;

-- 批量更新
UPDATE users SET status = 1;

-- 条件批量更新
UPDATE users SET status = 0 WHERE age < 18;

-- 原子操作
UPDATE users SET age = age + 1 WHERE name = '张三';                       -- age+1

-- Upsert（PostgreSQL: ON CONFLICT ... DO UPDATE）
INSERT INTO users (id, name, email, age) VALUES (1, '张三', 'new@example.com', 25)
ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, age = EXCLUDED.age;
```

### 3.4 删除

```sql
-- 删除单条（RETURNING 返回被删除的行）
DELETE FROM users WHERE name = '张三'
RETURNING id, name;

-- 按条件删除
DELETE FROM users WHERE age < 18;

-- 清空表（比 DELETE 快，WHERE 条件无效）
TRUNCATE TABLE users;
-- TRUNCATE TABLE users RESTART IDENTITY CASCADE;  -- 重置自增ID并级联清空关联表
```

---

## 四、聚合与分组

```sql
-- 计数
SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM users WHERE status = 1;                             -- 条件计数
SELECT COUNT(DISTINCT name) FROM users;                                   -- 去重计数

-- 分组统计 — 按 status 分组计数
SELECT status, COUNT(*) AS cnt
FROM users
GROUP BY status
ORDER BY cnt DESC;

-- 重名排查 — 按 name 分组，找出 count > 1 的
SELECT name, COUNT(*) AS cnt
FROM users
GROUP BY name
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

-- 平均年龄
SELECT AVG(age) FROM users;

-- 分组过滤
SELECT status, COUNT(*) AS cnt
FROM users
GROUP BY status
HAVING COUNT(*) > 1;

-- 联表查询 — users JOIN user_profiles
SELECT u.id, u.name, u.email, p.bio, p.tags
FROM users u
LEFT JOIN user_profiles p ON u.id = p.user_id
WHERE u.status = 1
LIMIT 10;

-- JSONB 数组展开 — 按 tag 统计分布
SELECT tag, COUNT(*) AS cnt
FROM user_profiles, jsonb_array_elements_text(tags) AS tag
GROUP BY tag
ORDER BY cnt DESC;

-- 窗口函数（PostgreSQL 强项）
SELECT name, age,
       ROW_NUMBER() OVER (ORDER BY age DESC) AS rank,
       AVG(age) OVER () AS global_avg
FROM users;
```

---

## 五、事务

```sql
-- 启动事务
BEGIN;
-- 或: BEGIN WORK; / START TRANSACTION;

-- 执行操作
INSERT INTO users (name, email) VALUES ('测试', 'test@example.com');
INSERT INTO user_profiles (user_id, bio) VALUES (currval('users_id_seq'), 'test bio');

-- 提交
COMMIT;

-- 或在失败时回滚
-- ROLLBACK;

-- 保存点
SAVEPOINT sp1;
UPDATE users SET age = 100 WHERE name = '张三';
ROLLBACK TO sp1;                                                         -- 回滚到保存点
COMMIT;
```

---

## 六、索引

```sql
-- 创建索引
CREATE INDEX idx_users_name ON users (name);                              -- 普通索引
CREATE UNIQUE INDEX idx_users_email ON users (email);                     -- 唯一索引
CREATE INDEX idx_users_name_age ON users (name, age);                     -- 复合索引
CREATE INDEX idx_tags ON user_profiles USING GIN (tags);                  -- GIN 索引（JSONB 高效查询）
CREATE INDEX idx_bio_gin ON user_profiles USING GIN (to_tsvector('simple', bio));  -- 全文搜索索引

-- 查看索引
\di                                                                      -- 列出所有索引
SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'users';    -- 查索引定义
\d users                                                                 -- 表结构（含索引）

-- 删除索引
DROP INDEX idx_users_name;

-- 重建索引（消除膨胀）
REINDEX INDEX idx_users_name;
REINDEX TABLE users;
```

---

## 七、实用命令

```sql
-- 统计与状态
SELECT COUNT(*) FROM users;                                               -- 计数
SELECT version();                                                         -- PostgreSQL 版本
SELECT * FROM pg_stat_activity;                                           -- 当前连接/查询
SELECT datname, numbackends FROM pg_stat_database;                        -- 各库连接数
SELECT pg_size_pretty(pg_database_size(current_database()));              -- 当前库大小

-- 执行计划
EXPLAIN SELECT * FROM users WHERE name = '张三';                           -- 查询计划
EXPLAIN ANALYZE SELECT * FROM users WHERE name = '张三';                   -- 实际执行耗时（含 Planning + Execution Time）

-- 配置查看
SHOW shared_buffers;                                                      -- 缓存大小
SHOW work_mem;                                                            -- 工作内存
SHOW max_connections;                                                     -- 最大连接数
SHOW ALL;                                                                 -- 全部配置

-- 表大小统计
SELECT relname, pg_size_pretty(pg_total_relation_size(relid))
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(relid) DESC;

-- 锁信息
SELECT * FROM pg_locks WHERE NOT granted;                                 -- 未获取的锁
```

---

## 八、psql 元命令速查

```bash
\dt           # 列出所有表
\dt+          # 表详情（含大小）
\d users      # 看表结构
\d+ users     # 表详情（含存储、注释、索引）
\di           # 列出所有索引
\dv           # 列出所有视图
\df           # 列出所有函数
\du           # 列出所有用户/角色
\dn           # 列出所有 schema
\l+           # 库详情（含大小、表空间、描述）
\c mydb       # 切换数据库
\conninfo     # 当前连接信息
\x            # 切换扩展显示模式（横向/纵向）
\e            # 打开外部编辑器
\i file.sql   # 执行外部 SQL 文件
\o file.txt   # 输出重定向到文件
\timing       # 切换查询耗时显示
\q            # 退出
```

---

## 九、Demo — 统一业务场景

> 场景：用户管理模块，包含 `users`（基础信息） 和 `user_profiles`（扩展档案） 两张表。
> 以下为同一套业务逻辑在 PostgreSQL 中的实现。

```sql
-- 切换库
\c demo

-- ────────── 基础查询 ──────────

-- 1. 查前 3 条用户
SELECT * FROM users LIMIT 3;

-- 2. 按 ID 查用户
SELECT * FROM users WHERE id = 1;

-- 3. 按姓名模糊搜索
SELECT * FROM users WHERE name LIKE '%周%';

-- 4. 按邮箱后缀模糊
SELECT * FROM users WHERE email LIKE '%@example.com';

-- ────────── 联表与嵌套查询 ──────────

-- 5. 按 user_id 查档案
SELECT * FROM user_profiles WHERE user_id = 1;

-- 6. 查找同时具备 "go" 和 "backend" 标签的用户
SELECT * FROM user_profiles WHERE tags @> '["go", "backend"]';

-- 7. 查找 bio 中包含 "工程师" 的用户
SELECT * FROM user_profiles WHERE bio LIKE '%工程师%';

-- 8. 去重列出所有标签
SELECT DISTINCT tag
FROM user_profiles, jsonb_array_elements_text(tags) AS tag;

-- ────────── 分页 ──────────

-- 9. 分页第1页（每页3条）
SELECT id, name, email FROM users ORDER BY id LIMIT 3 OFFSET 0;

-- 10. 分页第2页
SELECT id, name, email FROM users ORDER BY id LIMIT 3 OFFSET 3;

-- ────────── 聚合统计 ──────────

-- 11. 用户总数
SELECT COUNT(*) AS total_users FROM users;

-- 12. 重名排查
SELECT name, COUNT(*) AS cnt
FROM users
GROUP BY name
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

-- 13. 按状态分组统计
SELECT status, COUNT(*) AS cnt
FROM users
GROUP BY status
ORDER BY status;

-- 14. 用户 + 档案联表查询
SELECT u.id, u.name, u.email, p.bio, p.tags
FROM users u
LEFT JOIN user_profiles p ON u.id = p.user_id
LIMIT 5;
```

---

## 十、PostgreSQL vs MongoDB vs MySQL 对照速查

| 概念 | PostgreSQL | MongoDB | MySQL |
|------|------------|---------|-------|
| 数据库 | Database | Database | Database |
| 表 | TABLE | Collection | TABLE |
| 行 | Row | Document (BSON) | Row |
| 主键 | `PRIMARY KEY` (SERIAL) | `_id` (ObjectId) | `PRIMARY KEY` (自增) |
| JSON | JSONB (二进制,可索引) | 原生 BSON | JSON |
| 范式 | 严格范式 | 嵌入式/引用 | 严格范式 |
| JOIN | `JOIN ... ON` | `$lookup` | `JOIN ... ON` |
| 事务 | BEGIN | Session.startTransaction() | START TRANSACTION |
| 水平扩展 | 分区表 / Citus | 原生分片 (Sharding) | 分库分表中间件 |
| 特色 | 窗口函数/CTE/全文搜索/PostGIS | 文档模型/灵活Schema/聚合管道 | 插件生态/分库分表成熟方案 |
