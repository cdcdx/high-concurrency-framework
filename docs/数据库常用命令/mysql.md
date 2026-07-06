# MySQL 常用命令

> **场景**：高并发业务系统 — 千万级用户、百万级在线，用户数据存储与检索

---

## 一、连接与库管理

```bash
# 连接
mysql -h localhost -u root -p
mysql -h localhost -u root -p -D mydb                                    # 直接指定库
mysql -u root -p -e "SELECT 1"                                           # 执行单条命令后退出

# 库操作
SHOW DATABASES;                                                          # 看所有库
USE mydb;                                                                # 切换库
CREATE DATABASE demo CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;   # 创建库
SELECT DATABASE();                                                       # 当前库名
DROP DATABASE mydb;                                                      # 删库
```

---

## 二、数据表设计

```sql
-- 表操作
SHOW TABLES;                                                             -- 看所有表
DESC users;                                                              -- 看表结构
SHOW CREATE TABLE users;                                                 -- 看建表语句
DROP TABLE users;                                                        -- 删表
RENAME TABLE users TO old_users;                                        -- 重命名表
```

### 建表示例 — 用户表与用户档案表

```sql
-- ============ 用户基础表 ============
CREATE TABLE users (
    id         BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '用户ID',
    name       VARCHAR(100)  NOT NULL             COMMENT '用户名',
    email      VARCHAR(200)  NOT NULL             COMMENT '邮箱，全局唯一',
    age        INT           DEFAULT 0            COMMENT '年龄',
    status     TINYINT       DEFAULT 1            COMMENT '状态: 1正常 0禁用',
    created_at DATETIME      DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_email (email),
    INDEX idx_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户基础表';

-- ============ 用户档案表（1:1 关联 users）============
CREATE TABLE user_profiles (
    user_id    BIGINT       NOT NULL PRIMARY KEY  COMMENT '关联 users.id',
    avatar     VARCHAR(500) DEFAULT ''            COMMENT '头像URL',
    bio        TEXT                               COMMENT '个人简介',
    tags       JSON                               COMMENT '标签列表',
    settings   JSON                               COMMENT 'JSON配置',
    created_at DATETIME     DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户档案表';
```

---

## 三、增删改查 (CRUD)

### 3.1 插入

```sql
-- 单条插入
INSERT INTO users (name, email, age) VALUES ('张三', 'zhangsan@example.com', 18);

-- 批量插入
INSERT INTO users (name, email, age) VALUES
    ('李四', 'lisi@example.com', 20),
    ('王五', 'wangwu@example.com', 25);

-- 插入 user_profiles
INSERT INTO user_profiles (user_id, avatar, bio, tags, settings) VALUES (
    1, '/img/1.jpg',
    '资深后端工程师，专注于高并发系统架构',
    '["go", "backend", "distributed"]',
    '{"theme": "dark", "lang": "zh"}'
);

-- 从查询结果插入（INSERT ... SELECT）
INSERT INTO user_profiles (user_id, bio)
SELECT id, '新用户' FROM users WHERE id NOT IN (SELECT user_id FROM user_profiles);
```

### 3.2 查询

```sql
-- 全查
SELECT * FROM users;
SELECT id, name, email FROM users;                                        -- 投影（只返回指定列）

-- 等值查询
SELECT * FROM users WHERE name = '张三';

-- 比较运算符
SELECT * FROM users WHERE age > 20;                                      -- 大于20
SELECT * FROM users WHERE age BETWEEN 20 AND 30;                        -- 范围 20到30
SELECT * FROM users WHERE age >= 20 AND age <= 30;                      -- 等同上一条

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
SELECT * FROM users ORDER BY id LIMIT 0, 3;                              -- 分页第1页 (OFFSET=0, LIMIT=3)
SELECT * FROM users ORDER BY id LIMIT 3, 3;                              -- 分页第2页 (OFFSET=3, LIMIT=3)
-- 或标准语法: SELECT * FROM users ORDER BY id LIMIT 3 OFFSET 3;

-- 模糊匹配
SELECT * FROM users WHERE name LIKE '张%';                                -- name 以"张"开头
SELECT * FROM users WHERE name LIKE '%张%';                               -- name 含"张" （% 任意多字符, _ 单字符）
SELECT * FROM users WHERE email LIKE '%@example.com';                     -- 邮箱后缀
SELECT * FROM users WHERE name REGEXP '^张';                              -- 正则（以"张"开头）

-- JSON 字段查询 (user_profiles.tags, settings)
SELECT * FROM user_profiles WHERE JSON_CONTAINS(tags, '"go"');            -- tags 包含 "go"
SELECT * FROM user_profiles WHERE JSON_CONTAINS(tags, '["go","backend"]'); -- 同时包含
SELECT * FROM user_profiles WHERE JSON_EXTRACT(settings, '$.theme') = 'dark'; -- settings.theme=dark
SELECT user_id, JSON_LENGTH(tags) FROM user_profiles;                    -- tags 数组长度
```

### 3.3 更新

```sql
-- 更新单条
UPDATE users SET age = 30, updated_at = NOW() WHERE name = '张三';

-- 批量更新
UPDATE users SET status = 1;

-- 条件批量更新
UPDATE users SET status = 0 WHERE age < 18;

-- 原子操作（数值型）
UPDATE users SET age = age + 1 WHERE name = '张三';                       -- age+1

-- Upsert（MySQL: INSERT ... ON DUPLICATE KEY UPDATE）
INSERT INTO users (id, name, email, age) VALUES (1, '张三', 'new@example.com', 25)
ON DUPLICATE KEY UPDATE email = VALUES(email), age = VALUES(age);
```

### 3.4 删除

```sql
-- 删除单条
DELETE FROM users WHERE name = '张三';

-- 按条件删除
DELETE FROM users WHERE age < 18;

-- 清空表（重置自增ID，比 DELETE 快）
TRUNCATE TABLE users;
```

---

## 四、聚合与分组

```sql
-- 计数
SELECT COUNT(*) FROM users;                                              -- 行数
SELECT COUNT(*) FROM users WHERE status = 1;                            -- 条件计数
SELECT COUNT(DISTINCT name) FROM users;                                  -- 去重计数

-- 分组统计 — 按 status 分组计数
SELECT status, COUNT(*) AS cnt
FROM users
GROUP BY status
ORDER BY cnt DESC;

-- 重名排查 — 按 name 分组，找出 count > 1 的
SELECT name, COUNT(*) AS cnt
FROM users
GROUP BY name
HAVING cnt > 1
ORDER BY cnt DESC;

-- 平均年龄
SELECT AVG(age) FROM users;

-- 分组过滤（HAVING）
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

-- JSON 数组展开（MySQL 8.0+ JSON_TABLE，仅限于已知 key 结构）
SELECT user_id, tag
FROM user_profiles,
JSON_TABLE(tags, '$[*]' COLUMNS (tag VARCHAR(50) PATH '$')) AS jt;
```

---

## 五、事务

```sql
-- 启动事务
START TRANSACTION;
-- 或: BEGIN;

-- 执行操作
INSERT INTO users (name, email) VALUES ('测试', 'test@example.com');
INSERT INTO user_profiles (user_id, bio) VALUES (LAST_INSERT_ID(), 'test bio');

-- 提交
COMMIT;

-- 或在失败时回滚
-- ROLLBACK;

-- 设置保存点
SAVEPOINT sp1;
UPDATE users SET age = 100 WHERE name = '张三';
ROLLBACK TO sp1;                         -- 回滚到保存点，保留之前操作
```

---

## 六、索引

```sql
-- 创建索引
CREATE INDEX idx_users_name ON users (name);                              -- 普通索引
CREATE UNIQUE INDEX idx_users_email ON users (email);                     -- 唯一索引
CREATE INDEX idx_users_name_age ON users (name, age);                     -- 复合索引
CREATE FULLTEXT INDEX idx_users_bio ON user_profiles (bio);               -- 全文索引

-- 查看索引
SHOW INDEX FROM users;                                                    -- 列出所有索引
-- 或: SELECT * FROM information_schema.statistics WHERE table_name='users';

-- 删除索引
DROP INDEX idx_users_name ON users;
```

---

## 七、实用命令

```sql
-- 统计与状态
SELECT COUNT(*) FROM users;                                               -- 计数
SELECT VERSION();                                                         -- MySQL 版本
SHOW STATUS;                                                              -- 服务器状态
SHOW TABLE STATUS;                                                        -- 表状态（行数/大小）
SHOW PROCESSLIST;                                                         -- 当前连接/查询

-- 执行计划
EXPLAIN SELECT * FROM users WHERE name = '张三';                           -- 查询计划
EXPLAIN ANALYZE SELECT * FROM users WHERE name = '张三';                   -- 实际执行耗时 (8.0.18+)

-- 配置查看
SHOW VARIABLES LIKE 'char%';                                              -- 字符集配置
SHOW VARIABLES LIKE 'innodb%';                                            -- InnoDB 配置
SHOW ENGINES;                                                             -- 存储引擎

-- 慢查询
SHOW VARIABLES LIKE 'slow_query%';                                        -- 慢查询配置
SHOW VARIABLES LIKE 'long_query_time';                                    -- 慢查询阈值

-- 用户管理
SELECT USER();                                                            -- 当前用户
SHOW GRANTS;                                                              -- 当前用户权限
```

---

## 八、Demo — 统一业务场景

> 场景：用户管理模块，包含 `users`（基础信息） 和 `user_profiles`（扩展档案） 两张表。
> 以下为同一套业务逻辑在 MySQL 中的实现。

```sql
-- 切换库
USE demo;

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
SELECT * FROM user_profiles
WHERE JSON_CONTAINS(tags, '["go", "backend"]');

-- 7. 查找 bio 中包含 "工程师" 的用户
SELECT * FROM user_profiles WHERE bio LIKE '%工程师%';

-- 8. 去重列出所有标签（需展开 JSON 数组）
SELECT DISTINCT tag
FROM user_profiles,
JSON_TABLE(tags, '$[*]' COLUMNS (tag VARCHAR(50) PATH '$')) AS jt;

-- ────────── 分页 ──────────

-- 9. 分页第1页（每页3条）
SELECT id, name, email FROM users ORDER BY id LIMIT 0, 3;

-- 10. 分页第2页
SELECT id, name, email FROM users ORDER BY id LIMIT 3, 3;

-- ────────── 聚合统计 ──────────

-- 11. 用户总数
SELECT COUNT(*) AS total_users FROM users;

-- 12. 重名排查
SELECT name, COUNT(*) AS cnt
FROM users
GROUP BY name
HAVING cnt > 1
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

## 九、MySQL vs MongoDB vs PostgreSQL 对照速查

| 概念 | MySQL | MongoDB | PostgreSQL |
|------|-------|---------|------------|
| 数据库 | Database | Database | Database |
| 表 | TABLE | Collection | TABLE |
| 行 | Row | Document (BSON) | Row |
| 主键 | `PRIMARY KEY` (自增) | `_id` (ObjectId) | `PRIMARY KEY` (SERIAL) |
| JSON | JSON | 原生 BSON | JSONB |
| 范式 | 严格范式 | 嵌入式/引用 | 严格范式 |
| JOIN | `JOIN ... ON` | `$lookup` | `JOIN ... ON` |
| 事务 | START TRANSACTION | Session.startTransaction() | BEGIN |
| 水平扩展 | 分库分表中间件 | 原生分片 (Sharding) | 分区表 / Citus |
