# MongoDB 常用命令

> **场景**：高并发业务系统 — 千万级用户、百万级在线，用户数据存储与检索

---

## 一、连接与库管理

```bash
# 连接
mongosh "mongodb://localhost:27017"
mongosh "mongodb://user:pass@host:27017/mydb?authSource=admin"

# 库操作
show dbs                                   # 看所有库
use mydb                                   # 切换/隐式创建库（插入首条数据后生效）
db.createCollection("_init")               # 显式创建库（立即在 show dbs 可见）
db                                         # 当前库名
db.dropDatabase()                          # 删库
db.stats()                                 # 库统计（大小/文档数/索引数）
```

---

## 二、集合与文档设计

```bash
# 集合操作
show collections                           # 看所有集合
db.users.drop()                            # 删集合
db.user_profiles.drop()                    # 删集合
db.users.stats()                           # 集合统计
db.users.renameCollection("old_users")    # 重命名集合
```

### 建集合示例 — 用户表与用户档案表

```js
// ============ 用户基础表 ============
db.createCollection("users", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["name", "email", "created_at"],
      properties: {
        name:       { bsonType: "string", description: "用户名，不可为空" },
        email:      { bsonType: "string", description: "邮箱，全局唯一" },
        age:        { bsonType: "int", minimum: 0, description: "年龄" },
        status:     { bsonType: "int", description: "状态: 1正常 0禁用" },
        created_at: { bsonType: "date" },
        updated_at: { bsonType: "date" }
      }
    }
  }
})

// ============ 用户档案表（1:1 关联 users）============
db.createCollection("user_profiles", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["user_id", "created_at"],
      properties: {
        user_id:    { bsonType: "long", description: "关联 users 的 _id" },
        avatar:     { bsonType: "string" },
        bio:        { bsonType: "string", description: "个人简介" },
        tags:       { bsonType: "array", description: "标签列表" },
        settings:   { bsonType: "object", description: "JSON 配置" },
        created_at: { bsonType: "date" },
        updated_at: { bsonType: "date" }
      }
    }
  }
})

// 唯一索引：email
db.users.createIndex({ email: 1 }, { unique: true })

// user_profiles 外键关联（逻辑外键，非数据库级约束）
db.user_profiles.createIndex({ user_id: 1 }, { unique: true })
```

---

## 三、增删改查 (CRUD)

### 3.1 插入

```js
// 单条插入
db.users.insertOne({
  name: "张三", email: "zhangsan@example.com",
  age: 18, status: 1,
  created_at: new Date(), updated_at: new Date()
})

// 批量插入
db.users.insertMany([
  { name: "李四", email: "lisi@example.com", age: 20, status: 1, created_at: new Date(), updated_at: new Date() },
  { name: "王五", email: "wangwu@example.com", age: 25, status: 1, created_at: new Date(), updated_at: new Date() }
])

// 插入 user_profiles
db.user_profiles.insertOne({
  user_id: NumberLong(1),
  avatar: "/img/1.jpg",
  bio: "资深后端工程师，专注于高并发系统架构",
  tags: ["go", "backend", "distributed"],
  settings: { theme: "dark", lang: "zh" },
  created_at: new Date(), updated_at: new Date()
})
```

### 3.2 查询

```js
// 全查
db.users.find()
db.users.find({}, { name: 1, email: 1, _id: 0 })  // 投影（只返回指定字段）

// 等值查询
db.users.find({ name: "张三" })

// 比较运算符
db.users.find({ age: { $gt: 20 } })                 // 大于20
db.users.find({ age: { $gte: 20, $lte: 30 } })      // 范围 20到30 (类似 BETWEEN)

// 集合查询
db.users.find({ name: { $in: ["张三", "李四"] } })   // IN
db.users.find({ name: { $nin: ["张三"] } })          // NOT IN

// 逻辑运算
db.users.find({ $and: [{ age: { $gte: 18 } }, { status: 1 }] })
db.users.find({ $or: [{ name: "张三" }, { age: { $lt: 20 } }] })

// 只取一条
db.users.findOne({ name: "张三" })

// 排序 + 分页
db.users.find().sort({ _id: -1 }).limit(5)           // 最新5条
db.users.find().sort({ _id: -1 }).skip(0).limit(3)   // 分页第1页
db.users.find().sort({ _id: -1 }).skip(3).limit(3)   // 分页第2页

// 正则查询
db.users.find({ name: /张/ })                        // name 包含"张"
db.users.find({ bio: { $regex: "工程师" } })          // bio 含"工程师"

// 数组查询 (user_profiles.tags)
db.user_profiles.find({ tags: "go" })                // tags 包含 "go"
db.user_profiles.find({ tags: { $all: ["go", "backend"] } })  // 同时包含 go 和 backend
db.user_profiles.find({ tags: { $size: 3 } })        // tags 数组长度=3

// 嵌套对象查询 (user_profiles.settings)
db.user_profiles.find({ "settings.theme": "dark" })  // settings 内 theme=dark

// 字段存在性
db.users.find({ email: { $exists: true } })           // 有 email 字段的文档
```

### 3.3 更新

```js
// 更新单条
db.users.updateOne(
  { name: "张三" },
  { $set: { age: 30, updated_at: new Date() } }
)

// 批量更新
db.users.updateMany(
  {},
  { $set: { status: 1 } }
)

// 条件批量更新
db.users.updateMany(
  { age: { $lt: 18 } },
  { $set: { status: 0 } }
)

// 原子增减
db.users.updateOne({ name: "张三" }, { $inc: { age: 1 } })   // age+1

// Upsert（有则更新，无则插入）
db.users.updateOne(
  { email: "new@example.com" },
  { $set: { name: "新人", age: 22, created_at: new Date() } },
  { upsert: true }
)
```

### 3.4 删除

```js
// 删除单条
db.users.deleteOne({ name: "张三" })

// 按条件删除
db.users.deleteMany({ age: { $lt: 18 } })

// 清空集合（比逐条删除快，但保留索引）
db.users.deleteMany({})
```

---

## 四、聚合管道

```js
// 计数
db.users.countDocuments()
db.users.countDocuments({ status: 1 })                // 条件计数
db.users.distinct("name")                              // 去重值列表

// 分组统计 — 按 status 分组计数
db.users.aggregate([
  { $group: { _id: "$status", count: { $sum: 1 } } },
  { $sort: { count: -1 } }
])

// 重名排查 — 按 name 分组，找出 count > 1 的
db.users.aggregate([
  { $group: { _id: "$name", count: { $sum: 1 } } },
  { $match: { count: { $gt: 1 } } },
  { $sort: { count: -1 } }
])

// 平均年龄
db.users.aggregate([
  { $group: { _id: null, avg_age: { $avg: "$age" } } }
])

// 联表查询 — users JOIN user_profiles ($lookup)
db.users.aggregate([
  { $match: { status: 1 } },
  { $lookup: {
      from: "user_profiles",
      localField: "_id",
      foreignField: "user_id",
      as: "profile"
    }
  },
  { $unwind: { path: "$profile", preserveNullAndEmptyArrays: true } },
  { $project: { name: 1, email: 1, "profile.bio": 1, "profile.tags": 1 } },
  { $limit: 10 }
])

// 数组展开 — 按 tag 统计分布
db.user_profiles.aggregate([
  { $unwind: "$tags" },
  { $group: { _id: "$tags", count: { $sum: 1 } } },
  { $sort: { count: -1 } }
])

// 管道运算符速查
// $match   → WHERE
// $group   → GROUP BY
// $sort    → ORDER BY
// $limit   → LIMIT
// $skip    → OFFSET
// $project → SELECT
// $lookup  → JOIN
// $unwind  → 展开数组
// $addFields → 添加字段
```

---

## 五、索引

```js
// 创建索引
db.users.createIndex({ name: 1 })                     // 单字段升序索引
db.users.createIndex({ email: 1 }, { unique: true })   // 唯一索引
db.users.createIndex({ name: 1, age: -1 })             // 复合索引（name升序+age降序）
db.users.createIndex({ name: "text", bio: "text" })    // 全文索引（支持 $text 查询）

// 查看索引
db.users.getIndexes()                                  // 列出所有索引

// 删除索引
db.users.dropIndex("name_1")                           // 按名称删除
db.users.dropIndexes()                                 // 删除除 _id 外的所有索引

// 执行计划
db.users.find({ name: "张三" }).explain("executionStats") // 查看执行统计
```

---

## 六、事务（多文档 ACID）

```js
// MongoDB 4.0+ 支持多文档事务 (Replica Set 环境)
const session = db.getMongo().startSession()
session.startTransaction()
try {
  const usersCol = session.getDatabase("demo").users
  const profilesCol = session.getDatabase("demo").user_profiles

  usersCol.insertOne({ name: "测试", email: "test@example.com" })
  profilesCol.insertOne({ user_id: NumberLong(100), bio: "test" })

  session.commitTransaction()  // 提交
} catch (err) {
  session.abortTransaction()   // 回滚
} finally {
  session.endSession()
}
```

---

## 七、实用命令

```js
// 统计信息
db.users.countDocuments()                              // 文档计数
db.users.estimatedDocumentCount()                      // 快速估算（基于元数据）
db.serverStatus()                                      // 服务器状态
db.version()                                           // MongoDB 版本

// 执行计划
db.users.find({ name: "张三" }).explain()               // Query Planner 分析
db.users.find({ name: "张三" }).explain("executionStats") // 含实际执行耗时

// 集合管理
db.users.stats()                                       // 集合统计（大小/索引）
db.users.validate({ full: true })                      // 验证集合完整性

// 当前连接
db.currentOp()                                         // 当前操作
db.serverStatus().connections                          // 连接数统计

// 副本集状态
rs.status()                                            // 副本集状态
rs.conf()                                              // 副本集配置
```

---

## 八、Demo — 统一业务场景

> 场景：用户管理模块，包含 `users`（基础信息） 和 `user_profiles`（扩展档案） 两张表/集合。
> 以下为同一套业务逻辑在 MongoDB 中的实现。

```js
// 切换库
use demo

// ────────── 基础查询 ──────────

// 1. 查前 3 条用户
db.users.find().limit(3)

// 2. 按 ID 查用户
db.users.findOne({ _id: ObjectId("...") })
// 或用自增 ID 字段
db.users.findOne({ id: 1 })

// 3. 按姓名模糊搜索（正则）
db.users.find({ name: /周/ })

// 4. 按邮箱后缀模糊
db.users.find({ email: /@example\.com$/ })

// ────────── 联表与嵌套查询 ──────────

// 5. 按 user_id 查档案
db.user_profiles.findOne({ user_id: NumberLong(1) })

// 6. 查找同时具备 "go" 和 "backend" 标签的用户
db.user_profiles.find({ tags: { $all: ["go", "backend"] } })

// 7. 查找 bio 中包含 "工程师" 的用户
db.user_profiles.find({ bio: { $regex: "工程师" } })

// 8. 去重列出所有标签
db.user_profiles.distinct("tags")

// ────────── 分页 ──────────

// 9. 分页第1页（每页3条，按 id 排序）
db.users.find().sort({ _id: 1 }).skip(0).limit(3)

// 10. 分页第2页
db.users.find().sort({ _id: 1 }).skip(3).limit(3)

// ────────── 聚合统计 ──────────

// 11. 用户总数
db.users.countDocuments()

// 12. 重名排查
db.users.aggregate([
  { $group: { _id: "$name", count: { $sum: 1 } } },
  { $match: { count: { $gt: 1 } } },
  { $sort: { count: -1 } }
])

// 13. 按状态分组统计
db.users.aggregate([
  { $group: { _id: "$status", count: { $sum: 1 } } }
])

// 14. 用户 + 档案联表查询
db.users.aggregate([
  { $lookup: {
      from: "user_profiles",
      localField: "_id",
      foreignField: "user_id",
      as: "profile"
    }
  },
  { $unwind: { path: "$profile", preserveNullAndEmptyArrays: true } },
  { $project: { name: 1, email: 1, "profile.bio": 1, "profile.tags": 1 } },
  { $limit: 5 }
])
```

---

## 九、MongoDB vs MySQL vs PostgreSQL 对照速查

| 概念 | MongoDB | MySQL | PostgreSQL |
|------|---------|-------|------------|
| 数据库 | Database | Database | Database |
| 表 | Collection | TABLE | TABLE |
| 行 | Document (BSON) | Row | Row |
| 主键 | `_id` (ObjectId) | `PRIMARY KEY` (自增) | `PRIMARY KEY` (SERIAL) |
| JSON | 原生 BSON | JSON | JSONB |
| 范式 | 嵌入式/引用 | 严格范式 | 严格范式 |
| JOIN | `$lookup` | `JOIN ... ON` | `JOIN ... ON` |
| 事务 | Session.startTransaction() | START TRANSACTION | BEGIN |
| 水平扩展 | 原生分片 (Sharding) | 分库分表中间件 | 分区表 / Citus |
