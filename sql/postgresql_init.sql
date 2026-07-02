-- PostgreSQL 分析层表结构
-- 场景: 订单时序分析、日度报表、用户行为聚合

-- 订单日度统计表
CREATE TABLE IF NOT EXISTS orders_daily_stats (
    id            BIGSERIAL PRIMARY KEY,
    stat_date     DATE           NOT NULL,
    total_orders  BIGINT         NOT NULL DEFAULT 0,
    total_amount  DECIMAL(14,2)  NOT NULL DEFAULT 0,
    avg_amount    DECIMAL(12,2)  NOT NULL DEFAULT 0,
    paid_orders   BIGINT         NOT NULL DEFAULT 0,
    cancelled     BIGINT         NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    UNIQUE(stat_date)
);

CREATE INDEX IF NOT EXISTS idx_ods_stat_date ON orders_daily_stats(stat_date DESC);

-- 用户行为日志表 (时序数据, 按日分区推荐使用 TimescaleDB hybertable)
CREATE TABLE IF NOT EXISTS user_behavior_log (
    id            BIGSERIAL,
    user_id       BIGINT         NOT NULL,
    event_type    VARCHAR(32)   NOT NULL,   -- order_create / profile_update / search
    event_data    JSONB          DEFAULT '{}',
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ubl_user_time ON user_behavior_log(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ubl_type_time ON user_behavior_log(event_type, created_at DESC);

-- -- 插入测试数据
-- INSERT INTO orders_daily_stats (stat_date, total_orders, total_amount, avg_amount, paid_orders, cancelled)
-- VALUES
--     ('2026-06-28', 1523, 458230.50, 300.87, 1420, 45),
--     ('2026-06-29', 1680, 502100.00, 298.87, 1580, 52),
--     ('2026-06-30', 2105, 635800.20, 302.04, 1980, 60),
--     ('2026-07-01',  856, 258120.00, 301.54,  810, 25)
-- ON CONFLICT (stat_date) DO NOTHING;
