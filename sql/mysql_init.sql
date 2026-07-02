-- MySQL 核心交易表
-- 修改表结构请只改此文件，程序启动时会自动执行

-- 订单表 (OLTP核心)
CREATE TABLE IF NOT EXISTS orders (
    id         BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '订单ID',
    user_id    BIGINT       NOT NULL             COMMENT '用户ID',
    order_no   VARCHAR(64)  NOT NULL             COMMENT '订单编号',
    amount     DECIMAL(12,2) NOT NULL DEFAULT 0  COMMENT '订单金额',
    status     VARCHAR(20)  NOT NULL DEFAULT 'pending' COMMENT '订单状态: pending/paid/shipped/cancelled',
    created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY uk_order_no (order_no),
    INDEX idx_user_id (user_id),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单表';
