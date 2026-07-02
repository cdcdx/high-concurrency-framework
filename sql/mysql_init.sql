-- MySQL 核心表
-- 修改表结构请只改此文件，程序启动时会自动执行

-- 用户认证表 (JWT登录)
CREATE TABLE IF NOT EXISTS users (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '用户ID',
    username        VARCHAR(64)  NOT NULL             COMMENT '用户名',
    password_hash   VARCHAR(255) NOT NULL             COMMENT 'bcrypt密码哈希',
    email           VARCHAR(128) NOT NULL DEFAULT ''  COMMENT '邮箱',
    phone           VARCHAR(20)  NOT NULL DEFAULT ''  COMMENT '手机号',
    status          TINYINT      NOT NULL DEFAULT 1   COMMENT '状态: 1=正常 0=禁用',
    last_login_at   DATETIME     NULL                 COMMENT '最后登录时间',
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY uk_username (username),
    UNIQUE KEY uk_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户认证表';

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
