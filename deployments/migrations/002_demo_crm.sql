-- 演示用 CRM 业务库（同步源）
-- customer 1:N contact，均带 updated_at 水位列与 is_deleted 软删除标记
CREATE DATABASE IF NOT EXISTS crm
  DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

USE crm;

CREATE TABLE IF NOT EXISTS customer (
  id            BIGINT        NOT NULL AUTO_INCREMENT,
  -- 业务唯一键，跨库同步的幂等依据（不能用自增 id）
  customer_no   VARCHAR(32)   NOT NULL,
  name          VARCHAR(128)  NOT NULL,
  phone         VARCHAR(32)   NOT NULL DEFAULT '',
  email         VARCHAR(128)  NOT NULL DEFAULT '',
  id_card       VARCHAR(32)   NOT NULL DEFAULT '',
  region        VARCHAR(32)   NOT NULL DEFAULT '',
  -- active | inactive | churned
  status        VARCHAR(16)   NOT NULL DEFAULT 'active',
  -- 软删除：watermark 增量感知不到物理 DELETE，所以源端必须软删
  is_deleted    TINYINT(1)    NOT NULL DEFAULT 0,
  created_at    DATETIME(3)   NOT NULL,
  updated_at    DATETIME(3)   NOT NULL,

  PRIMARY KEY (id),
  UNIQUE KEY uk_customer_no (customer_no),
  KEY idx_customer_updated (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS contact (
  id            BIGINT        NOT NULL AUTO_INCREMENT,
  contact_no    VARCHAR(32)   NOT NULL,
  customer_no   VARCHAR(32)   NOT NULL,
  name          VARCHAR(128)  NOT NULL,
  phone         VARCHAR(32)   NOT NULL DEFAULT '',
  email         VARCHAR(128)  NOT NULL DEFAULT '',
  is_primary    TINYINT(1)    NOT NULL DEFAULT 0,
  is_deleted    TINYINT(1)    NOT NULL DEFAULT 0,
  created_at    DATETIME(3)   NOT NULL,
  updated_at    DATETIME(3)   NOT NULL,

  PRIMARY KEY (id),
  UNIQUE KEY uk_contact_no (contact_no),
  KEY idx_contact_customer (customer_no),
  KEY idx_contact_updated (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 造 5000 条客户 + 每客户 1~2 个联系人，让分片并行有实际意义
SET SESSION cte_max_recursion_depth = 100000;

INSERT INTO customer
  (customer_no, name, phone, email, id_card, region, status, is_deleted, created_at, updated_at)
WITH RECURSIVE seq(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 5000
)
SELECT
  CONCAT('C', LPAD(n, 8, '0')),
  CONCAT('客户_', n),
  CONCAT('138', LPAD(n, 8, '0')),
  CONCAT('user', n, '@example.com'),
  CONCAT('3101', LPAD(n, 14, '0')),
  ELT(1 + (n % 5), '华东', '华北', '华南', '西南', '东北'),
  ELT(1 + (n % 3), 'active', 'active', 'inactive'),
  IF(n % 97 = 0, 1, 0),
  NOW(3) - INTERVAL (n % 365) DAY,
  NOW(3) - INTERVAL (n % 72) HOUR
FROM seq
ON DUPLICATE KEY UPDATE customer_no = customer.customer_no;

INSERT INTO contact
  (contact_no, customer_no, name, phone, email, is_primary, is_deleted, created_at, updated_at)
SELECT
  CONCAT('T', LPAD(c.id, 8, '0'), '-1'),
  c.customer_no,
  CONCAT(c.name, '_主联系人'),
  c.phone,
  c.email,
  1,
  0,
  c.created_at,
  c.updated_at
FROM customer c
ON DUPLICATE KEY UPDATE contact_no = contact.contact_no;

INSERT INTO contact
  (contact_no, customer_no, name, phone, email, is_primary, is_deleted, created_at, updated_at)
SELECT
  CONCAT('T', LPAD(c.id, 8, '0'), '-2'),
  c.customer_no,
  CONCAT(c.name, '_备用联系人'),
  CONCAT('139', LPAD(c.id, 8, '0')),
  CONCAT('alt', c.id, '@example.com'),
  0,
  0,
  c.created_at,
  c.updated_at
FROM customer c
WHERE c.id % 2 = 0
ON DUPLICATE KEY UPDATE contact_no = contact.contact_no;
