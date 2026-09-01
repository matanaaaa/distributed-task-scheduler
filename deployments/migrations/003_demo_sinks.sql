-- 演示用下游库（同步目标）
--   marketing : 营销系统业务库，手机号脱敏、不含身份证，且有自己的业务字段
--   analytics : 分析/报表库，保留区域与生命周期用于统计，手机号只存 hash
--
-- 注意 marketing.audience 里的 send_status / unsubscribed 是营销系统自己写的字段，
-- 同步只能 upsert 映射列，不能覆盖它们 —— 这也是这里不能用只读从库的原因。
CREATE DATABASE IF NOT EXISTS marketing
  DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

USE marketing;

CREATE TABLE IF NOT EXISTS audience (
  id              BIGINT        NOT NULL AUTO_INCREMENT,
  -- 幂等写入的业务唯一键
  customer_no     VARCHAR(32)   NOT NULL,
  name            VARCHAR(128)  NOT NULL DEFAULT '',
  phone_masked    VARCHAR(32)   NOT NULL DEFAULT '',
  email           VARCHAR(128)  NOT NULL DEFAULT '',
  region          VARCHAR(32)   NOT NULL DEFAULT '',
  status          VARCHAR(16)   NOT NULL DEFAULT '',
  is_deleted      TINYINT(1)    NOT NULL DEFAULT 0,

  -- 营销系统自有字段，同步流程不得触碰
  send_status     VARCHAR(16)   NOT NULL DEFAULT 'pending',
  unsubscribed    TINYINT(1)    NOT NULL DEFAULT 0,
  last_touched_at DATETIME(3)   NULL,

  src_updated_at  DATETIME(3)   NULL,
  synced_at       DATETIME(3)   NULL,

  PRIMARY KEY (id),
  UNIQUE KEY uk_audience_customer_no (customer_no),
  KEY idx_audience_region (region)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS audience_contact (
  id              BIGINT        NOT NULL AUTO_INCREMENT,
  contact_no      VARCHAR(32)   NOT NULL,
  customer_no     VARCHAR(32)   NOT NULL,
  name            VARCHAR(128)  NOT NULL DEFAULT '',
  phone_masked    VARCHAR(32)   NOT NULL DEFAULT '',
  email           VARCHAR(128)  NOT NULL DEFAULT '',
  is_primary      TINYINT(1)    NOT NULL DEFAULT 0,
  is_deleted      TINYINT(1)    NOT NULL DEFAULT 0,
  src_updated_at  DATETIME(3)   NULL,
  synced_at       DATETIME(3)   NULL,

  PRIMARY KEY (id),
  UNIQUE KEY uk_audience_contact_no (contact_no),
  -- 不建外键：customer 与 contact 独立并行同步，容忍短暂孤儿数据，最终一致
  KEY idx_audience_contact_customer (customer_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE DATABASE IF NOT EXISTS analytics
  DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

USE analytics;

CREATE TABLE IF NOT EXISTS customer_snapshot (
  id              BIGINT        NOT NULL AUTO_INCREMENT,
  customer_no     VARCHAR(32)   NOT NULL,
  name            VARCHAR(128)  NOT NULL DEFAULT '',
  -- 分析库不存明文手机号，只存 hash 用于去重统计
  phone_hash      CHAR(64)      NOT NULL DEFAULT '',
  region          VARCHAR(32)   NOT NULL DEFAULT '',
  status          VARCHAR(16)   NOT NULL DEFAULT '',
  is_deleted      TINYINT(1)    NOT NULL DEFAULT 0,
  src_created_at  DATETIME(3)   NULL,
  src_updated_at  DATETIME(3)   NULL,
  synced_at       DATETIME(3)   NULL,

  PRIMARY KEY (id),
  UNIQUE KEY uk_snapshot_customer_no (customer_no),
  KEY idx_snapshot_region_status (region, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
