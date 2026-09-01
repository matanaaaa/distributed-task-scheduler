-- 同步平台元数据库
-- jobs      : 用户配置的同步作业（source -> transform -> sink）
-- job_runs  : 一次调度执行
-- sync_tasks: JobRun 拆出的分片，也就是 Redis 队列里的调度单元
CREATE DATABASE IF NOT EXISTS dts_meta
  DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

USE dts_meta;

CREATE TABLE IF NOT EXISTS jobs (
  id                VARCHAR(36)   NOT NULL,
  name              VARCHAR(128)  NOT NULL,
  description       VARCHAR(512)  NOT NULL DEFAULT '',

  -- 溯源三元组的前两段，例如 source_system='crm', object_type='customer'
  source_system     VARCHAR(64)   NOT NULL DEFAULT '',
  object_type       VARCHAR(64)   NOT NULL DEFAULT '',
  -- 源记录标识列，构成三元组第三段，例如 customer_no
  source_id_column  VARCHAR(64)   NOT NULL DEFAULT '',

  source_type       VARCHAR(32)   NOT NULL,
  source_config     JSON          NOT NULL,
  sink_type         VARCHAR(32)   NOT NULL,
  sink_config       JSON          NOT NULL,
  transform_config  JSON          NULL,

  -- full: 每次全量重扫；incremental: 按 watermark_column 增量
  sync_mode         VARCHAR(16)   NOT NULL DEFAULT 'full',
  watermark_column  VARCHAR(64)   NOT NULL DEFAULT '',
  -- 复合水位 (watermark_value, watermark_id)：
  -- 单用 updated_at 会在同一时刻多条记录处翻页丢数据，
  -- 故以自增 id 作为第二排序键打破并列
  watermark_value   VARCHAR(64)   NOT NULL DEFAULT '',
  watermark_id      BIGINT        NOT NULL DEFAULT 0,

  -- 分片：按 shard_column 的取值范围切成 shard_count 个 task
  shard_column      VARCHAR(64)   NOT NULL DEFAULT '',
  shard_count       INT           NOT NULL DEFAULT 1,

  batch_size        INT           NOT NULL DEFAULT 1000,
  -- 源库读取限流，0 表示不限
  read_qps          INT           NOT NULL DEFAULT 0,

  priority          VARCHAR(8)    NOT NULL DEFAULT 'normal',
  enabled           TINYINT(1)    NOT NULL DEFAULT 1,

  created_at        DATETIME(3)   NOT NULL,
  updated_at        DATETIME(3)   NOT NULL,

  PRIMARY KEY (id),
  UNIQUE KEY uk_jobs_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS job_runs (
  id                VARCHAR(36)   NOT NULL,
  job_id            VARCHAR(36)   NOT NULL,
  -- manual | cron
  trigger_type      VARCHAR(16)   NOT NULL DEFAULT 'manual',
  -- running | success | failed | partial
  status            VARCHAR(16)   NOT NULL DEFAULT 'running',

  sync_mode         VARCHAR(16)   NOT NULL DEFAULT 'full',
  -- 本次增量的复合水位区间 (from, to]，全量时为空
  watermark_from    VARCHAR(64)   NOT NULL DEFAULT '',
  watermark_from_id BIGINT        NOT NULL DEFAULT 0,
  watermark_to      VARCHAR(64)   NOT NULL DEFAULT '',
  watermark_to_id   BIGINT        NOT NULL DEFAULT 0,

  shard_total       INT           NOT NULL DEFAULT 0,
  shard_done        INT           NOT NULL DEFAULT 0,
  shard_failed      INT           NOT NULL DEFAULT 0,

  rows_read         BIGINT        NOT NULL DEFAULT 0,
  rows_written      BIGINT        NOT NULL DEFAULT 0,
  rows_failed       BIGINT        NOT NULL DEFAULT 0,

  error_reason      VARCHAR(1024) NOT NULL DEFAULT '',
  started_at        DATETIME(3)   NOT NULL,
  finished_at       DATETIME(3)   NULL,

  PRIMARY KEY (id),
  KEY idx_runs_job_started (job_id, started_at),
  KEY idx_runs_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS sync_tasks (
  id                VARCHAR(36)   NOT NULL,
  run_id            VARCHAR(36)   NOT NULL,
  job_id            VARCHAR(36)   NOT NULL,

  shard_index       INT           NOT NULL,
  -- 分片区间 [shard_lo, shard_hi)，字符串存储以兼容不同主键类型
  shard_lo          VARCHAR(64)   NOT NULL DEFAULT '',
  shard_hi          VARCHAR(64)   NOT NULL DEFAULT '',

  -- queued | running | success | failed | retrying | dead
  status            VARCHAR(16)   NOT NULL DEFAULT 'queued',
  priority          VARCHAR(8)    NOT NULL DEFAULT 'normal',
  attempt           INT           NOT NULL DEFAULT 0,
  retry_count       INT           NOT NULL DEFAULT 0,

  -- 断点：本分片已成功写入的最大位置，重试从这里继续（与水位同为复合值）
  checkpoint        VARCHAR(64)   NOT NULL DEFAULT '',
  checkpoint_id     BIGINT        NOT NULL DEFAULT 0,

  rows_read         BIGINT        NOT NULL DEFAULT 0,
  rows_written      BIGINT        NOT NULL DEFAULT 0,
  rows_failed       BIGINT        NOT NULL DEFAULT 0,

  error_reason      VARCHAR(1024) NOT NULL DEFAULT '',
  started_at        DATETIME(3)   NULL,
  finished_at       DATETIME(3)   NULL,
  created_at        DATETIME(3)   NOT NULL,
  updated_at        DATETIME(3)   NOT NULL,

  PRIMARY KEY (id),
  UNIQUE KEY uk_tasks_run_shard (run_id, shard_index),
  KEY idx_tasks_run_status (run_id, status),
  KEY idx_tasks_job (job_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 对账结果：每个 JobRun 结束后比对源与目标
-- mode 分三档逐步升级：count 只比数量，keyset 比主键集合，field 比关键字段
CREATE TABLE IF NOT EXISTS reconciliations (
  id                VARCHAR(36)   NOT NULL,
  run_id            VARCHAR(36)   NOT NULL,
  job_id            VARCHAR(36)   NOT NULL,

  -- count | keyset | field
  mode              VARCHAR(16)   NOT NULL DEFAULT 'count',

  source_count      BIGINT        NOT NULL DEFAULT 0,
  target_count      BIGINT        NOT NULL DEFAULT 0,
  -- 源有目标无
  missing_count     BIGINT        NOT NULL DEFAULT 0,
  -- 目标有源无（通常是源端物理删除导致，watermark 增量感知不到）
  extra_count       BIGINT        NOT NULL DEFAULT 0,
  -- 主键相同但关键字段不一致
  mismatch_count    BIGINT        NOT NULL DEFAULT 0,

  -- ok | mismatch | error
  result            VARCHAR(16)   NOT NULL DEFAULT 'ok',
  error_reason      VARCHAR(1024) NOT NULL DEFAULT '',
  -- 差异主键抽样，只存前 N 条，避免把整表差异灌进元数据库
  detail            JSON          NULL,

  checked_at        DATETIME(3)   NOT NULL,

  PRIMARY KEY (id),
  UNIQUE KEY uk_recon_run_mode (run_id, mode),
  KEY idx_recon_job (job_id, checked_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 坏数据记录：non_retryable 的行落这里，不再反复 retry
-- 有了这张表，Retry/DLQ 才能区分"基础设施抖动"和"这条数据本身有问题"
CREATE TABLE IF NOT EXISTS sync_error_records (
  id                BIGINT        NOT NULL AUTO_INCREMENT,
  run_id            VARCHAR(36)   NOT NULL,
  task_id           VARCHAR(36)   NOT NULL,
  job_id            VARCHAR(36)   NOT NULL,

  -- 溯源三元组：定位到源系统里究竟哪一条记录出了问题
  source_system     VARCHAR(64)   NOT NULL DEFAULT '',
  object_type       VARCHAR(64)   NOT NULL DEFAULT '',
  source_record_id  VARCHAR(128)  NOT NULL DEFAULT '',

  -- retryable | non_retryable
  error_type        VARCHAR(16)   NOT NULL DEFAULT 'non_retryable',
  -- type_mismatch | missing_required | illegal_value | value_too_long | db_deadlock | db_timeout | unknown
  error_code        VARCHAR(64)   NOT NULL DEFAULT 'unknown',
  error_msg         VARCHAR(1024) NOT NULL DEFAULT '',
  -- 出错行原始内容，便于人工修数后重放
  raw_row           JSON          NULL,

  created_at        DATETIME(3)   NOT NULL,

  PRIMARY KEY (id),
  KEY idx_err_run (run_id),
  KEY idx_err_task (task_id),
  KEY idx_err_record (source_system, object_type, source_record_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
