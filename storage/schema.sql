-- ============================================================
-- 质培运营看板核心数据底座（TiDB 兼容语法）
-- ============================================================
-- 说明：
--   使用 TiDB/MySQL 兼容类型
--   BOOLEAN → TINYINT(1)
--   VARCHAR 加长度限制
--   DATE_TRUNC → DATE_SUB
--   SPLIT_PART → SUBSTRING_INDEX
--   STRPOS → LOCATE / INSTR
--   ::DATE/::TIMESTAMP → CAST(... AS DATE/TIMESTAMP)
--   CREATE OR REPLACE VIEW → DROP VIEW IF EXISTS + CREATE VIEW
-- ============================================================

-- ═══════════════════════════════════════════════════════════════
--  FACT 层
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS fact_qa_event (
    event_id VARCHAR(64),
    biz_date DATE,
    qa_time DATETIME,
    import_date DATE,

    mother_biz VARCHAR(128),
    sub_biz VARCHAR(128),
    group_name VARCHAR(128),
    queue_name VARCHAR(128),
    scene_name VARCHAR(128),
    channel_name VARCHAR(128),
    content_type VARCHAR(64),

    inspect_type VARCHAR(16),
    workforce_type VARCHAR(16),

    reviewer_name VARCHAR(128),
    qa_owner_name VARCHAR(128),
    trainer_name VARCHAR(128),

    source_record_id VARCHAR(128),
    comment_id VARCHAR(128),
    dynamic_id VARCHAR(128),
    account_id VARCHAR(128),
    join_key VARCHAR(256),

    raw_label VARCHAR(64),
    final_label VARCHAR(64),
    raw_judgement VARCHAR(128),
    final_judgement VARCHAR(128),
    qa_result VARCHAR(128),

    error_type VARCHAR(128),
    error_level VARCHAR(64),
    error_reason VARCHAR(512),
    risk_level VARCHAR(64),
    training_topic VARCHAR(256),

    is_raw_correct TINYINT(1) DEFAULT 0,
    is_final_correct TINYINT(1) DEFAULT 0,
    is_misjudge TINYINT(1) DEFAULT 0,
    is_missjudge TINYINT(1) DEFAULT 0,
    is_appealed TINYINT(1) DEFAULT 0,
    is_appeal_reversed TINYINT(1) DEFAULT 0,

    appeal_status VARCHAR(64),
    appeal_reason VARCHAR(512),
    comment_text TEXT,
    qa_note TEXT,

    batch_id VARCHAR(64),
    source_file_name VARCHAR(256),
    row_hash VARCHAR(64),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fqe_biz_date ON fact_qa_event (biz_date);
CREATE INDEX IF NOT EXISTS idx_fqe_sub_biz ON fact_qa_event (sub_biz);
CREATE INDEX IF NOT EXISTS idx_fqe_join_key ON fact_qa_event (join_key (128));
CREATE INDEX IF NOT EXISTS idx_fqe_row_hash ON fact_qa_event (row_hash);
CREATE INDEX IF NOT EXISTS idx_fqe_inspect_type ON fact_qa_event (inspect_type);
CREATE INDEX IF NOT EXISTS idx_fqe_workforce_type ON fact_qa_event (workforce_type);

CREATE TABLE IF NOT EXISTS fact_appeal_event (
    appeal_event_id VARCHAR(64),
    biz_date DATE,
    appeal_time DATETIME,
    source_record_id VARCHAR(128),
    comment_id VARCHAR(128),
    dynamic_id VARCHAR(128),
    account_id VARCHAR(128),
    join_key VARCHAR(256),
    group_name VARCHAR(128),
    queue_name VARCHAR(128),
    reviewer_name VARCHAR(128),
    appeal_status VARCHAR(64),
    appeal_result VARCHAR(128),
    appeal_reason VARCHAR(512),
    appeal_operator VARCHAR(128),
    appeal_note TEXT,
    is_reversed TINYINT(1) DEFAULT 0,
    batch_id VARCHAR(64),
    source_file_name VARCHAR(256),
    row_hash VARCHAR(64),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fae_biz_date ON fact_appeal_event (biz_date);
CREATE INDEX IF NOT EXISTS idx_fae_join_key ON fact_appeal_event (join_key (128));
CREATE INDEX IF NOT EXISTS idx_fae_row_hash ON fact_appeal_event (row_hash);

CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id VARCHAR(64),
    job_name VARCHAR(256),
    start_time DATETIME,
    end_time DATETIME,
    run_status VARCHAR(32),
    source_rows BIGINT,
    inserted_rows BIGINT,
    dedup_rows BIGINT,
    warning_rows BIGINT,
    error_message TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_erl_job_name ON etl_run_log (job_name);

CREATE TABLE IF NOT EXISTS fact_upload_log (
    upload_id VARCHAR(64) PRIMARY KEY,
    upload_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    file_name VARCHAR(256) NOT NULL,
    file_type VARCHAR(32) NOT NULL,
    file_size_bytes BIGINT,
    source_rows BIGINT,
    inserted_rows BIGINT,
    dedup_rows BIGINT,
    business_line VARCHAR(128),
    upload_status VARCHAR(32),
    error_message TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_file_dedup (
    file_hash VARCHAR(64) PRIMARY KEY,
    file_name VARCHAR(256) NOT NULL,
    file_type VARCHAR(32) NOT NULL,
    first_upload_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    first_upload_id VARCHAR(64),
    upload_count INT DEFAULT 1
);

-- ═══════════════════════════════════════════════════════════════
--  DIM 层
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS dim_alert_rule (
    rule_code VARCHAR(64) PRIMARY KEY,
    rule_name VARCHAR(256),
    grain VARCHAR(16),
    target_level VARCHAR(32),
    metric_name VARCHAR(128),
    compare_op VARCHAR(8),
    threshold_value DOUBLE,
    severity VARCHAR(8),
    enabled TINYINT(1) DEFAULT 1,
    rule_desc TEXT
);

-- ═══════════════════════════════════════════════════════════════
--  ALERT 层
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS fact_alert_event (
    alert_id VARCHAR(64) PRIMARY KEY,
    alert_date DATE,
    grain VARCHAR(16),
    target_level VARCHAR(32),
    target_key VARCHAR(256),
    rule_code VARCHAR(64),
    severity VARCHAR(8),
    metric_name VARCHAR(128),
    metric_value DOUBLE,
    threshold_value DOUBLE,
    is_resolved TINYINT(1) DEFAULT 0,
    alert_message TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_faev_alert_date ON fact_alert_event (alert_date);
CREATE INDEX IF NOT EXISTS idx_faev_rule_code ON fact_alert_event (rule_code);

CREATE TABLE IF NOT EXISTS fact_alert_status (
    alert_id VARCHAR(64),
    alert_status VARCHAR(32),
    owner_name VARCHAR(128),
    handle_note TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fas_alert_id ON fact_alert_status (alert_id);

CREATE TABLE IF NOT EXISTS fact_alert_status_history (
    alert_id VARCHAR(64),
    alert_status VARCHAR(32),
    owner_name VARCHAR(128),
    handle_note TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fash_alert_id ON fact_alert_status_history (alert_id);

-- ═══════════════════════════════════════════════════════════════
--  MART 层（实体表，由 Python 层刷新）
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS mart_day_group (
    biz_date DATE,
    group_name VARCHAR(128),
    mother_biz VARCHAR(128),
    sub_biz VARCHAR(128),
    qa_cnt BIGINT,
    raw_correct_cnt BIGINT,
    final_correct_cnt BIGINT,
    raw_error_cnt BIGINT,
    final_error_cnt BIGINT,
    misjudge_cnt BIGINT,
    missjudge_cnt BIGINT,
    appeal_cnt BIGINT,
    appeal_reversed_cnt BIGINT,
    reviewer_cnt INT,
    raw_accuracy_rate DOUBLE,
    final_accuracy_rate DOUBLE,
    misjudge_rate DOUBLE,
    missjudge_rate DOUBLE,
    appeal_reverse_rate DOUBLE,
    PRIMARY KEY (biz_date, group_name)
);

CREATE INDEX IF NOT EXISTS idx_mdg_sub_biz ON mart_day_group (sub_biz);

CREATE TABLE IF NOT EXISTS mart_day_queue (
    biz_date DATE,
    group_name VARCHAR(128),
    queue_name VARCHAR(128),
    qa_cnt BIGINT,
    raw_correct_cnt BIGINT,
    final_correct_cnt BIGINT,
    raw_error_cnt BIGINT,
    final_error_cnt BIGINT,
    misjudge_cnt BIGINT,
    missjudge_cnt BIGINT,
    appeal_cnt BIGINT,
    appeal_reversed_cnt BIGINT,
    reviewer_cnt INT,
    raw_accuracy_rate DOUBLE,
    final_accuracy_rate DOUBLE,
    misjudge_rate DOUBLE,
    missjudge_rate DOUBLE,
    appeal_reverse_rate DOUBLE,
    PRIMARY KEY (biz_date, group_name, queue_name)
);

CREATE TABLE IF NOT EXISTS mart_day_auditor (
    biz_date DATE,
    group_name VARCHAR(128),
    queue_name VARCHAR(128),
    reviewer_name VARCHAR(128),
    qa_cnt BIGINT,
    raw_correct_cnt BIGINT,
    final_correct_cnt BIGINT,
    misjudge_cnt BIGINT,
    missjudge_cnt BIGINT,
    raw_accuracy_rate DOUBLE,
    final_accuracy_rate DOUBLE,
    misjudge_rate DOUBLE,
    missjudge_rate DOUBLE,
    appeal_reverse_rate DOUBLE,
    PRIMARY KEY (biz_date, group_name, queue_name, reviewer_name)
);

CREATE TABLE IF NOT EXISTS mart_week_group (
    week_begin_date DATE,
    group_name VARCHAR(128),
    mother_biz VARCHAR(128),
    sub_biz VARCHAR(128),
    qa_cnt BIGINT,
    active_days INT,
    raw_accuracy_rate DOUBLE,
    final_accuracy_rate DOUBLE,
    misjudge_rate DOUBLE,
    missjudge_rate DOUBLE,
    appeal_reverse_rate DOUBLE,
    PRIMARY KEY (week_begin_date, group_name)
);

CREATE TABLE IF NOT EXISTS mart_week_queue (
    week_begin_date DATE,
    group_name VARCHAR(128),
    queue_name VARCHAR(128),
    qa_cnt BIGINT,
    active_days INT,
    raw_accuracy_rate DOUBLE,
    final_accuracy_rate DOUBLE,
    misjudge_rate DOUBLE,
    missjudge_rate DOUBLE,
    appeal_reverse_rate DOUBLE,
    PRIMARY KEY (week_begin_date, group_name, queue_name)
);

CREATE TABLE IF NOT EXISTS mart_month_group (
    month_begin_date DATE,
    group_name VARCHAR(128),
    mother_biz VARCHAR(128),
    sub_biz VARCHAR(128),
    qa_cnt BIGINT,
    active_days INT,
    raw_accuracy_rate DOUBLE,
    final_accuracy_rate DOUBLE,
    misjudge_rate DOUBLE,
    missjudge_rate DOUBLE,
    appeal_reverse_rate DOUBLE,
    PRIMARY KEY (month_begin_date, group_name)
);

CREATE TABLE IF NOT EXISTS mart_month_queue (
    month_begin_date DATE,
    group_name VARCHAR(128),
    queue_name VARCHAR(128),
    qa_cnt BIGINT,
    reviewer_cnt INT,
    raw_accuracy_rate DOUBLE,
    final_accuracy_rate DOUBLE,
    misjudge_rate DOUBLE,
    missjudge_rate DOUBLE,
    appeal_reverse_rate DOUBLE,
    PRIMARY KEY (month_begin_date, group_name, queue_name)
);

CREATE TABLE IF NOT EXISTS mart_day_error_topic (
    biz_date DATE,
    group_name VARCHAR(128),
    queue_name VARCHAR(128),
    error_type VARCHAR(128),
    error_reason VARCHAR(512),
    issue_cnt BIGINT,
    affected_reviewer_cnt INT,
    PRIMARY KEY (biz_date, group_name, queue_name, error_type)
);

CREATE TABLE IF NOT EXISTS mart_week_error_topic (
    week_begin_date DATE,
    group_name VARCHAR(128),
    queue_name VARCHAR(128),
    error_type VARCHAR(128),
    error_reason VARCHAR(512),
    issue_cnt BIGINT,
    affected_reviewer_cnt INT,
    PRIMARY KEY (week_begin_date, group_name, queue_name, error_type)
);

CREATE TABLE IF NOT EXISTS mart_month_error_topic (
    month_begin_date DATE,
    group_name VARCHAR(128),
    queue_name VARCHAR(128),
    error_type VARCHAR(128),
    error_reason VARCHAR(512),
    issue_cnt BIGINT,
    affected_reviewer_cnt INT,
    PRIMARY KEY (month_begin_date, group_name, queue_name, error_type)
);

CREATE TABLE IF NOT EXISTS mart_day_content_type (
    biz_date DATE,
    group_name VARCHAR(128),
    content_type VARCHAR(64),
    qa_cnt BIGINT,
    raw_correct_cnt BIGINT,
    final_correct_cnt BIGINT,
    raw_error_cnt BIGINT,
    final_error_cnt BIGINT,
    raw_accuracy_rate DOUBLE,
    final_accuracy_rate DOUBLE,
    PRIMARY KEY (biz_date, group_name, content_type)
);

CREATE INDEX IF NOT EXISTS idx_mdct_group ON mart_day_content_type (group_name);

-- ═══════════════════════════════════════════════════════════════
--  DIM_REVIEWER 审核人画像维度表
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS dim_reviewer (
    reviewer_name VARCHAR(128) PRIMARY KEY,
    group_name VARCHAR(128),
    main_queue VARCHAR(128),
    inspect_type VARCHAR(16),
    workforce_type VARCHAR(16),
    first_qa_date DATE,
    last_qa_date DATE,
    active_days_30d INT,
    total_qa_cnt_30d BIGINT,
    rolling_accuracy_30d DOUBLE,
    misjudge_rate_30d DOUBLE,
    missjudge_rate_30d DOUBLE,
    top_error_type VARCHAR(128),
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dr_group ON dim_reviewer (group_name);
CREATE INDEX IF NOT EXISTS idx_dr_workforce ON dim_reviewer (workforce_type);

CREATE TABLE IF NOT EXISTS mart_training_action_recovery (
    action_id VARCHAR(64) PRIMARY KEY,
    alert_id VARCHAR(64),
    rule_code VARCHAR(64),
    severity VARCHAR(8),
    alert_date DATE,
    action_status VARCHAR(32),
    action_time DATETIME,
    action_date DATE,
    action_week_begin_date DATE,
    group_name VARCHAR(128),
    queue_name VARCHAR(128),
    error_type VARCHAR(128),
    owner_name VARCHAR(128),
    handle_note TEXT,
    baseline_issue_cnt BIGINT,
    baseline_qa_cnt BIGINT,
    baseline_issue_share DOUBLE,
    week1_issue_cnt BIGINT,
    week1_qa_cnt BIGINT,
    week1_issue_share DOUBLE,
    week1_issue_share_change_pp DOUBLE,
    is_recovered_week1 TINYINT(1),
    week2_issue_cnt BIGINT,
    week2_qa_cnt BIGINT,
    week2_issue_share DOUBLE,
    week2_issue_share_change_pp DOUBLE,
    is_recovered_week2 TINYINT(1),
    recovery_status VARCHAR(32)
);

-- ═══════════════════════════════════════════════════════════════
--  VIEW 层（TiDB 兼容）
-- ═══════════════════════════════════════════════════════════════

-- vw_appeal_latest：每个 join_key 取最新一条申诉记录
DROP VIEW IF EXISTS vw_appeal_latest;
CREATE VIEW vw_appeal_latest AS
WITH ranked AS (
    SELECT
        a.*,
        ROW_NUMBER() OVER (
            PARTITION BY a.join_key
            ORDER BY
                CASE WHEN COALESCE(TRIM(a.appeal_result), '') <> '' THEN 1 ELSE 0 END DESC,
                COALESCE(a.appeal_time, CAST(a.biz_date AS DATETIME), a.created_at) DESC,
                a.created_at DESC,
                a.appeal_event_id DESC
        ) AS rn
    FROM fact_appeal_event a
    WHERE COALESCE(a.join_key, '') <> ''
)
SELECT
    appeal_event_id,
    biz_date,
    appeal_time,
    source_record_id,
    comment_id,
    dynamic_id,
    account_id,
    join_key,
    group_name,
    queue_name,
    reviewer_name,
    appeal_status,
    appeal_result,
    appeal_reason,
    appeal_operator,
    appeal_note,
    is_reversed,
    batch_id,
    source_file_name,
    row_hash,
    created_at
FROM ranked
WHERE rn = 1;

-- vw_qa_base：核心基础视图（质检 + 申诉关联）
DROP VIEW IF EXISTS vw_qa_base;
CREATE VIEW vw_qa_base AS
SELECT
    q.biz_date,
    DATE_SUB(q.biz_date, INTERVAL WEEKDAY(q.biz_date) DAY) AS week_begin_date,
    DATE_SUB(q.biz_date, INTERVAL DAYOFMONTH(q.biz_date) - 1 DAY) AS month_begin_date,
    q.mother_biz,
    q.sub_biz,
    COALESCE(NULLIF(TRIM(q.group_name), ''), NULLIF(TRIM(q.mother_biz), ''), NULLIF(TRIM(q.sub_biz), '')) AS group_name,
    COALESCE(NULLIF(TRIM(q.queue_name), ''), NULLIF(TRIM(q.sub_biz), '')) AS queue_name,
    q.reviewer_name,
    q.qa_owner_name,
    q.content_type,
    q.inspect_type,
    q.workforce_type,
    q.scene_name,
    q.channel_name,
    q.source_record_id,
    q.comment_id,
    q.dynamic_id,
    q.account_id,
    q.join_key,
    q.raw_label,
    q.final_label,
    q.raw_judgement,
    q.final_judgement,
    q.qa_result,
    q.error_type,
    q.error_level,
    q.error_reason,
    q.risk_level,
    q.training_topic,
    q.comment_text,
    q.qa_note,
    COALESCE(q.is_raw_correct, 0) AS is_raw_correct,
    CASE
        WHEN COALESCE(TRIM(a.appeal_result), '') <> '' THEN TRIM(COALESCE(q.raw_judgement, '')) = TRIM(a.appeal_result)
        WHEN COALESCE(TRIM(q.final_judgement), '') <> '' OR COALESCE(TRIM(q.final_label), '') <> '' THEN COALESCE(q.is_final_correct, 0)
        ELSE COALESCE(q.is_raw_correct, 0)
    END AS is_final_correct,
    COALESCE(q.is_misjudge, 0) AS is_misjudge,
    COALESCE(q.is_missjudge, 0) AS is_missjudge,
    CASE
        WHEN a.join_key IS NOT NULL THEN 1
        ELSE COALESCE(q.is_appealed, 0)
    END AS is_appealed,
    CASE
        WHEN COALESCE(TRIM(a.appeal_result), '') <> '' AND TRIM(COALESCE(q.raw_judgement, '')) <> TRIM(a.appeal_result) THEN 1
        WHEN a.is_reversed IS NOT NULL THEN a.is_reversed
        ELSE COALESCE(q.is_appeal_reversed, 0)
    END AS is_appeal_reversed,
    COALESCE(a.appeal_status, q.appeal_status) AS appeal_status,
    COALESCE(a.appeal_reason, q.appeal_reason) AS appeal_reason,
    a.appeal_result,
    a.appeal_operator,
    a.appeal_note,
    a.appeal_time,
    COALESCE(
        NULLIF(TRIM(a.appeal_result), ''),
        NULLIF(TRIM(q.final_judgement), ''),
        NULLIF(TRIM(q.qa_result), ''),
        NULLIF(TRIM(q.raw_judgement), '')
    ) AS final_review_result,
    q.batch_id,
    q.source_file_name,
    a.source_file_name AS appeal_source_file_name,
    q.row_hash,
    q.qa_time,
    q.event_id
FROM fact_qa_event q
LEFT JOIN vw_appeal_latest a
  ON q.join_key = a.join_key;

-- vw_join_quality_detail
DROP VIEW IF EXISTS vw_join_quality_detail;
CREATE VIEW vw_join_quality_detail AS
SELECT
    q.event_id,
    q.biz_date,
    q.group_name,
    q.queue_name,
    q.reviewer_name,
    q.source_record_id,
    q.comment_id,
    q.dynamic_id,
    q.account_id,
    q.join_key,
    CASE
        WHEN COALESCE(q.join_key, '') = '' THEN 'missing_join_key'
        WHEN a.join_key IS NULL THEN 'unmatched'
        ELSE 'matched'
    END AS join_status,
    CASE
        WHEN COALESCE(q.join_key, '') = '' THEN 'unknown'
        WHEN q.join_key LIKE 'record:%' THEN 'record_id'
        WHEN q.join_key LIKE 'comment:%|dynamic:%' THEN 'comment_dynamic'
        WHEN q.join_key LIKE 'comment:%' THEN 'comment_id'
        WHEN q.join_key LIKE 'dynamic:%' THEN 'dynamic_id'
        WHEN q.join_key LIKE 'account:%' THEN 'account_id'
        ELSE 'other'
    END AS join_key_type,
    a.appeal_event_id,
    a.appeal_time,
    a.appeal_status,
    a.appeal_result,
    CASE
        WHEN a.join_key IS NOT NULL AND COALESCE(TRIM(a.appeal_result), '') <> '' THEN 1
        ELSE 0
    END AS matched_with_result
FROM fact_qa_event q
LEFT JOIN vw_appeal_latest a
  ON q.join_key = a.join_key;

-- vw_join_quality_daily
DROP VIEW IF EXISTS vw_join_quality_daily;
CREATE VIEW vw_join_quality_daily AS
SELECT
    biz_date,
    join_key_type,
    COUNT(*) AS qa_cnt,
    SUM(CASE WHEN join_status = 'matched' THEN 1 ELSE 0 END) AS matched_cnt,
    SUM(CASE WHEN join_status = 'unmatched' THEN 1 ELSE 0 END) AS unmatched_cnt,
    SUM(CASE WHEN join_status = 'missing_join_key' THEN 1 ELSE 0 END) AS missing_join_key_cnt,
    ROUND(SUM(CASE WHEN join_status = 'matched' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 4) AS match_rate,
    ROUND(SUM(CASE WHEN matched_with_result = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 4) AS result_backfill_rate
FROM vw_join_quality_detail
GROUP BY 1, 2;

-- vw_training_action_event
DROP VIEW IF EXISTS vw_training_action_event;
CREATE VIEW vw_training_action_event AS
WITH ranked AS (
    SELECT
        e.alert_id,
        e.rule_code,
        e.severity,
        e.alert_date,
        e.target_key,
        s.alert_status AS action_status,
        COALESCE(NULLIF(TRIM(s.owner_name), ''), '未指派') AS owner_name,
        COALESCE(NULLIF(TRIM(s.handle_note), ''), '') AS handle_note,
        s.updated_at AS action_time,
        ROW_NUMBER() OVER (
            PARTITION BY e.alert_id
            ORDER BY
                CASE s.alert_status WHEN 'claimed' THEN 1 WHEN 'resolved' THEN 2 ELSE 3 END,
                s.updated_at ASC
        ) AS rn
    FROM fact_alert_event e
    JOIN fact_alert_status_history s
      ON e.alert_id = s.alert_id
    WHERE e.rule_code = 'ERROR_TYPE_SHARE_GT_15_QUEUE_WEEK'
      AND s.alert_status IN ('claimed', 'resolved')
)
SELECT
    MD5(CONCAT(alert_id, '|action')) AS action_id,
    alert_id,
    rule_code,
    severity,
    alert_date,
    action_status,
    action_time AS action_time,
    CAST(action_time AS DATE) AS action_date,
    DATE_SUB(CAST(action_time AS DATE), INTERVAL WEEKDAY(CAST(action_time AS DATE)) DAY) AS action_week_begin_date,
    NULLIF(TRIM(SUBSTRING_INDEX(target_key, '｜', 1)), '') AS group_name,
    CASE
        WHEN INSTR(SUBSTRING_INDEX(target_key, '｜', 1), ' / ') > 0
        THEN NULLIF(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(target_key, '｜', 1), ' / ', -1)), '')
        ELSE NULL
    END AS queue_name,
    NULLIF(TRIM(REPLACE(SUBSTRING_INDEX(target_key, '｜', 2), '错误类型=', '')), '') AS error_type,
    owner_name,
    handle_note
FROM ranked
WHERE rn = 1;

-- ═══════════════════════════════════════════════════════════════
--  DIM_ALERT_RULE 种子数据
-- ═══════════════════════════════════════════════════════════════
DELETE FROM dim_alert_rule WHERE rule_code IN (
    'RAW_ACC_LT_99_DAY',
    'FINAL_ACC_LT_99_QUEUE_DAY',
    'MISS_RATE_GT_035_DAY',
    'APPEAL_REV_GT_18_DAY',
    'JOIN_MATCH_LT_85_DAY',
    'MISSING_JOIN_KEY_GT_10_DAY',
    'RAW_ACC_DROP_GT_1P5_WEEK',
    'MISS_RATE_SPIKE_GT_0P2_QUEUE_WEEK',
    'TOP_ERROR_SHARE_GT_35_QUEUE_MONTH',
    'ERROR_TYPE_SHARE_GT_15_QUEUE_WEEK'
);

INSERT INTO dim_alert_rule (rule_code, rule_name, grain, target_level, metric_name, compare_op, threshold_value, severity, enabled, rule_desc) VALUES
('RAW_ACC_LT_99_DAY', '日原始正确率低于99%', 'day', 'group', 'raw_accuracy_rate', '<', 99.00, 'P1', 1, '日监控首页红橙灯'),
('FINAL_ACC_LT_99_QUEUE_DAY', '日队列最终正确率低于99%', 'day', 'queue', 'final_accuracy_rate', '<', 99.00, 'P1', 1, '终审结果稳定性异常'),
('MISS_RATE_GT_035_DAY', '日漏判率高于0.35%', 'day', 'queue', 'missjudge_rate', '>', 0.35, 'P1', 1, '队列级漏判异常'),
('APPEAL_REV_GT_18_DAY', '日申诉改判率高于18%', 'day', 'group', 'appeal_reverse_rate', '>', 18.00, 'P2', 1, '申诉稳定性观察'),
('JOIN_MATCH_LT_85_DAY', '日质检样本与申诉数据关联率低于85%', 'day', 'system', 'qa_match_rate_when_key_present', '<', 85.00, 'P2', 1, '质检样本与申诉数据的关联比例。低关联率可能原因：①申诉数据导入不完整 ②质检样本质量好无需申诉 ③质检与申诉业务场景不重叠。需结合申诉数据量综合判断。'),
('MISSING_JOIN_KEY_GT_10_DAY', '日缺失关联主键占比高于10%', 'day', 'system', 'missing_join_key_rate', '>', 10.00, 'P1', 1, '关联主键缺失异常'),
('RAW_ACC_DROP_GT_1P5_WEEK', '周原始正确率较上周下跌超1.5个百分点', 'week', 'group', 'raw_accuracy_drop_pp', '>', 1.50, 'P1', 1, '组别周度质量波动异常'),
('MISS_RATE_SPIKE_GT_0P2_QUEUE_WEEK', '周队列漏判率较上周上升超0.2个百分点', 'week', 'queue', 'missjudge_rate_spike_pp', '>', 0.20, 'P1', 1, '队列周度漏判波动异常'),
('TOP_ERROR_SHARE_GT_35_QUEUE_MONTH', '月队列单一错误类型占比高于35%', 'month', 'queue', 'top_error_type_share', '>', 35.00, 'P2', 1, '队列月度问题结构过度集中'),
('ERROR_TYPE_SHARE_GT_15_QUEUE_WEEK', '周队列同类错误连续两周未收敛', 'week', 'queue', 'error_type_issue_share', '>', 15.00, 'P1', 1, '队列周度同类错误持续高位');
