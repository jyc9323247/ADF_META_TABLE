
CREATE SCHEMA IF NOT EXISTS "META_ADF";

SET search_path TO "META_ADF", public;

------------------------------------------------------------------------
-- 1) ctl_ingest_target_master : 수집 대상 마스터 (유일한 수작업 관리 테이블)
-- ---------------------------------------------------------------------
CREATE TABLE ctl_ingest_target_master (
    target_id            bigserial      NOT NULL,
    src_type             varchar(10)    NOT NULL,
    src_db_nm            varchar(20)    NULL,
    src_schema_nm        varchar(30)    NULL,
    src_table_nm         varchar(100)   NULL,
    partition_clause     varchar(100)   NULL,
    data_class           varchar(4)     NOT NULL,
    ingest_type          varchar(4)     NOT NULL,         
    exec_group           varchar(20)    NULL,
    condition_type       varchar(12)    NULL,
    condition_frequency  varchar(10)    NULL,
    condition_interval   int4           NULL,
    incr_column_nm       varchar(30)    NULL,
    incr_column_type     varchar(10)    NULL,
    incr_column_hw_val   varchar(100)   NULL,
    is_active            bpchar(1)      NOT NULL DEFAULT 'y',
    pending_yn           bpchar(1)      NOT NULL DEFAULT 'y',
    created_by           varchar(100)   NULL,
    created_dt           timestamptz    NOT NULL DEFAULT now(),
    update_by            varchar(100)   NULL,
    update_dt            timestamptz    NULL,

    CONSTRAINT ctl_ingest_target_master_pkey PRIMARY KEY (target_id),
    -- 값 열거 검증
    CONSTRAINT ck_data_class  CHECK (lower(data_class) IN ('meta','raw')),
    CONSTRAINT ck_ingest_type CHECK (lower(ingest_type) IN ('full','incr')),   
    CONSTRAINT ctl_ingest_target_master_condition_type_check
        CHECK (lower(condition_type) IN ('window','bizday','close')),
    CONSTRAINT ctl_ingest_target_master_condition_frequency_check
        CHECK (lower(condition_frequency) IN ('minutely','hourly','daily','weekly','monthly','yearly')),
    CONSTRAINT ck_incr_col_type
        CHECK (lower(incr_column_type) IN ('numeric','date','timestamp','string')),

    -- FULL: 조건/증분 컬럼 전부 NULL (부수: type/hw_val도 NULL 강제)
    CONSTRAINT ck_full_no_cond CHECK (
        lower(ingest_type) <> 'full'
        OR (condition_type IS NULL AND condition_frequency IS NULL
            AND condition_interval IS NULL AND incr_column_nm IS NULL
            AND incr_column_type IS NULL AND incr_column_hw_val IS NULL)
    ),
    -- INCR: condition_type 필수
    CONSTRAINT ck_incr_needs_type CHECK (
        lower(ingest_type) <> 'incr' OR condition_type IS NOT NULL
    ),
    -- window: frequency+interval+기준컬럼 필수
    CONSTRAINT ck_window_params CHECK (
        lower(condition_type) IS DISTINCT FROM 'window'
        OR (condition_frequency IS NOT NULL AND condition_interval IS NOT NULL
            AND incr_column_nm IS NOT NULL)
    ),
    -- frequency는 window에서만
    CONSTRAINT ck_freq_window_only CHECK (
        condition_frequency IS NULL OR lower(condition_type) = 'window'
    ),
    -- bizday/close: 기준컬럼 필수
    CONSTRAINT ck_bizday_col CHECK (
        lower(condition_type) IS DISTINCT FROM 'bizday' OR incr_column_nm IS NOT NULL
    ),
    CONSTRAINT ck_close_col CHECK (
        lower(condition_type) IS DISTINCT FROM 'close'  OR incr_column_nm IS NOT NULL
    ),
    -- ③ 증분 기준컬럼이 있으면 데이터타입(인용 처리용) 필수
    CONSTRAINT ck_incr_col_type_req CHECK (
        incr_column_nm IS NULL OR incr_column_type IS NOT NULL
    ),
    -- Y/N 플래그 검증
    CONSTRAINT ck_target_yn CHECK (lower(is_active::text) IN ('y','n') AND lower(pending_yn::text) IN ('y','n'))
);

CREATE INDEX ix_target_pending ON ctl_ingest_target_master
    USING btree (lower(data_class))
    WHERE (lower(is_active::text) = 'y' AND lower(pending_yn::text) = 'y');

CREATE INDEX ix_target_route ON ctl_ingest_target_master
    USING btree (lower(data_class), lower(ingest_type), lower(condition_type),
                 lower(condition_frequency), condition_interval)
    WHERE (lower(is_active::text) = 'y');


-- ---------------------------------------------------------------------
-- 2) ctl_master_pipeline_run : 마스터 수행 상태 + 동시수행 락 겸용
-- ---------------------------------------------------------------------
CREATE TABLE ctl_master_pipeline_run (
    master_run_id       varchar(100)   NOT NULL,
    master_pipeline_nm  varchar(200)   NOT NULL,
    trigger_nm          varchar(200)   NULL,
    ingest_type         varchar(4)     NULL,
    start_dt            timestamptz    NOT NULL,
    end_dt              timestamptz    NULL,
    child_total_cnt     int4           NULL DEFAULT 0,
    child_succ_cnt      int4           NULL DEFAULT 0,
    child_fail_cnt      int4           NULL DEFAULT 0,
    status              varchar(15)    NOT NULL,
    skip_type           varchar(10)    NULL,
    run_mode            varchar(10)    NULL,
    created_by          varchar(100)   NULL,
    created_dt          timestamptz    NOT NULL DEFAULT now(),
    update_by           varchar(100)   NULL,
    update_dt           timestamptz    NULL,

    CONSTRAINT ctl_master_run_status_pkey PRIMARY KEY (master_run_id),
    CONSTRAINT ctl_master_run_status_ingest_type_check CHECK (lower(ingest_type) IN ('full','incr')),
    CONSTRAINT ctl_master_run_status_run_mode_check  CHECK (lower(run_mode) IN ('schedule','manual')),
    CONSTRAINT ctl_master_run_status_skip_type_check CHECK (lower(skip_type) IN ('window','flag','locked')),
    CONSTRAINT ctl_master_run_status_status_check
        CHECK (lower(status) IN ('pending','running','succeeded','failed','skipped'))
);

CREATE INDEX ix_mpr_status_start ON ctl_master_pipeline_run
    USING btree (lower(status), start_dt);

-- 동시수행 방지: 동일 파이프라인+수집타입 RUNNING 1건 제한
CREATE UNIQUE INDEX ux_mrs_running ON ctl_master_pipeline_run
    USING btree (lower(master_pipeline_nm), lower(ingest_type))
    WHERE (lower(status) = 'running');


-- ---------------------------------------------------------------------
-- 3) ctl_run_skip : 트리거 skip 예외 목록(opt-in)
-- ---------------------------------------------------------------------
CREATE TABLE ctl_run_skip (
    trigger_nm          varchar(200)   NOT NULL,
    master_pipeline_nm  varchar(200)   NOT NULL,
    skip_yn             bpchar(1)      NOT NULL DEFAULT 'y',
    skip_from_dt        timestamptz    NULL,
    skip_to_dt          timestamptz    NULL,
    skip_reason         varchar(500)   NULL,
	created_by          varchar(100)   NULL,
    created_dt          timestamptz    NOT NULL DEFAULT now(),
    update_by           varchar(100)   NULL,                 
    update_dt           timestamptz    NULL,

    CONSTRAINT ctl_run_skip_pkey PRIMARY KEY (trigger_nm, master_pipeline_nm),
    CONSTRAINT ctl_run_skip_check  CHECK (skip_from_dt IS NULL OR skip_to_dt IS NULL OR skip_from_dt <= skip_to_dt),
    CONSTRAINT ctl_run_skip_check1 CHECK ((skip_from_dt IS NULL) = (skip_to_dt IS NULL)),
    CONSTRAINT ck_skip_yn CHECK (lower(skip_yn::text) IN ('y','n'))
);


-- ---------------------------------------------------------------------
-- 4) ctl_ingest_pipeline_run : 차일드(수집) 수행 이력 팩트 (수행시점 스냅샷)
-- ---------------------------------------------------------------------
CREATE TABLE ctl_ingest_pipeline_run (
    ingest_pipeline_id  varchar(100)   NOT NULL DEFAULT gen_random_uuid()::text,
   	master_run_id       varchar(100)   NOT NULL,
	master_pipeline_nm  varchar(200)   NOT NULL,
	ingest_pipeline_run_id  varchar(100) NOT NULL DEFAULT '-',
 	target_id           int8           NOT NULL,
    src_type            varchar(10)    NOT NULL,
    src_db_nm           varchar(20)    NULL,
    src_schema_nm       varchar(30)    NULL,
    src_table_nm        varchar(100)   NULL,
    data_class          varchar(4)     NOT NULL,
    ingest_type         varchar(4)     NOT NULL,             -- ① load_type → ingest_type
    exec_group          varchar(20)    NULL,
    condition_type      varchar(12)    NULL,
    condition_frequency varchar(10)    NULL,
    condition_interval  int4           NULL,
    incr_column_nm      varchar(30)    NULL,
    incr_column_type    varchar(10)    NULL,
    incr_column_hw_val  varchar(100)   NULL,
    is_active           bpchar(1)      NOT NULL DEFAULT 'y',
    pending_yn          bpchar(1)      NOT NULL DEFAULT 'n',
    extract_query       text           NOT NULL,
    landing_path        varchar(500)   NULL,
    file_name           varchar(100)   NULL,
    start_dt            timestamptz    NULL,
    end_dt              timestamptz    NULL,
    extract_cnt         int8           NULL,
    save_cnt            int8           NULL,
    attempt_no          int4           NOT NULL DEFAULT 0,
    status              varchar(15)    NULL,
    error_log           text           NULL,
    created_by          varchar(100)   NULL,
    created_dt          timestamptz    NOT NULL DEFAULT now(),
    update_by           varchar(100)   NULL,
    update_dt           timestamptz    NULL,

    CONSTRAINT ctl_ingest_run_pkey PRIMARY KEY (ingest_pipeline_id),   
    CONSTRAINT ctl_ingest_pipeline_run_master_run_id_fkey
        FOREIGN KEY (master_run_id) REFERENCES ctl_master_pipeline_run(master_run_id),
    CONSTRAINT ctl_ingest_pipeline_run_target_id_fkey
        FOREIGN KEY (target_id) REFERENCES ctl_ingest_target_master(target_id),
    CONSTRAINT ck_ipr_yn CHECK (lower(is_active::text) IN ('y','n') AND lower(pending_yn::text) IN ('y','n')),
	CONSTRAINT ctl_ingest_run_status_check
        CHECK (lower(status) IN ('pending','running','succeeded','failed','skipped'))
);

CREATE INDEX ix_ipr_created ON ctl_ingest_pipeline_run USING btree (created_dt);
CREATE INDEX ix_ipr_mri     ON ctl_ingest_pipeline_run USING btree (master_run_id);
CREATE INDEX ix_ipr_status  ON ctl_ingest_pipeline_run USING btree (master_run_id, lower(status));
CREATE INDEX ix_ipr_target  ON ctl_ingest_pipeline_run USING btree (target_id);


-- ---------------------------------------------------------------------
-- 5) ctl_dbx_ingest_history : Databricks 브론즈 적재 완료 이력
-- ---------------------------------------------------------------------
CREATE TABLE ctl_dbx_ingest_history (
    master_run_id           varchar(100) NOT NULL,
    ingest_pipeline_id      varchar(100) NOT NULL,
    bronze_table_nm         varchar(200) NOT NULL,
    landing_path            varchar(500) NULL,
    file_name               varchar(100) NULL,
    start_dt                timestamptz  NULL,
    end_dt                  timestamptz  NULL,
    extract_cnt             int8         NULL,
    save_cnt                int8         NULL,
    status                  varchar(15)  NULL,
    error_log               text         NULL,
    created_by              varchar(100) NULL,
    created_dt              timestamptz  NOT NULL DEFAULT now(),
    update_by               varchar(100) NULL,
    update_dt               timestamptz  NULL,

    CONSTRAINT ctl_dbx_ingest_history_pkey PRIMARY KEY (master_run_id, ingest_pipeline_id),
    CONSTRAINT ctl_dbx_ingest_history_ingest_pipeline_run_id_fkey
        FOREIGN KEY (ingest_pipeline_id) REFERENCES ctl_ingest_pipeline_run(ingest_pipeline_id),
    CONSTRAINT ctl_dbx_ingest_history_master_run_id_fkey
        FOREIGN KEY (master_run_id) REFERENCES ctl_master_pipeline_run(master_run_id),
	CONSTRAINT ctl_dbx_ingest_status_check
        CHECK (lower(status) IN ('pending','running','succeeded','failed','skipped'))
);
