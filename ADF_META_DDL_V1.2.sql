
--CREATE SCHEMA IF NOT EXISTS "META_ADF";

SET search_path TO "META_ADF", public;

------------------------------------------------------------------------
-- 1) ctl_ingest_target_master : 수집 대상 마스터 (유일한 수작업 관리 테이블)
-- ---------------------------------------------------------------------
CREATE TABLE ctl_ingest_target_master (
    target_id            bigserial      NOT NULL,
    src_type             varchar(10)    NOT NULL,
    src_db_nm            varchar(20)    NOT NULL,
    src_schema_nm        varchar(30)    NOT NULL,
    src_table_nm         varchar(100)   NOT NULL,
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
	trigger_id          varchar(100)   NOT NULL,
    trigger_nm          varchar(200)   NOT NULL,
    ingest_type         varchar(4)     NOT NULL,
    start_dt            timestamptz    NOT NULL,
    end_dt              timestamptz    NOT NULL DEFAULT '9999/12/31'::timestamp,
	exec_group          varchar(20)    NULL,
    child_total_cnt     int4           NULL DEFAULT 0,
    child_succ_cnt      int4           NULL DEFAULT 0,
    child_fail_cnt      int4           NULL DEFAULT 0,
    status              varchar(15)    NOT NULL,
    skip_type           varchar(10)    NULL,
    run_mode            varchar(10)    NULL,
    created_by          varchar(100)   NOT NULL,
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
-- 3) ctl_ingest_pipeline_run : 차일드(수집) 수행 이력 팩트 (수행시점 스냅샷)
-- ---------------------------------------------------------------------
CREATE TABLE ctl_ingest_pipeline_run (
    ingest_pipeline_id  varchar(100)   NOT NULL DEFAULT gen_random_uuid()::text,
   	master_run_id       varchar(100)   NOT NULL,
	master_pipeline_nm  varchar(200)   NOT NULL,
	ingest_pipeline_run_id  varchar(100) NOT NULL DEFAULT '-',
 	target_id           int8           NOT NULL,
    src_type            varchar(10)    NOT NULL,
    src_db_nm           varchar(20)    NOT NULL,
    src_schema_nm       varchar(30)    NOT NULL,
    src_table_nm        varchar(100)   NOT NULL,
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
    landing_path        varchar(500)   NOT NULL,
    file_name           varchar(100)   NOT NULL DEFAULT '-',
    start_dt            timestamptz    NULL,
    end_dt              timestamptz    NULL,
    extract_cnt         int8           NOT NULL DEFAULT 0,
    save_cnt            int8           NOT NULL DEFAULT 0,
    attempt_no          int4           NOT NULL DEFAULT 0,
    status              varchar(15)    NULL,
    error_log           text           NULL,
    created_by          varchar(100)   NOT NULL,
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
-- 4) ctl_dbx_ingest_history : Databricks 브론즈 적재 완료 이력
-- ---------------------------------------------------------------------
CREATE TABLE ctl_dbx_ingest_history (
    master_run_id           varchar(100) NOT NULL,
    ingest_pipeline_id      varchar(100) NOT NULL,
    bronze_table_nm         varchar(200) NOT NULL,
    landing_path            varchar(500) NOT NULL,
    file_name               varchar(100) NOT NULL,
    start_dt                timestamptz  NOT NULL,
    end_dt                  timestamptz  NOT NULL,
    extract_cnt             int8         NOT NULL,
    save_cnt                int8         NOT NULL,
    status                  varchar(15)  NOT NULL,
    error_log               text         NULL,
    created_by              varchar(100) NOT NULL,
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



-- ---------------------------------------------------------------------
-- 5) ctl_run_skip : 트리거 skip 예외 목록(opt-in)
-- ---------------------------------------------------------------------
CREATE TABLE ctl_run_skip (
	trigger_id          varchar(100)   NOT null,
    trigger_nm          varchar(200)   NOT NULL,
    skip_yn             bpchar(1)      NOT NULL DEFAULT 'n',
    skip_from_dt        timestamptz    NULL,
    skip_to_dt          timestamptz    NULL,
    skip_reason         varchar(500)   NULL,
	created_by          varchar(100)   NOT NULL,
    created_dt          timestamptz    NOT NULL DEFAULT now(),
    update_by           varchar(100)   NULL,                 
    update_dt           timestamptz    NULL,

    CONSTRAINT ctl_run_skip_pkey PRIMARY KEY (trigger_id),
    CONSTRAINT ctl_run_skip_check  CHECK (skip_from_dt IS NULL OR skip_to_dt IS NULL OR skip_from_dt <= skip_to_dt),
    CONSTRAINT ctl_run_skip_check1 CHECK ((skip_from_dt IS NULL) = (skip_to_dt IS NULL)),
    CONSTRAINT ck_skip_yn CHECK (lower(skip_yn::text) IN ('y','n'))
);


-- =====================================================================
-- [C] 테이블/컬럼 주석
-- =====================================================================
COMMENT ON TABLE  ctl_ingest_target_master IS '수집 대상 마스터 : 대상 정의 + 증분 HWM 상태. 개발자/운영자 관리 유일 수작업 테이블';
COMMENT ON COLUMN ctl_ingest_target_master.target_id           IS '수집 대상 PK';
COMMENT ON COLUMN ctl_ingest_target_master.src_type            IS '소스 커넥션 타입(CONN_TYPE): db2/mssql/oracle/mysql/postgres/rest/sftp';
COMMENT ON COLUMN ctl_ingest_target_master.src_db_nm           IS '소스 데이터베이스명(DATABASE): MLMTP/MLCRP 등';
COMMENT ON COLUMN ctl_ingest_target_master.src_schema_nm       IS '소스 스키마명';
COMMENT ON COLUMN ctl_ingest_target_master.src_table_nm        IS '소스 테이블명';
COMMENT ON COLUMN ctl_ingest_target_master.partition_clause    IS '초기적재 분할 추출용 조건절(대용량 파티션 분할)';
COMMENT ON COLUMN ctl_ingest_target_master.data_class          IS '데이터 분류: meta/raw (파이프라인 DATA_CLASS 라우팅)';
COMMENT ON COLUMN ctl_ingest_target_master.ingest_type         IS '수집 유형: full(초기/주기전체)/incr(증분)';
COMMENT ON COLUMN ctl_ingest_target_master.exec_group          IS '오케스트레이터 실행 그룹/SEQ: 초기적재 용량분산(01,02) 등';
COMMENT ON COLUMN ctl_ingest_target_master.condition_type      IS '증분 조건 유형: window/bizday/close (incr 전용)';
COMMENT ON COLUMN ctl_ingest_target_master.condition_frequency IS 'window 스케줄 주기: minutely~yearly (라우팅 전용)';
COMMENT ON COLUMN ctl_ingest_target_master.condition_interval  IS 'window 주기 단위 N (라우팅 전용). 증분 경계는 incr_column_hw_val이 담당';
COMMENT ON COLUMN ctl_ingest_target_master.incr_column_nm      IS '증분 기준(워터마크) 컬럼: IPDTM/CLYM 등';
COMMENT ON COLUMN ctl_ingest_target_master.incr_column_type    IS '워터마크 값 데이터타입: NUMERIC/DATE/TIMESTAMP/STRING (WHERE 인용 처리용). 기준컬럼 존재 시 필수';
COMMENT ON COLUMN ctl_ingest_target_master.incr_column_hw_val  IS '현재 하이워터마크 값. 수집 성공 시에만 갱신';
COMMENT ON COLUMN ctl_ingest_target_master.is_active           IS '활성 여부 Y/N';
COMMENT ON COLUMN ctl_ingest_target_master.pending_yn          IS '초기수집 대기 여부: Y=대기, N=완료';
COMMENT ON COLUMN ctl_ingest_target_master.created_by          IS '생성자';
COMMENT ON COLUMN ctl_ingest_target_master.created_dt          IS '생성 일시';
COMMENT ON COLUMN ctl_ingest_target_master.update_by           IS '수정자';
COMMENT ON COLUMN ctl_ingest_target_master.update_dt           IS '수정 일시';

COMMENT ON TABLE  ctl_master_pipeline_run IS '마스터 파이프라인 수행 상태. ux_mrs_running으로 동시수행 락 겸용';
COMMENT ON COLUMN ctl_master_pipeline_run.master_run_id      IS '마스터 파이프라인 실행 ID (@pipeline().RunId)';
COMMENT ON COLUMN ctl_master_pipeline_run.master_pipeline_nm IS '마스터 파이프라인명 (@pipeline().Pipeline)';
COMMENT ON COLUMN ctl_master_pipeline_run.trigger_id         IS '트리거명 (@pipeline().TriggerId)';
COMMENT ON COLUMN ctl_master_pipeline_run.trigger_nm         IS '트리거명 (@pipeline().TriggerName)';
COMMENT ON COLUMN ctl_master_pipeline_run.ingest_type        IS '수집 유형 full/incr';
COMMENT ON COLUMN ctl_master_pipeline_run.start_dt           IS '수행 시작 일시';
COMMENT ON COLUMN ctl_master_pipeline_run.end_dt             IS '수행 종료 일시';
COMMENT ON COLUMN ctl_master_pipeline_run.exec_group         IS '오케스트레이터 실행 그룹/SEQ: 초기적재 용량분산(01,02) 등';
COMMENT ON COLUMN ctl_master_pipeline_run.child_total_cnt    IS '차일드 전체 건수';
COMMENT ON COLUMN ctl_master_pipeline_run.child_succ_cnt     IS '차일드 성공 건수';
COMMENT ON COLUMN ctl_master_pipeline_run.child_fail_cnt     IS '차일드 실패 건수';
COMMENT ON COLUMN ctl_master_pipeline_run.status             IS '상태: PENDING/RUNNING/SUCCEEDED/FAILED/SKIPPED';
COMMENT ON COLUMN ctl_master_pipeline_run.skip_type          IS 'skip 유형: WINDOW/FLAG/LOCKED';
COMMENT ON COLUMN ctl_master_pipeline_run.run_mode           IS '실행 모드: SCHEDULE/MANUAL';
COMMENT ON COLUMN ctl_master_pipeline_run.created_by         IS '생성자';
COMMENT ON COLUMN ctl_master_pipeline_run.created_dt         IS '생성 일시';
COMMENT ON COLUMN ctl_master_pipeline_run.update_by          IS '수정자';
COMMENT ON COLUMN ctl_master_pipeline_run.update_dt          IS '수정 일시';

COMMENT ON TABLE  ctl_ingest_pipeline_run IS '차일드(수집) 파이프라인 수행 이력. 대상 마스터 값의 수행시점 스냅샷 보존';
COMMENT ON COLUMN ctl_ingest_pipeline_run.ingest_pipeline_id     IS '파이프라인 ID (PK)  UUID 자동생성';
COMMENT ON COLUMN ctl_ingest_pipeline_run.master_run_id          IS '마스터 실행 ID (FK → ctl_master_pipeline_run)';
COMMENT ON COLUMN ctl_ingest_pipeline_run.master_pipeline_nm     IS '마스터 파이프라인명';
COMMENT ON COLUMN ctl_ingest_pipeline_run.target_id              IS '수집 대상 ID (FK → ctl_ingest_target_master)';
COMMENT ON COLUMN ctl_ingest_pipeline_run.src_type               IS '[스냅샷] 소스 커넥션 타입';
COMMENT ON COLUMN ctl_ingest_pipeline_run.src_db_nm              IS '[스냅샷] 소스 DB명';
COMMENT ON COLUMN ctl_ingest_pipeline_run.src_schema_nm          IS '[스냅샷] 소스 스키마명';
COMMENT ON COLUMN ctl_ingest_pipeline_run.src_table_nm           IS '[스냅샷] 소스 테이블명';
COMMENT ON COLUMN ctl_ingest_pipeline_run.data_class             IS '[스냅샷] 데이터 분류 meta/raw';
COMMENT ON COLUMN ctl_ingest_pipeline_run.ingest_type            IS '[스냅샷] 수집 유형 full/incr';
COMMENT ON COLUMN ctl_ingest_pipeline_run.exec_group             IS '[스냅샷] 실행 그룹/SEQ';
COMMENT ON COLUMN ctl_ingest_pipeline_run.condition_type         IS '[스냅샷] 증분 조건 유형';
COMMENT ON COLUMN ctl_ingest_pipeline_run.condition_frequency    IS '[스냅샷] window 주기';
COMMENT ON COLUMN ctl_ingest_pipeline_run.condition_interval     IS '[스냅샷] window 주기 단위 N';
COMMENT ON COLUMN ctl_ingest_pipeline_run.incr_column_nm         IS '[스냅샷] 증분 기준 컬럼';
COMMENT ON COLUMN ctl_ingest_pipeline_run.incr_column_type       IS '[스냅샷] 워터마크 값 타입';
COMMENT ON COLUMN ctl_ingest_pipeline_run.incr_column_hw_val     IS '[스냅샷] 이번 수행에 사용된 HWM 값';
COMMENT ON COLUMN ctl_ingest_pipeline_run.is_active              IS '[스냅샷] 활성 여부';
COMMENT ON COLUMN ctl_ingest_pipeline_run.pending_yn             IS '[스냅샷] 초기수집 대기 여부';
COMMENT ON COLUMN ctl_ingest_pipeline_run.extract_query          IS '실제 실행된 추출 쿼리';
COMMENT ON COLUMN ctl_ingest_pipeline_run.landing_path           IS '실제 랜딩 경로';
COMMENT ON COLUMN ctl_ingest_pipeline_run.file_name              IS '산출 파일명';
COMMENT ON COLUMN ctl_ingest_pipeline_run.start_dt               IS '수행 시작 일시';
COMMENT ON COLUMN ctl_ingest_pipeline_run.end_dt                 IS '수행 종료 일시';
COMMENT ON COLUMN ctl_ingest_pipeline_run.extract_cnt            IS '추출 건수';
COMMENT ON COLUMN ctl_ingest_pipeline_run.save_cnt               IS '저장 건수';
COMMENT ON COLUMN ctl_ingest_pipeline_run.attempt_no             IS '재시도 회차';
COMMENT ON COLUMN ctl_ingest_pipeline_run.status                 IS '상태: RUNNING/SUCCEEDED/FAILED 등';
COMMENT ON COLUMN ctl_ingest_pipeline_run.error_log              IS '오류 로그';
COMMENT ON COLUMN ctl_ingest_pipeline_run.created_by             IS '생성자';
COMMENT ON COLUMN ctl_ingest_pipeline_run.created_dt             IS '생성 일시';
COMMENT ON COLUMN ctl_ingest_pipeline_run.update_by              IS '수정자';
COMMENT ON COLUMN ctl_ingest_pipeline_run.update_dt              IS '수정 일시';

COMMENT ON TABLE  ctl_dbx_ingest_history IS 'Databricks 브론즈 적재 완료 이력';
COMMENT ON COLUMN ctl_dbx_ingest_history.master_run_id          IS '마스터 실행 ID (PK, FK → ctl_master_pipeline_run)';
COMMENT ON COLUMN ctl_dbx_ingest_history.ingest_pipeline_id     IS '파이프라인 ID (PK, FK → ctl_ingest_pipeline_run)';
COMMENT ON COLUMN ctl_dbx_ingest_history.bronze_table_nm        IS '브론즈 테이블명';
COMMENT ON COLUMN ctl_dbx_ingest_history.landing_path           IS '랜딩 경로';
COMMENT ON COLUMN ctl_dbx_ingest_history.file_name              IS '적재 파일명';
COMMENT ON COLUMN ctl_dbx_ingest_history.start_dt               IS '적재 시작 일시';
COMMENT ON COLUMN ctl_dbx_ingest_history.end_dt                 IS '적재 종료 일시';
COMMENT ON COLUMN ctl_dbx_ingest_history.extract_cnt            IS '추출 건수';
COMMENT ON COLUMN ctl_dbx_ingest_history.save_cnt               IS '저장 건수';
COMMENT ON COLUMN ctl_dbx_ingest_history.status                 IS '상태';
COMMENT ON COLUMN ctl_dbx_ingest_history.error_log              IS '오류 로그';
COMMENT ON COLUMN ctl_dbx_ingest_history.created_by             IS '생성자';
COMMENT ON COLUMN ctl_dbx_ingest_history.created_dt             IS '생성 일시';
COMMENT ON COLUMN ctl_dbx_ingest_history.update_by              IS '수정자';
COMMENT ON COLUMN ctl_dbx_ingest_history.update_dt              IS '수정 일시';


COMMENT ON TABLE  ctl_run_skip IS '트리거 skip 예외 목록(opt-in). 행 없음=RUN, skip_yn=Y=SKIP';
COMMENT ON COLUMN ctl_run_skip.trigger_id         IS '트리ID (PK)';
COMMENT ON COLUMN ctl_run_skip.trigger_nm         IS '트리거명';
COMMENT ON COLUMN ctl_run_skip.skip_yn            IS 'skip 여부 Y/N';
COMMENT ON COLUMN ctl_run_skip.skip_from_dt       IS 'skip 시작 일시(WINDOW). NULL이면 무기한(FLAG)';
COMMENT ON COLUMN ctl_run_skip.skip_to_dt         IS 'skip 종료 일시(WINDOW). from/to는 동시 NULL 또는 동시 값';
COMMENT ON COLUMN ctl_run_skip.skip_reason        IS 'skip 사유';
COMMENT ON COLUMN ctl_run_skip.created_by         IS '생성자';
COMMENT ON COLUMN ctl_run_skip.created_dt         IS '생성 일시';
COMMENT ON COLUMN ctl_run_skip.update_by          IS '수정자';
COMMENT ON COLUMN ctl_run_skip.update_dt          IS '수정 일시';
