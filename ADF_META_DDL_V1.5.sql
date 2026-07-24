-- =====================================================================
-- ADF 메타 컨트롤 DDL  V1.5   (스키마: "META_ADF")
-- V1.4 대비 변경:
--   ⑧ ctl_ingest_pipeline_run 자식 레벨 동시수행 락 추가
--      (ux_ipr_running: 동일 target_id+ingest_type RUNNING 1건 제한)
-- V1.3 대비 변경:
--   ⑦ ctl_master_pipeline_run.skip_type 컬럼 및 skip_type_check CHECK 제약 삭제
-- V1.2 대비 변경:
--   ① is_active → delete_yn(논리삭제), pending_yn/adhoc_yn → init_wait_yn(초기수집 대기)
--   ② 코드성 값 전부 대문자 저장 + CHECK/인덱스의 lower() 전면 제거
--   ③ ctl_master_pipeline_run.end_dt NULL 허용(센티넬 기본값 제거)
--   ④ skip 판단은 ADF 로직 → ctl_trigger_history(수행 이력 로그)에 기록
--   ⑤ incr_start_val(증분 재수집 시작값), file_size_mb 등 신규 컬럼 주석 보강
--   ⑥ (버그수정) 존재하지 않는 is_active/pending_yn 참조 CHECK·인덱스 정정
-- ※ 코드성 값은 반드시 대문자로 저장(META/RAW/FULL/INCR/WINDOW/...)— 적재 시 대문자 정규화 필수
-- =====================================================================

-- CREATE SCHEMA IF NOT EXISTS "META_ADF";
SET search_path TO "META_ADF", public;


-- ---------------------------------------------------------------------
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
    delete_yn            bpchar(1)      NOT NULL DEFAULT 'N',   -- 논리삭제 (구 is_active 대체)
    init_wait_yn         bpchar(1)      NOT NULL DEFAULT 'Y',   -- 초기수집 대기 (구 pending_yn)
    created_by           varchar(100)   NULL,
    created_dt           timestamptz    NOT NULL DEFAULT now(),
    update_by            varchar(100)   NULL,
    update_dt            timestamptz    NULL,

    CONSTRAINT ctl_ingest_target_master_pkey PRIMARY KEY (target_id),

    -- 값 열거 검증 (대문자 저장)
    CONSTRAINT ck_data_class  CHECK (data_class  IN ('META','RAW')),
    CONSTRAINT ck_ingest_type CHECK (ingest_type IN ('FULL','INCR')),
    CONSTRAINT ctl_ingest_target_master_condition_type_check
        CHECK (condition_type IN ('WINDOW','BIZDAY','CLOSE')),
    CONSTRAINT ctl_ingest_target_master_condition_frequency_check
        CHECK (condition_frequency IN ('MINUTELY','HOURLY','DAILY','WEEKLY','MONTHLY','YEARLY')),
    CONSTRAINT ck_incr_col_type
        CHECK (incr_column_type IN ('NUMERIC','DATE','TIMESTAMP','STRING')),

    -- FULL: 조건/증분 컬럼 전부 NULL
    CONSTRAINT ck_full_no_cond CHECK (
        ingest_type <> 'FULL'
        OR (condition_type IS NULL AND condition_frequency IS NULL
            AND condition_interval IS NULL AND incr_column_nm IS NULL
            AND incr_column_type IS NULL AND incr_column_hw_val IS NULL)
    ),
    -- INCR: condition_type 필수
    CONSTRAINT ck_incr_needs_type CHECK (
        ingest_type <> 'INCR' OR condition_type IS NOT NULL
    ),
    -- WINDOW: frequency+interval+기준컬럼 필수
    CONSTRAINT ck_window_params CHECK (
        condition_type IS DISTINCT FROM 'WINDOW'
        OR (condition_frequency IS NOT NULL AND condition_interval IS NOT NULL
            AND incr_column_nm IS NOT NULL)
    ),
    -- frequency는 WINDOW에서만
    CONSTRAINT ck_freq_window_only CHECK (
        condition_frequency IS NULL OR condition_type = 'WINDOW'
    ),
    -- BIZDAY/CLOSE: 기준컬럼 필수
    CONSTRAINT ck_bizday_col CHECK (
        condition_type IS DISTINCT FROM 'BIZDAY' OR incr_column_nm IS NOT NULL
    ),
    CONSTRAINT ck_close_col CHECK (
        condition_type IS DISTINCT FROM 'CLOSE'  OR incr_column_nm IS NOT NULL
    ),
    -- 증분 기준컬럼이 있으면 타입(인용 처리용) 필수
    CONSTRAINT ck_incr_col_type_req CHECK (
        incr_column_nm IS NULL OR incr_column_type IS NOT NULL
    ),
    -- Y/N 플래그 검증
    CONSTRAINT ck_target_yn CHECK (delete_yn IN ('Y','N') AND init_wait_yn IN ('Y','N'))
);

-- 초기수집 대기 대상 Lookup (미삭제 + 초기수집 대기)
CREATE INDEX ix_target_pending ON ctl_ingest_target_master
    USING btree (data_class)
    WHERE (delete_yn = 'N' AND init_wait_yn = 'Y');

-- 오케스트레이터 라우팅 Lookup (미삭제 대상)
CREATE INDEX ix_target_route ON ctl_ingest_target_master
    USING btree (data_class, ingest_type, condition_type, condition_frequency, condition_interval)
    WHERE (delete_yn = 'N');


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
    end_dt              timestamptz    NULL,                    -- ③ NULL 허용(수행 중 = NULL)
    exec_group          varchar(20)    NULL,
    child_total_cnt     int4           NULL DEFAULT 0,
    child_succ_cnt      int4           NULL DEFAULT 0,
    child_fail_cnt      int4           NULL DEFAULT 0,
    status              varchar(15)    NOT NULL,
    run_mode            varchar(10)    NULL,
    created_by          varchar(100)   NOT NULL,
    created_dt          timestamptz    NOT NULL DEFAULT now(),
    update_by           varchar(100)   NULL,
    update_dt           timestamptz    NULL,

    CONSTRAINT ctl_master_run_status_pkey PRIMARY KEY (master_run_id),
    CONSTRAINT ctl_master_run_status_ingest_type_check CHECK (ingest_type IN ('FULL','INCR')),
    CONSTRAINT ctl_master_run_status_run_mode_check    CHECK (run_mode  IN ('SCHEDULE','MANUAL')),
    CONSTRAINT ctl_master_run_status_status_check
        CHECK (status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','SKIPPED'))
);

CREATE INDEX ix_mpr_status_start ON ctl_master_pipeline_run
    USING btree (status, start_dt);

-- 동시수행 방지: 동일 파이프라인+수집타입 RUNNING 1건 제한
CREATE UNIQUE INDEX ux_mrs_running ON ctl_master_pipeline_run
    USING btree (master_pipeline_nm, ingest_type)
    WHERE (status = 'RUNNING');


-- ---------------------------------------------------------------------
-- 3) ctl_ingest_pipeline_run : 차일드(수집) 수행 이력 팩트 (수행시점 스냅샷)
-- ---------------------------------------------------------------------
CREATE TABLE ctl_ingest_pipeline_run (
    ingest_pipeline_id     varchar(100) NOT NULL DEFAULT gen_random_uuid()::text,
    master_run_id          varchar(100) NOT NULL,
    master_pipeline_nm     varchar(200) NOT NULL,
    ingest_pipeline_run_id varchar(100) NOT NULL DEFAULT '-',
    target_id              int8         NOT NULL,
    src_type               varchar(10)  NOT NULL,
    src_db_nm              varchar(20)  NOT NULL,
    src_schema_nm          varchar(30)  NOT NULL,
    src_table_nm           varchar(100) NOT NULL,
    data_class             varchar(4)   NOT NULL,
    ingest_type            varchar(4)   NOT NULL,
    exec_group             varchar(20)  NULL,
    condition_type         varchar(12)  NULL,
    condition_frequency    varchar(10)  NULL,
    condition_interval     int4         NULL,
    incr_column_nm         varchar(30)  NULL,
    incr_column_type       varchar(10)  NULL,
    incr_column_hw_val     varchar(100) NULL,
    incr_start_val         varchar(100) NULL,                   -- 증분 재수집 시작값(override)
    init_wait_yn           bpchar(1)    NOT NULL DEFAULT 'N',   -- [스냅샷] 초기수집 대기
    extract_query          text         NOT NULL,
    landing_path           varchar(500) NOT NULL,
    file_name              varchar(100) NOT NULL DEFAULT '-',
    file_size_mb           int8         NULL DEFAULT 0,
    start_dt               timestamptz  NULL,
    end_dt                 timestamptz  NULL,
    extract_cnt            int8         NOT NULL DEFAULT 0,
    save_cnt               int8         NOT NULL DEFAULT 0,
    attempt_no             int4         NOT NULL DEFAULT 0,
    status                 varchar(15)  NULL,
    error_log              text         NULL,
    created_by             varchar(100) NOT NULL,
    created_dt             timestamptz  NOT NULL DEFAULT now(),
    update_by              varchar(100) NULL,
    update_dt              timestamptz  NULL,

    CONSTRAINT ctl_ingest_run_pkey PRIMARY KEY (ingest_pipeline_id),
    CONSTRAINT ctl_ingest_pipeline_run_master_run_id_fkey
        FOREIGN KEY (master_run_id) REFERENCES ctl_master_pipeline_run(master_run_id),
    CONSTRAINT ctl_ingest_pipeline_run_target_id_fkey
        FOREIGN KEY (target_id) REFERENCES ctl_ingest_target_master(target_id),
    CONSTRAINT ck_ipr_yn CHECK (init_wait_yn IN ('Y','N')),
    CONSTRAINT ctl_ingest_run_status_check
        CHECK (status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','SKIPPED'))
);

CREATE INDEX ix_ipr_created ON ctl_ingest_pipeline_run USING btree (created_dt);
CREATE INDEX ix_ipr_mri     ON ctl_ingest_pipeline_run USING btree (master_run_id);
CREATE INDEX ix_ipr_status  ON ctl_ingest_pipeline_run USING btree (master_run_id, status);
CREATE INDEX ix_ipr_target  ON ctl_ingest_pipeline_run USING btree (target_id);

-- 자식(수집) 동시수행 방지: 동일 대상+수집타입 RUNNING 1건 제한
CREATE UNIQUE INDEX ux_ipr_running ON ctl_ingest_pipeline_run
    USING btree (target_id, ingest_type)
    WHERE (status = 'RUNNING');


-- ---------------------------------------------------------------------
-- 4) ctl_dbx_ingest_history : Databricks 브론즈 적재 완료 이력
-- ---------------------------------------------------------------------
CREATE TABLE ctl_dbx_ingest_history (
    master_run_id       varchar(100) NOT NULL,
    ingest_pipeline_id  varchar(100) NOT NULL,
    bronze_table_nm     varchar(200) NOT NULL,
    landing_path        varchar(500) NOT NULL,
    file_name           varchar(100) NOT NULL,
    start_dt            timestamptz  NOT NULL,
    end_dt              timestamptz  NOT NULL,
    extract_cnt         int8         NOT NULL,
    save_cnt            int8         NOT NULL,
    status              varchar(15)  NOT NULL,
    error_log           text         NULL,
    created_by          varchar(100) NOT NULL,
    created_dt          timestamptz  NOT NULL DEFAULT now(),
    update_by           varchar(100) NULL,
    update_dt           timestamptz  NULL,

    CONSTRAINT ctl_dbx_ingest_history_pkey PRIMARY KEY (master_run_id, ingest_pipeline_id),
    CONSTRAINT ctl_dbx_ingest_history_ingest_pipeline_run_id_fkey
        FOREIGN KEY (ingest_pipeline_id) REFERENCES ctl_ingest_pipeline_run(ingest_pipeline_id),
    CONSTRAINT ctl_dbx_ingest_history_master_run_id_fkey
        FOREIGN KEY (master_run_id) REFERENCES ctl_master_pipeline_run(master_run_id),
    CONSTRAINT ctl_dbx_ingest_status_check
        CHECK (status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','SKIPPED'))
);


-- ---------------------------------------------------------------------
-- 5) ctl_trigger_history : 트리거 수행 이력 (skip 판단은 ADF 로직, 결과만 기록)
-- ---------------------------------------------------------------------
CREATE TABLE ctl_trigger_history (
    trigger_id          varchar(100) NOT NULL,
    trigger_nm          varchar(200) NOT NULL,
    master_run_id       varchar(100) NOT NULL,
    master_pipeline_nm  varchar(200) NOT NULL,
    skip_yn             bpchar(1)    NOT NULL DEFAULT 'N',
    skip_reason         varchar(200) NULL,                     -- 정상 실행 시 NULL
    created_by          varchar(100) NOT NULL,
    created_dt          timestamptz  NOT NULL DEFAULT now(),
    update_by           varchar(100) NULL,
    update_dt           timestamptz  NULL,

    CONSTRAINT ctl_trigger_history_pkey PRIMARY KEY (trigger_id, master_run_id),
    CONSTRAINT ck_trg_skip_yn CHECK (skip_yn IN ('Y','N'))
);


-- =====================================================================
-- [C] 테이블/컬럼 주석
-- =====================================================================
COMMENT ON TABLE  ctl_ingest_target_master IS '수집 대상 마스터 : 대상 정의 + 증분 HWM 상태. 개발자/운영자 관리 유일 수작업 테이블';
COMMENT ON COLUMN ctl_ingest_target_master.target_id           IS '수집 대상 PK';
COMMENT ON COLUMN ctl_ingest_target_master.src_type            IS '소스 커넥션 타입(CONN_TYPE): DB2/MSSQL/ORACLE/MYSQL/POSTGRES/REST/SFTP';
COMMENT ON COLUMN ctl_ingest_target_master.src_db_nm           IS '소스 데이터베이스명(DATABASE): MLMTP/MLCRP 등';
COMMENT ON COLUMN ctl_ingest_target_master.src_schema_nm       IS '소스 스키마명';
COMMENT ON COLUMN ctl_ingest_target_master.src_table_nm        IS '소스 테이블명';
COMMENT ON COLUMN ctl_ingest_target_master.partition_clause    IS '초기적재 분할 추출용 조건절(대용량 파티션 분할)';
COMMENT ON COLUMN ctl_ingest_target_master.data_class          IS '데이터 분류: META/RAW (파이프라인 DATA_CLASS 라우팅)';
COMMENT ON COLUMN ctl_ingest_target_master.ingest_type         IS '수집 유형: FULL(초기/주기전체)/INCR(증분)';
COMMENT ON COLUMN ctl_ingest_target_master.exec_group          IS '오케스트레이터 실행 그룹/SEQ: 초기적재 용량분산(01,02) 등';
COMMENT ON COLUMN ctl_ingest_target_master.condition_type      IS '증분 조건 유형: WINDOW/BIZDAY/CLOSE (INCR 전용)';
COMMENT ON COLUMN ctl_ingest_target_master.condition_frequency IS 'WINDOW 스케줄 주기: MINUTELY~YEARLY (라우팅 전용)';
COMMENT ON COLUMN ctl_ingest_target_master.condition_interval  IS 'WINDOW 주기 단위 N (라우팅 전용). 증분 경계는 incr_column_hw_val이 담당';
COMMENT ON COLUMN ctl_ingest_target_master.incr_column_nm      IS '증분 기준(워터마크) 컬럼: IPDTM/CLYM 등';
COMMENT ON COLUMN ctl_ingest_target_master.incr_column_type    IS '워터마크 값 데이터타입: NUMERIC/DATE/TIMESTAMP/STRING (WHERE 인용 처리용). 기준컬럼 존재 시 필수';
COMMENT ON COLUMN ctl_ingest_target_master.incr_column_hw_val  IS '현재 하이워터마크 값. 수집 성공 시에만 갱신';
COMMENT ON COLUMN ctl_ingest_target_master.delete_yn           IS '논리 삭제 여부: Y=삭제, N=사용';
COMMENT ON COLUMN ctl_ingest_target_master.init_wait_yn        IS '초기수집 대기 여부: Y=대기(초기 FULL 미완), N=완료';
COMMENT ON COLUMN ctl_ingest_target_master.created_by          IS '생성자';
COMMENT ON COLUMN ctl_ingest_target_master.created_dt          IS '생성 일시';
COMMENT ON COLUMN ctl_ingest_target_master.update_by           IS '수정자';
COMMENT ON COLUMN ctl_ingest_target_master.update_dt           IS '수정 일시';

COMMENT ON TABLE  ctl_master_pipeline_run IS '마스터 파이프라인 수행 상태. ux_mrs_running으로 동시수행 락 겸용';
COMMENT ON COLUMN ctl_master_pipeline_run.master_run_id      IS '마스터 파이프라인 실행 ID (@pipeline().RunId)';
COMMENT ON COLUMN ctl_master_pipeline_run.master_pipeline_nm IS '마스터 파이프라인명 (@pipeline().Pipeline)';
COMMENT ON COLUMN ctl_master_pipeline_run.trigger_id         IS '트리거 ID (@pipeline().TriggerId)';
COMMENT ON COLUMN ctl_master_pipeline_run.trigger_nm         IS '트리거명 (@pipeline().TriggerName)';
COMMENT ON COLUMN ctl_master_pipeline_run.ingest_type        IS '수집 유형 FULL/INCR';
COMMENT ON COLUMN ctl_master_pipeline_run.start_dt           IS '수행 시작 일시';
COMMENT ON COLUMN ctl_master_pipeline_run.end_dt             IS '수행 종료 일시 (NULL=수행 중)';
COMMENT ON COLUMN ctl_master_pipeline_run.exec_group         IS '오케스트레이터 실행 그룹/SEQ';
COMMENT ON COLUMN ctl_master_pipeline_run.child_total_cnt    IS '차일드 전체 건수';
COMMENT ON COLUMN ctl_master_pipeline_run.child_succ_cnt     IS '차일드 성공 건수';
COMMENT ON COLUMN ctl_master_pipeline_run.child_fail_cnt     IS '차일드 실패 건수';
COMMENT ON COLUMN ctl_master_pipeline_run.status             IS '상태: PENDING/RUNNING/SUCCEEDED/FAILED/SKIPPED';
COMMENT ON COLUMN ctl_master_pipeline_run.run_mode           IS '실행 모드: SCHEDULE/MANUAL';
COMMENT ON COLUMN ctl_master_pipeline_run.created_by         IS '생성자';
COMMENT ON COLUMN ctl_master_pipeline_run.created_dt         IS '생성 일시';
COMMENT ON COLUMN ctl_master_pipeline_run.update_by          IS '수정자';
COMMENT ON COLUMN ctl_master_pipeline_run.update_dt          IS '수정 일시';

COMMENT ON TABLE  ctl_ingest_pipeline_run IS '차일드(수집) 파이프라인 수행 이력. 대상 마스터 값의 수행시점 스냅샷 보존';
COMMENT ON COLUMN ctl_ingest_pipeline_run.ingest_pipeline_id     IS '수집 파이프라인 ID (PK). UUID 자동생성';
COMMENT ON COLUMN ctl_ingest_pipeline_run.master_run_id          IS '마스터 실행 ID (FK → ctl_master_pipeline_run)';
COMMENT ON COLUMN ctl_ingest_pipeline_run.master_pipeline_nm     IS '마스터 파이프라인명';
COMMENT ON COLUMN ctl_ingest_pipeline_run.ingest_pipeline_run_id IS 'ADF 차일드 파이프라인 실제 RunId (@pipeline().RunId). 미전달 시 ''-''';
COMMENT ON COLUMN ctl_ingest_pipeline_run.target_id              IS '수집 대상 ID (FK → ctl_ingest_target_master)';
COMMENT ON COLUMN ctl_ingest_pipeline_run.src_type               IS '[스냅샷] 소스 커넥션 타입';
COMMENT ON COLUMN ctl_ingest_pipeline_run.src_db_nm              IS '[스냅샷] 소스 DB명';
COMMENT ON COLUMN ctl_ingest_pipeline_run.src_schema_nm          IS '[스냅샷] 소스 스키마명';
COMMENT ON COLUMN ctl_ingest_pipeline_run.src_table_nm           IS '[스냅샷] 소스 테이블명';
COMMENT ON COLUMN ctl_ingest_pipeline_run.data_class             IS '[스냅샷] 데이터 분류 META/RAW';
COMMENT ON COLUMN ctl_ingest_pipeline_run.ingest_type            IS '[스냅샷] 수집 유형 FULL/INCR';
COMMENT ON COLUMN ctl_ingest_pipeline_run.exec_group             IS '[스냅샷] 실행 그룹/SEQ';
COMMENT ON COLUMN ctl_ingest_pipeline_run.condition_type         IS '[스냅샷] 증분 조건 유형';
COMMENT ON COLUMN ctl_ingest_pipeline_run.condition_frequency    IS '[스냅샷] WINDOW 주기';
COMMENT ON COLUMN ctl_ingest_pipeline_run.condition_interval     IS '[스냅샷] WINDOW 주기 단위 N';
COMMENT ON COLUMN ctl_ingest_pipeline_run.incr_column_nm         IS '[스냅샷] 증분 기준 컬럼';
COMMENT ON COLUMN ctl_ingest_pipeline_run.incr_column_type       IS '[스냅샷] 워터마크 값 타입';
COMMENT ON COLUMN ctl_ingest_pipeline_run.incr_column_hw_val     IS '[스냅샷] 이번 수행에 사용된 HWM 값';
COMMENT ON COLUMN ctl_ingest_pipeline_run.incr_start_val         IS '증분 재수집 시작값(override). NULL이면 저장 HWM 사용 (수동 재수행용)';
COMMENT ON COLUMN ctl_ingest_pipeline_run.init_wait_yn           IS '[스냅샷] 초기수집 대기 여부';
COMMENT ON COLUMN ctl_ingest_pipeline_run.extract_query          IS '실제 실행된 추출 쿼리';
COMMENT ON COLUMN ctl_ingest_pipeline_run.landing_path           IS '실제 랜딩 경로';
COMMENT ON COLUMN ctl_ingest_pipeline_run.file_name              IS '수집한 파일명';
COMMENT ON COLUMN ctl_ingest_pipeline_run.file_size_mb           IS '수집한 파일 사이즈(MB)';
COMMENT ON COLUMN ctl_ingest_pipeline_run.start_dt               IS '수행 시작 일시';
COMMENT ON COLUMN ctl_ingest_pipeline_run.end_dt                 IS '수행 종료 일시';
COMMENT ON COLUMN ctl_ingest_pipeline_run.extract_cnt            IS '추출 건수';
COMMENT ON COLUMN ctl_ingest_pipeline_run.save_cnt               IS '저장 건수';
COMMENT ON COLUMN ctl_ingest_pipeline_run.attempt_no             IS '재시도 회차';
COMMENT ON COLUMN ctl_ingest_pipeline_run.status                 IS '상태: PENDING/RUNNING/SUCCEEDED/FAILED/SKIPPED';
COMMENT ON COLUMN ctl_ingest_pipeline_run.error_log              IS '오류 로그';
COMMENT ON COLUMN ctl_ingest_pipeline_run.created_by             IS '생성자';
COMMENT ON COLUMN ctl_ingest_pipeline_run.created_dt             IS '생성 일시';
COMMENT ON COLUMN ctl_ingest_pipeline_run.update_by             IS '수정자';
COMMENT ON COLUMN ctl_ingest_pipeline_run.update_dt             IS '수정 일시';

COMMENT ON TABLE  ctl_dbx_ingest_history IS 'Databricks 브론즈 적재 완료 이력';
COMMENT ON COLUMN ctl_dbx_ingest_history.master_run_id          IS '마스터 실행 ID (PK, FK → ctl_master_pipeline_run)';
COMMENT ON COLUMN ctl_dbx_ingest_history.ingest_pipeline_id     IS '수집 파이프라인 ID (PK, FK → ctl_ingest_pipeline_run)';
COMMENT ON COLUMN ctl_dbx_ingest_history.bronze_table_nm        IS '브론즈 테이블명';
COMMENT ON COLUMN ctl_dbx_ingest_history.landing_path           IS '랜딩 경로';
COMMENT ON COLUMN ctl_dbx_ingest_history.file_name              IS '적재 파일명';
COMMENT ON COLUMN ctl_dbx_ingest_history.start_dt               IS '적재 시작 일시';
COMMENT ON COLUMN ctl_dbx_ingest_history.end_dt                 IS '적재 종료 일시';
COMMENT ON COLUMN ctl_dbx_ingest_history.extract_cnt            IS '추출 건수';
COMMENT ON COLUMN ctl_dbx_ingest_history.save_cnt               IS '저장 건수';
COMMENT ON COLUMN ctl_dbx_ingest_history.status                 IS '상태: PENDING/RUNNING/SUCCEEDED/FAILED/SKIPPED';
COMMENT ON COLUMN ctl_dbx_ingest_history.error_log              IS '오류 로그';
COMMENT ON COLUMN ctl_dbx_ingest_history.created_by             IS '생성자';
COMMENT ON COLUMN ctl_dbx_ingest_history.created_dt             IS '생성 일시';
COMMENT ON COLUMN ctl_dbx_ingest_history.update_by             IS '수정자';
COMMENT ON COLUMN ctl_dbx_ingest_history.update_dt             IS '수정 일시';

COMMENT ON TABLE  ctl_trigger_history                   IS '트리거 수행 이력 : 마스터 파이프라인 트리거 실행 및 Skip 판단 결과 기록(판단은 ADF 로직)';
COMMENT ON COLUMN ctl_trigger_history.trigger_id        IS '트리거 ID (PK, @pipeline().TriggerId)';
COMMENT ON COLUMN ctl_trigger_history.trigger_nm        IS '트리거명 (@pipeline().TriggerName)';
COMMENT ON COLUMN ctl_trigger_history.master_run_id     IS '마스터 실행 ID (PK, @pipeline().RunId)';
COMMENT ON COLUMN ctl_trigger_history.master_pipeline_nm IS '마스터 파이프라인명 (@pipeline().Pipeline)';
COMMENT ON COLUMN ctl_trigger_history.skip_yn           IS 'Skip 여부: Y(Skip됨)/N(정상 실행)';
COMMENT ON COLUMN ctl_trigger_history.skip_reason       IS 'Skip 사유 (정상 실행 시 NULL)';
COMMENT ON COLUMN ctl_trigger_history.created_by        IS '생성자';
COMMENT ON COLUMN ctl_trigger_history.created_dt        IS '생성 일시';
COMMENT ON COLUMN ctl_trigger_history.update_by         IS '수정자';
COMMENT ON COLUMN ctl_trigger_history.update_dt         IS '수정 일시';
