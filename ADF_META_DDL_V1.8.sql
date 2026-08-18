-- =====================================================================
-- ADF 메타 컨트롤 DDL  V1.8
--   대상 DB : Azure Database for PostgreSQL
--   스키마   : META_ADF
--   기준     : V1.7 + 파이프라인 설계 오버뷰 v1.0 반영분
--
--   [V1.7 → V1.8 변경 요약]
--   1) ctl_ingest_target_master
--      + last_success_dt      : 마지막 성공 완료 시각 (만회 모드 판정 + 모니터링)
--      + ingest_pipeline_nm   : 차일드 파이프라인명 override (NULL이면 명명규칙 조합)
--      + ix_target_last_success : 지연 대상 모니터링 인덱스
--   2) ctl_master_pipeline_run
--      + init_yn              : 초기수집 회차 여부 (기동방식 run_mode와 직교하는 축)
--      + data_class           : 회차 단위 조회 편의
--      · run_mode CHECK       : BACKFILL / TEST 값 추가 (D07 확장)
--      · ux_mrs_running       : 락 키에 init_yn 추가, 범위 PENDING+RUNNING,
--                               매뉴얼 한정 해제 (스케줄 초기수집 도입)
--   3) ctl_ingest_pipeline_run
--      + ingest_pipeline_nm   : [스냅샷] 실제 수행 차일드 파이프라인명 (디스패처 Switch 키)
--      · ux_ipr_running       : 범위 PENDING+RUNNING 으로 확대
--      + ix_ipr_dispatch      : 디스패처 Lookup 인덱스
--   4) ctl_dbx_api_call_history (신규)
--      Databricks REST API 호출 이력. ADF 책임 경계의 종점
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS "ADFADM";

SET search_path TO "ADFADM", public;

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
    last_success_dt      timestamptz    NULL,                   -- [V1.8] 마지막 성공 완료 시각
    ingest_pipeline_nm   varchar(200)   NULL,                   -- [V1.8] 차일드 파이프라인명 override
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

-- [V1.8] 지연 대상 모니터링 : "N일 이상 성공 없는 대상" 조회
CREATE INDEX ix_target_last_success ON ctl_ingest_target_master
    USING btree (last_success_dt)
    WHERE (delete_yn = 'N');

-- ---------------------------------------------------------------------
-- 2) ctl_master_pipeline_run : 마스터 수행 상태 + 동시수행 락 겸용
-- ---------------------------------------------------------------------
CREATE TABLE ctl_master_pipeline_run (
    master_run_id       varchar(100)   NOT NULL,
    master_pipeline_nm  varchar(200)   NOT NULL,
    trigger_id          varchar(100)   NOT NULL,
    trigger_nm          varchar(200)   NOT NULL,
    data_class          varchar(4)     NULL,                    -- [V1.8] 회차 단위 조회용
    ingest_type         varchar(4)     NOT NULL,
    init_yn             bpchar(1)      NOT NULL DEFAULT 'N',    -- [V1.8] 초기수집 회차 여부(락 키)
    start_dt            timestamptz    NOT NULL,
    end_dt              timestamptz    NULL,                    -- NULL 허용(수행 중 = NULL)
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
    CONSTRAINT ctl_master_run_status_data_class_check   CHECK (data_class  IN ('META','RAW')),
    CONSTRAINT ctl_master_run_status_ingest_type_check  CHECK (ingest_type IN ('FULL','INCR')),
    -- [V1.8] D07 확장 : 백필/테스트 회차를 이력에서 구분
    CONSTRAINT ctl_master_run_status_run_mode_check
        CHECK (run_mode IN ('SCHEDULE','MANUAL','BACKFILL','TEST')),
    CONSTRAINT ctl_master_run_status_status_check
        CHECK (status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','SKIPPED')),
    -- [V1.8] 락 키 컬럼은 NULL 불가 (NULL 허용 시 유니크 인덱스가 무력화됨)
    CONSTRAINT ck_mpr_init_yn CHECK (init_yn IN ('Y','N'))
);

CREATE INDEX ix_mpr_status_start ON ctl_master_pipeline_run
    USING btree (status, start_dt);

-- [V1.8] 동시수행 방지 (전 기동방식 적용)
--   · 락 키   : (파이프라인명, 수집유형, 초기수집여부)
--   · 락 범위 : PENDING + RUNNING  (RUN 액션이 PENDING으로 INSERT하므로 즉시 락)
--   · 매뉴얼 한정 해제 : 스케줄 초기수집 도입으로 스케줄 간 충돌도 차단 대상
--   · 초기수집 ↔ 정규 FULL : init_yn 으로 키가 갈려 동시 허용
--   · 초기수집 ↔ 초기수집(수동 포함) : 동일 키 → 차단
CREATE UNIQUE INDEX ux_mrs_running ON ctl_master_pipeline_run
    USING btree (master_pipeline_nm, ingest_type, init_yn)
    WHERE (status IN ('PENDING','RUNNING'));

-- ---------------------------------------------------------------------
-- 3) ctl_ingest_pipeline_run : 차일드(수집) 수행 이력 팩트 (수행시점 스냅샷)
-- ---------------------------------------------------------------------
CREATE TABLE ctl_ingest_pipeline_run (
    ingest_pipeline_id     varchar(100) NOT NULL DEFAULT gen_random_uuid()::text,
    master_run_id          varchar(100) NOT NULL,
    master_pipeline_nm     varchar(200) NOT NULL,
    ingest_pipeline_nm     varchar(200) NOT NULL DEFAULT '-',   -- [V1.8] 수행 차일드 파이프라인명
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

-- [V1.8] 디스패처 Lookup : 회차의 수행 대상 파이프라인 목록 조회
CREATE INDEX ix_ipr_dispatch ON ctl_ingest_pipeline_run
    USING btree (master_run_id, status, ingest_pipeline_nm);

-- [V1.8] 자식(수집) 동시수행 방지
--   · 락 범위를 PENDING+RUNNING 으로 확대
--     (이력이 PENDING 으로 생성되므로, 생성 시점부터 동일 대상 중복 편입을 차단)
--   · FULL 한정 유지 : 초기수집 ↔ 정규 FULL 경계 시점의 동일 테이블 중복 수집 방어
CREATE UNIQUE INDEX ux_ipr_running ON ctl_ingest_pipeline_run
    USING btree (target_id, ingest_type)
    WHERE (status IN ('PENDING','RUNNING') AND ingest_type = 'FULL');

-- ---------------------------------------------------------------------
-- 4) ctl_dbx_api_call_history : Databricks REST API 호출 이력  [V1.8 신규]
--    ADF 책임 경계의 종점. "제대로 요청했는가"를 증명한다.
-- ---------------------------------------------------------------------
CREATE TABLE ctl_dbx_api_call_history (
    api_call_id         varchar(100) NOT NULL DEFAULT gen_random_uuid()::text,
    master_run_id       varchar(100) NULL,                        -- 호출 맥락(마스터 실행)
    api_name            varchar(50)  NOT NULL,                    -- 논리 호출 구분(JOBS_RUN_NOW 등, 대문자)
    http_method         varchar(10)  NOT NULL,                    -- GET/POST/PUT/PATCH/DELETE
    api_endpoint        varchar(500) NOT NULL,                    -- 예) /api/2.1/jobs/run-now
    request_body        text         NULL,                        -- 요청 페이로드(민감정보 마스킹)
    dbx_job_id          varchar(100) NULL,                        -- Databricks Job ID
    dbx_run_id          varchar(100) NULL,                        -- Databricks Run ID
    http_status_code    int4         NULL,                        -- HTTP 상태코드(200/4xx/5xx)
    success_yn          bpchar(1)    NOT NULL,                    -- 호출 성공 여부 Y/N
    attempt_no          int4         NOT NULL DEFAULT 0,          -- 재시도 회차
    start_dt            timestamptz  NULL,                        -- 호출 시작
    end_dt              timestamptz  NULL,                        -- 호출 종료
    response_body       text         NULL,                        -- 응답 본문(요약/원문)
    error_log           text         NULL,                        -- 실패 시 오류 메시지
    created_by          varchar(100) NOT NULL,
    created_dt          timestamptz  NOT NULL DEFAULT now(),
    update_by           varchar(100) NULL,
    update_dt           timestamptz  NULL,
    CONSTRAINT ctl_dbx_api_call_history_pkey PRIMARY KEY (api_call_id),
    CONSTRAINT ctl_dbx_api_call_master_run_id_fkey
        FOREIGN KEY (master_run_id) REFERENCES ctl_master_pipeline_run(master_run_id),
    CONSTRAINT ck_dbx_api_success_yn CHECK (success_yn IN ('Y','N')),
    CONSTRAINT ck_dbx_api_http_method
        CHECK (http_method IN ('GET','POST','PUT','PATCH','DELETE'))
);

CREATE INDEX ix_dbx_api_mri     ON ctl_dbx_api_call_history USING btree (master_run_id);
CREATE INDEX ix_dbx_api_created ON ctl_dbx_api_call_history USING btree (created_dt);
-- 호출 실패 모니터링
CREATE INDEX ix_dbx_api_fail    ON ctl_dbx_api_call_history
    USING btree (created_dt) WHERE (success_yn = 'N');

-- ---------------------------------------------------------------------
-- 5) ctl_dbx_ingest_history : Databricks 브론즈 적재 완료 이력
--    Databricks 책임 경계. "실제로 실렸는가"를 증명한다.
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
-- 6) ctl_trigger_history : 트리거 수행 이력 (skip 판단은 ADF 로직, 결과만 기록)
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

COMMENT ON TABLE  ctl_ingest_target_master IS '수집 대상 마스터 : 대상 정의 + 증분 HWM/진행 상태. 개발자/운영자 관리 유일 수작업 테이블';
COMMENT ON COLUMN ctl_ingest_target_master.target_id            IS '수집 대상 PK';
COMMENT ON COLUMN ctl_ingest_target_master.src_type             IS '소스 커넥션 타입(CONN_TYPE): DB2/MSSQL/ORACLE/MYSQL/POSTGRES/REST/SFTP';
COMMENT ON COLUMN ctl_ingest_target_master.src_db_nm            IS '소스 데이터베이스명(DATABASE): MLMTP/MLCRP 등';
COMMENT ON COLUMN ctl_ingest_target_master.src_schema_nm        IS '소스 스키마명';
COMMENT ON COLUMN ctl_ingest_target_master.src_table_nm         IS '소스 테이블명';
COMMENT ON COLUMN ctl_ingest_target_master.partition_clause     IS '초기적재 분할 추출용 조건절(대용량 파티션 분할)';
COMMENT ON COLUMN ctl_ingest_target_master.data_class           IS '데이터 분류: META/RAW (파이프라인 DATA_CLASS 라우팅)';
COMMENT ON COLUMN ctl_ingest_target_master.ingest_type          IS '수집 유형: FULL(초기/주기전체)/INCR(증분)';
COMMENT ON COLUMN ctl_ingest_target_master.exec_group           IS '오케스트레이터 실행 그룹/SEQ: 초기적재 용량분산(001,002) 등';
COMMENT ON COLUMN ctl_ingest_target_master.condition_type       IS '증분 조건 유형: WINDOW/BIZDAY/CLOSE (INCR 전용)';
COMMENT ON COLUMN ctl_ingest_target_master.condition_frequency  IS 'WINDOW 스케줄 주기: MINUTELY~YEARLY. 만회 모드 판정 기준 단위로도 사용';
COMMENT ON COLUMN ctl_ingest_target_master.condition_interval   IS 'WINDOW 주기 단위 N (겹침 폭). 하한 표현식의 오프셋으로 사용';
COMMENT ON COLUMN ctl_ingest_target_master.incr_column_nm       IS '증분 기준(워터마크) 컬럼: IPDTM/CLYM 등';
COMMENT ON COLUMN ctl_ingest_target_master.incr_column_type     IS '워터마크 값 데이터타입: NUMERIC/DATE/TIMESTAMP/STRING (WHERE 인용 및 LEAST/GREATEST 캐스팅용). 기준컬럼 존재 시 필수';
COMMENT ON COLUMN ctl_ingest_target_master.incr_column_hw_val   IS '[HWM] 마지막 성공 회차가 사용한 하한값. 갱신 규칙: GREATEST(이번 하한, 기존값) — 후퇴하지 않음';
COMMENT ON COLUMN ctl_ingest_target_master.last_success_dt      IS '[V1.8] 마지막 성공 완료 시각. 만회 모드 판정(마지막 성공 < 직전 예정 회차 기준일) 및 지연 대상 모니터링에 사용. init_wait_yn(대상의 대기 상태)과 혼동 주의';
COMMENT ON COLUMN ctl_ingest_target_master.ingest_pipeline_nm   IS '[V1.8] 차일드 파이프라인명 override. NULL이면 명명규칙으로 조합: pl_{data_class}_ingest_{src_type}_{src_db_nm}';
COMMENT ON COLUMN ctl_ingest_target_master.delete_yn            IS '논리 삭제 여부: Y=삭제, N=사용';
COMMENT ON COLUMN ctl_ingest_target_master.init_wait_yn         IS '초기수집 대기 여부(대상의 상태): Y=대기(초기 FULL 미완), N=완료. 회차 성격을 나타내는 ctl_master_pipeline_run.init_yn 과 구분';
COMMENT ON COLUMN ctl_ingest_target_master.created_by           IS '생성자';
COMMENT ON COLUMN ctl_ingest_target_master.created_dt           IS '생성 일시';
COMMENT ON COLUMN ctl_ingest_target_master.update_by            IS '수정자';
COMMENT ON COLUMN ctl_ingest_target_master.update_dt            IS '수정 일시';

COMMENT ON TABLE  ctl_master_pipeline_run IS '마스터 파이프라인 수행 상태. ux_mrs_running으로 동시수행 락 겸용. 스킵 회차도 SKIPPED로 기록';
COMMENT ON COLUMN ctl_master_pipeline_run.master_run_id      IS '마스터 파이프라인 실행 ID (@pipeline().RunId). 공통 파이프라인 호출 시 반드시 마스터에서 파라미터로 전달';
COMMENT ON COLUMN ctl_master_pipeline_run.master_pipeline_nm IS '마스터 파이프라인명 (@pipeline().Pipeline)';
COMMENT ON COLUMN ctl_master_pipeline_run.trigger_id         IS '트리거 ID (@pipeline().TriggerId)';
COMMENT ON COLUMN ctl_master_pipeline_run.trigger_nm         IS '트리거명 (@pipeline().TriggerName). 디버그/Trigger Now 실행 시 SAND BOX 자동 부여';
COMMENT ON COLUMN ctl_master_pipeline_run.data_class         IS '[V1.8] 데이터 분류 META/RAW. 회차 단위 조회 편의';
COMMENT ON COLUMN ctl_master_pipeline_run.ingest_type        IS '수집 유형 FULL/INCR';
COMMENT ON COLUMN ctl_master_pipeline_run.init_yn            IS '[V1.8] 초기수집 회차 여부: Y=초기수집, N=정규. 기동방식(run_mode)과 직교하는 축이며 동시수행 락 키의 일부. NOT NULL 필수(NULL 허용 시 유니크 인덱스 무력화)';
COMMENT ON COLUMN ctl_master_pipeline_run.start_dt           IS '수행 시작 일시';
COMMENT ON COLUMN ctl_master_pipeline_run.end_dt             IS '수행 종료 일시 (NULL=수행 중). SKIPPED 회차는 start_dt와 동일 값';
COMMENT ON COLUMN ctl_master_pipeline_run.exec_group         IS '오케스트레이터 실행 그룹/SEQ';
COMMENT ON COLUMN ctl_master_pipeline_run.child_total_cnt    IS '차일드 전체 건수 (SKIPPED 회차는 0)';
COMMENT ON COLUMN ctl_master_pipeline_run.child_succ_cnt     IS '차일드 성공 건수';
COMMENT ON COLUMN ctl_master_pipeline_run.child_fail_cnt     IS '차일드 실패 건수';
COMMENT ON COLUMN ctl_master_pipeline_run.status             IS '상태: PENDING/RUNNING/SUCCEEDED/FAILED/SKIPPED. RUN 액션은 PENDING으로 INSERT하며 락 범위에 포함됨';
COMMENT ON COLUMN ctl_master_pipeline_run.run_mode           IS '[V1.8] 실행 모드: SCHEDULE/MANUAL/BACKFILL/TEST. TEST는 이력만 기록하고 HWM/last_success_dt를 갱신하지 않음';
COMMENT ON COLUMN ctl_master_pipeline_run.created_by         IS '생성자';
COMMENT ON COLUMN ctl_master_pipeline_run.created_dt         IS '생성 일시';
COMMENT ON COLUMN ctl_master_pipeline_run.update_by          IS '수정자';
COMMENT ON COLUMN ctl_master_pipeline_run.update_dt          IS '수정 일시';

COMMENT ON TABLE  ctl_ingest_pipeline_run IS '차일드(수집) 파이프라인 수행 이력. 대상 마스터 값의 수행시점 스냅샷 보존';
COMMENT ON COLUMN ctl_ingest_pipeline_run.ingest_pipeline_id     IS '수집 파이프라인 ID (PK). UUID 자동생성';
COMMENT ON COLUMN ctl_ingest_pipeline_run.master_run_id          IS '마스터 실행 ID (FK → ctl_master_pipeline_run)';
COMMENT ON COLUMN ctl_ingest_pipeline_run.master_pipeline_nm     IS '마스터 파이프라인명';
COMMENT ON COLUMN ctl_ingest_pipeline_run.ingest_pipeline_nm     IS '[V1.8][스냅샷] 수행 차일드 파이프라인명. 디스패처(pl_dispatch_ingest)의 Switch 분기 키. 마스터 override가 없으면 명명규칙으로 조합';
COMMENT ON COLUMN ctl_ingest_pipeline_run.ingest_pipeline_run_id IS 'ADF 차일드 파이프라인 실제 RunId (@pipeline().RunId). 미전달 시 ''-''';
COMMENT ON COLUMN ctl_ingest_pipeline_run.target_id              IS '수집 대상 ID (FK → ctl_ingest_target_master)';
COMMENT ON COLUMN ctl_ingest_pipeline_run.src_type               IS '[스냅샷] 소스 커넥션 타입';
COMMENT ON COLUMN ctl_ingest_pipeline_run.src_db_nm              IS '[스냅샷] 소스 DB명';
COMMENT ON COLUMN ctl_ingest_pipeline_run.src_schema_nm          IS '[스냅샷] 소스 스키마명';
COMMENT ON COLUMN ctl_ingest_pipeline_run.src_table_nm           IS '[스냅샷] 소스 테이블명';
COMMENT ON COLUMN ctl_ingest_pipeline_run.data_class             IS '[스냅샷] 데이터 분류 META/RAW';
COMMENT ON COLUMN ctl_ingest_pipeline_run.ingest_type            IS '[스냅샷] 수집 유형 FULL/INCR. 초기수집 회차는 INCR 대상도 FULL로 기록';
COMMENT ON COLUMN ctl_ingest_pipeline_run.exec_group             IS '[스냅샷] 실행 그룹/SEQ';
COMMENT ON COLUMN ctl_ingest_pipeline_run.condition_type         IS '[스냅샷] 증분 조건 유형';
COMMENT ON COLUMN ctl_ingest_pipeline_run.condition_frequency    IS '[스냅샷] WINDOW 주기';
COMMENT ON COLUMN ctl_ingest_pipeline_run.condition_interval     IS '[스냅샷] WINDOW 주기 단위 N';
COMMENT ON COLUMN ctl_ingest_pipeline_run.incr_column_nm         IS '[스냅샷] 증분 기준 컬럼';
COMMENT ON COLUMN ctl_ingest_pipeline_run.incr_column_type       IS '[스냅샷] 워터마크 값 타입';
COMMENT ON COLUMN ctl_ingest_pipeline_run.incr_column_hw_val     IS '[스냅샷] 이번 수행에 확정된 하한값. 마스터 동명 컬럼과 같은 의미(마지막 성공 하한)';
COMMENT ON COLUMN ctl_ingest_pipeline_run.incr_start_val         IS '증분 재수집 시작값(override). 백필 파라미터 start_val이 매핑됨. NULL이면 표현식/HWM 규칙 적용';
COMMENT ON COLUMN ctl_ingest_pipeline_run.init_wait_yn           IS '[스냅샷] 초기수집 대기 여부';
COMMENT ON COLUMN ctl_ingest_pipeline_run.extract_query          IS '실제 실행된 추출 쿼리. 하한값이 리터럴로 확정되어 있어 재시도 시 동일 구간을 읽음';
COMMENT ON COLUMN ctl_ingest_pipeline_run.landing_path           IS '실제 랜딩 경로';
COMMENT ON COLUMN ctl_ingest_pipeline_run.file_name              IS '수집한 파일명';
COMMENT ON COLUMN ctl_ingest_pipeline_run.file_size_mb           IS '수집한 파일 사이즈(MB)';
COMMENT ON COLUMN ctl_ingest_pipeline_run.start_dt               IS '수행 시작 일시';
COMMENT ON COLUMN ctl_ingest_pipeline_run.end_dt                 IS '수행 종료 일시';
COMMENT ON COLUMN ctl_ingest_pipeline_run.extract_cnt            IS '추출 건수';
COMMENT ON COLUMN ctl_ingest_pipeline_run.save_cnt               IS '저장 건수';
COMMENT ON COLUMN ctl_ingest_pipeline_run.attempt_no             IS '재시도 회차';
COMMENT ON COLUMN ctl_ingest_pipeline_run.status                 IS '상태: PENDING/RUNNING/SUCCEEDED/FAILED/SKIPPED. 디스패처 Switch 미매칭 건은 FAILED로 기록';
COMMENT ON COLUMN ctl_ingest_pipeline_run.error_log              IS '오류 로그. 미등록 파이프라인 등 디스패처 방어 결과도 기록';
COMMENT ON COLUMN ctl_ingest_pipeline_run.created_by             IS '생성자';
COMMENT ON COLUMN ctl_ingest_pipeline_run.created_dt             IS '생성 일시';
COMMENT ON COLUMN ctl_ingest_pipeline_run.update_by              IS '수정자';
COMMENT ON COLUMN ctl_ingest_pipeline_run.update_dt              IS '수정 일시';

COMMENT ON TABLE  ctl_dbx_api_call_history IS '[V1.8] Databricks REST API 호출 이력. ADF 책임 경계의 종점 — "제대로 요청했는가"를 증명. 적재 결과는 ctl_dbx_ingest_history 참조';
COMMENT ON COLUMN ctl_dbx_api_call_history.api_call_id       IS '호출 이력 PK. UUID 자동생성';
COMMENT ON COLUMN ctl_dbx_api_call_history.master_run_id     IS '호출 맥락(마스터 실행) (FK → ctl_master_pipeline_run)';
COMMENT ON COLUMN ctl_dbx_api_call_history.api_name          IS '논리 호출 구분: JOBS_RUN_NOW 등 (대문자)';
COMMENT ON COLUMN ctl_dbx_api_call_history.http_method       IS 'HTTP 메서드: GET/POST/PUT/PATCH/DELETE';
COMMENT ON COLUMN ctl_dbx_api_call_history.api_endpoint      IS '호출 엔드포인트. 예) /api/2.1/jobs/run-now';
COMMENT ON COLUMN ctl_dbx_api_call_history.request_body      IS '요청 페이로드. 토큰/연결정보는 저장 전 마스킹';
COMMENT ON COLUMN ctl_dbx_api_call_history.dbx_job_id        IS 'Databricks Job ID';
COMMENT ON COLUMN ctl_dbx_api_call_history.dbx_run_id        IS 'Databricks Run ID. ctl_dbx_ingest_history와 대조해 호출→적재 연결 추적';
COMMENT ON COLUMN ctl_dbx_api_call_history.http_status_code  IS 'HTTP 상태코드 (200/4xx/5xx)';
COMMENT ON COLUMN ctl_dbx_api_call_history.success_yn        IS '호출 성공 여부: Y/N';
COMMENT ON COLUMN ctl_dbx_api_call_history.attempt_no        IS '재시도 회차';
COMMENT ON COLUMN ctl_dbx_api_call_history.start_dt          IS '호출 시작 일시';
COMMENT ON COLUMN ctl_dbx_api_call_history.end_dt            IS '호출 종료 일시';
COMMENT ON COLUMN ctl_dbx_api_call_history.response_body     IS '응답 본문(요약/원문). 민감정보 마스킹';
COMMENT ON COLUMN ctl_dbx_api_call_history.error_log         IS '실패 시 오류 메시지';
COMMENT ON COLUMN ctl_dbx_api_call_history.created_by        IS '생성자';
COMMENT ON COLUMN ctl_dbx_api_call_history.created_dt        IS '생성 일시';
COMMENT ON COLUMN ctl_dbx_api_call_history.update_by         IS '수정자';
COMMENT ON COLUMN ctl_dbx_api_call_history.update_dt         IS '수정 일시';

COMMENT ON TABLE  ctl_dbx_ingest_history IS 'Databricks 브론즈 적재 완료 이력. Databricks 책임 경계 — "실제로 실렸는가"를 증명. ADF는 폴링하지 않으며 모니터링에서 호출이력과 대조';
COMMENT ON COLUMN ctl_dbx_ingest_history.master_run_id      IS '마스터 실행 ID (PK, FK → ctl_master_pipeline_run)';
COMMENT ON COLUMN ctl_dbx_ingest_history.ingest_pipeline_id IS '수집 파이프라인 ID (PK, FK → ctl_ingest_pipeline_run)';
COMMENT ON COLUMN ctl_dbx_ingest_history.bronze_table_nm    IS '브론즈 테이블명';
COMMENT ON COLUMN ctl_dbx_ingest_history.landing_path       IS '랜딩 경로';
COMMENT ON COLUMN ctl_dbx_ingest_history.file_name          IS '적재 파일명';
COMMENT ON COLUMN ctl_dbx_ingest_history.start_dt           IS '적재 시작 일시';
COMMENT ON COLUMN ctl_dbx_ingest_history.end_dt             IS '적재 종료 일시';
COMMENT ON COLUMN ctl_dbx_ingest_history.extract_cnt        IS '추출 건수';
COMMENT ON COLUMN ctl_dbx_ingest_history.save_cnt           IS '저장 건수';
COMMENT ON COLUMN ctl_dbx_ingest_history.status             IS '상태: PENDING/RUNNING/SUCCEEDED/FAILED/SKIPPED';
COMMENT ON COLUMN ctl_dbx_ingest_history.error_log          IS '오류 로그';
COMMENT ON COLUMN ctl_dbx_ingest_history.created_by         IS '생성자';
COMMENT ON COLUMN ctl_dbx_ingest_history.created_dt         IS '생성 일시';
COMMENT ON COLUMN ctl_dbx_ingest_history.update_by          IS '수정자';
COMMENT ON COLUMN ctl_dbx_ingest_history.update_dt          IS '수정 일시';

COMMENT ON TABLE  ctl_trigger_history                    IS '트리거 수행 이력 : 마스터 파이프라인 트리거 실행 및 Skip 판단 결과 기록(판단은 ADF 로직). 스킵 여부와 무관하게 전건 기록';
COMMENT ON COLUMN ctl_trigger_history.trigger_id         IS '트리거 ID (PK, @pipeline().TriggerId)';
COMMENT ON COLUMN ctl_trigger_history.trigger_nm         IS '트리거명 (@pipeline().TriggerName)';
COMMENT ON COLUMN ctl_trigger_history.master_run_id      IS '마스터 실행 ID (PK, @pipeline().RunId)';
COMMENT ON COLUMN ctl_trigger_history.master_pipeline_nm IS '마스터 파이프라인명 (@pipeline().Pipeline)';
COMMENT ON COLUMN ctl_trigger_history.skip_yn            IS 'Skip 여부: Y(Skip됨)/N(정상 실행)';
COMMENT ON COLUMN ctl_trigger_history.skip_reason        IS 'Skip 사유 (정상 실행 시 NULL). 운영 스킵과 락 스킵(''동시수행 차단: …'')을 구분';
COMMENT ON COLUMN ctl_trigger_history.created_by         IS '생성자';
COMMENT ON COLUMN ctl_trigger_history.created_dt         IS '생성 일시';
COMMENT ON COLUMN ctl_trigger_history.update_by          IS '수정자';
COMMENT ON COLUMN ctl_trigger_history.update_dt          IS '수정 일시';

-- =====================================================================
-- [D] V1.7 → V1.8 마이그레이션 (기존 환경 적용 시)
-- =====================================================================
/*
SET search_path TO "META_ADF", public;

-- 1) 수집대상마스터 컬럼 추가
ALTER TABLE ctl_ingest_target_master
    ADD COLUMN last_success_dt    timestamptz  NULL,
    ADD COLUMN ingest_pipeline_nm varchar(200) NULL;

CREATE INDEX ix_target_last_success ON ctl_ingest_target_master
    USING btree (last_success_dt) WHERE (delete_yn = 'N');

-- 2) 마스터 수행상태 컬럼 추가 + 락 재정의
ALTER TABLE ctl_master_pipeline_run
    ADD COLUMN data_class varchar(4) NULL,
    ADD COLUMN init_yn    bpchar(1)  NOT NULL DEFAULT 'N';

ALTER TABLE ctl_master_pipeline_run
    ADD CONSTRAINT ctl_master_run_status_data_class_check CHECK (data_class IN ('META','RAW')),
    ADD CONSTRAINT ck_mpr_init_yn CHECK (init_yn IN ('Y','N'));

-- run_mode 도메인 확장 (BACKFILL/TEST)
ALTER TABLE ctl_master_pipeline_run
    DROP CONSTRAINT ctl_master_run_status_run_mode_check;
ALTER TABLE ctl_master_pipeline_run
    ADD  CONSTRAINT ctl_master_run_status_run_mode_check
         CHECK (run_mode IN ('SCHEDULE','MANUAL','BACKFILL','TEST'));

-- 락 재정의 : 키에 init_yn 추가, 범위 PENDING+RUNNING, 매뉴얼 한정 해제
DROP INDEX IF EXISTS ux_mrs_running;
CREATE UNIQUE INDEX ux_mrs_running ON ctl_master_pipeline_run
    USING btree (master_pipeline_nm, ingest_type, init_yn)
    WHERE (status IN ('PENDING','RUNNING'));

-- 3) 수집 이력 컬럼 추가 + 락/인덱스 재정의
ALTER TABLE ctl_ingest_pipeline_run
    ADD COLUMN ingest_pipeline_nm varchar(200) NOT NULL DEFAULT '-';

CREATE INDEX ix_ipr_dispatch ON ctl_ingest_pipeline_run
    USING btree (master_run_id, status, ingest_pipeline_nm);

DROP INDEX IF EXISTS ux_ipr_running;
CREATE UNIQUE INDEX ux_ipr_running ON ctl_ingest_pipeline_run
    USING btree (target_id, ingest_type)
    WHERE (status IN ('PENDING','RUNNING') AND ingest_type = 'FULL');

-- 4) API 호출 이력 신규 테이블 : 본문 [4] 블록의 CREATE TABLE 실행

-- ※ 주의 : 락 재정의(DROP/CREATE INDEX) 는 진행 중인 회차가 없는 시점에 수행할 것.
--          범위가 RUNNING → PENDING+RUNNING 으로 넓어지므로,
--          기존 PENDING 상태로 남아 있는 미완결 회차가 있으면 유니크 위반이 발생할 수 있다.
--          사전 확인 :
--            SELECT master_pipeline_nm, ingest_type, init_yn, COUNT(*)
--            FROM   ctl_master_pipeline_run
--            WHERE  status IN ('PENDING','RUNNING')
--            GROUP  BY 1,2,3 HAVING COUNT(*) > 1;
*/
