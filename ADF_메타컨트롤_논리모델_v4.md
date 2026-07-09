# ADF 메타 컨트롤 논리 모델 v4

메타 DB: Azure Database for PostgreSQL
v3 대비 변경: ① 수집대상마스터에서 `incr_method` 제거(파생값化), `hwm_data_type` 추가 ② **FULL = 주기적 전체 수집** (1회성 초기적재 구분 아님 — 확정) ③ `ctl_child_run_log` → **`ctl_ingest_run_log`** 개명 + 수행 시점 스냅샷 컬럼(`child_pipeline_nm`, `trigger_nm`, `ingest_type`, `incr_column_nm`, `incr_condition`, `hwm_column_nm`, `hwm_value`, `new_hwm_value`) 추가 ④ Lookup 쿼리 확정 ⑤ skip 요구사항 거취: 고객 확인 대기 ⑥ `init_loaded_*` 제거 → **`pending_yn`** + PENDING/SCHEDULED 수행 모드 도입 (초기 수집 완료 후에만 정규 수행) ⑦ `incr_condition` 연산자 포함 저장 ⑧ UNIQUE에서 `ingest_type` 제거 ⑨ 대상 3,000건 미만 전제

> ⚠ = 확실하지 않아 확인 필요한 항목

---


<img width="1304" height="735" alt="image" src="https://github.com/user-attachments/assets/cce779e5-b49a-46cc-821e-1237f552ffec" />


## 1. 테이블 구성 (4개 + 선택 1개)

| 테이블 | 역할 | 입력 주체 |
|---|---|---|
| `ctl_ingest_target_master` | 수집 대상 정의 + 상태(HWM, 초기적재 완료) — 유일한 수작업 관리 지점 | 개발자/운영자 |
| `ctl_master_run_status` | 마스터 수행 상태 + 동시수행 락 겸용 | ADF upsert |
| `ctl_ingest_run_log` | 수집 수행 이력 팩트 (구 차일드 상세 로그) | ADF upsert |
| `ctl_dbx_ingest_history` | Databricks 브론즈 적재 완료 이력 | Databricks upsert |
| `ctl_run_skip` (선택, A안) | 트리거 skip 예외 목록 (opt-in) | 운영자 |

```
ctl_ingest_target_master (1) ──< ctl_ingest_run_log (N)      [target_id]
ctl_master_run_status    (1) ──< ctl_ingest_run_log (N)      [master_run_id]
ctl_ingest_run_log       (1) ──< ctl_dbx_ingest_history (N)  [master_run_id+pipeline_run_id]
```
<img width="908" height="764" alt="image" src="https://github.com/user-attachments/assets/3e5e8d3d-16fc-46c9-82c4-38435f990d28" />



### 수집 타입 의미 (확정)

- **FULL = 주기적 전체 데이터 수집** (매 수행마다 전체 추출, 1회성 초기적재 아님). `incr_column_nm`, `hwm_column_nm`은 반드시 NULL (CHECK로 강제).
- **INCR = 증분 수집**. `incr_column_nm` / `hwm_column_nm` 둘 중 정확히 하나만 값 보유 (CHECK로 강제).

### incr_method 파생 규칙 (컬럼 제거)

| ingest_type | incr_column_nm | hwm_column_nm | 파생 incr_method |
|---|---|---|---|
| FULL | NULL (강제) | NULL (강제) | FULL_REFRESH |
| INCR | 값 있음 | NULL | INCR_DATE |
| INCR | NULL | 값 있음 | INCR_HIGHWATERMARK |

```sql
CASE WHEN ingest_type = 'FULL'          THEN 'FULL_REFRESH'
     WHEN hwm_column_nm  IS NOT NULL    THEN 'INCR_HIGHWATERMARK'
     WHEN incr_column_nm IS NOT NULL    THEN 'INCR_DATE'
END
```

물리 컬럼으로 보고 싶으면 PostgreSQL 12+ 생성 컬럼으로 추가 가능(선택):
`incr_method VARCHAR(30) GENERATED ALWAYS AS (CASE ... END) STORED`

---

## 2. DDL

```sql
-- 1) 수집 대상 마스터 (고객 확정 DDL + 무결성 CHECK)
CREATE TABLE ctl_ingest_target_master (
    target_id            BIGSERIAL    PRIMARY KEY,
    src_type             VARCHAR(20)  NOT NULL,              -- dbms / file
    src_db_nm            VARCHAR(100),                       -- db2, ms-sql ...
    src_schema_nm        VARCHAR(100),
    src_table_nm         VARCHAR(200),
    ingest_type          VARCHAR(10)  CHECK (ingest_type IN ('FULL','INCR')),
    incr_column_nm       VARCHAR(100),
    incr_condition       VARCHAR(500),                       -- ★ 연산자 포함 저장. 예: '>= current_date - 1 day' (소스 DBMS 문법)
    hwm_column_nm        VARCHAR(100),
    hwm_data_type        VARCHAR(20)  CHECK (hwm_data_type IN
                           ('NUMERIC','DATE','TIMESTAMP','STRING')),  -- ★ HWM 인용 처리용
    hwm_value            VARCHAR(100),                       -- 성공 후에만 갱신
    landing_path_tpl     VARCHAR(500),                       -- 랜딩존 경로 템플릿(선택)
    is_active            CHAR(1)      NOT NULL DEFAULT 'Y',
    pending_yn           CHAR(1)      NOT NULL DEFAULT 'Y',  -- ★ 신규 등록 = 수시 초기 수집 대기, 성공 시 'N'
    created_by           VARCHAR(100),
    created_dt           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    update_by            VARCHAR(100),
    update_dt            TIMESTAMPTZ,
    UNIQUE (src_db_nm, src_schema_nm, src_table_nm),         -- 테이블당 1행 (FULL/INCR 택1)
    -- INCR: incr_column_nm / hwm_column_nm 둘 중 정확히 하나만
    CONSTRAINT ck_incr_xor CHECK (
        ingest_type <> 'INCR'
        OR ((incr_column_nm IS NOT NULL) <> (hwm_column_nm IS NOT NULL))
    ),
    -- FULL: 증분 컬럼 없어야 함 (incr_method=FULL_REFRESH 파생 보장)
    CONSTRAINT ck_full_no_incr CHECK (
        ingest_type <> 'FULL'
        OR (incr_column_nm IS NULL AND hwm_column_nm IS NULL)
    )
);
CREATE INDEX ix_target_lookup ON ctl_ingest_target_master(ingest_type)
    WHERE is_active = 'Y';

-- 2) 마스터 파이프라인 수행 상태 (로그 + 동시수행 락 겸용)
CREATE TABLE ctl_master_run_status (
    pipeline_nm       VARCHAR(200) NOT NULL,     -- @pipeline().Pipeline
    master_run_id     VARCHAR(100) NOT NULL,     -- @pipeline().RunId
    trigger_nm        VARCHAR(200),              -- @pipeline().TriggerName
    ingest_type       VARCHAR(10),
    start_dt          TIMESTAMPTZ  NOT NULL,
    end_dt            TIMESTAMPTZ,
    child_total_cnt   INT DEFAULT 0,
    child_succ_cnt    INT DEFAULT 0,
    child_fail_cnt    INT DEFAULT 0,
    status            VARCHAR(15)  NOT NULL
                        CHECK (status IN ('RUNNING','SUCCEEDED','FAILED','SKIPPED')),
    skip_type         VARCHAR(10)  CHECK (skip_type IN ('WINDOW','FLAG','LOCKED')),
    run_mode          VARCHAR(10)  CHECK (run_mode IN ('SCHEDULE','MANUAL')),
    create_timestamp  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    update_timestamp  TIMESTAMPTZ,
    PRIMARY KEY (pipeline_nm, master_run_id)
);
-- 동시 수행 방지: 동일 파이프라인+수집타입의 RUNNING 1건 제한 (락 테이블 불필요)
CREATE UNIQUE INDEX ux_mrs_running
    ON ctl_master_run_status(pipeline_nm, ingest_type)
    WHERE status = 'RUNNING';

-- 3) 수집 수행 이력 팩트 (구 ctl_child_run_log)
--    스냅샷 컬럼은 "그 수행에 사용된 값"의 박제 → HWM 이력 테이블 역할 흡수
CREATE TABLE ctl_ingest_run_log (
    master_run_id      VARCHAR(100) NOT NULL,
    pipeline_run_id    VARCHAR(100) NOT NULL,
    attempt_no         INT          NOT NULL DEFAULT 1,
    target_id          BIGINT       REFERENCES ctl_ingest_target_master(target_id),
    master_pipeline_nm VARCHAR(200),
    child_pipeline_nm  VARCHAR(200),
    trigger_nm         VARCHAR(200),
    ingest_type        VARCHAR(10),                 -- 수행 시점 스냅샷
    incr_column_nm     VARCHAR(100),
    incr_condition     VARCHAR(500),
    hwm_column_nm      VARCHAR(100),
    hwm_value          VARCHAR(100),                -- 이번 수행에 '사용된' HWM
    new_hwm_value      VARCHAR(100),                -- 이번 수행으로 '갱신된' HWM
    src_db_nm          VARCHAR(100),
    src_schema_nm      VARCHAR(100),
    src_table_nm       VARCHAR(200),
    extract_query      TEXT,
    start_dt           TIMESTAMPTZ,
    end_dt             TIMESTAMPTZ,
    landing_path       VARCHAR(500),
    extract_cnt        BIGINT,
    save_cnt           BIGINT,
    status             VARCHAR(15),
    error_log          TEXT,
    create_timestamp   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    update_timestamp   TIMESTAMPTZ,
    PRIMARY KEY (master_run_id, pipeline_run_id, attempt_no)
);
CREATE INDEX ix_irl_target ON ctl_ingest_run_log(target_id, create_timestamp);
CREATE INDEX ix_irl_retry  ON ctl_ingest_run_log(master_run_id, status);
CREATE INDEX ix_irl_create ON ctl_ingest_run_log(create_timestamp);

-- 4) Databricks 수집 완료 이력
CREATE TABLE ctl_dbx_ingest_history (
    master_run_id      VARCHAR(100) NOT NULL,
    pipeline_run_id    VARCHAR(100) NOT NULL,
    master_pipeline_nm VARCHAR(200),
    bronze_table_nm    VARCHAR(200) NOT NULL,
    start_dt           TIMESTAMPTZ,
    end_dt             TIMESTAMPTZ,
    landing_path       VARCHAR(500),
    extract_cnt        BIGINT,
    save_cnt           BIGINT,
    status             VARCHAR(15),
    error_log          TEXT,
    create_timestamp   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    update_timestamp   TIMESTAMPTZ,
    PRIMARY KEY (master_run_id, pipeline_run_id, bronze_table_nm)
);

-- 5) [선택, A안] 트리거 skip 예외 테이블
CREATE TABLE ctl_run_skip (
    trigger_nm    VARCHAR(200) NOT NULL,
    pipeline_nm   VARCHAR(200) NOT NULL,
    skip_yn       CHAR(1)      NOT NULL DEFAULT 'Y',
    skip_from_dt  TIMESTAMPTZ,
    skip_to_dt    TIMESTAMPTZ,
    skip_reason   VARCHAR(500),
    upd_user      VARCHAR(100),
    upd_dt        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (trigger_nm, pipeline_nm),
    CHECK (skip_from_dt IS NULL OR skip_to_dt IS NULL OR skip_from_dt <= skip_to_dt),
    CHECK ((skip_from_dt IS NULL) = (skip_to_dt IS NULL))
);
-- 판정: 행 없음 → RUN. skip_yn='Y' → SKIP
--   from-to에 수행 시작 시각이 걸치면 WINDOW(to까지 한시적), NULL/미걸침이면 FLAG(무기한)
```

---

## 3. ADF Lookup 쿼리 (확정)

### 수행 모드: PENDING(초기 수집) / SCHEDULED(정규)

마스터 파이프라인 파라미터 `p_run_mode` (기본 `'SCHEDULED'`):

- **PENDING (초기 수집)**: 신규 등록 직후 수동 트리거로 `'PENDING'` 전달. `pending_yn='Y'` 건만 대상, ingest_type과 무관하게 **전체 추출(1=1)**로 초기 적재. 성공 시 `pending_yn='N'` + HWM 방식은 초기 hwm_value 세팅.
- **SCHEDULED (정규)**: 스케줄 트리거 기본값. `pending_yn='N'`(초기 수집 완료) 건만 대상 — **초기 수집이 완료되지 않은 테이블은 INCR 증분 수행되지 않음** (확정 요건).
- 재적재 필요 시 운영자가 `pending_yn='Y'` 리셋 후 PENDING 모드 수행.

마스터에서 If Condition(`@equals(pipeline().parameters.p_run_mode,'PENDING')`)으로 3-0 / 3-1·3-2 Lookup 분기.

### 3-0. PENDING 모드 — 초기 수집 대상 조회 (전체 추출)

```sql
SELECT t.target_id,
       t.src_type, t.src_db_nm, t.src_schema_nm, t.src_table_nm,
       t.landing_path_tpl, t.ingest_type,
       t.hwm_column_nm, t.hwm_data_type,   -- HWM 방식: 초기 적재분 max로 hwm_value 세팅용
       'INIT' AS incr_method,
       '1=1'  AS incr_where                -- 전체 추출
FROM   ctl_ingest_target_master t
WHERE  t.is_active  = 'Y'
AND    t.pending_yn = 'Y'
ORDER  BY t.target_id;
```

### 3-1. SCHEDULED — 증분(INCR) 대상 조회

```sql
SELECT t.target_id,
       t.src_type, t.src_db_nm, t.src_schema_nm, t.src_table_nm,
       t.landing_path_tpl,
       t.incr_column_nm, t.incr_condition,
       t.hwm_column_nm, t.hwm_data_type, t.hwm_value,        -- 로그 스냅샷용
       CASE WHEN t.hwm_column_nm IS NOT NULL THEN 'INCR_HIGHWATERMARK'
            ELSE 'INCR_DATE' END AS incr_method,
       CASE
         WHEN t.hwm_column_nm IS NOT NULL THEN
              CASE
                WHEN t.hwm_data_type = 'NUMERIC'
                     THEN t.hwm_column_nm || ' > ' || t.hwm_value          -- 인용 없음
                ELSE t.hwm_column_nm || ' > ' || quote_literal(t.hwm_value) -- DATE/TIMESTAMP/STRING
              END
         ELSE t.incr_column_nm || ' ' || t.incr_condition   -- 연산자는 조건 문자열에 포함됨
       END AS incr_where
FROM   ctl_ingest_target_master t
WHERE  t.is_active   = 'Y'
AND    t.ingest_type = 'INCR'
AND    t.pending_yn  = 'N'          -- 초기 수집(PENDING) 완료 건만 증분 수행
AND    ( t.hwm_column_nm IS NULL OR t.hwm_value IS NOT NULL )  -- 방어: HWM 미세팅 건 제외
ORDER  BY t.target_id;
```

### 3-2. SCHEDULED — 전체 수집(FULL) 대상 조회

FULL은 주기적 전체 수집. 신규 테이블은 PENDING 모드로 초기 수집 완료 후(pending_yn='N') 스케줄에 자동 편입.

```sql
SELECT t.target_id,
       t.src_type, t.src_db_nm, t.src_schema_nm, t.src_table_nm,
       t.landing_path_tpl,
       'FULL_REFRESH' AS incr_method,
       '1=1'          AS incr_where       -- 차일드 동적쿼리 형식 통일용
FROM   ctl_ingest_target_master t
WHERE  t.is_active   = 'Y'
AND    t.ingest_type = 'FULL'
AND    t.pending_yn  = 'N'          -- 초기 수집 완료 건만 스케줄 수행
ORDER  BY t.target_id;
```

차일드 동적 쿼리(공용): `@concat('SELECT * FROM ', item().src_schema_nm, '.', item().src_table_nm, ' WHERE ', item().incr_where)`

### 3-3. 차일드 성공 시 갱신 (익명 블록)

```sql
-- PENDING(초기 수집) 성공 시: 대기 해제 + HWM 방식은 초기 HWM 세팅
UPDATE ctl_ingest_target_master
SET    pending_yn = 'N',
       hwm_value  = COALESCE(:init_max_hwm_value, hwm_value),  -- HWM 방식만 값 전달(추출분 max)
       update_dt  = now(), update_by = :run_id
WHERE  target_id = :target_id;

-- SCHEDULED 증분(HWM 방식) 성공 시
UPDATE ctl_ingest_target_master
SET    hwm_value = :new_hwm_value, update_dt = now(), update_by = :run_id
WHERE  target_id = :target_id;
```

`ctl_ingest_run_log`에는 같은 블록에서 `hwm_value`(사용값)와 `new_hwm_value`(갱신값)를 함께 upsert — 마스터는 현재값만 갖고, 회차별 변경 이력은 로그가 보존.

---

## 4. 확정된 사항

- FULL = 주기적 전체 데이터 수집 (1회성 초기적재 구분 아님). FULL이면 `incr_column_nm`/`hwm_column_nm` NULL 강제.
- INCR = `incr_column_nm` / `hwm_column_nm` 중 정확히 하나만 값 보유.
- `incr_method` 컬럼 제거 — ingest_type + 두 컬럼 값 여부로 파생.
- `hwm_data_type` 추가 — NUMERIC이면 인용 없이, 그 외(DATE/TIMESTAMP/STRING)는 인용하여 HWM 조건 생성.
- `incr_condition`은 **연산자 포함 저장** (예: `'>= current_date - 1 day'`) — 쿼리는 컬럼명과 조건 문자열을 그대로 연결.
- UNIQUE에서 `ingest_type` 제거 — 소스 테이블당 1행, FULL/INCR 택1.
- `init_loaded_*` 제거 → `pending_yn`(DEFAULT 'Y') 도입. **PENDING 모드 = 초기 수집(전체 추출)**: 신규 건은 수동 트리거 + `p_run_mode='PENDING'`으로 즉시 초기 적재, 성공 시 'N'. **SCHEDULED 모드는 pending_yn='N' 건만 수행** — 초기 수집 완료 전에는 INCR 증분이 돌지 않음.
- 수집 대상 규모는 **3,000건 미만 전제** — Lookup 5,000행/4MB 제한 내이므로 페이지네이션·그룹 분할 불필요 (초과 시 재검토).

## 5. 확실하지 않은 부분 (⚠ 확인 필요)

- **skip 요구사항(트리거 중지 없이 skip) 거취 — 고객 확인 대기.** A안(opt-in 예외 테이블 `ctl_run_skip`) / B안(ADF 트리거 UI 중지, 요구 포기) / C안(수집대상 단위 skip). 미결 시 `ctl_run_skip`은 생성 보류.
- PENDING(초기 수집)에서 HWM 방식의 초기 `hwm_value` 산출: 초기 적재분의 max를 어떻게 구할지 — "추출 쿼리에 max 포함", "수행 시작 시각을 상한으로 고정", "적재 후 랜딩/브론즈에서 산출" 중 방식 결정 필요 (SCHEDULED의 `new_hwm_value` 산출과 동일 이슈).
- `incr_condition`은 소스 DBMS 문법으로 저장됨(DB2/MS-SQL 날짜 연산 문법 상이) — 입력 시 검증 절차 권장.
- 대상이 3,000건 미만 전제를 초과 성장할 가능성 — 초과 시 exec_group 분할/페이지네이션 재도입 필요.
- DB2 커넥터 링크드서비스 파라미터화 지원 범위 — v3와 동일하게 미확정.
