# ctl_ingest_target_master 모델링 개선 (명명규칙 반영) — v5 draft

전제: 메타 DB = Azure PostgreSQL. 주기 마스터(오케스트레이터) 파이프라인이 DB별 ingest 파이프라인을 호출.
목표: **명명규칙 기반으로 full / incr(조건 타입별) lookup 쿼리를 깔끔히 생성**하도록 마스터 테이블을 개선.

> ⚠ = 고객/설계 확정 필요 항목

---

## 0. 핵심 진단 — "오케스트레이터 = 조건 튜플"

명명규칙 `pl_{DATA_CLASS}_orch_{LOAD_TYPE}[_{CONDITION}][_{SEQ}]` 에서 **오케스트레이터 이름이 곧 조건 튜플의 직렬화**다.

| 오케스트레이터 | data_class | load_type | condition_type | frequency | interval | group |
|---|---|---|---|---|---|---|
| pl_meta_orch_full | meta | full | – | – | – | – |
| pl_raw_orch_full_01 | raw | full | – | – | – | 01 |
| pl_raw_orch_full_02 | raw | full | – | – | – | 02 |
| pl_raw_orch_incr_window-based-daily1 | raw | incr | window | daily | 1 | – |
| pl_raw_orch_incr_window-based-daily3 | raw | incr | window | daily | 3 | – |
| pl_raw_orch_incr_window-based-weekly1 | raw | incr | window | weekly | 1 | – |
| pl_raw_orch_incr_bizday-based | raw | incr | bizday | – | (N?) | – |
| pl_raw_orch_incr_close-based | raw | incr | close | – | – | – |

**함의**: 각 마스터는 "자기 튜플에 해당하는 대상만" lookup으로 골라야 한다. 이 튜플을 마스터 테이블 컬럼으로 구조화하면 —
- **window 계열 전 변형(daily1/daily3/weekly1…)을 파라미터화된 lookup 쿼리 1개로 커버** (오케스트레이터가 `p_frequency`, `p_interval`만 전달).
- WHERE 절도 같은 구조화 값에서 소스 DBMS 문법으로 생성 → v3/v4에서 미결이던 "incr_condition을 DBMS 문법 자유텍스트로 저장" 이식성 문제 해소.

이것이 incr 복잡도를 잡는 핵심이다.

---

## 1. 확정 필요 판단 지점 (⚠)

1. **✅ full 의미 (확정)** — `full` 오케스트레이터는 **초기 수집(1회 초기적재)일 수도, 주기적 full refresh 수집일 수도 있음** — 이름은 동일. `pending_yn` + 실행모드(`p_run_mode`)로 구분:
   - `pending_yn='Y'` → 초기적재(1=1 전체 추출). **load_type 무관** (incr 대상도 최초 1회는 full 추출 필요). 성공 시 'N'.
   - `load_type='full'` + `pending_yn='N'` → 주기적 full refresh (정규 반복).
   - full 오케스트레이터가 PENDING(초기)/SCHEDULED(주기) 두 실행모드를 가짐 → §3-1 참조.

2. **⚠ 저장형 HWM 존치 여부 (미결 — 증분 수집 조건 추가 분석 필요)** — 명명규칙 incr 3종(window/bizday/close)은 실행일/달력 기준 **무상태(stateless)** 로 보이나, 순수 증가키(숫자 PK) 등 **저장형 HWM이 필요한 증분 케이스가 존재하는지 증분 수집 조건 전수 분석 후 결정**. 결정 전까지 `hwm_*` 컬럼 제거는 **보류**. 저장형 HWM 필요 시 `condition_type='hwm'` + `hwm_value` 유지(§6).

3. **⚠ close-based는 Decommission 대상** — 예시 주석: "ADF 조회 간 Bulk 마감 적재는 Decommission 대상, Downstream Databricks Gold Layer에서 구성 예정". ADF 마스터 모델에 close를 1급으로 넣을지, 최소만 두고 제외 예고할지 확정 필요. 본 문서는 컬럼만 수용하되 lookup은 최소 형태.

4. **⚠ bizday 세부** — 예시: "IPDTM 등 워터마크 컬럼 **또는** CLYM 마감년월에 대한 영업일 기준". 즉 기준 컬럼이 IPDTM(날짜)일 수도 CLYM(YYYYMM)일 수도 있음. 또 "N영업일 전"의 interval이 필요한지 확인. **영업일 캘린더 테이블**(공휴일 반영)이 메타 DB에 필요 → 없으면 신설.

5. **⚠ DB2 날짜연산 매핑** — WHERE를 DB2 labeled duration으로 생성. DB2에 **WEEKS labeled duration 없음** → weekly는 `(N*7) DAYS`로 변환. minutely/hourly는 `CURRENT TIMESTAMP`, daily 이상은 `CURRENT DATE` 기준. 소스가 MS-SQL 등 혼재면 `src_type`별 분기 필요.

---

## 2. 개선 DDL (핵심 변경만)

```sql
-- ctl_ingest_target_master (명명규칙 라우팅 키 + 조건 구조화)
CREATE TABLE ctl_ingest_target_master (
    target_id            BIGSERIAL    PRIMARY KEY,

    -- ── 명명규칙 라우팅 키 (오케스트레이터가 자기 슬라이스를 lookup으로 집계) ──
    data_class           VARCHAR(4)   NOT NULL CHECK (data_class IN ('meta','raw')),   -- DATA_CLASS
    load_type            VARCHAR(4)   NOT NULL CHECK (load_type  IN ('full','incr')),  -- LOAD_TYPE (구 ingest_type)
    exec_group           VARCHAR(20),                 -- SEQ/그룹: full 초기적재 용량분산(01,02), incr 세분화(선택)

    -- ── 증분 조건 (load_type='incr'에서만 유효) ──
    condition_type       VARCHAR(12)  CHECK (condition_type IN ('window','bizday','close')),
    condition_frequency  VARCHAR(10)  CHECK (condition_frequency IN
                           ('minutely','hourly','daily','weekly','monthly','yearly')),  -- window 전용
    condition_interval   INT,                          -- window: N전일/전주. bizday: N영업일 전(옵션)
    incr_column_nm       VARCHAR(100),                 -- 기준/워터마크 컬럼: IPDTM(window/bizday), CLYM(close/bizday)

    -- ── 소스 식별 ──
    src_type             VARCHAR(20)  NOT NULL,        -- = CONN_TYPE (db2/mssql/oracle/mysql/postgres/rest/sftp)
    src_db_nm            VARCHAR(100),                 -- = DATABASE (MLMTP/MLCRP/MLSQP/MLVOCP…)
    src_schema_nm        VARCHAR(100),
    src_table_nm         VARCHAR(200),
    landing_path_tpl     VARCHAR(500),

    -- ── 상태/운영 ──
    is_active            CHAR(1)      NOT NULL DEFAULT 'Y',
    pending_yn           CHAR(1)      NOT NULL DEFAULT 'Y',  -- 초기적재 대기(=full로 시딩), 완료 시 'N'
    created_by           VARCHAR(100),
    created_dt           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    update_by            VARCHAR(100),
    update_dt            TIMESTAMPTZ,

    UNIQUE (src_db_nm, src_schema_nm, src_table_nm),

    -- FULL: 조건 컬럼 전부 NULL
    CONSTRAINT ck_full_no_cond CHECK (
        load_type <> 'full'
        OR (condition_type IS NULL AND condition_frequency IS NULL
            AND condition_interval IS NULL AND incr_column_nm IS NULL)
    ),
    -- INCR: condition_type 필수
    CONSTRAINT ck_incr_needs_type CHECK (
        load_type <> 'incr' OR condition_type IS NOT NULL
    ),
    -- window: frequency+interval+기준컬럼 필수
    CONSTRAINT ck_window_params CHECK (
        condition_type IS DISTINCT FROM 'window'
        OR (condition_frequency IS NOT NULL AND condition_interval IS NOT NULL
            AND incr_column_nm IS NOT NULL)
    ),
    -- frequency는 window에서만
    CONSTRAINT ck_freq_window_only CHECK (
        condition_frequency IS NULL OR condition_type = 'window'
    ),
    -- bizday/close: 기준컬럼 필수
    CONSTRAINT ck_bizday_col CHECK (
        condition_type IS DISTINCT FROM 'bizday' OR incr_column_nm IS NOT NULL
    ),
    CONSTRAINT ck_close_col CHECK (
        condition_type IS DISTINCT FROM 'close'  OR incr_column_nm IS NOT NULL
    )
);

-- lookup 필터 인덱스 (오케스트레이터 튜플 기준)
CREATE INDEX ix_target_route ON ctl_ingest_target_master
    (data_class, load_type, condition_type, condition_frequency, condition_interval)
    WHERE is_active = 'Y';
```

**v4 대비 변경 요약**
- `ingest_type(FULL/INCR)` → `load_type(full/incr)` 로 명명규칙 용어 일치.
- 신설: `data_class`, `exec_group`, `condition_type`, `condition_frequency`, `condition_interval`.
- `incr_column_nm` 재사용(IPDTM/CLYM). **별도 컬럼 신설 안 함.**
- `hwm_column_nm` / `hwm_data_type` / `hwm_value` / `incr_condition` **제거는 보류(⚠ 증분 조건 추가 분석 후 결정)** — window/bizday/close는 실행일 기준 무상태라 불필요해 보이나, 저장형 HWM 증분 케이스 존치 여부 확인 전까지 유지 판단 보류.

> 최초 제안(condition_bizday_based CHAR(1) + condition_closed_based CHAR(4)) 대신, **판별 컬럼 `condition_type` 하나 + interval 공유** 로 대체. 무효 조합(둘 다 Y 등)을 CHECK로 원천 차단하고, 명명규칙의 배타적 CONSTANT 열거와 1:1 매칭.

---

## 3. Lookup 쿼리 (오케스트레이터별, 파라미터화)

차일드 공용 동적쿼리 템플릿:
`@concat('SELECT * FROM ', item().src_schema_nm, '.', item().src_table_nm, ' WHERE ', item().incr_where, ' WITH UR')`

### 3-1. FULL — `pl_{dc}_orch_full_{group}` (초기적재 + 주기 full refresh 겸용)
파라미터: `p_data_class`, `p_exec_group`, `p_run_mode`('PENDING'|'SCHEDULED')

```sql
SELECT t.target_id, t.src_type, t.src_db_nm, t.src_schema_nm, t.src_table_nm,
       t.landing_path_tpl,
       'FULL_REFRESH' AS incr_method,
       '1=1'          AS incr_where          -- WITH UR 은 차일드 템플릿에서 부착
FROM   ctl_ingest_target_master t
WHERE  t.is_active  = 'Y'
AND    t.data_class = :p_data_class
AND    (:p_exec_group IS NULL OR t.exec_group = :p_exec_group)
AND    CASE
         WHEN :p_run_mode = 'PENDING'
              THEN (t.pending_yn = 'Y')                          -- 초기적재: load_type 무관(incr 대상도 최초 1회 full 추출)
         ELSE (t.load_type = 'full' AND t.pending_yn = 'N')      -- 주기 full refresh: 정규 반복
       END
ORDER  BY t.src_db_nm, t.target_id;    -- DB별로 묶어 ingest 파이프라인 호출
```
- **PENDING(초기)**: `pending_yn='Y'` 전건 1=1 추출 → 성공 시 `pending_yn='N'`. 이후 incr 대상은 incr 오케스트레이터로, full 대상은 주기 full로 자동 편입.
- **SCHEDULED(주기)**: `load_type='full'` 대상만 반복 전체수집.

### 3-2. INCR window — `pl_{dc}_orch_incr_window-based-{freq}{interval}` (전 변형 공용)
파라미터: `p_data_class`, `p_frequency`, `p_interval`

```sql
SELECT t.target_id, t.src_type, t.src_db_nm, t.src_schema_nm, t.src_table_nm,
       t.landing_path_tpl, t.incr_column_nm,
       'INCR_WINDOW' AS incr_method,
       t.incr_column_nm || ' >= ' ||
       CASE
         WHEN t.condition_frequency IN ('minutely','hourly') THEN
              'CURRENT TIMESTAMP - ' ||
              CASE t.condition_frequency
                   WHEN 'minutely' THEN t.condition_interval || ' MINUTES'
                   WHEN 'hourly'   THEN t.condition_interval || ' HOURS'
              END
         ELSE
              'CURRENT DATE - ' ||
              CASE t.condition_frequency
                   WHEN 'daily'   THEN t.condition_interval        || ' DAYS'
                   WHEN 'weekly'  THEN (t.condition_interval * 7)   || ' DAYS'   -- DB2 WEEKS 없음
                   WHEN 'monthly' THEN t.condition_interval        || ' MONTHS'
                   WHEN 'yearly'  THEN t.condition_interval        || ' YEARS'
              END
       END AS incr_where          -- 예: IPDTM >= CURRENT DATE - 3 DAYS
FROM   ctl_ingest_target_master t
WHERE  t.is_active         = 'Y'
AND    t.data_class        = :p_data_class
AND    t.load_type         = 'incr'
AND    t.condition_type    = 'window'
AND    t.condition_frequency = :p_frequency
AND    t.condition_interval  = :p_interval
AND    t.pending_yn        = 'N'         -- 초기적재 완료 건만 증분
ORDER  BY t.src_db_nm, t.target_id;
```
→ daily1 / daily3 / weekly1 오케스트레이터는 **파라미터만 다르고 쿼리는 동일**. 신규 조합 추가 시 쿼리 수정 불필요.
※ WHERE 문법은 소스=DB2 기준. MS-SQL 등 혼재면 `src_type`별 CASE 분기 추가.

### 3-3. INCR bizday — `pl_{dc}_orch_incr_bizday-based`
영업일 캘린더 테이블 필요(⚠). 직전 영업일을 Postgres에서 리터럴로 산출해 부착.

```sql
-- 사전: ctl_bizday_calendar(cal_date DATE PRIMARY KEY, is_bizday CHAR(1))
WITH bz AS (
  SELECT MAX(cal_date) AS bizdate
  FROM   ctl_bizday_calendar
  WHERE  is_bizday = 'Y' AND cal_date < CURRENT_DATE   -- 직전 영업일 (N영업일 전이면 offset 로직 확장)
)
SELECT t.target_id, t.src_type, t.src_db_nm, t.src_schema_nm, t.src_table_nm,
       t.landing_path_tpl, t.incr_column_nm,
       'INCR_BIZDAY' AS incr_method,
       t.incr_column_nm || ' >= ' || quote_literal(to_char(bz.bizdate,'YYYY-MM-DD')) AS incr_where
FROM   ctl_ingest_target_master t CROSS JOIN bz
WHERE  t.is_active      = 'Y'
AND    t.data_class     = :p_data_class
AND    t.load_type      = 'incr'
AND    t.condition_type = 'bizday'
AND    t.pending_yn     = 'N'
ORDER  BY t.src_db_nm, t.target_id;
```
※ 기준 컬럼이 CLYM(YYYYMM)인 대상은 비교식이 달라짐 → `incr_column_nm` 값에 따라 분기 필요(⚠).

### 3-4. INCR close — `pl_{dc}_orch_incr_close-based` (Decom 대상, 최소)
```sql
SELECT t.target_id, t.src_type, t.src_db_nm, t.src_schema_nm, t.src_table_nm,
       t.landing_path_tpl, t.incr_column_nm,
       'INCR_CLOSE' AS incr_method,
       t.incr_column_nm || ' = ' || quote_literal(to_char(CURRENT_DATE,'YYYYMM')) AS incr_where  -- 예: CLYM = '202607'
FROM   ctl_ingest_target_master t
WHERE  t.is_active      = 'Y'
AND    t.data_class     = :p_data_class
AND    t.load_type      = 'incr'
AND    t.condition_type = 'close'
AND    t.pending_yn     = 'N'
ORDER  BY t.src_db_nm, t.target_id;
```
⚠ close는 Decommission 예정 → ADF에서 구현 최소화, Databricks Gold Layer 이관 전제. 마감 확정 시점/직전 마감년월 규칙 별도 확인.

---

## 4. 등록 예시

```sql
-- window daily 3전일
INSERT INTO ctl_ingest_target_master
 (data_class, load_type, condition_type, condition_frequency, condition_interval,
  incr_column_nm, src_type, src_db_nm, src_schema_nm, src_table_nm, pending_yn)
VALUES
 ('raw','incr','window','daily',3, 'IPDTM','db2','MLCRP','CRP','TRANSACTION','Y');

-- full 초기적재 그룹 01
INSERT INTO ctl_ingest_target_master
 (data_class, load_type, exec_group, src_type, src_db_nm, src_schema_nm, src_table_nm)
VALUES
 ('raw','full','01','db2','MLSQP','SQP','CUSTOMER');
```

---

## 5. 요약

- **incr 복잡도의 근원 = 오케스트레이터가 조건 튜플별로 나뉘는 것** → 튜플을 컬럼(`data_class/load_type/condition_type/frequency/interval/exec_group`)으로 구조화하면 window 전 변형이 파라미터화 lookup 1개로 통합됨.
- 최초 제안 4컬럼(플래그 2 + freq/interval)은 **판별컬럼 `condition_type` + interval 공유**로 대체 — 무효 조합 CHECK 차단, 명명규칙 열거와 1:1.
- bizday/close는 문자열 concat만으론 WHERE 생성 불가 → bizday=캘린더 조인, close=Decom 전제.
- 구조화의 부가 이득: WHERE를 DBMS 문법으로 생성 → v4 §5 이식성 미결 해소.

## 6. 확정 대기 (재게시)

1. ✅ full 의미 — **확정**: 초기적재 + 주기 full refresh 겸용(같은 이름), `pending_yn` + `p_run_mode`로 구분(§1-1, §3-1).
2. ⚠ 저장형 HWM 존치 여부 — **미결. 증분 수집 조건 전수 분석 필요.** window/bizday/close 무상태 3종 외에 저장형 HWM(순수 증가키 등) 케이스가 있는지 확인 후 결정. 그 전까지 `hwm_*` 제거 보류, 필요 시 `condition_type='hwm'` 추가.
3. ⚠ close(Decom) ADF 구현 범위.
4. ⚠ bizday: 영업일 캘린더 테이블 신설, interval(N영업일) 정의, IPDTM/CLYM 기준 분기.
5. ⚠ 소스 DBMS 혼재 시 `src_type`별 날짜연산 분기(WEEKS 없음 등).
