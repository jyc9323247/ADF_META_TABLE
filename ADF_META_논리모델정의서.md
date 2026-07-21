# ADF 메타 컨트롤 — 논리 모델 정의서

- 대상 메타 DB: Azure Database for PostgreSQL
- 스키마: `META_ADF`
- 기준 DDL: `ADF_META_DDL_V1.1.sql`
- 구성: 엔티티 정의서 → 관계 정의서 → 도메인/코드 정의서 → 속성 정의서
- 표기: PK=기본키, FK=외래키, UK=유니크키 / Null 열 `N`=NOT NULL, `Y`=허용 / 도메인 열은 §3 코드ID(Dxx)
- ※ 한글 논리명은 초안이며 검수 대상

---

## 1. 엔티티 정의서

| 엔티티 논리명 | 물리명 | 엔티티 유형 | 관리 주체 | 설명 |
|---|---|---|---|---|
| 수집대상마스터 | ctl_ingest_target_master | 기준(마스터) | 개발자/운영자(수작업) | 수집 대상 정의 + 증분 HWM 상태. 유일한 수작업 관리 지점 |
| 마스터파이프라인수행상태 | ctl_master_pipeline_run | 이력/상태 | ADF upsert | 마스터 파이프라인 수행 상태. 동시수행 락 겸용 |
| 수집파이프라인수행이력 | ctl_ingest_pipeline_run | 이력(팩트) | ADF upsert | 차일드(수집) 파이프라인 수행 이력. 수행시점 대상값 스냅샷 보존 |
| Databricks적재이력 | ctl_dbx_ingest_history | 이력 | Databricks upsert | Databricks 브론즈 적재 완료 이력 |
| 트리거Skip예외 | ctl_run_skip | 예외(옵션) | 운영자 | 트리거 skip 예외 목록(opt-in) |

---

## 2. 관계 정의서

| 부모 엔티티 | 자식 엔티티 | 연결키 | 카디널리티 | 식별성 | 관계 설명 |
|---|---|---|---|---|---|
| 수집대상마스터 | 수집파이프라인수행이력 | target_id | 1 : N | 비식별 | 수집대상을 매 수행마다 수집한다 |
| 마스터파이프라인수행상태 | 수집파이프라인수행이력 | master_run_id | 1 : N | 비식별 | 마스터 1회 수행이 차일드 N건을 포함한다 |
| 마스터파이프라인수행상태 | Databricks적재이력 | master_run_id | 1 : N | 식별 | 마스터 수행분이 브론즈로 적재된다 |
| 수집파이프라인수행이력 | Databricks적재이력 | ingest_pipeline_id | 1 : N | 식별 | 차일드 수집분이 브론즈로 적재된다 |
| (마스터파이프라인수행상태) | 트리거Skip예외 | trigger_nm / master_pipeline_nm | — | 논리(물리 FK 없음) | 파이프라인명·트리거명 문자열로만 연관. 독립 엔티티 |

> Databricks적재이력의 PK는 (master_run_id, ingest_pipeline_id) 로 두 부모 키를 모두 포함 → 양쪽 모두 식별관계.

---

## 3. 도메인 / 코드 정의서

| 도메인ID | 도메인 논리명 | 적용 속성 | 코드값 | 의미 | 제약 |
|---|---|---|---|---|---|
| D01 | 데이터분류 | data_class | meta | 메타 시스템 데이터(채널 시스템의 메타 테이블 데이터) | CHECK |
| | | | raw | 채널 시스템 실제 데이터 | |
| D02 | 수집유형 | ingest_type | full | 전체 수집(초기적재 + 주기적 full refresh 겸용, WHERE 1=1) | CHECK (TM·MPR·IPR) |
| | | | incr | 증분 수집(부분 데이터) | |
| D03 | 증분조건유형 | condition_type | window | 워터마크 컬럼(IPDTM 등) HWM 기준 증분. frequency/interval은 스케줄 라우팅 | CHECK |
| | | | bizday | 영업일 기준 데이터 조회 | |
| | | | close | 마감년월(CLYM) 기준 마감 데이터 (⚠Decommission 대상) | |
| D04 | 수집주기 | condition_frequency | minutely/hourly/daily/weekly/monthly/yearly | 분/시/일/주/월/년 단위 스케줄 주기 (interval N과 결합) | CHECK · window 전용 |
| D05 | 증분기준컬럼타입 | incr_column_type | NUMERIC | 숫자(인용 없이 비교) | CHECK |
| | | | DATE / TIMESTAMP / STRING | 날짜/타임스탬프/문자(작은따옴표 인용) | |
| D06 | 수행상태 | status | PENDING | 대기 | CHECK (MPR·IPR·DBX) |
| | | | RUNNING | 수행 중(동시수행 락 대상) | |
| | | | SUCCEEDED | 성공 | |
| | | | FAILED | 실패 | |
| | | | SKIPPED | skip됨 | |
| D07 | Skip유형 | skip_type | WINDOW | 기간(from~to) 한정 skip | CHECK |
| | | | FLAG | 무기한 skip | |
| | | | LOCKED | 락에 의한 skip | |
| D08 | 실행모드 | run_mode | SCHEDULE | 스케줄 트리거 수행 | CHECK |
| | | | MANUAL | 수동 수행 | |
| D09 | 여부플래그 | is_active / pending_yn / skip_yn | Y | 활성 / 초기수집 대기 / skip 적용 | CHECK |
| | | | N | 비활성 / 초기수집 완료 / skip 해제 | |

---

## 4. 속성 정의서

### 4.1 수집대상마스터 (ctl_ingest_target_master)

| 속성 논리명 | 물리명 | 타입 | 키 | Null | 도메인 | 설명 |
|---|---|---|---|---|---|---|
| 수집대상ID | target_id | bigserial | PK | N | | 수집 대상 대리키 |
| 소스커넥션타입 | src_type | varchar(10) | | N | | CONN_TYPE: db2/mssql/oracle/mysql/postgres/rest/sftp |
| 소스DB명 | src_db_nm | varchar(20) |  | Y | | DATABASE: MLMTP/MLCRP 등 |
| 소스스키마명 | src_schema_nm | varchar(30) |  | Y | | 소스 스키마명 |
| 소스테이블명 | src_table_nm | varchar(100) |  | Y | | 소스 테이블명 |
| 파티션분할조건절 | partition_clause | varchar(100) | | Y | | 초기적재 분할 추출용 조건절 |
| 랜딩경로템플릿 | landing_path_tpl | varchar(100) | | Y | | 랜딩존 경로 템플릿 |
| 데이터분류 | data_class | varchar(4) | | N | D01 | meta/raw |
| 수집유형 | ingest_type | varchar(4) | | N | D02 | full/incr |
| 실행그룹 | exec_group | varchar(20) | | Y | | 오케스트레이터 SEQ/그룹(초기적재 용량분산 01,02 등) |
| 증분조건유형 | condition_type | varchar(12) | | Y | D03 | incr 전용 |
| 수집주기 | condition_frequency | varchar(10) | | Y | D04 | window 전용 |
| 수집주기간격 | condition_interval | int4 | | Y | | window 주기 단위 N(라우팅용) |
| 증분기준컬럼명 | incr_column_nm | varchar(30) | | Y | | 워터마크 컬럼: IPDTM/CLYM 등 |
| 증분기준컬럼타입 | incr_column_type | varchar(10) | | Y | D05 | 기준컬럼 존재 시 필수(ck_incr_col_type_req) |
| 증분하이워터마크값 | incr_column_hw_val | varchar(100) | | Y | | 현재 HWM 값. 수집 성공 시에만 갱신 |
| 활성여부 | is_active | bpchar(1) | | N | D09 | 기본값 'Y' |
| 초기수집대기여부 | pending_yn | bpchar(1) | | N | D09 | 기본값 'Y'. Y=대기, N=완료 |
| 생성자 | created_by | varchar(100) | | Y | | |
| 생성일시 | created_dt | timestamptz | | N | | 기본값 now() |
| 수정자 | update_by | varchar(100) | | Y | | |
| 수정일시 | update_dt | timestamptz | | Y | | |

주요 업무 규칙(제약): full은 증분·조건 컬럼 전부 NULL(ck_full_no_cond) / incr은 condition_type 필수(ck_incr_needs_type) / window는 frequency·interval·기준컬럼 필수(ck_window_params) / frequency는 window에서만(ck_freq_window_only) / bizday·close는 기준컬럼 필수(ck_bizday_col, ck_close_col).

### 4.2 마스터파이프라인수행상태 (ctl_master_pipeline_run)

| 속성 논리명 | 물리명 | 타입 | 키 | Null | 도메인 | 설명 |
|---|---|---|---|---|---|---|
| 마스터수행ID | master_run_id | varchar(100) | PK | N | | @pipeline().RunId |
| 마스터파이프라인명 | master_pipeline_nm | varchar(200) | | N | | @pipeline().Pipeline |
| 트리거명 | trigger_nm | varchar(200) | | Y | | @pipeline().TriggerName |
| 수집유형 | ingest_type | varchar(4) | | Y | D02 | full/incr |
| 시작일시 | start_dt | timestamptz | | N | | |
| 종료일시 | end_dt | timestamptz | | Y | | |
| 차일드전체건수 | child_total_cnt | int4 | | Y | | 기본값 0 |
| 차일드성공건수 | child_succ_cnt | int4 | | Y | | 기본값 0 |
| 차일드실패건수 | child_fail_cnt | int4 | | Y | | 기본값 0 |
| 수행상태 | status | varchar(15) | | N | D06 | |
| Skip유형 | skip_type | varchar(10) | | Y | D07 | |
| 실행모드 | run_mode | varchar(10) | | Y | D08 | |
| 생성자 | created_by | varchar(100) | | Y | | |
| 생성일시 | created_dt | timestamptz | | N | | 기본값 now() |
| 수정자 | update_by | varchar(100) | | Y | | |
| 수정일시 | update_dt | timestamptz | | Y | | |

동시수행 제어: `ux_mrs_running` 유니크 인덱스로 (master_pipeline_nm, ingest_type) 당 status='RUNNING' 1건 제한.

### 4.3 수집파이프라인수행이력 (ctl_ingest_pipeline_run)

| 속성 논리명 | 물리명 | 타입 | 키 | Null | 도메인 | 설명 |
|---|---|---|---|---|---|---|
| 수집수행ID | ingest_pipeline_id | varchar(100) | PK | N | | 대리키. 기본값 gen_random_uuid() |
| 마스터수행ID | master_run_id | varchar(100) | FK | N | | → 마스터파이프라인수행상태 |
| 마스터파이프라인명 | master_pipeline_nm | varchar(200) | | N | | |
| 수집파이프라인RunID | ingest_pipeline_run_id | varchar(100) | | N | | ADF 차일드 실제 RunId. 기본값 '-' |
| 수집대상ID | target_id | int8 | FK | N | | → 수집대상마스터 |
| 소스커넥션타입 | src_type | varchar(10) | | N | | [스냅샷] |
| 소스DB명 | src_db_nm | varchar(20) | | Y | | [스냅샷] |
| 소스스키마명 | src_schema_nm | varchar(30) | | Y | | [스냅샷] |
| 소스테이블명 | src_table_nm | varchar(100) | | Y | | [스냅샷] |
| 데이터분류 | data_class | varchar(4) | | N | D01 | [스냅샷] |
| 수집유형 | ingest_type | varchar(4) | | N | D02 | [스냅샷] |
| 실행그룹 | exec_group | varchar(20) | | Y | | [스냅샷] |
| 증분조건유형 | condition_type | varchar(12) | | Y | D03 | [스냅샷] |
| 수집주기 | condition_frequency | varchar(10) | | Y | D04 | [스냅샷] |
| 수집주기간격 | condition_interval | int4 | | Y | | [스냅샷] |
| 증분기준컬럼명 | incr_column_nm | varchar(30) | | Y | | [스냅샷] |
| 증분기준컬럼타입 | incr_column_type | varchar(10) | | Y | D05 | [스냅샷] |
| 증분하이워터마크값 | incr_column_hw_val | varchar(100) | | Y | | [스냅샷] 이번 수행에 사용된 HWM |
| 활성여부 | is_active | bpchar(1) | | N | D09 | [스냅샷] 기본값 'Y' |
| 초기수집대기여부 | pending_yn | bpchar(1) | | N | D09 | [스냅샷] 기본값 'Y' |
| 추출쿼리 | extract_query | text | | N | | 실제 실행된 추출 쿼리 |
| 랜딩경로 | landing_path | varchar(500) | | Y | | |
| 파일명 | file_name | varchar(100) | | Y | | |
| 시작일시 | start_dt | timestamptz | | Y | | |
| 종료일시 | end_dt | timestamptz | | Y | | |
| 추출건수 | extract_cnt | int8 | | Y | | |
| 저장건수 | save_cnt | int8 | | Y | | |
| 재시도회차 | attempt_no | int4 | | N | | 기본값 0 |
| 수행상태 | status | varchar(15) | | Y | D06 | |
| 오류로그 | error_log | text | | Y | | |
| 생성자 | created_by | varchar(100) | | Y | | |
| 생성일시 | created_dt | timestamptz | | N | | 기본값 now() |
| 수정자 | update_by | varchar(100) | | Y | | |
| 수정일시 | update_dt | timestamptz | | Y | | |

### 4.4 Databricks적재이력 (ctl_dbx_ingest_history)

| 속성 논리명 | 물리명 | 타입 | 키 | Null | 도메인 | 설명 |
|---|---|---|---|---|---|---|
| 마스터수행ID | master_run_id | varchar(100) | PK, FK | N | | → 마스터파이프라인수행상태 |
| 수집수행ID | ingest_pipeline_id | varchar(100) | PK, FK | N | | → 수집파이프라인수행이력 |
| 브론즈테이블명 | bronze_table_nm | varchar(200) | | N | | |
| 랜딩경로 | landing_path | varchar(500) | | Y | | |
| 파일명 | file_name | varchar(100) | | Y | | |
| 시작일시 | start_dt | timestamptz | | Y | | |
| 종료일시 | end_dt | timestamptz | | Y | | |
| 추출건수 | extract_cnt | int8 | | Y | | |
| 저장건수 | save_cnt | int8 | | Y | | |
| 수행상태 | status | varchar(15) | | Y | D06 | |
| 오류로그 | error_log | text | | Y | | |
| 생성자 | created_by | varchar(100) | | Y | | |
| 생성일시 | created_dt | timestamptz | | N | | 기본값 now() |
| 수정자 | update_by | varchar(100) | | Y | | |
| 수정일시 | update_dt | timestamptz | | Y | | |

### 4.5 트리거Skip예외 (ctl_run_skip)

| 속성 논리명 | 물리명 | 타입 | 키 | Null | 도메인 | 설명 |
|---|---|---|---|---|---|---|
| 트리거명 | trigger_nm | varchar(200) | PK | N | | |
| 마스터파이프라인명 | master_pipeline_nm | varchar(200) | PK | N | | |
| Skip여부 | skip_yn | bpchar(1) | | N | D09 | 기본값 'Y' |
| Skip시작일시 | skip_from_dt | timestamptz | | Y | | WINDOW형. NULL이면 무기한(FLAG) |
| Skip종료일시 | skip_to_dt | timestamptz | | Y | | from/to는 동시 NULL 또는 동시 값 |
| Skip사유 | skip_reason | varchar(500) | | Y | | |
| 생성자 | created_by | varchar(100) | | Y | | |
| 생성일시 | created_dt | timestamptz | | N | | 기본값 now() |
| 수정자 | update_by | varchar(100) | | Y | | |
| 수정일시 | update_dt | timestamptz | | Y | | 기본값 now() |

판정 규칙: 행 없음=RUN, skip_yn='Y'=SKIP. from~to에 수행시각이 걸치면 WINDOW(한시적), NULL/미걸침이면 FLAG(무기한).

---

## 5. 참고

- 수집파이프라인수행이력은 대리키 `ingest_pipeline_id`(UUID, PK)와 ADF 실제 `ingest_pipeline_run_id`('-' 기본)를 분리 보유. 관계·조인은 대리키 기준.
