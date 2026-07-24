# ADF 메타 컨트롤 — 논리 모델 정의서

- 대상 메타 DB: Azure Database for PostgreSQL
- 스키마: `META_ADF`
- 기준 DDL: `ADF_META_DDL_V1.5.sql`
- 구성: 엔티티 정의서 → 관계 정의서 → 도메인/코드 정의서 → 속성 정의서
- 표기: PK=기본키, FK=외래키, UK=유니크키 / Null 열 `N`=NOT NULL, `Y`=허용 / 도메인 열은 §3 코드ID(Dxx)
- ※ 코드성 값은 반드시 **대문자**로 저장(META/RAW/FULL/INCR/WINDOW 등). 적재 시 대문자 정규화 필수
- ※ 한글 논리명은 초안이며 검수 대상

### V1.4 대비 주요 변경(V1.5)
- 수집파이프라인수행이력(`ctl_ingest_pipeline_run`)에 자식 레벨 동시수행 락 `ux_ipr_running` 추가 — 동일 대상(target_id)+수집타입(ingest_type) RUNNING 1건 제한

### V1.3 대비 주요 변경(V1.4)
- `ctl_master_pipeline_run.skip_type` 컬럼 및 관련 CHECK 제약, 도메인 정의(구 D07) 삭제

### V1.2 대비 주요 변경(V1.3)
- `is_active` → `delete_yn`(논리삭제, 의미 반전: Y=삭제, N=사용), `pending_yn` → `init_wait_yn`(초기수집 대기)
- 코드성 값 전부 대문자 저장 + CHECK/인덱스의 `lower()` 전면 제거
- **트리거Skip예외(`ctl_run_skip`) 엔티티 폐지 → 트리거수행이력(`ctl_trigger_history`)으로 대체.** Skip 판단은 ADF 로직이 수행하고 결과만 이력으로 기록
- `ctl_master_pipeline_run`에 `trigger_id`·`exec_group` 추가, `trigger_nm`·`created_by` NOT NULL로 강화
- `ctl_ingest_pipeline_run`에 `incr_start_val`(증분 재수집 시작값)·`file_size_mb` 신규 컬럼, `init_wait_yn`으로 플래그 단일화
- 이력/적재 테이블의 NOT NULL 강화(ctl_dbx_ingest_history의 일시·건수·상태 등)

---

## 1. 엔티티 정의서

| 엔티티 논리명 | 물리명 | 엔티티 유형 | 관리 주체 | 설명 |
|---|---|---|---|---|
| 수집대상마스터 | ctl_ingest_target_master | 기준(마스터) | 개발자/운영자(수작업) | 수집 대상 정의 + 증분 HWM 상태. 유일한 수작업 관리 지점 |
| 마스터파이프라인수행상태 | ctl_master_pipeline_run | 이력/상태 | ADF upsert | 마스터 파이프라인 수행 상태. 동시수행 락 겸용 |
| 수집파이프라인수행이력 | ctl_ingest_pipeline_run | 이력(팩트) | ADF upsert | 차일드(수집) 파이프라인 수행 이력. 수행시점 대상값 스냅샷 보존 |
| Databricks적재이력 | ctl_dbx_ingest_history | 이력 | Databricks upsert | Databricks 브론즈 적재 완료 이력 |
| 트리거수행이력 | ctl_trigger_history | 이력(로그) | ADF insert | 마스터 파이프라인 트리거 실행 및 Skip 판단 결과 기록(판단은 ADF 로직) |

---

## 2. 관계 정의서

| 부모 엔티티 | 자식 엔티티 | 연결키 | 카디널리티 | 식별성 | 관계 설명 |
|---|---|---|---|---|---|
| 수집대상마스터 | 수집파이프라인수행이력 | target_id | 1 : N | 비식별 | 수집대상을 매 수행마다 수집한다 |
| 마스터파이프라인수행상태 | 수집파이프라인수행이력 | master_run_id | 1 : N | 비식별 | 마스터 1회 수행이 차일드 N건을 포함한다 |
| 마스터파이프라인수행상태 | Databricks적재이력 | master_run_id | 1 : N | 식별 | 마스터 수행분이 브론즈로 적재된다 |
| 수집파이프라인수행이력 | Databricks적재이력 | ingest_pipeline_id | 1 : N | 식별 | 차일드 수집분이 브론즈로 적재된다 |
| (마스터파이프라인수행상태) | 트리거수행이력 | master_run_id / master_pipeline_nm | — | 논리(물리 FK 없음) | 실행ID·파이프라인명 문자열로만 연관. 독립 엔티티 |

> Databricks적재이력의 PK는 (master_run_id, ingest_pipeline_id) 로 두 부모 키를 모두 포함 → 양쪽 모두 식별관계.
> 트리거수행이력은 물리 FK 없이 master_run_id로만 마스터수행과 연관한다(1:1 성격, 트리거 실행 1회당 마스터 실행 1건 기록).

---

## 3. 도메인 / 코드 정의서

| 도메인ID | 도메인 논리명 | 적용 속성 | 코드값 | 의미 | 제약 |
|---|---|---|---|---|---|
| D01 | 데이터분류 | data_class | META | 메타 시스템 데이터(채널 시스템의 메타 테이블 데이터) | CHECK |
| | | | RAW | 채널 시스템 실제 데이터 | |
| D02 | 수집유형 | ingest_type | FULL | 전체 수집(초기적재 + 주기적 full refresh 겸용, WHERE 1=1) | CHECK (TM·MPR·IPR) |
| | | | INCR | 증분 수집(부분 데이터) | |
| D03 | 증분조건유형 | condition_type | WINDOW | 워터마크 컬럼(IPDTM 등) HWM 기준 증분. frequency/interval은 스케줄 라우팅 | CHECK |
| | | | BIZDAY | 영업일 기준 데이터 조회 | |
| | | | CLOSE | 마감년월(CLYM) 기준 마감 데이터 (⚠Decommission 대상) | |
| D04 | 수집주기 | condition_frequency | MINUTELY/HOURLY/DAILY/WEEKLY/MONTHLY/YEARLY | 분/시/일/주/월/년 단위 스케줄 주기 (interval N과 결합) | CHECK · WINDOW 전용 |
| D05 | 증분기준컬럼타입 | incr_column_type | NUMERIC | 숫자(인용 없이 비교) | CHECK |
| | | | DATE / TIMESTAMP / STRING | 날짜/타임스탬프/문자(작은따옴표 인용) | |
| D06 | 수행상태 | status | PENDING | 대기 | CHECK (MPR·IPR·DBX) |
| | | | RUNNING | 수행 중(동시수행 락 대상) | |
| | | | SUCCEEDED | 성공 | |
| | | | FAILED | 실패 | |
| | | | SKIPPED | skip됨 | |
| D07 | 실행모드 | run_mode | SCHEDULE | 스케줄 트리거 수행 | CHECK |
| | | | MANUAL | 수동 수행 | |
| D08 | 논리삭제여부 | delete_yn | Y | 삭제(논리) | CHECK |
| | | | N | 사용 | |
| D09 | 초기수집대기여부 | init_wait_yn | Y | 초기수집 대기(초기 FULL 미완) | CHECK |
| | | | N | 초기수집 완료 | |
| D10 | Skip여부 | skip_yn | Y | Skip됨 | CHECK |
| | | | N | 정상 실행 | |

> ⚠ V1.2의 D09(is_active/pending_yn/skip_yn 통합 여부플래그)는 컬럼 재편에 따라 D08(delete_yn, 의미 반전)·D09(init_wait_yn)·D10(skip_yn)로 분리되었다.
> ※ skip_type 컬럼 및 관련 CHECK 제약, 도메인 정의(구 D07)는 불필요 판단으로 삭제되었다.

> ix_target_route(data_class·ingest_type·condition_type·frequency·interval)는 오케스트레이터별 수집대상 Lookup을 위한 라우팅 키다. 파이프라인명↔조건 매핑과 lookup 쿼리는 [파이프라인 설계서] 참조.

---

## 4. 속성 정의서

### 4.1 수집대상마스터 (ctl_ingest_target_master)

| 속성 논리명 | 물리명 | 타입 | 키 | Null | 도메인 | 설명 |
|---|---|---|---|---|---|---|
| 수집대상ID | target_id | int8 | PK | N | | 수집 대상 대리키(bigserial) |
| 소스커넥션타입 | src_type | varchar(10) | | N | | CONN_TYPE: DB2/MSSQL/ORACLE/MYSQL/POSTGRES/REST/SFTP |
| 소스DB명 | src_db_nm | varchar(20) | | N | | DATABASE: MLMTP/MLCRP 등 |
| 소스스키마명 | src_schema_nm | varchar(30) | | N | | 소스 스키마명 |
| 소스테이블명 | src_table_nm | varchar(100) | | N | | 소스 테이블명 |
| 파티션분할조건절 | partition_clause | varchar(100) | | Y | | 초기적재 분할 추출용 조건절 예) CLYM='202501' |
| 데이터분류 | data_class | varchar(4) | | N | D01 | META/RAW |
| 수집유형 | ingest_type | varchar(4) | | N | D02 | FULL/INCR |
| 실행그룹 | exec_group | varchar(20) | | Y | | 오케스트레이터 SEQ 예) 01 {파이프라인명}_01 |
| 증분조건유형 | condition_type | varchar(12) | | Y | D03 | INCR 전용 WINDOW/BIZDAY/CLOSE |
| 수집주기 | condition_frequency | varchar(10) | | Y | D04 | WINDOW 전용 |
| 수집주기간격 | condition_interval | int4 | | Y | | WINDOW 주기 단위 N(라우팅용) |
| 증분기준컬럼명 | incr_column_nm | varchar(30) | | Y | | 워터마크 컬럼: IPDTM/CLYM 등 |
| 증분기준컬럼타입 | incr_column_type | varchar(10) | | Y | D05 | 기준컬럼 존재 시 필수(ck_incr_col_type_req) |
| 증분하이워터마크값 | incr_column_hw_val | varchar(100) | | Y | | 현재 HWM 값. 수집 성공 시에만 갱신 |
| 논리삭제여부 | delete_yn | bpchar(1) | | N | D08 | 기본값 'N'. Y=삭제, N=사용 |
| 초기수집대기여부 | init_wait_yn | bpchar(1) | | N | D09 | 기본값 'Y'. Y=대기, N=완료 |
| 생성자 | created_by | varchar(100) | | Y | | |
| 생성일시 | created_dt | timestamptz | | N | | 기본값 now() |
| 수정자 | update_by | varchar(100) | | Y | | |
| 수정일시 | update_dt | timestamptz | | Y | | |

주요 업무 규칙(제약): FULL은 증분·조건 컬럼 전부 NULL(ck_full_no_cond) / INCR은 condition_type 필수(ck_incr_needs_type) / WINDOW는 frequency·interval·기준컬럼 필수(ck_window_params) / frequency는 WINDOW에서만(ck_freq_window_only) / BIZDAY·CLOSE는 기준컬럼 필수(ck_bizday_col, ck_close_col) / delete_yn·init_wait_yn ∈ Y/N(ck_target_yn).

### 4.2 마스터파이프라인수행상태 (ctl_master_pipeline_run)

| 속성 논리명 | 물리명 | 타입 | 키 | Null | 도메인 | 설명 |
|---|---|---|---|---|---|---|
| 마스터수행ID | master_run_id | varchar(100) | PK | N | | @pipeline().RunId |
| 마스터파이프라인명 | master_pipeline_nm | varchar(200) | | N | | @pipeline().Pipeline |
| 트리거ID | trigger_id | varchar(100) | | N | | @pipeline().TriggerId |
| 트리거명 | trigger_nm | varchar(200) | | N | | @pipeline().TriggerName |
| 수집유형 | ingest_type | varchar(4) | | N | D02 | FULL/INCR |
| 시작일시 | start_dt | timestamptz | | N | | |
| 종료일시 | end_dt | timestamptz | | Y | | NULL=수행 중 |
| 실행그룹 | exec_group | varchar(20) | | Y | | 오케스트레이터 실행 그룹/SEQ |
| 차일드전체건수 | child_total_cnt | int4 | | Y | | 기본값 0 |
| 차일드성공건수 | child_succ_cnt | int4 | | Y | | 기본값 0 |
| 차일드실패건수 | child_fail_cnt | int4 | | Y | | 기본값 0 |
| 수행상태 | status | varchar(15) | | N | D06 | |
| 실행모드 | run_mode | varchar(10) | | Y | D07 | |
| 생성자 | created_by | varchar(100) | | N | | |
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
| 소스DB명 | src_db_nm | varchar(20) | | N | | [스냅샷] |
| 소스스키마명 | src_schema_nm | varchar(30) | | N | | [스냅샷] |
| 소스테이블명 | src_table_nm | varchar(100) | | N | | [스냅샷] |
| 데이터분류 | data_class | varchar(4) | | N | D01 | [스냅샷] |
| 수집유형 | ingest_type | varchar(4) | | N | D02 | [스냅샷] |
| 실행그룹 | exec_group | varchar(20) | | Y | | [스냅샷] |
| 증분조건유형 | condition_type | varchar(12) | | Y | D03 | [스냅샷] |
| 수집주기 | condition_frequency | varchar(10) | | Y | D04 | [스냅샷] |
| 수집주기간격 | condition_interval | int4 | | Y | | [스냅샷] |
| 증분기준컬럼명 | incr_column_nm | varchar(30) | | Y | | [스냅샷] |
| 증분기준컬럼타입 | incr_column_type | varchar(10) | | Y | D05 | [스냅샷] |
| 증분하이워터마크값 | incr_column_hw_val | varchar(100) | | Y | | [스냅샷] 이번 수행에 사용된 HWM |
| 증분재수집시작값 | incr_start_val | varchar(100) | | Y | | override. NULL이면 저장 HWM 사용(수동 재수행용) |
| 초기수집대기여부 | init_wait_yn | bpchar(1) | | N | D09 | [스냅샷] 기본값 'N' |
| 추출쿼리 | extract_query | text | | N | | 실제 실행된 추출 쿼리 |
| 랜딩경로 | landing_path | varchar(500) | | N | | |
| 파일명 | file_name | varchar(100) | | N | | 기본값 '-' |
| 파일사이즈MB | file_size_mb | int8 | | Y | | 기본값 0 |
| 시작일시 | start_dt | timestamptz | | Y | | |
| 종료일시 | end_dt | timestamptz | | Y | | |
| 추출건수 | extract_cnt | int8 | | N | | 기본값 0 |
| 저장건수 | save_cnt | int8 | | N | | 기본값 0 |
| 재시도회차 | attempt_no | int4 | | N | | 기본값 0 |
| 수행상태 | status | varchar(15) | | Y | D06 | |
| 오류로그 | error_log | text | | Y | | |
| 생성자 | created_by | varchar(100) | | N | | |
| 생성일시 | created_dt | timestamptz | | N | | 기본값 now() |
| 수정자 | update_by | varchar(100) | | Y | | |
| 수정일시 | update_dt | timestamptz | | Y | | |

동시수행 제어: `ux_ipr_running` 유니크 인덱스로 (target_id, ingest_type) 당 status='RUNNING' 1건 제한. 동일 대상 테이블을 같은 수집유형으로 중복 수집하는 것을 자식 레벨에서 차단.

### 4.4 Databricks적재이력 (ctl_dbx_ingest_history)

| 속성 논리명 | 물리명 | 타입 | 키 | Null | 도메인 | 설명 |
|---|---|---|---|---|---|---|
| 마스터수행ID | master_run_id | varchar(100) | PK, FK | N | | → 마스터파이프라인수행상태 |
| 수집수행ID | ingest_pipeline_id | varchar(100) | PK, FK | N | | → 수집파이프라인수행이력 |
| 브론즈테이블명 | bronze_table_nm | varchar(200) | | N | | |
| 랜딩경로 | landing_path | varchar(500) | | N | | |
| 파일명 | file_name | varchar(100) | | N | | |
| 시작일시 | start_dt | timestamptz | | N | | |
| 종료일시 | end_dt | timestamptz | | N | | |
| 추출건수 | extract_cnt | int8 | | N | | |
| 저장건수 | save_cnt | int8 | | N | | |
| 수행상태 | status | varchar(15) | | N | D06 | |
| 오류로그 | error_log | text | | Y | | |
| 생성자 | created_by | varchar(100) | | N | | |
| 생성일시 | created_dt | timestamptz | | N | | 기본값 now() |
| 수정자 | update_by | varchar(100) | | Y | | |
| 수정일시 | update_dt | timestamptz | | Y | | |

### 4.5 트리거수행이력 (ctl_trigger_history)

| 속성 논리명 | 물리명 | 타입 | 키 | Null | 도메인 | 설명 |
|---|---|---|---|---|---|---|
| 트리거ID | trigger_id | varchar(100) | PK | N | | @pipeline().TriggerId |
| 트리거명 | trigger_nm | varchar(200) | | N | | @pipeline().TriggerName |
| 마스터수행ID | master_run_id | varchar(100) | PK | N | | @pipeline().RunId |
| 마스터파이프라인명 | master_pipeline_nm | varchar(200) | | N | | @pipeline().Pipeline |
| Skip여부 | skip_yn | bpchar(1) | | N | D10 | 기본값 'N'. Y=Skip됨, N=정상 실행 |
| Skip사유 | skip_reason | varchar(200) | | Y | | 정상 실행 시 NULL |
| 생성자 | created_by | varchar(100) | | N | | |
| 생성일시 | created_dt | timestamptz | | N | | 기본값 now() |
| 수정자 | update_by | varchar(100) | | Y | | |
| 수정일시 | update_dt | timestamptz | | Y | | |

판정 규칙: Skip 판단은 ADF 로직이 수행하고, 본 테이블에는 결과만 기록한다. skip_yn='Y'면 Skip(skip_reason에 사유), 'N'이면 정상 실행(skip_reason NULL).

---

## 5. 참고

- 수집파이프라인수행이력은 대리키 `ingest_pipeline_id`(UUID, PK)와 ADF 실제 `ingest_pipeline_run_id`('-' 기본)를 분리 보유. 관계·조인은 대리키 기준.
- V1.2의 트리거Skip예외(`ctl_run_skip`, opt-in 예외 목록 + skip_from_dt/skip_to_dt 기간)는 폐지되었다. Skip 판단 로직이 ADF로 이관되고 DB는 수행 이력만 로그로 남기는 `ctl_trigger_history`로 대체되었다.
- 코드성 값은 전부 대문자 저장이며 CHECK/인덱스에서 `lower()` 처리를 제거했다. 적재 계층에서 대문자 정규화 필수.
