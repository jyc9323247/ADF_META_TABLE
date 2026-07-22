# ADF 메타 컨트롤 — 물리 모델 정의서 (Table Spec)

- DBMS: Azure Database for PostgreSQL 
- 스키마: `META_ADF`
- 기준 DDL: `ADF_META_DDL_V1.2.sql`
- 표기: `NN` = NOT NULL(●), `PK`/`FK` = 키 여부(●). 데이터타입은 물리 타입·길이 그대로.
- 구성: 테이블 목록 → 테이블별 명세(컬럼/제약/인덱스) → 인덱스·FK·CHECK 종합 → 특이사항

---

## 0. 테이블 목록

| # | 물리 테이블명 | 논리명 | PK | 성격 |
|---|---|---|---|---|
| 1 | ctl_ingest_target_master | 수집대상마스터 | target_id | 기준(마스터) |
| 2 | ctl_master_pipeline_run | 마스터파이프라인수행상태 | master_run_id | 상태/이력 |
| 3 | ctl_run_skip | 트리거Skip예외 | trigger_nm, master_pipeline_nm | 예외(옵션) |
| 4 | ctl_ingest_pipeline_run | 수집파이프라인수행이력 | ingest_pipeline_id | 이력(팩트) |
| 5 | ctl_dbx_ingest_history | Databricks적재이력 | master_run_id, ingest_pipeline_id | 이력 |

---

## 1. ctl_ingest_target_master (수집대상마스터)

수집 대상 정의 + 증분 HWM 상태. 개발자/운영자가 관리하는 유일한 수작업 테이블.

| 물리컬럼명 | 논리명 | 데이터타입 | NN | PK | FK | 기본값 | 설명 |
|---|---|---|:--:|:--:|:--:|---|---|
| target_id | 수집대상ID | bigserial | ● | ● | | | 수집 대상 PK (애플리케이션 채번) |
| src_type | 소스커넥션타입 | varchar(10) | ● | | | | db2/mssql/oracle/mysql/postgres/rest/sftp |
| src_db_nm | 소스DB명 | varchar(20) | | | | | DBNAME |
| src_schema_nm | 소스스키마명 | varchar(30) | | | | | |
| src_table_nm | 소스테이블명 | varchar(100) | | | | | |
| partition_clause | 파티션분할조건절 | varchar(100) | | | | | 초기적재 분할 추출용 |
| landing_path_tpl | 랜딩경로템플릿 | varchar(100) | | | | | |
| data_class | 데이터분류 | varchar(4) | ● | | | | meta/raw |
| ingest_type | 수집유형 | varchar(4) | ● | | | | full/incr |
| exec_group | 실행그룹 | varchar(20) | | | | | 초기적재 용량분산 SEQ(01,02) |
| condition_type | 증분조건유형 | varchar(12) | | | | | window/bizday/close |
| condition_frequency | 수집주기 | varchar(10) | | | | | minutely~yearly (window 전용) |
| condition_interval | 수집주기간격 | int4 | | | | | window 주기 단위 N |
| incr_column_nm | 증분기준컬럼명 | varchar(30) | | | | | IPDTM/CLYM |
| incr_column_type | 증분기준컬럼타입 | varchar(10) | | | | | NUMERIC/DATE/TIMESTAMP/STRING |
| incr_column_hw_val | 증분하이워터마크값 | varchar(100) | | | | | 성공 시에만 갱신 |
| is_active | 활성여부 | bpchar(1) | ● | | | 'Y' | Y/N |
| pending_yn | 초기수집대기여부 | bpchar(1) | ● | | | 'Y' | Y=대기, N=완료 |
| created_by | 생성자 | varchar(100) | | | | | |
| created_dt | 생성일시 | timestamptz | ● | | | now() | |
| update_by | 수정자 | varchar(100) | | | | | |
| update_dt | 수정일시 | timestamptz | | | | | |

제약조건
- PK: `ctl_ingest_target_master_pkey` (target_id)
- CHECK: `ck_data_class`(meta/raw), `ck_ingest_type`(full/incr), `..._condition_type_check`(window/bizday/close), `..._condition_frequency_check`(minutely~yearly), `ck_incr_col_type`(NUMERIC/DATE/TIMESTAMP/STRING), `ck_full_no_cond`, `ck_incr_needs_type`, `ck_window_params`, `ck_freq_window_only`, `ck_bizday_col`, `ck_close_col`, `ck_incr_col_type_req`, `ck_target_yn`(is_active·pending_yn ∈ Y/N)

인덱스
- `ix_target_pending` : (data_class) WHERE is_active='Y' AND pending_yn='Y' — 부분(PENDING lookup)
- `ix_target_route` : (data_class, ingest_type, condition_type, condition_frequency, condition_interval) WHERE is_active='Y' — 부분(라우팅 lookup)

---

## 2. ctl_master_pipeline_run (마스터파이프라인수행상태)

마스터 파이프라인 수행 상태. `ux_mrs_running`으로 동시수행 락 겸용.

| 물리컬럼명 | 논리명 | 데이터타입 | NN | PK | FK | 기본값 | 설명 |
|---|---|---|:--:|:--:|:--:|---|---|
| master_run_id | 마스터수행ID | varchar(100) | ● | ● | | | @pipeline().RunId |
| master_pipeline_nm | 마스터파이프라인명 | varchar(200) | ● | | | | @pipeline().Pipeline |
| trigger_nm | 트리거명 | varchar(200) | | | | | @pipeline().TriggerName |
| ingest_type | 수집유형 | varchar(4) | | | | | full/incr |
| start_dt | 시작일시 | timestamptz | ● | | | | |
| end_dt | 종료일시 | timestamptz | | | | | |
| child_total_cnt | 차일드전체건수 | int4 | | | | 0 | |
| child_succ_cnt | 차일드성공건수 | int4 | | | | 0 | |
| child_fail_cnt | 차일드실패건수 | int4 | | | | 0 | |
| status | 수행상태 | varchar(15) | ● | | | | PENDING/RUNNING/SUCCEEDED/FAILED/SKIPPED |
| skip_type | Skip유형 | varchar(10) | | | | | WINDOW/FLAG/LOCKED |
| run_mode | 실행모드 | varchar(10) | | | | | SCHEDULE/MANUAL |
| created_by | 생성자 | varchar(100) | | | | | |
| created_dt | 생성일시 | timestamptz | ● | | | now() | |
| update_by | 수정자 | varchar(100) | | | | | |
| update_dt | 수정일시 | timestamptz | | | | | |

제약조건
- PK: `ctl_master_run_status_pkey` (master_run_id)
- CHECK: `..._ingest_type_check`(full/incr), `..._run_mode_check`(SCHEDULE/MANUAL), `..._skip_type_check`(WINDOW/FLAG/LOCKED), `..._status_check`(5종)

인덱스
- `ix_mpr_status_start` : (status, start_dt) — 모니터링
- `ux_mrs_running` : **UNIQUE** (master_pipeline_nm, ingest_type) WHERE status='RUNNING' — 동시수행 락

---

## 3. ctl_run_skip (트리거Skip예외)

트리거 skip 예외 목록(opt-in). 행 없음=RUN, skip_yn='Y'=SKIP.

| 물리컬럼명 | 논리명 | 데이터타입 | NN | PK | FK | 기본값 | 설명 |
|---|---|---|:--:|:--:|:--:|---|---|
| trigger_nm | 트리거명 | varchar(200) | ● | ● | | | |
| master_pipeline_nm | 마스터파이프라인명 | varchar(200) | ● | ● | | | |
| skip_yn | Skip여부 | bpchar(1) | ● | | | 'Y' | Y/N |
| skip_from_dt | Skip시작일시 | timestamptz | | | | | NULL이면 무기한(FLAG) |
| skip_to_dt | Skip종료일시 | timestamptz | | | | | from/to 동시 NULL 또는 동시 값 |
| skip_reason | Skip사유 | varchar(500) | | | | | |
| created_by | 생성자 | varchar(100) | | | | | |
| created_dt | 생성일시 | timestamptz | ● | | | now() | |
| update_by | 수정자 | varchar(100) | | | | | |
| update_dt | 수정일시 | timestamptz | | | | | |

제약조건
- PK: `ctl_run_skip_pkey` (trigger_nm, master_pipeline_nm)
- CHECK: `ctl_run_skip_check`(from≤to), `ctl_run_skip_check1`(from/to 동시 NULL성), `ck_skip_yn`(Y/N)

인덱스: 없음 (PK 인덱스로 조회 커버)

---

## 4. ctl_ingest_pipeline_run (수집파이프라인수행이력)

차일드(수집) 파이프라인 수행 이력. 대상 마스터 값의 수행시점 스냅샷 보존.

| 물리컬럼명 | 논리명 | 데이터타입 | NN | PK | FK | 기본값 | 설명 |
|---|---|---|:--:|:--:|:--:|---|---|
| ingest_pipeline_id | 수집수행ID | varchar(100) | ● | ● | | gen_random_uuid()::text | 대리키 |
| master_run_id | 마스터수행ID | varchar(100) | ● | | ● | | → ctl_master_pipeline_run |
| master_pipeline_nm | 마스터파이프라인명 | varchar(200) | ● | | | | |
| ingest_pipeline_run_id | 수집파이프라인RunID | varchar(100) | ● | | | '-' | ADF 차일드 실제 RunId |
| target_id | 수집대상ID | int8 | ● | | ● | | → ctl_ingest_target_master |
| src_type | 소스커넥션타입 | varchar(10) | ● | | | | [스냅샷] |
| src_db_nm | 소스DB명 | varchar(20) | | | | | [스냅샷] |
| src_schema_nm | 소스스키마명 | varchar(30) | | | | | [스냅샷] |
| src_table_nm | 소스테이블명 | varchar(100) | | | | | [스냅샷] |
| data_class | 데이터분류 | varchar(4) | ● | | | | [스냅샷] |
| ingest_type | 수집유형 | varchar(4) | ● | | | | [스냅샷] |
| exec_group | 실행그룹 | varchar(20) | | | | | [스냅샷] |
| condition_type | 증분조건유형 | varchar(12) | | | | | [스냅샷] |
| condition_frequency | 수집주기 | varchar(10) | | | | | [스냅샷] |
| condition_interval | 수집주기간격 | int4 | | | | | [스냅샷] |
| incr_column_nm | 증분기준컬럼명 | varchar(30) | | | | | [스냅샷] |
| incr_column_type | 증분기준컬럼타입 | varchar(10) | | | | | [스냅샷] |
| incr_column_hw_val | 증분하이워터마크값 | varchar(100) | | | | | [스냅샷] 사용된 HWM |
| is_active | 활성여부 | bpchar(1) | ● | | | 'Y' | [스냅샷] |
| pending_yn | 초기수집대기여부 | bpchar(1) | ● | | | 'Y' | [스냅샷] |
| extract_query | 추출쿼리 | text | ● | | | | 실제 실행 쿼리 |
| landing_path | 랜딩경로 | varchar(500) | | | | | |
| file_name | 파일명 | varchar(100) | | | | | |
| start_dt | 시작일시 | timestamptz | | | | | |
| end_dt | 종료일시 | timestamptz | | | | | |
| extract_cnt | 추출건수 | int8 | | | | | |
| save_cnt | 저장건수 | int8 | | | | | |
| attempt_no | 재시도회차 | int4 | ● | | | 0 | |
| status | 수행상태 | varchar(15) | | | | | PENDING/RUNNING/SUCCEEDED/FAILED/SKIPPED |
| error_log | 오류로그 | text | | | | | |
| created_by | 생성자 | varchar(100) | | | | | |
| created_dt | 생성일시 | timestamptz | ● | | | now() | |
| update_by | 수정자 | varchar(100) | | | | | |
| update_dt | 수정일시 | timestamptz | | | | | |

제약조건
- PK: `ctl_ingest_run_pkey` (ingest_pipeline_id)
- FK: `..._master_run_id_fkey` (master_run_id → ctl_master_pipeline_run.master_run_id), `..._target_id_fkey` (target_id → ctl_ingest_target_master.target_id)
- CHECK: `ck_ipr_yn`(is_active·pending_yn ∈ Y/N), `ctl_ingest_run_status_check`(status 5종)

인덱스
- `ix_ipr_created` : (created_dt)
- `ix_ipr_mri` : (master_run_id)
- `ix_ipr_status` : (master_run_id, status)
- `ix_ipr_target` : (target_id)

---

## 5. ctl_dbx_ingest_history (Databricks적재이력)

Databricks 브론즈 적재 완료 이력.

| 물리컬럼명 | 논리명 | 데이터타입 | NN | PK | FK | 기본값 | 설명 |
|---|---|---|:--:|:--:|:--:|---|---|
| master_run_id | 마스터수행ID | varchar(100) | ● | ● | ● | | → ctl_master_pipeline_run |
| ingest_pipeline_id | 수집수행ID | varchar(100) | ● | ● | ● | | → ctl_ingest_pipeline_run |
| bronze_table_nm | 브론즈테이블명 | varchar(200) | ● | | | | |
| landing_path | 랜딩경로 | varchar(500) | | | | | |
| file_name | 파일명 | varchar(100) | | | | | |
| start_dt | 시작일시 | timestamptz | | | | | |
| end_dt | 종료일시 | timestamptz | | | | | |
| extract_cnt | 추출건수 | int8 | | | | | |
| save_cnt | 저장건수 | int8 | | | | | |
| status | 수행상태 | varchar(15) | | | | | PENDING/RUNNING/SUCCEEDED/FAILED/SKIPPED |
| error_log | 오류로그 | text | | | | | |
| created_by | 생성자 | varchar(100) | | | | | |
| created_dt | 생성일시 | timestamptz | ● | | | now() | |
| update_by | 수정자 | varchar(100) | | | | | |
| update_dt | 수정일시 | timestamptz | | | | | |

제약조건
- PK: `ctl_dbx_ingest_history_pkey` (master_run_id, ingest_pipeline_id)
- FK: `..._ingest_pipeline_run_id_fkey` (ingest_pipeline_id → ctl_ingest_pipeline_run.ingest_pipeline_id), `..._master_run_id_fkey` (master_run_id → ctl_master_pipeline_run.master_run_id)
- CHECK: `ctl_dbx_ingest_status_check`(status 5종)

인덱스: 없음 (PK 인덱스로 조회 커버)

---

## 6. 인덱스 종합

| 인덱스명 | 테이블 | 유형 | 컬럼 | 조건(WHERE) |
|---|---|---|---|---|
| ctl_ingest_target_master_pkey | 수집대상마스터 | UNIQUE(PK) | target_id | |
| ix_target_pending | 수집대상마스터 | 부분 | data_class | is_active='Y' AND pending_yn='Y' |
| ix_target_route | 수집대상마스터 | 부분 | data_class, ingest_type, condition_type, condition_frequency, condition_interval | is_active='Y' |
| ctl_master_run_status_pkey | 마스터파이프라인수행상태 | UNIQUE(PK) | master_run_id | |
| ix_mpr_status_start | 마스터파이프라인수행상태 | 일반 | status, start_dt | |
| ux_mrs_running | 마스터파이프라인수행상태 | UNIQUE(부분) | master_pipeline_nm, ingest_type | status='RUNNING' |
| ctl_run_skip_pkey | 트리거Skip예외 | UNIQUE(PK) | trigger_nm, master_pipeline_nm | |
| ctl_ingest_run_pkey | 수집파이프라인수행이력 | UNIQUE(PK) | ingest_pipeline_id | |
| ix_ipr_created | 수집파이프라인수행이력 | 일반 | created_dt | |
| ix_ipr_mri | 수집파이프라인수행이력 | 일반 | master_run_id | |
| ix_ipr_status | 수집파이프라인수행이력 | 일반 | master_run_id, status | |
| ix_ipr_target | 수집파이프라인수행이력 | 일반 | target_id | |
| ctl_dbx_ingest_history_pkey | Databricks적재이력 | UNIQUE(PK) | master_run_id, ingest_pipeline_id | |

---

## 7. 외래키(FK) 종합

| FK명 | 자식(컬럼) | 부모(컬럼) |
|---|---|---|
| ctl_ingest_pipeline_run_master_run_id_fkey | ctl_ingest_pipeline_run(master_run_id) | ctl_master_pipeline_run(master_run_id) |
| ctl_ingest_pipeline_run_target_id_fkey | ctl_ingest_pipeline_run(target_id) | ctl_ingest_target_master(target_id) |
| ctl_dbx_ingest_history_ingest_pipeline_run_id_fkey | ctl_dbx_ingest_history(ingest_pipeline_id) | ctl_ingest_pipeline_run(ingest_pipeline_id) |
| ctl_dbx_ingest_history_master_run_id_fkey | ctl_dbx_ingest_history(master_run_id) | ctl_master_pipeline_run(master_run_id) |

※ ON DELETE/UPDATE 옵션은 미지정(기본 NO ACTION). 부모 삭제 시 자식 존재하면 삭제 거부.

---

## 8. CHECK 제약 종합

| 테이블 | 제약명 | 내용 |
|---|---|---|
| 수집대상마스터 | ck_data_class | data_class ∈ (meta, raw) |
| 수집대상마스터 | ck_ingest_type | ingest_type ∈ (full, incr) |
| 수집대상마스터 | ..._condition_type_check | condition_type ∈ (window, bizday, close) |
| 수집대상마스터 | ..._condition_frequency_check | condition_frequency ∈ (minutely~yearly) |
| 수집대상마스터 | ck_incr_col_type | incr_column_type ∈ (NUMERIC, DATE, TIMESTAMP, STRING) |
| 수집대상마스터 | ck_full_no_cond | full이면 조건/증분 컬럼 전부 NULL |
| 수집대상마스터 | ck_incr_needs_type | incr이면 condition_type 필수 |
| 수집대상마스터 | ck_window_params | window이면 frequency·interval·기준컬럼 필수 |
| 수집대상마스터 | ck_freq_window_only | frequency는 window에서만 |
| 수집대상마스터 | ck_bizday_col / ck_close_col | bizday·close이면 기준컬럼 필수 |
| 수집대상마스터 | ck_incr_col_type_req | 기준컬럼 있으면 타입 필수 |
| 수집대상마스터 | ck_target_yn | is_active·pending_yn ∈ (Y, N) |
| 마스터파이프라인수행상태 | ..._ingest_type_check | ingest_type ∈ (full, incr) |
| 마스터파이프라인수행상태 | ..._run_mode_check | run_mode ∈ (SCHEDULE, MANUAL) |
| 마스터파이프라인수행상태 | ..._skip_type_check | skip_type ∈ (WINDOW, FLAG, LOCKED) |
| 마스터파이프라인수행상태 | ..._status_check | status ∈ (PENDING, RUNNING, SUCCEEDED, FAILED, SKIPPED) |
| 트리거Skip예외 | ctl_run_skip_check | skip_from_dt ≤ skip_to_dt |
| 트리거Skip예외 | ctl_run_skip_check1 | from/to 동시 NULL 또는 동시 값 |
| 트리거Skip예외 | ck_skip_yn | skip_yn ∈ (Y, N) |
| 수집파이프라인수행이력 | ck_ipr_yn | is_active·pending_yn ∈ (Y, N) |
| 수집파이프라인수행이력 | ctl_ingest_run_status_check | status ∈ (5종) |
| Databricks적재이력 | ctl_dbx_ingest_status_check | status ∈ (5종) |

---

## 9. 특이사항 / 확인 필요

1. **참고** — FK ON DELETE 미지정. 이력 보존 정책상 부모 삭제를 막는 기본 동작이 적절하나, 운영 정리(purge) 시 자식→부모 순 삭제 필요.
