# ADF 메타 컨트롤 — 물리 모델 정의서 (Table Spec)

- DBMS: Azure Database for PostgreSQL
- 스키마: `META_ADF`
- 기준 DDL: `ADF_META_DDL_V1.7.sql`
- 표기: `NN` = NOT NULL(●), `PK`/`FK` = 키 여부(●). 데이터타입은 물리 타입·길이 그대로.
- ※ 코드성 값은 반드시 **대문자**로 저장(META/RAW/FULL/INCR/WINDOW 등). CHECK/인덱스의 `lower()` 전면 제거
- 구성: 테이블 목록 → 테이블별 명세(컬럼/제약/인덱스) → 인덱스·FK·CHECK 종합 → 특이사항

---

## 0. 테이블 목록

| # | 물리 테이블명 | 논리명 | PK | 성격 |
|---|---|---|---|---|
| 1 | ctl_ingest_target_master | 수집대상마스터 | target_id | 기준(마스터) |
| 2 | ctl_master_pipeline_run | 마스터파이프라인수행상태 | master_run_id | 상태/이력 |
| 3 | ctl_ingest_pipeline_run | 수집파이프라인수행이력 | ingest_pipeline_id | 이력(팩트) |
| 4 | ctl_dbx_ingest_history | Databricks적재이력 | master_run_id, ingest_pipeline_id | 이력 |
| 5 | ctl_trigger_history | 트리거수행이력 | trigger_id, master_run_id | 이력(로그) |

---

## 1. ctl_ingest_target_master (수집대상마스터)

수집 대상 정의 + 증분 HWM 상태. 개발자/운영자가 관리하는 유일한 수작업 테이블.

| 물리컬럼명 | 논리명 | 데이터타입 | NN | PK | FK | 기본값 | 설명 |
|---|---|---|:--:|:--:|:--:|---|---|
| target_id | 수집대상ID | bigserial | ● | ● | | | 수집 대상 PK (애플리케이션 채번) |
| src_type | 소스커넥션타입 | varchar(10) | ● | | | | DB2/MSSQL/ORACLE/MYSQL/POSTGRES/REST/SFTP |
| src_db_nm | 소스DB명 | varchar(20) | ● | | | | DATABASE |
| src_schema_nm | 소스스키마명 | varchar(30) | ● | | | | |
| src_table_nm | 소스테이블명 | varchar(100) | ● | | | | |
| partition_clause | 파티션분할조건절 | varchar(100) | | | | | 초기적재 분할 추출용 |
| data_class | 데이터분류 | varchar(4) | ● | | | | META/RAW |
| ingest_type | 수집유형 | varchar(4) | ● | | | | FULL/INCR |
| exec_group | 실행그룹 | varchar(20) | | | | | 초기적재 용량분산 SEQ(01,02) |
| condition_type | 증분조건유형 | varchar(12) | | | | | WINDOW/BIZDAY/CLOSE |
| condition_frequency | 수집주기 | varchar(10) | | | | | MINUTELY~YEARLY (WINDOW 전용) |
| condition_interval | 수집주기간격 | int4 | | | | | WINDOW 주기 단위 N |
| incr_column_nm | 증분기준컬럼명 | varchar(30) | | | | | IPDTM/CLYM |
| incr_column_type | 증분기준컬럼타입 | varchar(10) | | | | | NUMERIC/DATE/TIMESTAMP/STRING |
| incr_column_hw_val | 증분하이워터마크값 | varchar(100) | | | | | 성공 시에만 갱신 |
| delete_yn | 논리삭제여부 | bpchar(1) | ● | | | 'N' | Y=삭제, N=사용 |
| init_wait_yn | 초기수집대기여부 | bpchar(1) | ● | | | 'Y' | Y=대기, N=완료 |
| created_by | 생성자 | varchar(100) | | | | | |
| created_dt | 생성일시 | timestamptz | ● | | | now() | |
| update_by | 수정자 | varchar(100) | | | | | |
| update_dt | 수정일시 | timestamptz | | | | | |

제약조건
- PK: `ctl_ingest_target_master_pkey` (target_id)
- CHECK: `ck_data_class`(META/RAW), `ck_ingest_type`(FULL/INCR), `..._condition_type_check`(WINDOW/BIZDAY/CLOSE), `..._condition_frequency_check`(MINUTELY~YEARLY), `ck_incr_col_type`(NUMERIC/DATE/TIMESTAMP/STRING), `ck_full_no_cond`, `ck_incr_needs_type`, `ck_window_params`, `ck_freq_window_only`, `ck_bizday_col`, `ck_close_col`, `ck_incr_col_type_req`, `ck_target_yn`(delete_yn·init_wait_yn ∈ Y/N)

인덱스
- `ix_target_pending` : (data_class) WHERE delete_yn='N' AND init_wait_yn='Y' — 부분(초기수집 대기 lookup)
- `ix_target_route` : (data_class, ingest_type, condition_type, condition_frequency, condition_interval) WHERE delete_yn='N' — 부분(라우팅 lookup)

---

## 2. ctl_master_pipeline_run (마스터파이프라인수행상태)

마스터 파이프라인 수행 상태. `ux_mrs_running`으로 동시수행 락 겸용.

| 물리컬럼명 | 논리명 | 데이터타입 | NN | PK | FK | 기본값 | 설명 |
|---|---|---|:--:|:--:|:--:|---|---|
| master_run_id | 마스터수행ID | varchar(100) | ● | ● | | | @pipeline().RunId |
| master_pipeline_nm | 마스터파이프라인명 | varchar(200) | ● | | | | @pipeline().Pipeline |
| trigger_id | 트리거ID | varchar(100) | ● | | | | @pipeline().TriggerId |
| trigger_nm | 트리거명 | varchar(200) | ● | | | | @pipeline().TriggerName |
| ingest_type | 수집유형 | varchar(4) | ● | | | | FULL/INCR |
| start_dt | 시작일시 | timestamptz | ● | | | | |
| end_dt | 종료일시 | timestamptz | | | | | NULL=수행 중 |
| exec_group | 실행그룹 | varchar(20) | | | | | 오케스트레이터 실행 그룹/SEQ |
| child_total_cnt | 차일드전체건수 | int4 | | | | 0 | |
| child_succ_cnt | 차일드성공건수 | int4 | | | | 0 | |
| child_fail_cnt | 차일드실패건수 | int4 | | | | 0 | |
| status | 수행상태 | varchar(15) | ● | | | | PENDING/RUNNING/SUCCEEDED/FAILED/SKIPPED |
| run_mode | 실행모드 | varchar(10) | | | | | SCHEDULE/MANUAL |
| created_by | 생성자 | varchar(100) | ● | | | | |
| created_dt | 생성일시 | timestamptz | ● | | | now() | |
| update_by | 수정자 | varchar(100) | | | | | |
| update_dt | 수정일시 | timestamptz | | | | | |

제약조건
- PK: `ctl_master_run_status_pkey` (master_run_id)
- CHECK: `..._ingest_type_check`(FULL/INCR), `..._run_mode_check`(SCHEDULE/MANUAL), `..._status_check`(5종)

인덱스
- `ix_mpr_status_start` : (status, start_dt) — 모니터링
- `ux_mrs_running` : **UNIQUE** (master_pipeline_nm, ingest_type, trigger_nm) WHERE status='RUNNING' AND trigger_nm='SAND BOX' — 동시수행 락(매뉴얼 실행 한정)

---

## 3. ctl_ingest_pipeline_run (수집파이프라인수행이력)

차일드(수집) 파이프라인 수행 이력. 대상 마스터 값의 수행시점 스냅샷 보존.

| 물리컬럼명 | 논리명 | 데이터타입 | NN | PK | FK | 기본값 | 설명 |
|---|---|---|:--:|:--:|:--:|---|---|
| ingest_pipeline_id | 수집수행ID | varchar(100) | ● | ● | | gen_random_uuid()::text | 대리키 |
| master_run_id | 마스터수행ID | varchar(100) | ● | | ● | | → ctl_master_pipeline_run |
| master_pipeline_nm | 마스터파이프라인명 | varchar(200) | ● | | | | |
| ingest_pipeline_run_id | 수집파이프라인RunID | varchar(100) | ● | | | '-' | ADF 차일드 실제 RunId |
| target_id | 수집대상ID | int8 | ● | | ● | | → ctl_ingest_target_master |
| src_type | 소스커넥션타입 | varchar(10) | ● | | | | [스냅샷] |
| src_db_nm | 소스DB명 | varchar(20) | ● | | | | [스냅샷] |
| src_schema_nm | 소스스키마명 | varchar(30) | ● | | | | [스냅샷] |
| src_table_nm | 소스테이블명 | varchar(100) | ● | | | | [스냅샷] |
| data_class | 데이터분류 | varchar(4) | ● | | | | [스냅샷] |
| ingest_type | 수집유형 | varchar(4) | ● | | | | [스냅샷] |
| exec_group | 실행그룹 | varchar(20) | | | | | [스냅샷] |
| condition_type | 증분조건유형 | varchar(12) | | | | | [스냅샷] |
| condition_frequency | 수집주기 | varchar(10) | | | | | [스냅샷] |
| condition_interval | 수집주기간격 | int4 | | | | | [스냅샷] |
| incr_column_nm | 증분기준컬럼명 | varchar(30) | | | | | [스냅샷] |
| incr_column_type | 증분기준컬럼타입 | varchar(10) | | | | | [스냅샷] |
| incr_column_hw_val | 증분하이워터마크값 | varchar(100) | | | | | [스냅샷] 사용된 HWM |
| incr_start_val | 증분재수집시작값 | varchar(100) | | | | | override. NULL이면 저장 HWM 사용(수동 재수행) |
| init_wait_yn | 초기수집대기여부 | bpchar(1) | ● | | | 'N' | [스냅샷] |
| extract_query | 추출쿼리 | text | ● | | | | 실제 실행 쿼리 |
| landing_path | 랜딩경로 | varchar(500) | ● | | | | |
| file_name | 파일명 | varchar(100) | ● | | | '-' | |
| file_size_mb | 파일사이즈MB | int8 | | | | 0 | 수집한 파일 사이즈(MB) |
| start_dt | 시작일시 | timestamptz | | | | | |
| end_dt | 종료일시 | timestamptz | | | | | |
| extract_cnt | 추출건수 | int8 | ● | | | 0 | |
| save_cnt | 저장건수 | int8 | ● | | | 0 | |
| attempt_no | 재시도회차 | int4 | ● | | | 0 | |
| status | 수행상태 | varchar(15) | | | | | PENDING/RUNNING/SUCCEEDED/FAILED/SKIPPED |
| error_log | 오류로그 | text | | | | | |
| created_by | 생성자 | varchar(100) | ● | | | | |
| created_dt | 생성일시 | timestamptz | ● | | | now() | |
| update_by | 수정자 | varchar(100) | | | | | |
| update_dt | 수정일시 | timestamptz | | | | | |

제약조건
- PK: `ctl_ingest_run_pkey` (ingest_pipeline_id)
- FK: `..._master_run_id_fkey` (master_run_id → ctl_master_pipeline_run.master_run_id), `..._target_id_fkey` (target_id → ctl_ingest_target_master.target_id)
- CHECK: `ck_ipr_yn`(init_wait_yn ∈ Y/N), `ctl_ingest_run_status_check`(status 5종)

인덱스
- `ix_ipr_created` : (created_dt)
- `ix_ipr_mri` : (master_run_id)
- `ix_ipr_status` : (master_run_id, status)
- `ix_ipr_target` : (target_id)
- `ux_ipr_running` : **UNIQUE** (target_id, ingest_type) WHERE status='RUNNING' AND ingest_type='FULL' — 자식 동시수행 락(FULL 한정. 동일 대상 FULL RUNNING 1건)

---

## 4. ctl_dbx_ingest_history (Databricks적재이력)

Databricks 브론즈 적재 완료 이력.

| 물리컬럼명 | 논리명 | 데이터타입 | NN | PK | FK | 기본값 | 설명 |
|---|---|---|:--:|:--:|:--:|---|---|
| master_run_id | 마스터수행ID | varchar(100) | ● | ● | ● | | → ctl_master_pipeline_run |
| ingest_pipeline_id | 수집수행ID | varchar(100) | ● | ● | ● | | → ctl_ingest_pipeline_run |
| bronze_table_nm | 브론즈테이블명 | varchar(200) | ● | | | | |
| landing_path | 랜딩경로 | varchar(500) | ● | | | | |
| file_name | 파일명 | varchar(100) | ● | | | | |
| start_dt | 시작일시 | timestamptz | ● | | | | |
| end_dt | 종료일시 | timestamptz | ● | | | | |
| extract_cnt | 추출건수 | int8 | ● | | | | |
| save_cnt | 저장건수 | int8 | ● | | | | |
| status | 수행상태 | varchar(15) | ● | | | | PENDING/RUNNING/SUCCEEDED/FAILED/SKIPPED |
| error_log | 오류로그 | text | | | | | |
| created_by | 생성자 | varchar(100) | ● | | | | |
| created_dt | 생성일시 | timestamptz | ● | | | now() | |
| update_by | 수정자 | varchar(100) | | | | | |
| update_dt | 수정일시 | timestamptz | | | | | |

제약조건
- PK: `ctl_dbx_ingest_history_pkey` (master_run_id, ingest_pipeline_id)
- FK: `..._ingest_pipeline_run_id_fkey` (ingest_pipeline_id → ctl_ingest_pipeline_run.ingest_pipeline_id), `..._master_run_id_fkey` (master_run_id → ctl_master_pipeline_run.master_run_id)
- CHECK: `ctl_dbx_ingest_status_check`(status 5종)

인덱스: 없음 (PK 인덱스로 조회 커버)

---

## 5. ctl_trigger_history (트리거수행이력)

마스터 파이프라인 트리거 실행 및 Skip 판단 결과 기록(판단은 ADF 로직, 결과만 로그). V1.2의 `ctl_run_skip`(opt-in 예외 목록)을 대체.

| 물리컬럼명 | 논리명 | 데이터타입 | NN | PK | FK | 기본값 | 설명 |
|---|---|---|:--:|:--:|:--:|---|---|
| trigger_id | 트리거ID | varchar(100) | ● | ● | | | @pipeline().TriggerId |
| trigger_nm | 트리거명 | varchar(200) | ● | | | | @pipeline().TriggerName |
| master_run_id | 마스터수행ID | varchar(100) | ● | ● | | | @pipeline().RunId |
| master_pipeline_nm | 마스터파이프라인명 | varchar(200) | ● | | | | @pipeline().Pipeline |
| skip_yn | Skip여부 | bpchar(1) | ● | | | 'N' | Y=Skip됨, N=정상 실행 |
| skip_reason | Skip사유 | varchar(200) | | | | | 정상 실행 시 NULL |
| created_by | 생성자 | varchar(100) | ● | | | | |
| created_dt | 생성일시 | timestamptz | ● | | | now() | |
| update_by | 수정자 | varchar(100) | | | | | |
| update_dt | 수정일시 | timestamptz | | | | | |

제약조건
- PK: `ctl_trigger_history_pkey` (trigger_id, master_run_id)
- CHECK: `ck_trg_skip_yn`(skip_yn ∈ Y/N)

인덱스: 없음 (PK 인덱스로 조회 커버)

판정: Skip 판단은 ADF 로직이 수행. skip_yn='Y'면 Skip(skip_reason에 사유), 'N'이면 정상 실행(skip_reason NULL). 물리 FK 없이 master_run_id로 마스터수행과 논리 연관.

---

## 6. 인덱스 종합

| 인덱스명 | 테이블 | 유형 | 컬럼 | 조건(WHERE) |
|---|---|---|---|---|
| ctl_ingest_target_master_pkey | 수집대상마스터 | UNIQUE(PK) | target_id | |
| ix_target_pending | 수집대상마스터 | 부분 | data_class | delete_yn='N' AND init_wait_yn='Y' |
| ix_target_route | 수집대상마스터 | 부분 | data_class, ingest_type, condition_type, condition_frequency, condition_interval | delete_yn='N' |
| ctl_master_run_status_pkey | 마스터파이프라인수행상태 | UNIQUE(PK) | master_run_id | |
| ix_mpr_status_start | 마스터파이프라인수행상태 | 일반 | status, start_dt | |
| ux_mrs_running | 마스터파이프라인수행상태 | UNIQUE(부분) | master_pipeline_nm, ingest_type, trigger_nm | status='RUNNING' AND trigger_nm='SAND BOX' |
| ctl_ingest_run_pkey | 수집파이프라인수행이력 | UNIQUE(PK) | ingest_pipeline_id | |
| ix_ipr_created | 수집파이프라인수행이력 | 일반 | created_dt | |
| ix_ipr_mri | 수집파이프라인수행이력 | 일반 | master_run_id | |
| ix_ipr_status | 수집파이프라인수행이력 | 일반 | master_run_id, status | |
| ix_ipr_target | 수집파이프라인수행이력 | 일반 | target_id | |
| ux_ipr_running | 수집파이프라인수행이력 | UNIQUE(부분) | target_id, ingest_type | status='RUNNING' AND ingest_type='FULL' |
| ctl_dbx_ingest_history_pkey | Databricks적재이력 | UNIQUE(PK) | master_run_id, ingest_pipeline_id | |
| ctl_trigger_history_pkey | 트리거수행이력 | UNIQUE(PK) | trigger_id, master_run_id | |

---

## 7. 외래키(FK) 종합

| FK명 | 자식(컬럼) | 부모(컬럼) |
|---|---|---|
| ctl_ingest_pipeline_run_master_run_id_fkey | ctl_ingest_pipeline_run(master_run_id) | ctl_master_pipeline_run(master_run_id) |
| ctl_ingest_pipeline_run_target_id_fkey | ctl_ingest_pipeline_run(target_id) | ctl_ingest_target_master(target_id) |
| ctl_dbx_ingest_history_ingest_pipeline_run_id_fkey | ctl_dbx_ingest_history(ingest_pipeline_id) | ctl_ingest_pipeline_run(ingest_pipeline_id) |
| ctl_dbx_ingest_history_master_run_id_fkey | ctl_dbx_ingest_history(master_run_id) | ctl_master_pipeline_run(master_run_id) |

※ ON DELETE/UPDATE 옵션은 미지정(기본 NO ACTION). 부모 삭제 시 자식 존재하면 삭제 거부.
※ ctl_trigger_history는 물리 FK 없음(master_run_id로 논리 연관만).

---

## 8. CHECK 제약 종합

| 테이블 | 제약명 | 내용 |
|---|---|---|
| 수집대상마스터 | ck_data_class | data_class ∈ (META, RAW) |
| 수집대상마스터 | ck_ingest_type | ingest_type ∈ (FULL, INCR) |
| 수집대상마스터 | ..._condition_type_check | condition_type ∈ (WINDOW, BIZDAY, CLOSE) |
| 수집대상마스터 | ..._condition_frequency_check | condition_frequency ∈ (MINUTELY~YEARLY) |
| 수집대상마스터 | ck_incr_col_type | incr_column_type ∈ (NUMERIC, DATE, TIMESTAMP, STRING) |
| 수집대상마스터 | ck_full_no_cond | FULL이면 조건/증분 컬럼 전부 NULL |
| 수집대상마스터 | ck_incr_needs_type | INCR이면 condition_type 필수 |
| 수집대상마스터 | ck_window_params | WINDOW이면 frequency·interval·기준컬럼 필수 |
| 수집대상마스터 | ck_freq_window_only | frequency는 WINDOW에서만 |
| 수집대상마스터 | ck_bizday_col / ck_close_col | BIZDAY·CLOSE이면 기준컬럼 필수 |
| 수집대상마스터 | ck_incr_col_type_req | 기준컬럼 있으면 타입 필수 |
| 수집대상마스터 | ck_target_yn | delete_yn·init_wait_yn ∈ (Y, N) |
| 마스터파이프라인수행상태 | ..._ingest_type_check | ingest_type ∈ (FULL, INCR) |
| 마스터파이프라인수행상태 | ..._run_mode_check | run_mode ∈ (SCHEDULE, MANUAL) |
| 마스터파이프라인수행상태 | ..._status_check | status ∈ (PENDING, RUNNING, SUCCEEDED, FAILED, SKIPPED) |
| 수집파이프라인수행이력 | ck_ipr_yn | init_wait_yn ∈ (Y, N) |
| 수집파이프라인수행이력 | ctl_ingest_run_status_check | status ∈ (5종) |
| Databricks적재이력 | ctl_dbx_ingest_status_check | status ∈ (5종) |
| 트리거수행이력 | ck_trg_skip_yn | skip_yn ∈ (Y, N) |

---

## 9. 특이사항 / 확인 필요

1. **FK ON DELETE 미지정** — 이력 보존 정책상 부모 삭제를 막는 기본 동작이 적절하나, 운영 정리(purge) 시 자식→부모 순 삭제 필요.
2. **논리삭제 의미 반전** — `is_active`(Y=활성) → `delete_yn`(Y=삭제). 마이그레이션 시 값 반전 필요(`delete_yn = NOT is_active`).
3. **코드 대문자화** — 기존 소문자 저장 데이터(meta/raw/full/incr 등)는 대문자로 일괄 정규화 후 CHECK 적용 필요.
4. **ctl_run_skip 폐지** — 기존 opt-in 예외 목록(skip_from_dt/skip_to_dt 기간 포함)은 `ctl_trigger_history`(수행 결과 로그)로 대체됨. Skip 판단 로직은 ADF로 이관.
