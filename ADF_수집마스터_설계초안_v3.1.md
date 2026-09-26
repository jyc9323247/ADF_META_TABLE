# ADF 수집 마스터 파이프라인 설계 초안 v3.1

- 작성일: 2026-09-24
- 상태: **초안 (문서·DDL·스크립트 미반영)**. 확정 후 프로젝트 문서에 일괄 반영
- 기준: v8.3 메타 모델 (`adf_meta_ddl_v83.sql`) 대비 변경
- 표기: **[확인필요]** = 확실하지 않거나 실제 확인·결정이 남은 항목

---

## 1. 설계 원칙

| # | 원칙 | 비고 |
|---|---|---|
| 1 | 그룹 1개 = 실행유형 1개 | 그룹은 `job_type_cd` 하나만 가짐 (META_FULL / RAW_FULL / RAW_REFRESH / RAW_INCR / RAW_CHECK) |
| 2 | 스크립트는 단순·가독성 우선 | 날짜·기간 계산은 ADF 식으로 하고 `pv_` 변수에 저장 |
| 3 | 컨트롤 테이블 모델 유지 | 수집 경로, 파일, 원천 정보, `load_method_cd` (Databricks 담당자가 사용) |
| 4 | DBX 관련 사항은 사람 간 소통 | 파이프라인이 DBX 담당자에게 자동으로 정보를 넘기지 않음 |
| 5 | 이름 규칙 | 파라미터 `p_`, 파이프라인 변수 `pv_`, 전역 파라미터 `gp_` |
| 6 | 스케줄 PARTIAL_REFRESH 불허 | 정기 FULL_REFRESH(RAW_REFRESH 그룹)는 허용. PARTIAL은 정합성 체크 후 또는 시그널 후에만 |
| 7 | ADF 표현식 검증 필요 | 아래 §15의 검증 항목은 실제 ADF에서 확인 [확인필요] |

---

## 2. 파이프라인 구성

```text
Trigger (스케줄)
 └ pl_cmn_orch_trigger_entry (진입)
     └ ForEach(p_sched_json) : 그룹별 병렬
         └ Switch(job_type_cd) → 마스터

Trigger (분·시간 INCR)
 └ pl_raw_orch_incr_time (진입 생략, 직접 호출)

[마스터 8개]
 pl_raw_orch_full          RAW FULL (비파티션 / 파티션 전체, 정기)
 pl_meta_orch_full         META 수집 (TABLE·GEN2 두 Sink)
 pl_raw_orch_init          INIT_LOAD / 정기 FULL_REFRESH / PARTIAL_REFRESH / REQUEST
 pl_raw_orch_incr_day      INCR 일 단위
 pl_raw_orch_incr_month    INCR 월 단위
 pl_raw_orch_incr_time     INCR 분·시간 단위
 pl_raw_chk_partition      파티션 정합성 체크
 pl_raw_orch_adhoc         비메타 adhoc 쿼리 → GEN2

[재수행]
 pl_cmn_orch_retry         이전 마스터의 실패 차일드만 재수행

[공통 자식]
 pl_cmn_master_start       마스터 이력 등록 + 시작 검증
 pl_cmn_run_check          BIZDAY 판정 · 전역 skip · Trigger 이력
 pl_cmn_dbx_run  (신규)    Databricks REST 호출
 pl_dispatcher_ingest      자식 수집 파이프라인 호출

[자식 수집 파이프라인]
 pl_raw_ingest_db2_{DB}    DB별. Sink = GEN2 (RAW, adhoc, META→GEN2)
 pl_meta_ingest_db2_MLMTP  META → PG 테이블 (별도)
```

- 진입 파이프라인은 마스터를 감싸는 얇은 층입니다. 마스터 자체는 진입 없이도 단독 실행됩니다.
- 분·시간 INCR Trigger는 진입을 생략하고 `pl_raw_orch_incr_time`을 직접 호출합니다.
- 마스터가 8개인 이유: 실행유형·단위가 다르면 시작값·범위 계산이 달라지기 때문입니다.

---

## 3. 요건과 마스터 대응

| 요건 | 처리 마스터 | 적재방식 |
|---|---|---|
| 1.1 비파티션 FULL (초기 / 정기 / 요청) | `full` (정기), `init` (초기·요청) | FULL_DELETE_INSERT |
| 1.2 파티션 전체 분할 파일 | `full`, `init` | FULL_DELETE_INSERT |
| 2.1 부분 파티션 delete&insert (연 / 연월) | `init` (PARTIAL_REFRESH) | RANGE_DELETE_INSERT |
| 2.2.1 범위 파티션 컬럼, N개월 전 | `incr_month` | RANGE_DELETE_INSERT |
| 2.2.2 생성·변경 일시, N일 전 | `incr_day` | UPSERT |
| 분·시간 단위 증분 | `incr_time` | UPSERT (Auto Loader) |
| 정합성 체크 후 재수집 | `chk_partition` → `init`(REQUEST) | PARTIAL / FULL |
| META → PG 테이블 / GEN2 | `meta` | (TABLE은 DBX 대상 아님) |
| 비메타 adhoc → GEN2 | `adhoc` | FULL_DELETE_INSERT |
| 실패 차일드만 재수행 | `retry` | 원래 방식 유지 |

---

## 4. 파라미터

### 4.1 진입 `pl_cmn_orch_trigger_entry`

| 파라미터 | 형식 | 설명 |
|---|---|---|
| `p_scheduled_time` | String | Trigger 예정시각. `@trigger().scheduledTime`을 Trigger 파라미터로 전달해야 함 |
| `p_sched_json` | Array | 요소: `{run_group_nm, bizday_n, bizday_offset, dbx_last_run_yn}`. 같은 파이프라인을 한 Trigger에 다른 파라미터로 두 번 등록할 수 없어 배열로 전달 |

### 4.2 full / meta / incr_day / incr_month / incr_time

| 파라미터 | 대상 | 설명 |
|---|---|---|
| `p_run_group_nm` | 전체 | 그룹명 |
| `p_scheduled_time` | 전체 | 예정시각 |
| `p_bizday_n`, `p_bizday_offset` | 전체 | BIZDAY 판정용 |
| `p_target_ids` | 전체 | 선택. 일부 Target만 (예: `[1001,1002]`) |
| `p_incr_from_val` | incr 3종 | 선택. 수동 시작값 (있으면 자동 계산값보다 우선) |

### 4.3 `pl_raw_orch_init`

진입 방식(A·B·C)은 **정확히 하나만** 지정합니다. 동시 지정·미지정은 활동 실패입니다.

| 진입 | 파라미터 | 의미 | BIZDAY |
|---|---|---|---|
| A. INIT_LOAD | `p_init_load_group` | 초기 적재 그룹 | 안 함 (값이 오면 **무시**) |
| B. REFRESH | `p_run_group_nm` (+ 선택 `p_run_type_override='PARTIAL_REFRESH'`) | 정기 FULL_REFRESH 또는 수동 PARTIAL | **함** (`p_bizday_n`, `p_bizday_offset` 필요) |
| C. REQUEST | `p_request_yn = Y` | 정합성 체크 후 재수집 요청 처리 | 안 함 (값이 오면 **무시**) |

| 공통 파라미터 | 설명 |
|---|---|
| `p_start_val`, `p_end_val` | 범위 (PARTIAL 등) |
| `p_partition_val` | 파티션 값 목록 |
| `p_target_ids`, `p_scheduled_time` | 선택 |
| `p_dbx_last_run_yn` | 기본 Y. 진입 파이프라인이 호출할 때는 N (마지막 파이프라인에서 DBX 호출) |

### 4.4 `pl_raw_chk_partition`

| 파라미터 | 설명 |
|---|---|
| `p_run_group_nm` | RAW_CHECK 그룹 |
| `p_check_mode` | 체크 방식 |
| `p_target_ids` | 선택 |
| `p_scheduled_time` | 선택 |

- BIZDAY 파라미터는 받지 않습니다.

### 4.5 `pl_raw_orch_adhoc`

| 파라미터 | 설명 |
|---|---|
| `p_src_db_nm` | 원천 DB |
| `p_query` | 실행 쿼리 (안전 규칙 검증 없음) |
| `p_adhoc_nm` | adhoc 이름 (파일명 등에 사용) |
| `p_landing_path` | 적재 경로 |
| `p_dbx_last_run_yn` | 기본 Y |
| `p_requester`, `p_desc` | 요청자, 사유 (이력 기록용) |

### 4.6 `pl_cmn_orch_retry`

| 파라미터 | 필수 | 설명 |
|---|---|---|
| `p_retry_master_run_id` | Y | 원래 마스터의 ADF RunId |
| `p_run_group_nm` / `p_init_load_group` | 선택 | 검증용 (택일). 원래 마스터의 그룹과 일치 확인 |
| `p_target_ids` | 선택 | 실패 건 중 일부만. 비우면 실패 전체 |
| `p_dbx_last_run_yn` | 선택 | 기본 Y |
| `p_requester`, `p_desc` | 선택 | 재수행 사유 기록 |

- `p_scheduled_time`, `p_bizday_*`, `p_incr_from_val`은 받지 않습니다. FAILED 행에 이전 값이 있기 때문입니다.

---

## 5. 파이프라인 변수·전역 파라미터

| 이름 | 종류 | 용도 |
|---|---|---|
| `pv_job_types` | 변수 | 마스터가 허용하는 job_type 고정값 (`pl_cmn_master_start` 호출 시 `p_job_types`로 전달) |
| `pv_condition_unit`, `pv_condition_interval` | 변수 | INCR 마스터의 단위·간격 |
| `pv_base_dt` | 변수 | `p_scheduled_time`의 KST (없으면 utcnow의 KST) |
| `pv_incr_from_val` | 변수 | INCR 시작값 |
| `pv_yyyyMMddHHmmss` | 변수 | 파일명 시각 |
| `pv_start_val`, `pv_end_val`, `pv_partition_val` | 변수 | init 범위·파티션 |
| `gp_pipeline_run_skip` | 전역 | 전체 수집 중지 (행은 SKIPPED로 남음) |
| `gp_meta_purge_col` | 전역 | META TABLE 보관 정리 기준 컬럼 |
| `gp_meta_purge_days` | 전역 | 보관 일수 (7) |

---

## 6. `pl_cmn_master_start` (공통 자식)

모든 마스터의 첫 단계에서 호출됩니다. 이력 등록과 시작 조건 검증을 담당합니다.

### 6.1 하는 일

```text
1. ctl_master_pipeline_run 에 이번 실행을 PENDING 으로 INSERT
   (master_run_id = 호출한 마스터의 RunId, trigger 정보, 그룹, run_param, entry_run_id)
2. 검증
3. 통과 → 다음 단계(pl_cmn_run_check)
```

### 6.2 입력 파라미터

| 파라미터 | 설명 |
|---|---|
| `p_master_run_id`, `p_master_pipeline_nm` | 마스터 식별 |
| `p_trigger_type`, `p_trigger_nm` | 실행 구분, Trigger 이름 |
| `p_job_types` | 허용 job_type 목록 (마스터의 `pv_job_types`) |
| `p_run_group_nm`, `p_scheduled_time` | 그룹, 예정시각 |
| `p_target_ids` | 선택. 대상 일부 |
| `p_incr_from_val` | 수동 시작값 |
| `p_bizday_n`, `p_bizday_offset` | BIZDAY 값 (이력 기록) |
| `p_dbx_last_run_yn` | DBX 호출 여부 기록 |
| `p_cond_unit`, `p_cond_interval` | INCR 단위·간격 검증용 |

### 6.3 검증 코드

| 코드 | 조건 |
|---|---|
| `RUN_GROUP_NOT_FOUND_OR_JOB_TYPE` | 그룹이 없거나 job_type 불일치 |
| `TARGET_NOT_IN_GROUP` | `p_target_ids`가 그룹 소속이 아님 |
| `INCR_COND_VAR_INVALID` | 단위·간격 값이 잘못됨 (현재 DAY/MONTH/MINUTE만 허용) |
| `INCR_POLICY_MISMATCH` | 마스터의 단위와 그룹 정책이 다름 |

### 6.4 v3 변경

| 변경 | 내용 |
|---|---|
| HOUR 허용 | `INCR_COND_VAR_INVALID`에서 HOUR 추가 (`incr_time`이 분·시간 처리) |
| `p_cond_interval` 선택 | init·full 마스터는 넘기지 않음 |
| `entry_run_id` 기록 | 진입 RunId를 마스터 이력에 저장 |

- 검증 4종의 상세 조건은 이전에 읽은 스크립트 기준입니다. 반영 전에 스크립트를 다시 확인해야 합니다. **[확인필요]**
- 같은 master_run_id로 재호출 시 무시(ON CONFLICT)되는 동작이 `cmn_master_start`에도 있는지 확인이 필요합니다 (`master_ins_init`에서는 확인됨). **[확인필요]**

---

## 7. 자식 수집 파이프라인과 디스패처

### 7.1 자식 선택 (META 포함)

- 수집대상 마스터 테이블의 `sink_type_cd`(GEN2 / TABLE)로 자식을 구분합니다.
- **Start 스크립트가 행마다 자식 파이프라인명을 계산**해서 디스패처에 전달합니다. 디스패처에는 `sink_type_cd` 분기가 없습니다.

| 수집 | Sink | 자식 파이프라인 | 마스터 |
|---|---|---|---|
| RAW / adhoc | GEN2 | `pl_raw_ingest_db2_{DB}` | full, init, incr*, adhoc |
| META → GEN2 | GEN2 | `pl_raw_ingest_db2_{DB}` (기존 그대로) | `pl_meta_orch_full` |
| META → PG | TABLE | `pl_meta_ingest_db2_MLMTP` | `pl_meta_orch_full` |

```text
Start_ingest (SQL)
  sink_type_cd = 'TABLE' → ingest_pipeline_nm = 'pl_meta_ingest_db2_MLMTP'
  sink_type_cd = 'GEN2'  → ingest_pipeline_nm = 'pl_raw_ingest_db2_' || src_db_nm
        ↓ 출력 컬럼
ForEach → pl_dispatcher_ingest (자식 이름 전달)
```

- META→GEN2는 새 자식을 만들지 않습니다. META→GEN2 저장이 나중에 빠질 수 있기 때문입니다.
- `ingest_pipeline_nm`은 기존 출력 컬럼이므로 새 컬럼은 필요 없습니다. Start 스크립트의 계산식만 바뀝니다.
- 자식명은 `ctl_ingest_pipeline_run`에 저장되므로 재수행은 저장된 이름을 그대로 사용합니다.
- 디스패처의 호출 방식: ADF의 Execute Pipeline은 이름을 식으로 지정할 수 없어 Switch로 분기하는 방식이 일반적입니다. 현재 디스패처가 Switch 방식인지 확인이 필요합니다. **[확인필요]**

### 7.2 자식 처리 내용

| 자식 | 처리 |
|---|---|
| GEN2 자식 | `{ts}` 갱신 (사용자 로직, Copy 시작 전 실제 시작시각) → Copy(parquet) → `ctl_ingest_pipeline_run` 갱신. 파티션이면 `init_yn` 갱신 |
| META→PG 자식 | 보관 정리(DELETE) → Copy(PG) → 이력 갱신. DBX 대상 아님 |

- `landing_path`의 날짜와 `{ts}`가 어긋나도 허용합니다.

---

## 8. 실행 흐름

### 8.1 정기 (진입 경유)

```text
Trigger → 진입
  └ ForEach(p_sched_json, 그룹 병렬)
      └ Switch(job_type_cd) → 마스터
          ├ pl_cmn_master_start   (PENDING → RUNNING)
          ├ pl_cmn_run_check      (BIZDAY · 전역 skip 판정)
          ├ Start_ingest          (ctl_ingest_pipeline_run PENDING 행 생성)
          ├ Filter → ForEach → 디스패처 → 자식 수집
          └ End_master_run        (자식 집계로 상태 확정)
  └ 그날 스케줄상 마지막 파이프라인에서
    dbx_last_run_yn = Y → pl_cmn_dbx_run
```

- 진입이 마스터를 호출할 때 `p_dbx_last_run_yn = N`을 넘기고, 진입 안의 마지막 시점에서 한 번만 REST를 호출합니다.
- 그룹 일부가 실패해도 DBX는 호출합니다.
- 진입 RunId는 마스터 이력의 `entry_run_id`와 `ctl_dbx_bronze_trigger_run`에 기록됩니다.
- 새벽 4시처럼 여러 그룹이 있으면 병렬로 실행합니다. 진입 ForEach 동시성 상한은 추후 결정입니다.

### 8.2 비스케줄 (시그널 / 정합성 체크 후 재수집)

```text
정합성 체크 / 시그널 → 마스터 → 성공 후
  → pl_cmn_dbx_run 즉시 (p_dbx_last_run_yn = Y)
```

### 8.3 분·시간 INCR

```text
Trigger → pl_raw_orch_incr_time (진입 생략)
  └ Databricks Auto Loader 가 파일 처리
    REST 호출 없음, ctl_dbx_ingest_history 기록 없음
```

### 8.4 정합성 체크 후 재수집

```text
pl_raw_chk_partition
  Chk_start → ForEach Lookup_chk → Chk_save / Chk_fail
  → Chk_compare → pl_raw_orch_init(p_request_yn = Y) → Chk_end
```

---

## 9. INCR 시작값 규칙

| 단위 | 시작값 | 형식 |
|---|---|---|
| DAY | `pv_base_dt` − N일 | 일자 |
| MONTH | (`pv_base_dt` − N개월)의 **월 1일** | 일자 |
| MINUTE / HOUR | `pv_base_dt` − N분 / N시간 | `yyyy-MM-dd HH:mm` (초 절삭) |

- `pv_base_dt` = `p_scheduled_time`의 KST. 없으면 utcnow의 KST.
- N과 단위는 그룹의 정책에서 읽습니다. 그룹당 정책은 1개입니다 (C21).
- `p_incr_from_val`이 있으면 그 값을 우선합니다.
- 분 단위 수집은 겹치는 기간(overlap)으로 수집합니다.

### 9.1 Refresh 후 첫 INCR

- (직전 성공 INCR의 시작값)과 (일반 계산값) 중 **더 오래된 값**을 사용합니다. 날짜·시간으로 비교합니다.
- 직전 성공 INCR 이력이 없으면 일반 계산값입니다.
- INSERT_ONLY 적재방식의 예외는 요건이 없어 보류입니다. **[확인필요]**

---

## 10. 적재 규칙과 파티션 값

### 10.1 적재방식

| 컬럼 성격 | 적재방식 |
|---|---|
| 생성·변경 일시 | UPSERT |
| 범위 파티션 컬럼 (yyyymm 문자열, DATE, TIMESTAMP) | RANGE_DELETE_INSERT |
| FULL 파티션 전체 | FULL_DELETE_INSERT |
| adhoc | FULL_DELETE_INSERT |

### 10.2 init 파티션 값

| 유형 | 값 |
|---|---|
| FULL_REFRESH | 원천 DISTINCT 전체 (NULL 포함). `partition_clause`가 없으면 테이블 전체를 파일 1개 |
| INIT_LOAD | `partition_clause`의 partition_val JSON, 없으면 파라미터 |
| PARTIAL_REFRESH | 시작·끝 범위 / 값 목록(IN) / 시작값만 |
| REQUEST | `ctl_refresh_request`에 등록된 파티션 |

- 쿼리 템플릿의 `{WHERE}`는 `1=1`로 치환합니다. `{WHERE}`가 없으면 쿼리를 그대로 사용합니다.

---

## 11. META 수집

| 항목 | 내용 |
|---|---|
| 허용 대상 | 메타 컨트롤 테이블 기반만 |
| Sink | TABLE(PG) 또는 GEN2 |
| 마스터 | `pl_meta_orch_full` 하나 (§7.1로 자식 구분) |
| TABLE 보관 정리 | Copy 전에 `DELETE … WHERE gp_meta_purge_col < (KST 실행일 − gp_meta_purge_days)`, 그 뒤 insert |
| 보관 일수 | 7일 |
| 기준 컬럼 | 메타 테이블 모두 동일 |
| 중복 적재 | 삭제하지 않음 (전체 스냅샷) |
| DEFAULT 날짜 | KST |
| GEN2 Sink | 위 정리 규칙 적용 안 함 |

- 컬럼 DEFAULT를 KST로 정의하려면 `(now() AT TIME ZONE 'Asia/Seoul')::date` 형태가 필요합니다. `CURRENT_DATE`는 세션 시간대에 따라 UTC 날짜가 나올 수 있습니다. 메타 테이블 DDL의 DEFAULT 정의 확인이 필요합니다. **[확인필요]**
- 삭제 기준일도 같은 KST 날짜를 써야 삭제·DEFAULT 경계가 어긋나지 않습니다.

---

## 12. adhoc 수집

| 항목 | 내용 |
|---|---|
| Sink | GEN2 전용 (TABLE 적재 요건 삭제) |
| Target | DB별 adhoc Target을 미리 생성 (`src_schema=ADHOC`, `src_table=ADHOC`, GEN2) |
| 기록 | `ctl_ingest_pipeline_run`에 수집 기록 |
| 구분 | 증분 / 전체 구분 없음. `FULL_DELETE_INSERT`로 기록 |
| 쿼리 안전 규칙 | 적용 안 함 |
| DBX | adhoc GEN2 파일의 Bronze 적재도 수집으로 봄 |
| 설정 점검 | C07에서 ADHOC Target 제외 |

---

## 13. 재수행 `pl_cmn_orch_retry`

### 13.1 흐름

```text
pl_cmn_orch_retry
 ├─ Chk_retry      검증 6종 + 원래 마스터 FAILED → RUNNING (UPDATE … WHERE status='FAILED')
 ├─ Start_retry    FAILED 행 → PENDING, attempt_no + 1, start_dt/end_dt = NULL, file_name {ts} 새로 생성
 ├─ ForEach → pl_dispatcher_ingest (저장된 자식명 그대로)
 ├─ End_retry      원래 master_run_id 로 재집계
 ├─ [INIT_LOAD 마스터일 때만] 초기적재 확인·갱신 공통 스크립트 호출
 └─ If p_dbx_last_run_yn = Y → pl_cmn_dbx_run (즉시, 비스케줄)
```

### 13.2 검증 코드

| 코드 | 조건 |
|---|---|
| `RETRY_MASTER_NOT_FOUND` | run_id가 없음 |
| `RETRY_MASTER_NOT_FAILED` | 마스터 상태가 FAILED가 아님 |
| `RETRY_GROUP_MISMATCH` | 넘긴 그룹명이 마스터와 다름 |
| `RETRY_NO_FAILED_CHILD` | FAILED 자식이 없음 |
| `RETRY_TARGET_NOT_FAILED` | `p_target_ids` 중 FAILED가 아닌 건 포함 |
| `RETRY_NOT_ALLOWED_REQUEST` | 원래 마스터가 REQUEST (재수행 제외, 다음 정합성 체크가 재탐지) |

### 13.3 확정 규칙

| 항목 | 내용 |
|---|---|
| 행 재사용 | 같은 `ingest_pipeline_id`, `attempt_no`+1, status PENDING |
| 원래 master_run_id 유지 | FULL_REFRESH 파티션 묶음(`partition_cnt` 성공 건수 판정)이 유효하려면 필요 |
| 동시 재수행 방지 | 마스터 `FAILED → RUNNING` 조건부 UPDATE |
| run_check·BIZDAY | 건너뜀 (수동 재수행) |
| `gp_pipeline_run_skip` | 따름 |
| 좀비 RUNNING/PENDING | 운영자가 먼저 FAILED로 변경 |
| 오류 이력 | `run_param.retries`에 `{retry_run_id, start, target_ids, prev_error(200자), requester}` 추가. DDL 변경 없음 |
| META TABLE 재수행 | 보관 정리를 다시 실행. 부분 insert가 남으면 중복 가능 (허용) |
| 부분 parquet 파일 | 무해 (DBX는 SUCCEEDED 행의 파일만 적재) |

- `prev_error` 200자는 제안값입니다. 재수행 오류를 `run_param.retries`에 기록하는 것으로 이해했습니다. **[확인필요]**

### 13.4 INIT_LOAD 후처리

| 수집 | 후처리 위치 | 재수행 영향 |
|---|---|---|
| 일반 FULL | 자식 파이프라인 | 자식이 다시 실행되므로 자동 처리 |
| 파티션 수집 | 자식이 파티션 파일별 `ctl_ingest_pipeline_run.init_yn`을 N으로 갱신 → **마스터 로직**이 확인 후 Target Master의 초기적재 플래그를 N으로 갱신 | 재수행 경로에서도 마스터 확인·갱신을 다시 실행해야 함 |

- **A안 확정**: 마스터의 확인·갱신 로직을 공통 스크립트로 분리하고, 마스터와 재수행이 같은 파라미터(`p_master_run_id`)로 호출합니다.
- 이 스크립트는 **아직 작성 전**이므로 신규 작성 대상입니다.
- 성공 판정은 원래 master_run_id 기준으로 파티션 전체 SUCCEEDED를 제안합니다. **[확인필요: 판정 조건 확정]**
- 수집 이력은 `init_yn`, Target Master는 `init_wait_yn`으로 언급되었는데 실제 컬럼명은 DDL 확인이 필요합니다. **[확인필요]**
- End_retry는 `master_end`와 같은 재집계 SQL을 쓰되 키를 `pipeline().RunId`가 아닌 파라미터로 받습니다.

---

## 14. Databricks 연계

### 14.1 호출 규칙

| 상황 | DBX 호출 |
|---|---|
| 정기 스케줄 | 그날 스케줄상 마지막 파이프라인에서 `dbx_last_run_yn = Y`일 때 한 번 (그룹 일부 실패해도 호출) |
| 비스케줄 (시그널, 정합성 체크 후 재수집, adhoc, 재수행) | 성공 후 즉시 (`p_dbx_last_run_yn = Y`) |
| 분·시간 INCR | 호출 없음 (Auto Loader) |
| 중복 호출 | 실행 중 두 번째 호출은 무의미 |

### 14.2 DBX 동작

| 항목 | 내용 |
|---|---|
| 기동 | REST 호출로 VM 기동 |
| 대상 | 메타 컨트롤 테이블의 SUCCEEDED parquet 파일 |
| 적재 판정 | `ctl_dbx_ingest_history` |
| 분·시간 INCR | Auto Loader. 이력 기록 안 함 |
| 미적재 조회 | `vw_dbx_pending_file` (ADF·DBX 공통 사용) |
| FULL_DELETE_INSERT 묶음 | 같은 `(master_run_id, target_id)`가 `partition_cnt`개 모두 SUCCEEDED일 때 1회 전체 삭제 후 모든 파일 INSERT |

### 14.3 `vw_dbx_pending_file` 조건 (제안)

- Sink = GEN2, 상태 = SUCCEEDED
- `ctl_dbx_ingest_history`에 SUCCEEDED 이력이 없음
- 최근 N일 (N은 추후 결정)
- INCR 중 정책 단위가 MINUTE / HOUR인 대상은 제외 (Auto Loader 처리)

### 14.4 DBX 전달 항목 (추후 결정, 사람 간 소통)

- 뷰 사용 방법, REST 호출 시점·큐잉
- adhoc Bronze 자동 생성·이름 규칙·덮어쓰기(replace) 방식
- META→GEN2의 Bronze 적재 여부
- 범위 값 형식, PARTIAL의 IN 값·NULL·0건 파일
- FULL_REFRESH 묶음 삭제 규칙
- 재수행 시 동일 `master_run_id`, `ingest_pipeline_id` 재사용 + `attempt_no` 증가 + `file_name` 변경
- `file_name`의 `{ts}` 갱신 방식

---

## 15. 변경 목록

### 15.1 DDL

| 대상 | 변경 |
|---|---|
| `ck_ingest_type` | ADHOC 추가 |
| `ck_ingest_type_method` | ADHOC → FULL_DELETE_INSERT |
| `ck_master_override` | ADHOC 추가 |
| `ck_master_entry` | ADHOC은 그룹 없이 허용 |
| `ctl_master_pipeline_run` | `entry_run_id` 추가 (nullable) |
| `ctl_dbx_bronze_trigger_run` | `entry_run_id` 추가 (nullable) |
| 신규 뷰 | `vw_dbx_pending_file` |
| `vw_ctl_config_check` | C07에서 ADHOC Target 제외 |

### 15.2 스크립트·파이프라인

| 대상 | 변경 |
|---|---|
| `master_ins_init` | 파라미터 4개 추가, REFRESH(B)만 BIZDAY 판단·A·C는 값 무시 |
| `cmn_master_start` | HOUR 허용, `p_cond_interval` 선택, `entry_run_id` 기록 |
| INCR 스크립트 | Target별 하한, 월 1일, 시간 단위 |
| Start 스크립트 전반 | `ingest_pipeline_nm` 계산을 `sink_type_cd` 기준으로 (META 마스터) |
| 신규 파이프라인 | 진입, `pl_cmn_dbx_run`, `incr_time`, `meta`, `adhoc`, `pl_cmn_orch_retry` |
| 신규 스크립트 | retry 3종(Chk / Start / End), 초기적재 확인·갱신 공통 스크립트 |
| META TABLE 자식 | 보관 정리(DELETE) |
| 자식 파이프라인 | `{ts}` 갱신 (사용자 로직) |

### 15.3 기존 문서

- `ADF_개발가이드_pl_cmn_orch_trigger-entry.md`는 v8.2 기준이라 v3.1 확정 후 재작성 대상입니다.
- 논리·물리 모델 문서는 DDL 반영 후 갱신합니다.

---

## 16. 미결·확인 항목

| 구분 | 항목 |
|---|---|
| 추후 결정 | 진입 ForEach 동시성 상한 (DB 부하 고려) |
| 추후 결정 | ADF 검증 항목 (아래) |
| 추후 결정 | `vw_dbx_pending_file` 조회 기간 N일 |
| 추후 결정 | DBX 전달 항목 (§14.4) |
| 미결 | INSERT_ONLY 적재방식의 refresh 후 첫 INCR 예외 |
| 미결 | 시그널 연동 (추후) |
| 확인필요 | 초기적재 확인·갱신 스크립트의 성공 판정 조건과 컬럼명 (`init_yn` / `init_wait_yn`) |
| 확인필요 | `cmn_master_start`의 현재 검증 조건과 재호출 시 동작 |
| 확인필요 | 디스패처의 자식 호출 방식 (Switch 여부) |
| 확인필요 | 메타 테이블 DEFAULT 날짜의 KST 정의 |
| 확인필요 | 재수행 `prev_error` 기록 (200자 제안) |

**ADF 검증 항목** (실제 ADF에서 확인, 모두 [확인필요])

- ForEach 안에 Switch 중첩 가능 여부
- Trigger의 Array/Object 파라미터 전달
- `@pipeline().TriggeredByPipelineRunId` 사용 가능 여부
- 파이프라인당 활동 수 제한
- `pv_*` 변수를 참조하는 식 문법

---

## 17. 부록: 이번 대화에서 확정된 결정 요약

| # | 결정 |
|---|---|
| 1 | 그룹 1개 = 실행유형 1개 |
| 2 | 스케줄 PARTIAL_REFRESH 불허, 정기 FULL_REFRESH 허용 (BIZDAY 파라미터 필요) |
| 3 | 마스터는 진입 + 유형별 8개, 분·시간 INCR은 진입 생략 |
| 4 | 월 마스터 시작값은 N개월 전 월 1일 |
| 5 | Refresh 후 첫 INCR은 더 오래된 값 사용 |
| 6 | META는 `pl_meta_orch_full` 하나가 TABLE·GEN2 처리, 자식은 `sink_type_cd`로 Start 스크립트가 결정 |
| 7 | META TABLE은 7일 보관 정리 (KST), GEN2는 정리 없음, 중복 허용 |
| 8 | adhoc은 GEN2 전용, DBX Bronze 적재는 수집으로 인정 |
| 9 | DBX 호출: 정기는 마지막 파이프라인에서, 비스케줄은 즉시, 분·시간 INCR은 없음 |
| 10 | 재수행: 행 재사용, REQUEST 제외, INIT_LOAD 후처리는 공통 스크립트로 공유 (A안) |
| 11 | INIT_LOAD·REQUEST 진입에서 `p_bizday_*`는 무시 |
