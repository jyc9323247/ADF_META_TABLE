# ADF 메타 테이블 · 파이프라인 설계 

> 메타데이터 기반 ADF 수집 파이프라인의 **전체 구조와 설계 방향**을 공유·확정하기 위한 최상단 문서.
> 컬럼·제약 상세는 논리/물리 모델 정의서에서 다룬다.

| 항목 | 내용 |
|---|---|
| 오케스트레이션 | Azure Data Factory |
| 메타 DB | Azure Database for PostgreSQL — `META_ADF` |
| 기준 모델 | 논리모델 정의서|
| 소스 | DB2 / MSSQL / COSMOS 등 (온프레미스는 SHIR HA 2노드 경유) |
| 적재 | 랜딩존(ADLS Gen2) → Databricks Bronze |
| 버전 | v1.0 (draft) / 2026-07-28 |

### 문서 체계

| 문서 | 범위 |
|---|---|
| **본 문서** | 전체 구조, 메타 테이블, 파이프라인 구성, 수행 모드, 제어 장치, 모니터링 범위 |
| 초기 / FULL 수집 설계 상세 | 대상 선별, `exec_group`·`partition_clause` 분할, `init_wait_yn` 라이프사이클 |
| 증분(INCR) 수집 설계 상세 | 조건 튜플, 하한 표현식, 만회 모드, 백필, BIZDAY / CLOSE |
| 논리 · 물리 모델 정의서 | 엔티티 · 관계 · 도메인 · 속성 상세 |

- [ADF META 논리모델 정의서](https://github.com/jyc9323247/ADF_META_TABLE/blob/main/ADF_META_%EB%85%BC%EB%A6%AC%EB%AA%A8%EB%8D%B8%EC%A0%95%EC%9D%98%EC%84%9C.md) · [물리모델 정의서](https://github.com/jyc9323247/ADF_META_TABLE/blob/main/ADF_META_%EB%AC%BC%EB%A6%AC%EB%AA%A8%EB%8D%B8%EC%A0%95%EC%9D%98%EC%84%9C.md)

> **표기 원칙** — 메타 코드성 값은 전부 **대문자** 저장(`META`/`RAW`, `FULL`/`INCR`, `WINDOW`/`BIZDAY`/`CLOSE`, `DAILY`/`WEEKLY` 등). 파이프라인 **명명**은 소문자 규칙을 따르므로 둘을 구분한다.

---

## 1. 설계 방향

수집 대상 정의를 메타 테이블에서 관리하고, 파이프라인은 그 정의를 읽어 실행만 한다. **대상 테이블이 늘거나 주기가 바뀌어도 ADF 개발·배포 없이 메타 행 추가·수정으로 대응한다.**

| 원칙 | 내용 |
|---|---|
| **대상 스냅샷** | 마스터 시작 시점에 대상 정의를 수행이력으로 복사한다. 정의가 바뀌어도 그 회차에 무엇을 어떤 쿼리로 수집했는지가 보존된다. |
| **값 → 컬럼 → 쿼리** | 증분 하한은 **ADF 날짜 함수 표현식으로 값을 만들어 메타 컬럼에 입력**한 뒤, **그 컬럼 값으로 추출 쿼리를 조립**한다. 쿼리는 항상 컬럼에서 나오므로 재시도·백필이 같은 경로를 탄다. |
| **원인이 아니라 결과를 본다** | 미수집을 유형별로 처리하지 않고 **"마지막으로 성공한 시각"** 하나로 판정한다. 실패·스킵·트리거 미발화가 같은 경로로 회수된다. |
| **누락보다 중복** | 만회 시 하한을 마지막 성공 지점까지 되돌린다. 겹쳐 읽은 구간은 Bronze의 dedup/upsert가 흡수하므로, 상한·경계 규칙 없이 누락을 구조적으로 막는다. |
| **차일드는 판단하지 않는다** | 무엇을·어떤 쿼리로·어디에 적재할지는 마스터가 확정해 이력에 기록한다. 차일드는 그 쿼리를 실행할 뿐이다. |
| **상태는 이력에서 집계** | 성공/실패 건수를 활동 출력으로 누적하지 않고 이력 테이블을 집계한다. 병렬 실행에서 안전하다. |
| **공통 로직은 util로** | 마스터 회차 기록·차일드 분기·API 호출은 공통 파이프라인으로 묶는다. 오케스트레이터가 몇 개가 되어도 수정은 한 곳에서 끝난다. |

---

## 2. 전체 흐름

마스터(orch) 파이프라인은 수집 유형별로 두 가지 흐름을 가진다. 공통 골격은 동일하며, **증분만 하한 산출 단계가 추가**된다.

### 2-1. 초기 / FULL 수집

```
Trigger  (초기수집 트리거 / 정규 FULL 트리거 / 수동)
  │
  ▼
[If] Skip 판정
  │
  ├─(Y : 건너뜀)─▶ [Execute Pipeline] pl_set_master_pl
  │                    p_action='STATUS'      p_status='SKIPPED'
  │                    p_master_run_id=@{pipeline().RunId}
  │                    p_skip_yn='Y'          p_skip_reason=…
  │                   │
  │                   ▼
  │               [Filter] skip ──▶ 종료
  │
  └─(N : 수행)───▶ [Execute Pipeline] pl_set_master_pl
                       p_action='RUN'         p_status='PENDING'
                       p_master_run_id=@{pipeline().RunId}
                       p_data_class='RAW'     p_ingest_type='FULL'
                       p_exec_group='001'     p_init_yn='Y'/'N'
                       p_skip_yn='N'
                      │
                      ▼
                   [Script] 수집 수행이력 생성  (단일 트랜잭션)
                       ← 수집대상마스터 조회 (기준 입력)
                       ← P_BACKFILL_JSON (NOT NULL이면 참조 · 초기수집은 미사용)
                         ① 대상 선별 — init_wait_yn / ingest_type
                         ② 추출쿼리 조립 (WHERE 1=1)
                         ③ ingest_pipeline_nm 확정
                      │
                      ▼
                   [Filter] run ── 수행 게이트 (Skip·락 통과 시)
                      │
                      ▼
                   [Execute Pipeline] pl_dispatch_ingest
                       p_master_run_id=@{pipeline().RunId}
                      │
                      ▼
                   [Execute Pipeline] pl_set_master_pl
                       p_action='STATUS_UPDATE'
                       p_master_run_id=@{pipeline().RunId}
                      │
                      ▼
                   [Execute Pipeline] pl_call_dbx_api
                       p_master_run_id=@{pipeline().RunId}
                      │
                      ▼
              ADF 책임 종료 ─ ─ ─ ─ ─ ─ ─ ─
                      ▼
        Bronze 적재 → Databricks적재이력 기록 (Databricks 담당)
```

**하한 산출 단계가 없다** — 추출 조건이 `1=1` 로 고정이므로 증분 흐름의 `Set Variable` 단계가 빠진다. 마스터가 넘기는 것은 **대상을 고르는 조건**뿐이다.

**초기수집도 스케줄로 돈다** — 기동 방식(스케줄/수동)과 수집 모드(초기/정규)는 **직교하는 두 축**이다. 파이프라인은 하나를 두고 트리거별 파라미터로 갈린다.

| 트리거 | 주기 | `p_init_yn` | 용도 |
|---|---|---|---|
| 초기수집 트리거 | 이행 기간 매일 야간 | `Y` | 미완 대상 소진. 실패분은 다음 야간 자동 재편입 |
| 정규 FULL 트리거 | 주기 (주 1회 등) | `N` | 전체 재적재 |

이행이 끝나면 초기수집 트리거를 비활성화한다.

### 2-2. 증분(INCR) 수집

```
Trigger  (조건 튜플별 스케줄 / 수동 / 백필)
  │
  ▼
[If] Skip 판정
  │
  ├─(Y : 건너뜀)─▶ [Execute Pipeline] pl_set_master_pl
  │                    p_action='STATUS'      p_status='SKIPPED'
  │                    p_master_run_id=@{pipeline().RunId}
  │                    p_skip_yn='Y'          p_skip_reason=…
  │                   │
  │                   ▼
  │               [Filter] skip ──▶ 종료
  │
  └─(N : 수행)───▶ [Execute Pipeline] pl_set_master_pl
                       p_action='RUN'         p_status='PENDING'
                       p_master_run_id=@{pipeline().RunId}
                       p_data_class='RAW'     p_ingest_type='INCR'
                       p_exec_group=NULL      p_init_yn='N'
                       p_skip_yn='N'
                      │
                      ▼
                   [Set Variable] 증분 하한값 산출
                       v_start_dt_kst / v_start_dt_utc
                       (BIZDAY는 캘린더 Lookup 결과 사용)
                      │
                      ▼
                   [Script] 수집 수행이력 생성  (단일 트랜잭션)
                       ← 수집대상마스터 조회 (기준 입력)
                       ← v_start_dt_kst / v_start_dt_utc
                       ← P_BACKFILL_JSON (NOT NULL이면 참조)
                         ① 백필 파라미터 검증 (NOT NULL인 경우)
                         ② 대상 선별 — 조건 튜플 · init_wait_yn='N'
                                       (백필이면 지정 target_id로 한정)
                         ③ 만회 모드 판정 (last_success_dt)
                         ④ 하한 확정 — start_val > LEAST(표현식, HWM) > 표현식
                         ⑤ 추출쿼리 조립 · ingest_pipeline_nm 확정
                      │
                      ▼
                   [Filter] run ── 수행 게이트 (Skip·락 통과 시)
                      │
                      ▼
                   [Execute Pipeline] pl_dispatch_ingest
                       p_master_run_id=@{pipeline().RunId}
                      │
                      ▼
                   [Execute Pipeline] pl_set_master_pl
                       p_action='STATUS_UPDATE'
                      │
                      ▼
                   [Execute Pipeline] pl_call_dbx_api
                      │
                      ▼
              ADF 책임 종료 ─ ─ ─ ─ ─ ─ ─ ─
```

**If 는 Skip 판정 하나뿐이다** — 증분 오케스트레이터는 항상 `INCR` 이므로 유형 분기가 불필요하다.

### 두 흐름의 차이

| 지점 | 초기 / FULL | 증분 (INCR) |
|---|---|---|
| 하한 산출 단계 | 없음 (`1=1`) | `Set Variable` — KST/UTC 두 값 |
| Script 입력 | 수집대상마스터 | 수집대상마스터 + 하한 변수 + `P_BACKFILL_JSON`(선택) |
| Script 내부 | 대상 선별 + 쿼리 조립 | + 백필 검증 · 만회 판정 · 하한 확정 |
| `P_BACKFILL_JSON` | 미사용 | 지원 (하한 override) |
| `p_exec_group` | `001` / `002` … (용량 분산) | NULL (분산 축이 조건 튜플) |
| `p_init_yn` | `Y`(초기) / `N`(정규) | `N` 고정 |
| 성공 후 갱신 | `init_wait_yn` 리셋 / HWM 초기 세팅 | HWM `GREATEST` 전진 + `last_success_dt` |

### 2-3. 수집 수행이력 생성 — 입력과 우선순위

두 흐름이 공유하는 핵심 단계다. **기준 입력은 언제나 수집대상마스터**이고, 백필 파라미터는 그 위에 얹히는 선택 입력이다.

| 입력 | 출처 | 역할 | 필수 |
|---|---|---|---|
| **`ctl_ingest_target_master`** | 메타 DB 조회 | **기준** — 무엇을, 어디서, 어떤 조건으로 수집할지 | **항상** |
| 오케스트레이터 변수 | `v_data_class`·`v_ingest_type`·조건 튜플 | 대상 선별 범위 | 항상 |
| `v_start_dt_kst` / `v_start_dt_utc` | Set Variable (2단계) | 증분 하한 표현식 값 | INCR |
| **`P_BACKFILL_JSON`** | 마스터 파이프라인 파라미터 | **NOT NULL이면 참조** — 대상 한정 + 하한 override | 선택 |

**분기 규칙**

```
P_BACKFILL_JSON 이 NULL / 빈 배열
  → 대상 : 마스터에서 조건 튜플 · init_wait_yn 으로 선별
  → 하한 : 표현식 / 만회 규칙 (INCR)  ·  1=1 (FULL)

P_BACKFILL_JSON 이 NOT NULL
  → ① 파라미터 검증 (6항목, 위반 시 전건 중단 — 8장)
  → ② 대상 : 마스터를 조회하되 지정 target_id 로 한정
             (조건 튜플·주기 조건 무시)
  → ③ 하한 : JSON 의 start_val 우선, 없으면 정규 규칙
```

**백필이어도 마스터 조회는 생략하지 않는다.** 파라미터는 `target_id` 와 `start_val` 만 담고, 소스 정보·증분 컬럼·랜딩 경로 등 이력에 남길 나머지 값은 전부 마스터에서 가져온다. 파라미터는 **대상을 좁히고 하한을 덮어쓰는 역할**만 한다.

```sql
INSERT INTO ctl_ingest_pipeline_run (...)
SELECT ...,
       COALESCE(bf.start_val, <만회/표현식 규칙>) AS incr_column_hw_val,
       bf.start_val                              AS incr_start_val,
       ...
FROM   ctl_ingest_target_master t
LEFT JOIN LATERAL (
         SELECT (b->>'target_id')::int8 AS target_id,
                 b->>'start_val'        AS start_val
         FROM   jsonb_array_elements(:p_backfill_json::jsonb) b
       ) bf ON bf.target_id = t.target_id
WHERE  t.delete_yn = 'N'
AND    t.data_class = :p_data_class
AND    ( :p_backfill_json IS NULL              -- 정규 : 조건 튜플로 선별
         OR bf.target_id IS NOT NULL )         -- 백필 : 지정 대상만
AND    ( :p_backfill_json IS NOT NULL          -- 백필은 조건 튜플 무시
         OR ( <조건 튜플 · init_wait_yn 조건> ) )
AND    NOT EXISTS (                            -- 진행 중 대상 사전 배제
         SELECT 1 FROM ctl_ingest_pipeline_run r
         WHERE  r.target_id = t.target_id
         AND    r.status IN ('PENDING','RUNNING'))
;
```

> **`incr_start_val` 은 이력에 그대로 남긴다** — 확정 하한(`incr_column_hw_val`)과 별개로 "이 회차는 파라미터로 override됐다"는 사실이 기록되어, 사후에 정규 회차와 백필 회차를 구분할 수 있다.
>
> **초기수집은 백필 파라미터를 쓰지 않는다** — 추출 조건이 `1=1` 이라 하한 override가 의미 없다. 초기수집 대상 지정이 필요하면 별도 파라미터를 두어야 한다.
>
> **진행 중 대상 사전 배제** — 다른 회차가 잡고 있는 대상은 애초에 이력을 만들지 않는다. 유니크 제약(`ux_ipr_running`) 위반으로 실패하는 대신 **다음 회차로 이월**되고, 기존 복구 장치(재편입·만회 모드)가 받아준다. 제약은 경합 방어용 최후 수단으로 남는다.

### 공통 규칙

**모든 트리거 실행은 두 곳에 남는다** — Skip 판단은 ADF에서 수행하고, 결과와 무관하게 `ctl_trigger_history` 와 `ctl_master_pipeline_run` **양쪽에 기록**한다. 스킵 회차도 `SKIPPED` 로 남아 "안 돌았다"와 "기록이 없다"가 구분된다. `Filter` 가 "건너뛰지 않은 경우에만 후속 수행"하는 게이트 역할을 한다.

**ADF는 Bronze 적재 완료를 기다리지 않는다** — API 호출 성공까지가 ADF 책임이다. Bronze 결과는 Databricks가 `ctl_dbx_ingest_history` 에 기록하고, End-to-End 완결 확인은 모니터링(10장)에서 호출이력 × 적재이력 대조로 판단한다. 폴링(Until)을 두지 않아 마스터가 Databricks 처리 시간만큼 점유되지 않고 책임 경계도 명확하다.

### 차일드(ingest) — 지시받은 쿼리를 실행한다

Lookup(이력에서 자기 대상 조회) → ForEach → Copy Data → 이력 UPDATE

| 결과 | 수집수행이력 | 수집대상마스터 |
|---|---|---|
| 성공 | `status='SUCCEEDED'`, 추출/저장 건수, 파일명·용량, 종료일시 | `last_success_dt` 갱신 + 유형별 컬럼 (6장) |
| 실패 | `status='FAILED'`, `error_log`, `attempt_no` | 변경 없음 → 다음 회차 만회 모드로 자동 회수 |

> 차일드는 **소스 DB 종류만큼만 존재**하며, 수집 대상 테이블이 늘어도 개수가 늘지 않는다.
> 차일드가 실패해도 마스터는 실패로 종료되지 않는다. 실패는 이력에 기록되고, 상태 집계와 Databricks 알림은 반드시 수행된다.

---

## 3. 메타 테이블

| 엔티티 | 물리명 | 관리 주체 | 역할 |
|---|---|---|---|
| 수집대상마스터 | `ctl_ingest_target_master` | 운영자 (수작업) | **무엇을** 수집할 것인가 — 유일한 수작업 관리 지점 |
| 마스터파이프라인수행상태 | `ctl_master_pipeline_run` | ADF | **이번 배치가** 어떻게 끝났는가. 스킵 회차도 `SKIPPED` 로 기록. 동시수행 락 겸용 |
| 수집파이프라인수행이력 | `ctl_ingest_pipeline_run` | ADF | **대상 1건이** 어떻게 처리됐는가. 수행시점 스냅샷 보존 |
| 트리거수행이력 | `ctl_trigger_history` | ADF | **모든** 트리거 실행 로그. Skip 판정 결과(`skip_yn`·`skip_reason`) 포함 |
| Databricks API호출이력 | `ctl_dbx_api_call_history` | ADF | REST API **호출**의 성공/실패. ADF 책임 경계의 종점 |
| Databricks적재이력 | `ctl_dbx_ingest_history` | Databricks | Bronze **적재** 완료 이력 |

```
수집대상마스터 ──target_id──▶ 수집파이프라인수행이력 ──┐
                                                    ├──▶ Databricks적재이력
마스터파이프라인수행상태 ──master_run_id──────────────┘
        ├── (논리) ──▶ 트리거수행이력
        └── master_run_id ──▶ Databricks API호출이력
```

> **호출 이력과 적재 이력은 다른 것을 증명한다** — `ctl_dbx_api_call_history` 는 "ADF가 제대로 요청했는가"(ADF 책임), `ctl_dbx_ingest_history` 는 "Bronze에 실제로 실렸는가"(Databricks 책임). 장애 시 어느 경계에서 끊겼는지가 이력만으로 판별된다.
>
> **수작업 지점은 수집대상마스터 하나** — 나머지 다섯은 ADF·Databricks가 쓰는 이력이다. 단, 수집대상마스터의 **대상별 진행 상태**(`incr_column_hw_val`, `last_success_dt`, `init_wait_yn`)는 초기·FULL·INCR 전 유형에서 ADF가 갱신한다(6장). 정의와 상태를 한 테이블에 둔 것은 Lookup을 조인 없이 끝내기 위한 의도적 비정규화다.

---

## 4. 파이프라인 구성 · 명명 규칙

| 계층 | 역할 | 명명 예 |
|---|---|---|
| **orch** (마스터) | 대상 선별 · 이력 생성 · 디스패처 호출 · 상태 기록 | `pl_raw_orch_incr_window-based-daily1` |
| **ingest** (차일드) | 소스 DB별 추출 · 랜딩 | `pl_raw_ingest_db2_MLCRP` |
| **util** (공용) | 마스터 회차 관리 · 차일드 분기 · 외부 API 호출 | `pl_set_master_pl` / `pl_dispatch_ingest` / `pl_call_dbx_api` |

```
orch  : pl_{DATA_CLASS}_orch_{LOAD_TYPE}[_{CONDITION}][_{SEQ}]
ingest: pl_{DATA_CLASS}_ingest_{CONN_TYPE}_{DATABASE}
util  : pl_{동사}_{대상}
```

### 오케스트레이터 = 조건 튜플

파이프라인 **이름이 곧 조건 튜플의 직렬화**다. 각 마스터는 자기 튜플에 해당하는 대상만 골라 수행하며, 라우팅은 인덱스 `ix_target_route` 가 받친다.

| 오케스트레이터 | INGEST_TYPE | CONDITION_TYPE | FREQUENCY | INTERVAL | EXEC_GROUP |
|---|---|---|---|---|---|
| pl_raw_orch_full_01 / _02 | FULL | – | – | – | 001 / 002 |
| pl_raw_orch_incr_window-based-daily1 | INCR | WINDOW | DAILY | 1 | – |
| pl_raw_orch_incr_window-based-daily3 | INCR | WINDOW | DAILY | 3 | – |
| pl_raw_orch_incr_window-based-weekly1 | INCR | WINDOW | WEEKLY | 1 | – |
| pl_raw_orch_incr_bizday-based | INCR | BIZDAY | – | – | – |
| pl_raw_orch_incr_close-based | INCR | CLOSE | – | – | – |

> **확장성** — WINDOW 계열 전 변형이 **동일 스크립트 1벌**을 쓰고 파이프라인 변수만 다르다. `FREQUENCY` 는 MINUTELY~YEARLY 6단계를 지원하므로 신규 주기 조합이 생겨도 SQL 수정이 없다.

### 4-1. `pl_set_master_pl` — 마스터 회차 관리 (공통)

마스터 수행상태를 건드리는 처리를 **하나의 파이프라인**으로 묶고, 행위를 `p_action` 으로 지정한다.

| `p_action` | 처리 | 호출 위치 |
|---|---|---|
| `RUN` | **락 확인 후** 마스터 회차 INSERT (`status='PENDING'`) + 트리거수행이력 INSERT. 락 충돌 시 `SKIPPED` 로 기록하고 `proceed_yn='N'` 반환 | 1단계 — 수행 분기 |
| `STATUS` | 마스터 회차 INSERT — `p_status` 값을 그대로 기록. 종료 상태면 `end_dt` 세팅 + 트리거수행이력 INSERT | 1단계 — 스킵 분기 |
| `STATUS_UPDATE` | 수집 수행이력을 **집계**해 `child_succ/fail_cnt`·`status`·`end_dt` UPDATE | 상태 집계 단계 |

**`STATUS` 와 `STATUS_UPDATE` 의 차이** — 전자는 받은 값을 그대로 기록하고, 후자는 이력에서 산출한다. `STATUS` 는 스킵 외의 종료 상태(백필 검증 실패 등)에도 재사용된다.

**파라미터**

| 파라미터 | `RUN` | `STATUS` | `STATUS_UPDATE` | 값 |
|---|:---:|:---:|:---:|---|
| `p_action` | O | O | O | `RUN` / `STATUS` / `STATUS_UPDATE` |
| **`p_master_run_id`** | **O** | **O** | **O** | `@pipeline().RunId` — **반드시 마스터에서 전달** |
| `p_master_pipeline_nm` | O | O | – | `@pipeline().Pipeline` |
| `p_trigger_id` / `p_trigger_nm` | O | O | – | `@pipeline().TriggerId` / `TriggerName` |
| `p_run_mode` | O | O | – | `SCHEDULE`/`MANUAL`/`BACKFILL`/`TEST` |
| `p_data_class` | O | – | – | `RAW` / `META` |
| `p_ingest_type` | O | – | – | `FULL` / `INCR` |
| `p_exec_group` | O | – | – | `001` / `002` … (INCR은 NULL) |
| `p_init_yn` | O | O | – | `Y`(초기수집 회차) / `N`(정규) |
| `p_status` | – | O | – | `SKIPPED` 등 |
| `p_skip_yn` / `p_skip_reason` | O | O | – | 트리거수행이력용 |

> ⚠ **시스템 변수는 반드시 파라미터로 넘긴다** — 공통 파이프라인 안에서 `@pipeline().RunId` 를 읽으면 **자기 자신의 RunId** 가 반환된다. 그대로 INSERT하면 `master_run_id` 가 어긋나 차일드와의 **이력 조인이 통째로 깨진다.**
>
> **1단계 공통화의 이유** — Skip 분기와 수행 분기는 `status`·`skip_yn` 값만 다른 동일 로직이다. Script를 양쪽에 복사하면 컬럼 하나 추가할 때 두 곳을 고쳐야 하고, 한쪽만 고쳐지는 사고가 난다.
>
> **수집이력 생성(Script)은 공통화하지 않는다** — 조건 튜플·수행 모드에 따라 선별·하한 로직이 갈려 파라미터가 과도해지고 분기가 숨는다. 오케스트레이터의 Script로 두고 차이는 변수값으로 흡수한다.

### 4-2. `pl_dispatch_ingest` — 차일드 분기 (공통)

마스터는 소스 DB 목록을 모른다. **디스패처가 이력에서 조회해 차일드를 분기**한다.

```
파라미터: p_master_run_id
  │
  ▼
[Lookup] SELECT DISTINCT ingest_pipeline_nm, src_type, src_db_nm
         FROM   ctl_ingest_pipeline_run
         WHERE  master_run_id = '@{pipeline().parameters.p_master_run_id}'
         AND    status = 'PENDING'
  │
  ▼
[ForEach] (병렬, batchCount = N)
    └ [Switch] @item().ingest_pipeline_nm
         ├ case 'pl_raw_ingest_db2_MLCRP' → [Execute Pipeline]
         ├ case 'pl_raw_ingest_db2_MLMTP' → [Execute Pipeline]
         └ default → [Script] 해당 건 이력 UPDATE
                     status='FAILED', error_log='미등록 파이프라인: '||…
```

- **차일드 파이프라인명은 데이터다** — 수집이력 생성 Script가 `ingest_pipeline_nm` 을 확정해 이력에 기록한다. 평시엔 명명규칙으로 조합하고, 수집대상마스터의 override 컬럼(NULL 허용)에 값이 있으면 그 값을 쓴다: `COALESCE(t.ingest_pipeline_nm, 'pl_'||lower(data_class)||'_ingest_'||lower(src_type)||'_'||src_db_nm)`
- **`default` 분기가 방어선이다** — Switch에 없는 이름(메타 오타, 신규 DB의 case 미추가)은 조용히 넘어가지 않고 `FAILED` 로 기록된다. 실패분은 HWM이 전진하지 않으므로 case 추가 후 **다음 회차 만회 모드로 자동 회수**된다.
- **병렬도 제어** — ForEach `batchCount` 로 동시 실행 수를 조절한다(9장 SHIR 실측 후 조정). 대상 0건인 DB는 Lookup 결과에 없어 호출 자체가 일어나지 않는다.
- **점진 전환 경로** — 파라미터화 Linked Service 검증이 끝나면, override 컬럼을 공통 차일드(`pl_raw_ingest_common`)로 바꾸는 것만으로 대상별 전환·원복이 가능하다. ADF 배포 없이 메타 UPDATE로 진행한다. ⚠ *SHIR 경유 DB2에서 파라미터화 Linked Service 동작 검증 필요*

### 4-3. `pl_call_dbx_api` — Databricks API 호출 (공통)

REST API 호출은 Web Activity를 직접 두지 않고 공통 파이프라인으로 감싼다.

| 파라미터 | 내용 |
|---|---|
| `p_master_run_id` | 호출 맥락 (FK) — 마스터에서 전달 |
| `p_api_name` | 논리 호출 구분 — `JOBS_RUN_NOW` 등 대문자 |
| `p_http_method` | `GET` / `POST` / `PUT` / `PATCH` / `DELETE` |
| `p_api_endpoint` | 예) `/api/2.1/jobs/run-now` |
| `p_request_body` | 요청 페이로드 |

**처리 순서** — 호출 이력 INSERT(시작) → Web Activity 실행 → 이력 UPDATE(HTTP 상태·응답·`dbx_job_id`·`dbx_run_id`·성공 여부)

> **공통화하는 이유** — 호출 지점이 늘어도 이력 기록·재시도·인증 처리가 복제되지 않는다. 토큰·베이스 URL 관리도 한 곳에 모인다.
>
> ⚠ **민감정보 마스킹** — `request_body`·`response_body` 에 토큰·연결정보가 남지 않도록 저장 전 마스킹하고, Web Activity의 `secureOutput`·`secureInput` 을 활성화한다.

---

## 5. 수집 유형

공통 골격은 동일하고, 아래 지점만 갈린다.

| 구분 | FULL (초기 / 전체) | INCR (증분) |
|---|---|---|
| 대상 선별 | `init_wait_yn='Y'` (초기) / `ingest_type='FULL'` AND `init_wait_yn='N'` (정규) | `ingest_type='INCR'` + 조건 튜플 일치 + `init_wait_yn='N'` |
| 추출 조건 | `1=1` — 대용량은 `partition_clause` 로 분할 | `{증분컬럼} >= {확정된 하한값}` |
| 분산 축 | `exec_group` (용량 기준) | `frequency` / `interval` (주기 기준) |
| 성공 후 처리 | `init_wait_yn` → `'N'` 리셋 | `incr_column_hw_val`·`last_success_dt` 갱신 |

**메타 제약이 유형별 정합성을 강제한다** — FULL은 증분·조건 컬럼이 전부 NULL(`ck_full_no_cond`), INCR은 `condition_type` 필수(`ck_incr_needs_type`), WINDOW는 주기·간격·기준컬럼 필수(`ck_window_params`). 잘못된 조합이 애초에 등록되지 않는다.

> **선후 관계** — 초기수집이 끝나지 않은(`init_wait_yn='Y'`) 대상은 정규 수집에서 자동 제외된다. 초기 적재 없이 증분만 도는 상황이 구조적으로 발생하지 않는다.

### 기동 방식 × 수집 모드 — 직교하는 두 축

```
                    수집 모드 (init_yn)
                    ┌─────────────┬──────────────┐
                    │  Y (초기)    │  N (정규)     │
  ┌─────────────────┼─────────────┼──────────────┤
기│ SCHEDULE        │ 이행 기간    │ 주기 수집     │
동│                 │ 야간 배치    │              │
방├─────────────────┼─────────────┼──────────────┤
식│ MANUAL          │ 긴급 재수집  │ 임시 재수행   │
  └─────────────────┴─────────────┴──────────────┘
```

`run_mode`(기동 방식)로는 초기수집 여부를 알 수 없다. **두 값을 각각 기록**해야 이력에서 "무엇을, 어떻게 돌렸는가"가 복원된다. 마스터 수행상태에 `init_yn` 컬럼을 두는 이유다.

### 성공 시 대상 마스터 갱신

**모든 유형이 `last_success_dt` 를 기록**하고, 나머지는 유형별이다.

| 수집 유형 | `init_wait_yn` | `last_success_dt` | `incr_column_hw_val` |
|---|---|---|---|
| **초기수집** — INCR 대상 | `'Y'` → **`'N'`** | 갱신 | **실행 기준일로 초기 세팅** |
| **초기수집** — FULL 대상 | `'Y'` → **`'N'`** | 갱신 | NULL 유지 |
| **정규 FULL** | – | 갱신 | NULL 유지 |
| **정규 INCR** | – | 갱신 | `GREATEST` 갱신 |
| **실패 (전 유형)** | 변경 없음 | 변경 없음 | 변경 없음 |

- **`init_wait_yn` 리셋** — 초기수집 성공 시 차일드의 이력 UPDATE와 동일 스크립트에서 대상별로 전환한다. 성공한 건만 정확히 전환되어 부분 성공에 강하고, 실패 건은 다음 초기수집 회차에 자동 재편입된다.
- **HWM 초기 세팅** — 초기수집은 `1=1` 로 읽어 "사용한 하한"이 없으므로 **실행 기준일**을 넣는다. 실행 시점까지 전부 읽었으므로 안전한 재시작점이다. 이후 정규 증분 시작이 늦어져도 첫 회차가 만회 모드로 공백을 메운다.
- **FULL 대상의 `last_success_dt`** — 하한 계산에는 안 쓰이지만 모니터링("며칠째 성공 없는 대상")이 수집 유형과 무관하게 동작하도록 기록한다.

---

## 6. 증분 하한 — 표현식 + HWM 하이브리드

증분 하한은 **ADF 표현식으로 만든 날짜 값**과 **수집대상마스터에 저장된 HWM**을 함께 쓴다. 표현식은 주기 규칙을, HWM은 진행 상태를 담당한다.

```
① ADF 표현식으로 값 생성      ② 하한 확정              ③ 쿼리 조립        ④ 성공 시 상태 갱신
────────────────────────    ───────────────────    ──────────────    ─────────────────
날짜 함수 평가            ▶  만회 모드 판정 후    ▶  extract_query  ▶  incr_column_hw_val
  → v_start_dt               하한 결정                                 ← GREATEST 갱신
                             → incr_column_hw_val                       last_success_dt
                               (백필은 incr_start_val)                  ← 완료 시각
```

| 단계 | 담당 | 처리 |
|---|---|---|
| ① **값 생성** | ADF Set Variable | 날짜 함수 표현식을 평가해 문자열 값 생성 → `v_start_dt_kst` / `v_start_dt_utc` |
| ② **하한 확정** | Script (메타 DB) | 만회 모드를 판정해 하한을 정하고 이력 컬럼에 입력. **타임존 변수도 여기서 `src_type` 별 선택** |
| ③ **쿼리 조립** | Script (메타 DB) | ②의 컬럼 값과 `incr_column_type` 으로 소스 문법 WHERE 절 생성 |
| ④ **상태 갱신** | Script (차일드) | 정상 완료 시 `incr_column_hw_val`·`last_success_dt` 갱신 |

**핵심은 ③이 ①이 아니라 ②를 읽는다는 점이다.** 쿼리 생성 경로가 컬럼 하나로 단일화되므로, 값이 어디서 왔든(표현식 / HWM / 백필 파라미터) 이후 처리가 동일하다.

### 두 개의 상태 컬럼

| 컬럼 | 의미 | 용도 |
|---|---|---|
| `incr_column_hw_val` | **마지막 성공 회차가 사용한 하한** | 만회 시 되돌아갈 안전 지점 |
| `last_success_dt` | **마지막 성공 완료 타임스탬프** | 만회 모드 판정 + 운영 모니터링 |

> `incr_column_hw_val` 은 수집수행이력의 동명 컬럼("이번 수행에 사용된 HWM")과 같은 의미가 되어 두 테이블이 정합한다.

### ② 하한 확정 규칙

**1단계 — 만회 모드 판정**

```
만회 모드 = ( last_success_dt 의 날짜 < 직전 예정 회차 기준일 )
```

마지막 **성공**이 직전 예정 회차보다 이전이면 만회 모드다. **원인을 묻지 않고 결과만 본다.**

| 미수집 원인 | 이력에 `FAILED` 행 | `last_success_dt` 판정 |
|---|---|---|
| 차일드 Copy 실패 | O | 잡음 |
| 트리거 스킵 / 락 스킵 | ✗ | **잡음** |
| 트리거 미발화 (오설정) | ✗ | **잡음** |
| 마스터가 이력 생성 전 중단 | ✗ | **잡음** |
| 조건 튜플 오등록으로 대상 누락 | ✗ | **잡음** |

"실패했다"는 여러 형태지만 "성공했다"는 한 가지 형태다. 성공 시각만 기록하면 나머지가 전부 포착된다.

기준일 산출은 `condition_type` 별로 다르다 (`condition_frequency` 는 WINDOW 전용이므로).

| CONDITION_TYPE | 직전 예정 회차 기준일 |
|---|---|
| **WINDOW** | 실행일 − 1 × `condition_frequency` 단위 (DAILY→1일, WEEKLY→7일 …) |
| **BIZDAY** | **직전 영업일** — 하한 산출(①)에서 조회한 캘린더 값 재사용 |
| **CLOSE** | 실행일 − 1개월 ⚠ *메타 수용성 협의 결과에 종속* |

**2단계 — 하한 결정**

```
백필      : 하한 = incr_start_val
만회 모드 : 하한 = LEAST(표현식 값, incr_column_hw_val)
정상      : 하한 = 표현식 값
```

| 상황 | HWM | 표현식 | 만회 | 확정 하한 | 동작 |
|---|---|---|---|---|---|
| 신규 대상 | NULL | 2026-07-27 | – | 2026-07-27 | HWM 초기 입력 |
| 정상 회차 | 2026-07-24 | 2026-07-27 | N | 2026-07-27 | 표현식 → 정상 전진 |
| 직전 회차 실패·스킵 | 2026-07-24 | 2026-07-27 | **Y** | **2026-07-24** | HWM → **누락 만회** |
| 백필 | (무관) | (무관) | – | `incr_start_val` | 파라미터 우선 |

> **왜 만회 시 더 이른 값인가** — HWM은 마지막 성공 회차의 하한이고, 그 이후 놓친 회차들의 하한은 모두 그보다 늦다. HWM부터 읽으면 **놓친 구간 전체가 항상 포함**된다. 누락과 중복 중 중복을 택하는 안전 우선 원칙이며, 중복은 Bronze dedup/upsert가 흡수한다.

### ④ 상태 갱신 규칙

```
incr_column_hw_val ← GREATEST(이번 회차 하한, 기존 HWM)   -- 후퇴하지 않는다
last_success_dt    ← 완료 시각
```

| 회차 종류 | 이번 하한 | GREATEST 결과 | 효과 |
|---|---|---|---|
| 정상 | 표현식 (최신) | 표현식 | **전진** |
| 만회 | HWM | HWM | 유지 — 다음 정상 회차에서 전진 |
| 백필 | 과거 값 | 기존 HWM | **후퇴 차단** |
| 실패 | – | 갱신 안 함 | 다음 회차 만회 모드로 자동 회수 |

> **`GREATEST` 를 일반 규칙으로** — "HWM은 후퇴하지 않는다" 한 줄로 백필이 특수 케이스가 아니게 된다. PostgreSQL `GREATEST` 는 NULL을 무시하므로 신규 대상도 별도 처리가 없다.

### 비교 시 타입 캐스팅

`incr_column_hw_val` 이 `varchar` 라 `LEAST`·`GREATEST` 가 사전순 비교를 한다. DATE·TIMESTAMP·고정폭 STRING은 안전하지만 **NUMERIC은 `'9' > '10'` 이 되어 깨진다.**

```sql
CASE WHEN incr_column_type = 'NUMERIC'
     THEN LEAST(v_start_dt::numeric, incr_column_hw_val::numeric)::text
     ELSE LEAST(v_start_dt,          incr_column_hw_val)
END
```

> 하한 결정(`LEAST`)과 상태 갱신(`GREATEST`) **양쪽에 동일 적용**한다. 한쪽만 캐스팅하면 하한과 HWM이 어긋난다.

### ① 조건 유형별 ADF 표현식

`utcNow()` 는 UTC를 반환하므로 `convertFromUtc` 로 KST 변환이 필수다. 포맷 문자열은 소스 컬럼 포맷과 일치해야 한다.

| CONDITION_TYPE / 주기 | ADF 표현식 | 생성 값 예 |
|---|---|---|
| WINDOW / DAILY 1 | `@addDays(convertFromUtc(utcNow(),'Korea Standard Time'), -1, 'yyyy-MM-dd')` | `2026-07-27` |
| WINDOW / DAILY 3 | `@addDays(convertFromUtc(utcNow(),'Korea Standard Time'), -3, 'yyyy-MM-dd')` | `2026-07-25` |
| WINDOW / WEEKLY 1 | `@addDays(convertFromUtc(utcNow(),'Korea Standard Time'), -7, 'yyyy-MM-dd')` | `2026-07-21` |
| WINDOW / TIMESTAMP 정밀도 | `@addHours(convertFromUtc(utcNow(),'Korea Standard Time'), -24, 'yyyy-MM-dd HH:mm:ss')` | `2026-07-27 09:00:00` |
| CLOSE (마감년월) | `@addToTime(convertFromUtc(utcNow(),'Korea Standard Time'), -1, 'Month', 'yyyyMM')` ⚠ | `202606` |
| BIZDAY | ADF 함수로 산출 불가 — **고객사 DB의 영업일 캘린더 테이블 Lookup** | `2026-07-27` |

> ⚠ **CLOSE는 메타 수용성 확인 필요** — 위 표현식은 "전월"을 기계적으로 산출할 뿐이다. 실제 마감년월이 업무 마감 완료 여부에 좌우된다면 날짜 함수로 표현되지 않는다.

**interval 파라미터화** — `P_INTERVAL` 을 넘기면 표현식 하나로 DAILY 전 변형을 커버한다. "동일 스크립트 1벌"이 성립하는 지점이다.

```
@addDays(convertFromUtc(utcNow(), 'Korea Standard Time'),
         mul(int(pipeline().parameters.P_INTERVAL), -1), 'yyyy-MM-dd')
```

### 소스 타임존 — KST / UTC 두 값을 만든다

**COSMOS는 UTC로 저장**하므로 KST 하한을 그대로 주면 9시간 어긋난다.

| src_type | 저장 기준 | 사용할 하한 |
|---|---|---|
| DB2 / MSSQL | KST (로컬 시각) ⚠ *실값 확인 필요* | `v_start_dt_kst` |
| **COSMOS** | **UTC (확정)** | `v_start_dt_utc` |

```
v_start_dt_kst = @addDays(convertFromUtc(utcNow(),'Korea Standard Time'), -N, 'yyyy-MM-dd')
v_start_dt_utc = @addDays(utcNow(), -N, 'yyyy-MM-dd')
```

**변수 선택은 ②(하한 확정)에서 이뤄진다** — `LEAST` 비교가 ②에서 일어나므로 그 시점에 대상의 `src_type` 에 맞는 변수를 골라야 한다. **한 대상의 하한·HWM·저장값은 항상 같은 타임존 기준**이어야 하며, ②③④가 같은 변수를 쓰는지 구현 시 확인한다.

### 포맷 문자열 · 소스 문법 조립

| incr_column_type | 포맷 | DB2 | MSSQL | COSMOS |
|---|---|---|---|---|
| DATE | `'yyyy-MM-dd'` | `DATE('…')` | `CAST('…' AS DATE)` | `'…'` |
| TIMESTAMP | `'yyyy-MM-dd HH:mm:ss'` | `TIMESTAMP('…')` | `CAST('…' AS DATETIME2)` | `'…'` |
| STRING | 소스 저장 포맷 그대로 | `'…'` | `'…'` | `'…'` |
| NUMERIC | (날짜 함수 대신 HWM) | `12345` 인용 없음 | `12345` | `12345` |

> ⚠ **STRING 주의** — 고정폭 포맷이어야 정렬이 맞는다. 소스가 `yyyyMMdd` 면 표현식 포맷도 `'yyyyMMdd'`. 불일치 시 조건이 조용히 어긋난다.

### 이 구조의 이점

| 이점 | 내용 |
|---|---|
| **원인 무관 자동 복구** | 실패·스킵·트리거 미발화·초기 중단을 구분하지 않고 모두 만회한다 |
| **수동 실행만으로 회수** | 파라미터 없이 수동 기동해도 만회 모드가 자동으로 켜진다. "며칠부터 다시 읽을지" 계산이 불필요하다 |
| **재시도 멱등성** | 하한이 컬럼에 박혀 있어 몇 시간 뒤 재시도해도 같은 구간을 읽는다 |
| **재수행 단순화** | 실패 건 재처리는 이력의 `extract_query` 를 그대로 재실행하면 끝이다 |
| **백필 분기 불필요** | 백필은 ② 컬럼(`incr_start_val`)에 값을 넣을 뿐, ③④는 동일 코드를 탄다 |
| **모니터링 일원화** | `last_success_dt` 하나로 지연 대상 조회가 마스터 테이블 한 번에 끝난다 |
| **사후 추적** | 회차별 하한값이 이력에 남아 누락 조사의 근거가 된다 |

> **상한을 두지 않는다** — 수집 중 유입 행은 다음 회차가 겹쳐 읽고 Bronze가 흡수한다. 상한 컬럼·경계 규칙이 불필요하다.
>
> ⚠ **만회 회차의 조회량** — 중단 기간이 길수록 읽는 범위가 커진다. 장기 중단 후 첫 성공 회차는 부하가 평소의 수 배가 될 수 있어 대용량 대상은 사전 확인이 필요하다.

---

## 7. 수행 모드 · 파라미터

| 모드 | 파라미터 | `run_mode` | 대상 | 하한 |
|---|---|---|---|---|
| **정규 수집** | 없음 (기본) | `SCHEDULE` / `MANUAL` | 조건 튜플 일치 대상 전체 | 표현식 / 만회 시 HWM |
| **초기 수집** | `p_init_yn='Y'` (트리거 지정) | `SCHEDULE` / `MANUAL` | `init_wait_yn='Y'` 대상 | `1=1` — 성공 시 HWM 초기 세팅 |
| **백필** | `P_BACKFILL_JSON` | `BACKFILL` | 파라미터 지정 대상만 | `incr_start_val` |
| **테스트** | `P_TEST_YN='Y'` | `TEST` | 파라미터/조건에 따름 | 정규와 동일 |

### 모드별 상태 갱신

| `run_mode` | 수집수행이력 | `incr_column_hw_val` | `last_success_dt` |
|---|---|---|---|
| `SCHEDULE` / `MANUAL` | 기록 | `GREATEST` 갱신 | 갱신 |
| `BACKFILL` | 기록 | `GREATEST` → 후퇴 차단 | 갱신 |
| **`TEST`** | 기록 | **갱신 안 함** | **갱신 안 함** |

> **테스트 모드** — 동작 확인용 1회 실행이 진행점을 움직이면 이후 정규 회차가 그만큼 덜 읽는다. `TEST` 는 이력에는 남겨 추적하되 진행점은 건드리지 않는다.

### 만회 모드와 백필의 역할 분담

수동 실행 시 대부분의 누락은 **파라미터 없이** 회수된다. 백필은 만회 모드가 닿지 않는 영역만 담당한다.

| 상황 | `last_success_dt` | 만회 모드 | 회수 방법 |
|---|---|---|---|
| 차일드 실패 / 트리거 스킵·미발화 / 초기 중단 | 안 움직임 | ON | **수동 실행만으로 회수** |
| 소스가 과거분 정정 (증분 컬럼 미갱신) | 최신 | OFF | 백필 |
| 지연 도착이 `interval` 겹침 폭 초과 | 최신 | OFF | 백필 |
| 신규 컬럼 소급 적재 | 최신 | OFF | 백필 |
| 부분 적재인데 `SUCCEEDED` 로 기록 | 최신 | OFF | 백필 |

> **만회 모드 = 미수행 복구, 백필 = 기수행 정정.** 두 장치는 겹치지 않는다.

### 백필 파라미터

```json
[
  { "target_id": 101, "start_val": "2026-06-01" },
  { "target_id": 300, "start_val": "2026-06-01" },
  { "target_id": 412 }
]
```

| 필드 | 필수 | 매핑 컬럼 | 의미 |
|---|---|---|---|
| `target_id` | O | `target_id` | 백필 대상 |
| `start_val` | – | `incr_start_val` | 하한 override. 생략 시 정규 규칙 적용 |

**동작 규칙**

| # | 규칙 |
|---|---|
| 0 | **대상 정보는 여전히 수집대상마스터에서 가져온다.** 파라미터는 대상을 좁히고 하한을 덮어쓸 뿐이다 (2-3) |
| 1 | 파라미터가 NOT NULL이면 **지정된 `target_id` 만** 대상. 조건 튜플·주기 무시 |
| 2 | 하한 우선순위: **`start_val` > 만회/표현식**. 확정값은 `incr_column_hw_val`, 원본 파라미터는 `incr_start_val` 에 각각 기록 |
| 3 | **상한 없음.** `start_val` 부터 실행 시점까지 전부 읽는다 |
| 4 | ② 컬럼만 백필 값으로 바뀔 뿐 ③④는 정규와 동일 코드 — 전용 분기 없음 |
| 5 | 성공 시 `GREATEST` 로 HWM 후퇴 차단, `last_success_dt` 갱신 |
| 6 | 별도 파이프라인 없이 **기존 오케스트레이터를 수동 트리거**로 기동 |
| 7 | 이력 생성 전 **파라미터 검증**, 하나라도 실패하면 회차 전체 중단 |

### 백필 파라미터 검증

JSON 수기 작성 오타가 엉뚱한 테이블 재수집으로 이어지지 않도록, 이력 생성 스크립트 초입(단일 트랜잭션 내)에서 검증하고 위반 시 롤백한다.

| # | 검증 | 위반 의미 |
|---|---|---|
| 1 | `target_id` 실존 + `delete_yn='N'` | 없는/삭제된 대상 |
| 2 | `ingest_type='INCR'` | FULL 대상은 하한 개념 없음 |
| 3 | `init_wait_yn='N'` | 초기수집 미완 대상 백필 불가 |
| 4 | `start_val` 이 `incr_column_type` 으로 캐스팅 가능 | 포맷 불일치 |
| 5 | `start_val` ≤ 현재 시각 | **미래 값은 이후 수집을 무기한 중단시킴** |
| 6 | 건수 ≤ 등록 대상 수, `target_id` 중복 없음 | JSON 작성 오류 |

> **전건 중단** — 일부만 유효할 때 진행하면 "무엇이 수집됐는지" 확인이 어렵다. 검증 실패는 `pl_set_master_pl`(`STATUS`)로 회차를 `FAILED` 기록 후 종료하고, JSON 수정 후 재기동한다. 특히 5번은 HWM이 미래로 전진해 이후 회차가 0건을 읽고도 `SUCCEEDED` 로 끝나는 **조용한 사고**로 이어진다.

---

## 8. 제어 장치

| 장치 | 방식 |
|---|---|
| **Skip 판정** | **글로벌/파이프라인 파라미터** 근거로 ADF 로직이 판단. 결과 무관 `ctl_trigger_history` + 마스터 회차(`SKIPPED`) 양쪽 기록. Filter가 수행 게이트 |
| **마스터 동시수행** | `ux_mrs_running` — **(파이프라인명, 수집유형, `init_yn`)** 당 `PENDING`/`RUNNING` 1건. 충돌 시 `SKIPPED` 기록 (8-1) |
| **대상 동시수집** | `ux_ipr_running` — **FULL에 한해** `target_id` 당 `RUNNING` 1건. 경계 시점 동일 테이블 중복 수집 차단 |
| **수집 실패 재시도** | **Copy Activity retry 옵션** (8-2). 별도 재수행 파이프라인 없음 — 회수는 만회 모드에 위임 |
| **파라미터 검증** | 백필 JSON은 이력 생성 전 검증, 위반 시 전건 중단 (7장) |
| **디스패처 방어** | Switch `default` → 미등록 파이프라인 건을 `FAILED` 기록 (4-2) |
| **트랜잭션** | 이력 생성은 단일 트랜잭션 — 불완전 회차 방지 |
| **규모 전제** | 대상 3,000건 미만 (ADF Lookup 5,000행 / 4MB 한계). 초과 시 `exec_group` 분할 |
| **IR 용량** | 온프레미스 소스는 **SHIR HA 2노드** 경유. 리소스 모니터링 후 스케일 아웃/업 요청 (8-3) |

### 8-1. 마스터 동시수행 락

**락 체계 (확정)**

| 항목 | 확정안 |
|---|---|
| 모델 | `ingest_type` 유지 + `ctl_master_pipeline_run.init_yn` 신규 |
| 락 키 | `(master_pipeline_nm, ingest_type, init_yn)` |
| 락 범위 | `status IN ('PENDING','RUNNING')` |
| 매뉴얼 한정 | **해제** — 스케줄 초기수집 도입으로 스케줄 간 충돌도 막아야 함 |
| 초기 ↔ 정규 FULL | **동시 허용** (`init_yn` 으로 키 분리). 부하는 트리거 시간 분리로 관리 |
| 초기 ↔ 초기 (수동 포함) | **차단** — 동일 키 |
| 충돌 시 | `SKIPPED` + `skip_reason='동시수행 차단: 선행 회차 {run_id}'` 기록 |

```sql
CREATE UNIQUE INDEX ux_mrs_running
ON ctl_master_pipeline_run (master_pipeline_nm, ingest_type, init_yn)
WHERE status IN ('PENDING', 'RUNNING');
```

**락 충돌은 실패가 아니라 스킵으로 처리한다** — `pl_set_master_pl` 의 `RUN` 액션이 진행 중 회차를 확인하고, 있으면 `SKIPPED` 회차를 INSERT한 뒤 `proceed_yn='N'` 을 반환한다. Filter 게이트가 이를 받아 후속을 중단한다. 유니크 인덱스는 사전 체크 사이의 경합을 막는 **최후 방어선**으로, INSERT가 제약에 걸리면 EXCEPTION에서 `SKIPPED` 전환한다.

**스킵돼도 자기 복구된다** — 초기수집 미완분은 `init_wait_yn='Y'` 유지로 다음 회차 재편입, 정규 FULL은 다음 주기가 전체 커버, 증분은 만회 모드. 락 스킵 전용 복구 절차가 필요 없다.

> `init_yn` 은 **`NOT NULL DEFAULT 'N'` 필수** — PostgreSQL 유니크 인덱스에서 NULL은 서로 다른 값으로 취급되어, NULL 허용 시 락이 조용히 무력화된다.
>
> 초기 ↔ 정규 FULL 동시 허용 시 경계 시점(초기 성공 → `'N'` 전환 직후)의 동일 테이블 중복은 `ux_ipr_running` 이 자식 레벨에서 차단한다. 부하 중첩은 트리거 시간대 분리로 회피한다.

**`SAND BOX` 참고** — ADF에서 디버그 실행 또는 Trigger Now(1회 수동 실행) 시 `TriggerName` 이 `SAND BOX` 로 자동 부여된다. 하드코딩이 아니라 수동 기동의 판별자다.

### 8-2. 수집 실패 재시도

**Copy Activity 자체 retry 옵션을 사용하고, 별도 재수행 파이프라인은 만들지 않는다.**

| 실패 성격 | 예 | 대응 |
|---|---|---|
| **일시적** | 네트워크 순단, 소스 락 경합, IR 일시 과부하 | Copy retry — 초~분 단위 재시도로 회복 |
| **지속적** | 소스 점검, 권한 만료, 스키마 변경, 대용량 타임아웃 | 재시도로 안 풀림 → **만회 모드**가 다음 회차에 회수 |

별도 재수행(실패 건 메타 조회 후 재실행)을 만들지 않는 이유 — 실패 회수는 이미 만회 모드가 담당한다. 중복 구축하면 같은 일을 하는 장치가 둘이 되고, 재수행 중 정규 회차와 겹칠 수 있다. 즉시 회수가 필요하면 파라미터 없이 수동 기동하면 된다.

| 설정 | 권고값 | 근거 |
|---|---|---|
| `retry` | 2 (총 3회) | 일시적 장애는 대부분 1~2회 내 회복 |
| `retryIntervalInSeconds` | 30~60 | 락·순단 해소 최소 대기 |
| `timeout` | 배치 창·조각 크기에 맞춰 조정 ⚠ | 기본값이 길어 행 걸림 탐지가 늦어짐. **초기수집은 파티션 분할과 세트로 짧게** 설정해야 timeout이 "행 걸림" 판별자로 기능 |
| `secureOutput` | 필요 시 활성 | `error_log` 에 연결 정보 노출 방지 |

> ⚠ **`attempt_no`** — Copy 내부 재시도는 output에 최종 결과만 남아 중간 횟수 확보가 어렵다 *(버전별 상이 — 실측 확인)*. 미확보 시 `attempt_no` 는 차일드 Execute Pipeline 레벨 재시도 또는 회차 간 재수집 횟수로 의미를 조정한다.

### 8-3. SHIR 용량 계획

소스 DB별 차일드 병렬 × ForEach 병렬 × Copy 동시 실행이 모두 같은 노드 풀을 공유한다.

| 항목 | 방침 |
|---|---|
| 구성 | HA 2노드 (가용성 + 처리량) |
| 용량 판단 | CPU·메모리·동시 job 수 모니터링, 실측 기반 |
| 부족 시 | **스케일 아웃(노드 추가) / 스케일 업(사양 상향)을 고객사에 요청** |
| 주의 시점 | 초기 이행, 장기 중단 후 만회 회차 — 조회량 급증 구간 |

> 병렬도 상한(디스패처 `batchCount` 등)은 SHIR 실측 후 조정한다. 설계 단계에서 고정하지 않는다.

### 8-4. 스킵 회차 기록 규칙

스킵도 **완결된 회차**로 취급한다.

| 컬럼 | 값 |
|---|---|
| `status` | `SKIPPED` |
| `start_dt` / `end_dt` | 판정 시각 (동일 값 — 즉시 종료) |
| `child_total_cnt` / `succ` / `fail` | 0 |
| `run_mode` / `init_yn` | 원래 기동 값 유지 |
| `ctl_trigger_history` | `skip_yn='Y'` + `skip_reason` |

**성공률 집계에서는 스킵을 제외**한다. 의도된 미수행이 분모에 들어가면 지표가 왜곡된다.

```sql
SELECT master_pipeline_nm,
       COUNT(*) FILTER (WHERE status = 'SUCCEEDED') AS succ_cnt,
       COUNT(*) FILTER (WHERE status = 'FAILED')    AS fail_cnt,
       COUNT(*) FILTER (WHERE status = 'SKIPPED')   AS skip_cnt,
       ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'SUCCEEDED')
             / NULLIF(COUNT(*) FILTER (WHERE status IN ('SUCCEEDED','FAILED')), 0), 1) AS succ_rate
FROM   ctl_master_pipeline_run
WHERE  start_dt >= :from_dt
GROUP  BY master_pipeline_nm;
```

> 스킵 건수는 **별도 지표로 노출**한다. `skip_reason` 으로 운영 스킵과 **락 스킵이 구분**되므로, 락 스킵이 반복되면 트리거 시간 중복 신호로 읽고 스케줄을 조정한다.

---

## 9. 초기수집 파티셔닝 · timeout

초기수집은 정상 소요시간도 길어, 분할 없이 timeout을 짧게 잡으면 정상 건이 죽는다. **파티셔닝으로 조각 크기를 균일하게 만든 뒤 timeout을 짧게** 잡아야 "행 걸림" 판별이 성립한다.

| 순위 | 방식 | 조건 |
|---|---|---|
| 1 | **Copy `partitionOption = PhysicalPartitionsOfTable`** | 소스 테이블이 물리 파티션되어 있으면 자동 병렬. 카탈로그 조회 불필요, 이력 모델 불변 ⚠ *커넥터 지원 범위 확인* |
| 2 | **`DynamicRange`** — 카탈로그에서 파라미터 자동 산출 | `SYSCAT`(LUW)/`SYSIBM`(z/OS)의 CARD·LOW2KEY·HIGH2KEY로 분할 컬럼·상하한 산출 → 메타 컬럼에 입력. 분할 키 우선순위: 클러스터링 인덱스 선두 → 숫자형 PK → 날짜 컬럼 |
| 3 | 쿼리 N개 생성 (이력 N행) | **비권장** — `ux_ipr_running` 제약과 충돌, 이력 모델 변경 필요 |

> ⚠ 카탈로그 통계는 RUNSTATS 시점 값이라 최신성이 보장되지 않고 균등 분포를 가정한다. 자동 산출값은 초안으로 쓰고 대용량 상위 대상은 실측 검증한다.

---

## 10. 모니터링 — 별도 구축 범위

> **본 설계(ADF 구축)의 범위는 "모니터링에 필요한 데이터를 메타 테이블에 남기는 것"까지다.** 대시보드·알림·리포트는 ADF 범위 밖이며 **별도 구축이 필요하다.**

### ADF 자체 모니터링으로는 부족하다

ADF Monitor는 파이프라인 실행 단위만 보여준다. 운영에 필요한 정보는 그 아래 층위에 있다.

| 필요한 정보 | ADF Monitor | 메타 이력 |
|---|---|---|
| 파이프라인이 성공했나 | O | O |
| **대상 테이블별** 수집 성공 여부 | ✗ | `ctl_ingest_pipeline_run` |
| **며칠째 성공 없는 대상** | ✗ | `ctl_ingest_target_master.last_success_dt` |
| 수집 건수·용량 추이 | ✗ | `extract_cnt` / `save_cnt` / `file_size_mb` |
| 회차별 사용 하한값 | ✗ | `incr_column_hw_val` (이력) |
| 스킵 현황·사유 (락 스킵 포함) | ✗ | `ctl_trigger_history` |
| API 호출 실패 | ✗ | `ctl_dbx_api_call_history` |
| Bronze 적재 지연 | ✗ | `ctl_dbx_ingest_history` |
| 이력 보존 | 45일 (기본) ⚠ | 정책에 따름 |

마스터가 `SUCCEEDED` 로 끝나도 그 안에서 대상 몇 건이 실패했을 수 있다. **차일드 실패가 마스터를 실패시키지 않도록 의도적으로 설계**했으므로(2장), ADF 화면만 보면 문제를 놓친다.

### 메타 이력만으로 산출되는 지표

| 지표 | 소스 | 용도 |
|---|---|---|
| 회차 성공률 / 스킵률 | `ctl_master_pipeline_run` | 배치 안정성 추이 |
| **지연 대상 목록** | `last_success_dt` | **가장 중요** — 조용히 멈춘 대상 탐지 |
| 실패 대상 + 원인 | `ctl_ingest_pipeline_run` (`error_log`) | 장애 분석 |
| 대상별 건수·용량·소요시간 추이 | `extract_cnt` / `file_size_mb` / `start·end_dt` | 이상 탐지, 배치 창 관리 |
| API 호출 성공률 | `ctl_dbx_api_call_history` | ADF↔Databricks 연계 건전성 |
| **End-to-End 완결** | 호출이력 × 적재이력 대조 | 호출은 성공했는데 적재가 없는 회차 탐지 |

> **폴링을 없앤 대신 대조로 확인한다** — "호출 성공(`success_yn='Y'`) 후 일정 시간이 지나도 `ctl_dbx_ingest_history` 에 해당 `master_run_id` 기록이 없는 경우"의 탐지가 End-to-End 누락을 잡는 유일한 경로다.

### 별도 구축이 필요한 것

| 구성 | 내용 | 담당 ⚠ |
|---|---|---|
| 대시보드 | 위 지표 시각화 (Power BI / Grafana / Databricks SQL / 고객사 표준) | 미정 |
| 알림 | 최소 2종: ① 회차 실패 ② **N일 이상 성공 없는 대상 발생** | 미정 |
| 정기 점검 | 지연 대상 조회 일 1회 이상 자동 실행 | 미정 |

> **알림이 없으면 만회 모드는 잠자는 기능이다** — 만회는 다음 회차가 돌아야 작동한다. **트리거 자체가 멈춘 경우**는 다음 회차가 오지 않으므로, 유일한 탐지 수단이 `last_success_dt` 기반 알림이다.
>
> 도구 확정 전 잠정 대안: ADF 내 일 1회 점검 파이프라인(지연 대상 Lookup → 임계 초과 시 Web으로 Webhook 발송). 임시 방편이며 정식 체계를 대체하지 않는다.

---

## 11. 설계 검토 관점

| 관점 | 확인 사항 |
|---|---|
| 확장성 | 신규 조건·주기·소스 추가 시 파이프라인·SQL 변경 없이 수용되는가 |
| 가독성 | 파이프라인명만 보고 무엇을·어떻게 수집하는지 이해되는가 |
| 일관성 | 한 오케스트레이터명으로 다른 이름을 유추할 수 있는가 |
| 유지보수성 | 로직 변경 시 **한 곳만** 고치면 되는가 (공통 파이프라인 구조) |
| 추적성 | 회차별 실제 대상·쿼리·하한값·건수를 이력만으로 재구성할 수 있는가 |
| 복구성 | 미수집이 원인 구분 없이 다음 회차로 자동 만회되는가 (`last_success_dt` 판정) |
| 운영성 | 파라미터 없이 수동 재실행만으로 대부분의 누락이 회수되는가 |
| 관측성 | 대상별 수집 결과·지연을 이력만으로 파악할 수 있는가 (모니터링 구축은 별도 범위) |

---

## 12. 확정 사항 / 남은 협의

### 확정 사항

| # | 항목 | 결정 |
|---|---|---|
| 1 | **증분 하한 기준** | 표현식 + HWM 하이브리드. 정상은 표현식, 만회 모드는 `LEAST(표현식, HWM)` |
| 2 | **만회 모드 판정** | `last_success_dt` 기반 — 마지막 성공이 직전 예정 회차보다 이전이면 ON. 원인 무관 |
| 3 | **만회 판정 기준일** | `CONDITION_TYPE` 별 — WINDOW는 `frequency` 단위, BIZDAY는 직전 영업일, CLOSE는 1개월 |
| 4 | **HWM 정의 / 갱신** | 마지막 성공 회차가 사용한 하한. `GREATEST(이번 하한, 기존 HWM)` — 후퇴하지 않음 |
| 5 | **상한 처리** | 상한 컬럼 없음. 유입분은 다음 회차가 겹쳐 읽고 Bronze가 흡수 |
| 6 | **Bronze 중복 처리** | **Databricks에서 수행** — 키 기준 dedup/upsert. 본 설계의 전제 조건이며 키 정의는 Databricks 파트 담당 |
| 7 | **Bronze 완료 확인** | **ADF는 폴링하지 않음(Until 미사용).** 책임 경계는 API 호출 성공까지. 완결 확인은 호출이력 × 적재이력 대조 |
| 8 | **Databricks API 호출** | 공통 파이프라인 `pl_call_dbx_api` 일원화. `ctl_dbx_api_call_history` 에 호출별 기록 |
| 9 | **공통 파이프라인** | `pl_set_master_pl`(`p_action`: `RUN`/`STATUS`/`STATUS_UPDATE`) · `pl_dispatch_ingest` · `pl_call_dbx_api`. 수집이력 생성 Script는 오케스트레이터에 유지 |
| 9-1 | **수집이력 생성 입력** | **기준은 수집대상마스터 조회**. `P_BACKFILL_JSON` 이 NOT NULL이면 스크립트에 전달해 **대상 한정 + 하한 override** 용도로 참조 (2-3) |
| 10 | **시스템 변수 전달** | 공통 파이프라인은 `RunId` 등을 직접 읽지 않고 **마스터에서 파라미터로 수신** |
| 11 | **차일드 분기** | 디스패처가 이력의 `ingest_pipeline_nm` 으로 Switch 분기. `default` 는 `FAILED` 기록 |
| 12 | **마스터 락** | 키 `(pipeline_nm, ingest_type, init_yn)`, 범위 `PENDING`+`RUNNING`, 매뉴얼 한정 해제. 초기↔정규 FULL 동시 허용, 초기↔초기 차단. 충돌 시 `SKIPPED` 기록 |
| 13 | **수집 실패 재시도** | Copy Activity retry(권고 2회/30~60초). 별도 재수행 파이프라인 없음 — 만회 모드에 위임 |
| 14 | **Skip 판정 근거** | 글로벌 파라미터 또는 파이프라인 파라미터 |
| 15 | **스킵 회차 기록** | `SKIPPED` + `end_dt`=`start_dt`, 건수 0. 성공률 집계 제외, 건수는 별도 지표 |
| 16 | **테스트 실행** | `P_TEST_YN='Y'` → `run_mode='TEST'`. 이력 기록, HWM·`last_success_dt` 갱신 제외 |
| 17 | **백필** | 상한 없음, HWM 후퇴 차단, 검증 6항목 전건 중단, 기존 오케스트레이터 수동 기동 |
| 18 | **NUMERIC 비교** | `::numeric` 캐스팅 후 비교. `LEAST`·`GREATEST` 양쪽 동일 적용 |
| 19 | **COSMOS 타임존** | UTC 저장 확정. 하한 변수 KST·UTC 이원화, ② 하한 확정 단계에서 `src_type` 별 선택 |
| 20 | **영업일 캘린더** | 고객사 DB 테이블로 관리. BIZDAY는 Lookup으로 직전 영업일 조회 |
| 21 | **IR 구성** | SHIR HA 2노드. 리소스 모니터링 후 스케일 아웃/업 요청 |
| 22 | **모니터링 범위** | ADF 범위는 이력 데이터 적재까지. 대시보드·알림은 별도 구축 |

### DDL 변경

| # | 대상 | 변경 |
|---|---|---|
| 1 | `ctl_ingest_target_master` | `last_success_dt timestamptz NULL`, `ingest_pipeline_nm varchar(200) NULL`(override) 추가 |
| 2 | `ctl_ingest_pipeline_run` | `ingest_pipeline_nm varchar(200) NOT NULL DEFAULT '-'` 추가 ([스냅샷]) |
| 3 | `ctl_master_pipeline_run` | `init_yn bpchar(1) NOT NULL DEFAULT 'N'` + CHECK, `data_class varchar(4) NULL` 추가 |
| 4 | `ux_mrs_running` | `(master_pipeline_nm, ingest_type, init_yn) WHERE status IN ('PENDING','RUNNING')` 로 재정의, 매뉴얼 한정 제거 |
| 5 | 도메인 D07 (`run_mode`) | `BACKFILL`, `TEST` 값 추가 + CHECK 수정 |
| 6 | `ctl_dbx_api_call_history` | **신규 테이블** — 논리·물리 모델 정의서에 엔티티 추가 |

```sql
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
```

> `dbx_run_id` 는 `ctl_dbx_ingest_history` 와 대조해 **호출 → 적재** 연결을 추적하는 키다. ⚠ *Databricks 측 이력에 `dbx_run_id` 를 남기는지 확인 — 없으면 `master_run_id` 로만 대조*
>
> `init_yn`(마스터 회차의 성격)과 `init_wait_yn`(대상의 대기 상태)은 **다른 컬럼**이다. 도메인 정의서에 인접 배치하고 차이를 명시한다.

### 남은 협의

| # | 항목 | 내용 | 영향 |
|---|---|---|---|
| 1 | **Databricks 이력 연계 키** | `ctl_dbx_ingest_history` 에 `dbx_run_id` 기록 여부 — **Databricks 파트 협의** | 중 |
| 2 | **CLOSE 조건의 메타 수용성** | 마감년월(CLYM) 증분을 메타 조건 체계로 표현 가능한지. 업무 마감 상태 의존이면 별도 관리 필요 | 중 |
| 3 | **DB2 / MSSQL 타임존** | 증분 컬럼 실값을 KST 현재시각과 대조해 저장 기준 확인 (COSMOS는 UTC 확정) | 중 |
| 4 | **BIZDAY 조회 방식** | 고객사 캘린더 테이블 위치·스키마, Lookup 시점 | 중 |
| 5 | **만회 회차 조회량** | 장기 중단 후 첫 성공 회차의 조회 범위 급증 — 대용량 대상·SHIR 부하 확인 | 중 |
| 6 | **이력 보존 기간** | 이력 5종의 보존·정리(파티션/아카이브) 주기 — **고객 협의**. `request/response_body` 가 `text` 라 증가 빠를 수 있음. 이력 정리는 만회 판정(`last_success_dt` 가 마스터에 있음)에 영향 없음 | 중 |
| 7 | **모니터링 구축** | 대시보드·알림의 구축 주체·도구·일정 — ADF 범위 밖(10장). "N일 이상 성공 없는 대상" 알림은 트리거 중단을 탐지하는 유일한 수단 | **높음** |
| 8 | **파라미터화 Linked Service** | SHIR 경유 DB2에서 동작 검증 — 공통 차일드 전환의 전제 (4-2) | 중 |
| 9 | **Copy `timeout` 값** | 환경 기본값 실측 후 대상 특성별 조정. 초기수집은 파티셔닝과 세트로 결정 | 낮음 |
