# ADF 시스템 함수로 트리거 타입 및 트리거 명 확인하기

Azure Data Factory(ADF) 파이프라인 내에서 시스템 변수(System Variables)를 사용하여 파이프라인을 실행한 **트리거의 유형(Type)과 이름(Name)** 을 확인하는 방법을 정리합니다.

## 목차

- [1. 개요](#1-개요)
- [2. 설정 방법](#2-설정-방법)
- [3. 변수 설정 예시](#3-변수-설정-예시)
- [4. 출력 결과 예시](#4-출력-결과-예시)
- [5. 참고](#5-참고)

## 1. 개요

ADF는 파이프라인 실행 컨텍스트 정보를 담은 시스템 변수를 제공합니다. 이 중 트리거 관련 변수는 다음과 같습니다.

| 시스템 변수 | 설명 |
|---|---|
| `@pipeline().TriggerType` | 파이프라인을 실행한 트리거의 유형 (예: `Manual`, `ScheduleTrigger`, `TumblingWindowTrigger`) |
| `@pipeline().TriggerName` | 파이프라인을 실행한 트리거의 이름 |

## 2. 설정 방법

**변수 설정(Set variable)** 활동(Activity)을 캔버스에 추가하여 시스템 변수 값을 파이프라인 변수에 할당합니다.

- **변수 형식:** 파이프라인 변수 (Pipeline variable)
- **트리거 타입 확인 식:** `@pipeline().TriggerType`
- **트리거 이름 확인 식:** `@pipeline().TriggerName`

## 3. 변수 설정 예시

'변수 설정' 활동의 **설정(Settings)** 탭에서 다음과 같이 입력합니다.

| 항목 | 값 |
|---|---|
| 이름 | `trigger_type` (사용자 지정 변수명) |
| 값 | `@pipeline().TriggerType` (동적 콘텐츠 추가) |

## 4. 출력 결과 예시

파이프라인 실행 후, '변수 설정' 활동의 **출력(Output)** 아이콘을 클릭하면 JSON 포맷의 결과를 확인할 수 있습니다.

### 4.1 트리거 타입 (Trigger Type)

수동(디버그 등)으로 실행했을 경우의 출력 예시입니다.

```json
{
    "name": "trigger_type",
    "value": "Manual"
}
```

### 4.2 트리거 이름 (Trigger Name)

트리거로 실행했을 경우, 해당 트리거의 이름이 출력됩니다.

```json
{
    "name": "trigger_name",
    "value": "Sandbox"
}
```

> **Note**
> 위 예시의 `Sandbox`는 임의의 트리거 이름입니다. 실제 환경의 트리거 이름이 출력됩니다.

## 5. 참고

- [Azure Data Factory - System variables (공식 문서)](https://learn.microsoft.com/azure/data-factory/control-flow-system-variables)
