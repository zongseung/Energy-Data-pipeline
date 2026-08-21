# SourceSpec — <소스 이름>

> EM_planning이 소스마다 이 양식을 채워 `intake/<source>.spec.md`로 전달한다.
> 이 프로젝트는 이 계약을 기준으로 수집·적재 파이프라인을 구현하고, 라이브 소스와 1:1 대조 검증한다.
> **누락 = 구현 왕복**이므로 빈칸 없이 채울 것. 모르면 "미정"이라 명시.
> **복수 테이블/엔드포인트 소스**는 §2 Acquisition을 플로우별 행으로, §3~6(Schema/Time/Transform/Target)을 테이블별 블록으로 반복한다. (워크드 예제: `fetch_data/smp/SPEC.md` — 3테이블·4플로우)

```yaml
# --- frontmatter (기계 판독용) ---
source: <kebab-case 식별자, 예: krx-kau>
owner: <EM_planning 측 담당>
consumer: <용도/KRI, 예: kau_kri (탄소배출권 리스크)>
status: draft | agreed | implemented
spec_version: 1
```

## 1. Identity
- **소스명**:
- **무엇/왜**: 이 데이터가 무엇이고 EM_planning에서 어떤 리스크/지표에 쓰이는지 한 줄.

## 2. Acquisition (어떻게 수집)
- **endpoint(s)**: URL / API / 파일 위치
- **method**: GET / POST / CSV다운로드 / 스크래핑 …
- **auth**: 키 / 세션 / CSRF / 없음  *(키 값은 `.env`로, 스펙에 직접 쓰지 말 것)*
- **rate limit / pagination**:
- **수집 주기**: 예) 매일 06:00 KST / 매월 2일 …
- **가용 범위**: 최초 날짜 ~ 현재

## 3. Schema (원본 → 타입)
| 원본 필드 | 타입 | 설명 |
|----------|------|------|
|  |  |  |

## 4. Time (시간 규약) — 중요
- **원본 시간 표기**: 예) KPX 1~24시 hour-ending(구간 종료)
- **적재 변환**: 예) 구간시작(0~23시)으로. 이 프로젝트 기존 시계열(PV/풍력/기상/SMP)과 조인되도록. *(KPX는 `smp` 규약 재사용)*
- **타임존**: KST 고정

## 5. Transform (전처리 규칙)
- **정제/파싱**: 예) 콤마 제거, 플레이스홀더 → null (`_common.parse_price` 식)
- **결측 처리**: 예) spline 보간(`fetch_data/common/impute_missing.py` 재사용) / 0채움 / skip
- **중복/유일성**:
- **단위 변환**: 함정 명시 (예: "MWh 라벨이지만 실제 kWh")
- **태깅**: 파생 컬럼(regime 등) 필요 시

## 6. Target (적재 대상)
- **테이블명**: `<source>_...`
- **컬럼**: 스키마 → DB 컬럼 매핑
- **unique key**: upsert 충돌 기준
- **단일 writer**: 이 프로젝트만 이 테이블에 씀(EM_planning은 read만)

## 7. Quality (검증 단언)
- **값 범위**: 예) price >= 0, 또는 음수 허용
- **결측 허용 한도**:
- **행 수/연속성 기대**:

## 8. Provenance (출처 — 보험/FM 감사용)
- **1차 출처 URL/기관**:
- **근거 문서/규칙**:
- **참조한 EM_planning 문서**(함께 전달됨):
