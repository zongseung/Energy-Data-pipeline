# (EM_planning 측 배치용) 수집·전처리 결정 → SourceSpec 작성 가이드

> 이 문서는 Energy-Data-pipeline(NAS)에서 초안 작성됨.
> **EM_planning 레포로 옮겨 배치하세요** (예: `docs/SPEC_AUTHORING_GUIDE.md`).
> 두 Claude 인스턴스는 직접 대화 불가 — 역방향 채널 없이 사용자가 수동 이동.

## 역할 분담
- **EM_planning** = "어떻게 수집·전처리할지" 논의·결정(리서치·표준).
- **Energy-Data-pipeline**(NAS) = 결정을 받아 실제 수집·적재 파이프라인 구현·운영.
- 핸드오프는 **SourceSpec 문서**가 유일한 채널.

## 결정 체크리스트 (소스마다 합의해 SourceSpec으로 정리)
1. **수집**: 엔드포인트 / 인증 / 주기 / 가용범위 / 페이지네이션
2. **스키마**: 원본 필드 → 타입
3. **시간**: 원본 표기와 적재 변환 (이 프로젝트는 구간시작 0~23시로 통일)
4. **전처리**: 정제 · 결측 · 중복 · 단위함정 · 파생태깅
5. **적재 대상**: 테이블 · unique key · **단일 writer**
6. **품질**: 값 범위 · 결측 허용 · 연속성
7. **출처**: 1차 출처 · 근거 (보험/FM 감사 대비)

## 작성·전달
1. 양식: Energy-Data-pipeline의 `docs/SOURCE_SPEC_TEMPLATE.md` 사용.
2. 파일명: `<source>.spec.md`, 한 소스 = 한 파일.
3. **전달**: `nas_exec.py` 브리지로 `/mnt/nvme/Energy-Data-pipeline/intake/`에 write(이 경로만 허용·하드닝됨).
4. **self-contained**: 스펙이 참조하는 다른 EM_planning 문서(regime 정의·리스크 매트릭스 등)도 같은 전달에 포함.

## 좋은 스펙의 기준
- **빈칸 없음**(모르면 "미정" 명시) — 누락은 구현 왕복을 만든다.
- **단일 writer 명시** — 같은 테이블을 양쪽이 쓰지 않는다(드리프트 방지).
- **시간 규약 1:1 대조 가능** — 라벨/구간 명확히.
- **출처 인용** — "이 숫자가 왜 이 값인가" 감사 가능하게.
