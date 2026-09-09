# SourceSpec — smp (계통한계가격)

> 기존 SMP 파이프라인을 `docs/SOURCE_SPEC_TEMPLATE.md`에 **역으로 채운 워크드 예제 겸 양식 자체검증본**.
> 복수 테이블(3)·복수 플로우(4) 소스라, 양식이 실제를 빠짐없이 담는지 점검하는 용도이기도 함.

```yaml
source: smp
owner: 이 프로젝트(기존 구현)
consumer: 이 프로젝트 가격 시계열(PV/풍력 조인) + EM_planning smp_kri(계통한계가격 리스크)
status: implemented
spec_version: 1
```

## 1. Identity
- **소스명**: KPX 계통한계가격(SMP) + EPSIS 공식 가중평균
- **무엇/왜**: 하루전/실시간 전력시장 한계가격. 발전 시계열과 조인되는 가격축이자 마켓 리스크 지표.

## 2. Acquisition (복수 엔드포인트/플로우)
| 플로우 | 엔드포인트 | method | 스케줄 |
|--------|-----------|--------|--------|
| 시간별 + 일별 wavg | kpx `smpInland.es`/`smpJeju.es` (POST+CSRF) | 스크래핑 | 매일 06:00 |
| 월/연 wavg + BLMP | EPSIS `selectEkmaSmpSmp.ajax` (GET쿠키→POST) | API | 매월 2일 07:00 |
| 제주 실시간 15분 | kpx `bidSmpLfdDataRt.es` | 스크래핑 | 매일 19:00 |
| 개인DB 백업 sync | 공통DB → 개인DB | sync | 매주 월 07:00 |
- **auth**: CSRF 토큰(KPX), 쿠키 선취득(EPSIS). API키 불요.
- **가용범위**: land 2001-05~, jeju 2010-01~, 제주실시간 2024-03~.

## 3~6. Schema/Time/Transform/Target — 테이블 3개

### T1. `smp_hourly` (하루전 시간별)
- **schema**: timestamp(DateTime), region(land|jeju), price(Float, 원/kWh)
- **time**: KPX 1~24시 hour-ending → 구간시작 (N → (N-1)시; 1시→00:00)
- **transform**: `parse_price`(콤마제거·플레이스홀더→null); 결측 0; jeju 2020-05-19/20은 EPSIS wide CSV 보충
- **target**: unique (timestamp, region). 2010 이전은 통합가(unified). **단일 writer = 이 프로젝트**.

### T2. `smp_weighted_avg` (일/월/연 가중평균)
- **schema**: period_type(daily|monthly|yearly), period(Date), region(land|jeju|unified), price_type(smp|blmp), weighted_avg(Float)
- **target**: unique (period_type, period, region, price_type).
- **quality**: BLMP는 2001~2006만 존재(이후 0 skip); unified 2010~; EPSIS 공식값 100% 일치 검증.

### T3. `smp_realtime_jeju` (제주 실시간 15분)
- **schema**: timestamp(15분), region(jeju), price(Float, **음수 가능**), is_confirmed(Bool)
- **time**: Nh K구간 → (N-1)시 + (K-1)×15분 (96구간/일)
- **target**: unique (timestamp, region). 확정값 D+1 18시 공표.

## 7. Quality (공통)
- 라이브 원본과 1:1 대조(시간 한 칸 밀림 없는지). DB=CSV 미러(`smp_data/`) 일치.

## 8. Provenance
- **출처**: KPX `new.kpx.or.kr`(smpInland/smpJeju/bidSmpLfdDataRt), EPSIS, 전력시장운영규칙 `marketrule.kpx.or.kr`
- **외부 참고**: github.com/Lee-Zzun/SMP_collector

---
**양식 자체검증 결과**: 단일 테이블·단일 엔드포인트 전제의 §2/§3~6이 복수 테이블·복수 플로우 소스(SMP)를 담으려면 **반복 구조**가 필요함 → 템플릿에 multi 지원 주석 추가함. 그 외 필드(identity/time/transform/quality/provenance)는 SMP 현실을 빠짐없이 표현 가능.
