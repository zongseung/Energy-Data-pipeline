"""SMP(계통한계가격) 수집 패키지.

KPX 전력거래소의 SMP 데이터를 수집·적재한다.
- 하루전시장 시간별 SMP (육지/제주)      -> smp_hourly
- 공식 가중평균 SMP (일/월/연, 육지/제주/통합) -> smp_weighted_avg
- 제주 실시간시장 SMP (15분 단위)         -> smp_realtime_jeju

기존 PV/Wind/Weather/Gen 수집 시스템과 독립적으로 동작한다.
"""
