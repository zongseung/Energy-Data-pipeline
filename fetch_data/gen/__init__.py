"""
남동발전(KOEN) 비태양광 발전원 데이터 수집 모듈.

태양광(fetch_data/pv)·풍력(fetch_data/wind)과 동일한 koenergy.kr 소스에서
해양소수력 / 연료전지 / 화력 의 시간대별 발전실적을 수집한다.

Submodules:
- namdong_collect: 월별 분할 + 배치 병렬 비동기 CSV 수집
- transform_gen: 원본 와이드 CSV -> wind 패턴 long 변환 (카테고리별 데이터셋)
- pipeline: 수집 -> 변환 자동화 오케스트레이터
- locations: 발전소(발전구분) -> 위경도/주소 매핑
"""
