"""
남동발전(KOEN) 비태양광 발전소 위경도/주소 매핑.

koenergy.kr 시간대별 발전실적 CSV(해양소수력/연료전지/화력) 응답에는
위경도·주소 컬럼이 없다. 따라서 응답의 '발전구분' 값을 KOEN 발전본부 단위
좌표에 매핑한다.

좌표는 발전본부(사업소) 단위의 근사 좌표이며, 호기/개별 설비 단위가 아니다.
출처: KOEN 사업소 소재지 공개 주소 기반(2026-05 기준).
필요 시 좌표/주소를 보정해 사용한다.

resolve_location()은 '삼천포해양소수력', '여수연료전지', '영흥' 처럼
접미사가 달라도 지역 키워드 부분매칭으로 좌표를 찾는다.
"""

from __future__ import annotations

from typing import Optional, TypedDict


class Location(TypedDict):
    lat: float
    lon: float
    address: str
    site: str  # KOEN 발전본부(사업소)명


# 지역 키워드 -> 발전본부 좌표 (근사)
# key 는 '발전구분' 문자열에 포함되는 지역 키워드.
KOEN_SITE_LOCATIONS: dict[str, Location] = {
    "영흥": {
        "lat": 37.2542,
        "lon": 126.4278,
        "address": "인천광역시 옹진군 영흥면 영흥로 293",
        "site": "영흥발전본부",
    },
    "삼천포": {
        "lat": 34.9294,
        "lon": 128.0656,
        "address": "경상남도 고성군 하이면 하이로 1156-31",
        "site": "삼천포발전본부",
    },
    "영동": {
        "lat": 37.7447,
        "lon": 128.9949,
        "address": "강원특별자치도 강릉시 강동면 헌화로 1289-43",
        "site": "영동에코발전본부",
    },
    "여수": {
        "lat": 34.8559,
        "lon": 127.7177,
        "address": "전라남도 여수시 여수산단로 727",
        "site": "여수발전본부",
    },
    "분당": {
        "lat": 37.3539,
        "lon": 127.1124,
        "address": "경기도 성남시 분당구 분당로 51",
        "site": "분당발전본부",
    },
    "안산": {
        "lat": 37.3107,
        "lon": 126.8056,
        "address": "경기도 안산시 단원구 별망로 178",
        "site": "안산연료전지",
    },
}


def resolve_location(plant_name: str) -> Optional[Location]:
    """발전구분 문자열에서 지역 키워드를 찾아 좌표를 반환한다.

    매칭 실패 시 None (호출부에서 미매핑으로 처리).
    """
    if not plant_name:
        return None
    name = plant_name.strip()
    for keyword, loc in KOEN_SITE_LOCATIONS.items():
        if keyword in name:
            return loc
    return None
