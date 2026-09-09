"""
남동발전(KOEN) 비태양광 발전소 호기별 정격용량(설비용량) 매핑.

koenergy.kr 시간대별 발전실적 CSV 에는 용량 컬럼이 없다(시간별 발전량만 제공).
따라서 호기별 정격용량을 KOEN 공식 '발전현황' 페이지/공개자료에서 조사해
plant_name(= 발전구분_호기) 단위로 매핑한다. (2026-06 조사 기준)

값은 정격(설비)용량 MW. confidence:
  - "확실" : KOEN 공식 발전현황 페이지에서 호기별로 직접 확인
  - "근사" : 출처는 확인했으나 호기 매핑/단대수에 약간의 불확실성
  - "불확실": 공개자료에 호기 분할 용량이 없어 미확정(capacity_mw=None)

출처 URL 은 SOURCES 에 모아둔다. 필요 시 보정해 사용한다.
관련: locations.py(좌표/주소), [[koenergy-ssl-incomplete-chain]]
"""

from __future__ import annotations

from typing import Optional, TypedDict


class Capacity(TypedDict):
    capacity_mw: Optional[float]
    confidence: str  # 확실 | 근사 | 불확실
    source: str      # SOURCES 키
    note: str


# plant_name(발전구분_호기) -> 정격용량 정보
KOEN_PLANT_CAPACITIES: dict[str, Capacity] = {
    # ===== 해양소수력 (ocean small hydro) =====
    # _1/_2/_3 ↔ (1·2호기군/3·4호기군/5·6호기군) 매핑은 합리적 추정(근사).
    "삼천포해양소수력_1": {"capacity_mw": 6.0, "confidence": "근사", "source": "koen_samcheonpo",
                          "note": "1MW급 다수대 합산 6MW. KOEN 5대(5MW) vs 뉴스 6대(6MW) 출처 불일치, 6MW 다수설"},
    "영흥해양소수력_1": {"capacity_mw": 4.6, "confidence": "근사", "source": "waterjournal",
                        "note": "1·2호기용 해양소수력 4.6MW(1.533MW×3), 2011.10 준공"},
    "영흥해양소수력_2": {"capacity_mw": 3.0, "confidence": "근사", "source": "waterjournal",
                        "note": "3·4호기용 3MW(1MW×3)"},
    "영흥해양소수력_3": {"capacity_mw": 5.0, "confidence": "근사", "source": "waterjournal",
                        "note": "5·6호기용 5MW, 2014.06 준공. 영흥 합계 12.6MW"},

    # ===== 연료전지 (fuel cell) =====
    "분당연료전지_2": {"capacity_mw": 3.08, "confidence": "확실", "source": "koen_bundang",
                      "note": "2단계, 2013.04 준공, PAFC"},
    "분당연료전지_3": {"capacity_mw": 5.72, "confidence": "확실", "source": "koen_bundang",
                      "note": "3단계, 2016.09 준공, PAFC"},
    "분당연료전지_4": {"capacity_mw": 16.72, "confidence": "확실", "source": "koen_bundang",
                      "note": "4단계, 2018.08 준공, PAFC"},
    "분당연료전지_5": {"capacity_mw": 5.72, "confidence": "확실", "source": "koen_bundang",
                      "note": "5단계, 2018.02 준공, PAFC"},
    "분당연료전지_6-1": {"capacity_mw": None, "confidence": "불확실", "source": "energy_news",
                        "note": "6단계 전체 8.35MW(SOFC, 2018.11). _6-1/_6-2 호기 분할 용량 공개자료 없음"},
    "분당연료전지_6-2": {"capacity_mw": None, "confidence": "불확실", "source": "energy_news",
                        "note": "6단계 전체 8.35MW(SOFC, 2018.11). _6-1/_6-2 호기 분할 용량 공개자료 없음"},
    "안산연료전지_1": {"capacity_mw": 2.64, "confidence": "근사", "source": "edaily_koen_fc",
                      "note": "PAFC. 남동발전 합계 71.71MW 보도 기준, KOEN 공식 호기표 미확인"},
    "여수연료전지_1": {"capacity_mw": 9.68, "confidence": "확실", "source": "newenergy_yeosu_fc",
                      "note": "440kW×22EA=9.68MW, PAFC, 2020.10 준공"},

    # ===== 화력 (thermal) =====
    # 분당복합화력: KPX 신고 설비용량 기준(KOEN 분당 발전현황).
    #   1BLOCK = GT 77.75MW×5 + ST 185MW = 573.75MW
    #   2BLOCK = GT 77.75MW×3 + ST 115MW = 348.25MW
    # CG1~5=1블록 GT, CG6~8=2블록 GT 로 매핑(근사). CS1=1블록 ST, CS2=2블록 ST.
    "분당화력_CG1": {"capacity_mw": 77.75, "confidence": "확실", "source": "koen_bundang", "note": "1BLOCK GT"},
    "분당화력_CG2": {"capacity_mw": 77.75, "confidence": "확실", "source": "koen_bundang", "note": "1BLOCK GT"},
    "분당화력_CG3": {"capacity_mw": 77.75, "confidence": "확실", "source": "koen_bundang", "note": "1BLOCK GT"},
    "분당화력_CG4": {"capacity_mw": 77.75, "confidence": "확실", "source": "koen_bundang", "note": "1BLOCK GT"},
    "분당화력_CG5": {"capacity_mw": 77.75, "confidence": "확실", "source": "koen_bundang", "note": "1BLOCK GT"},
    "분당화력_CG6": {"capacity_mw": 77.75, "confidence": "근사", "source": "koen_bundang", "note": "2BLOCK GT(호기 매핑 근사)"},
    "분당화력_CG7": {"capacity_mw": 77.75, "confidence": "근사", "source": "koen_bundang", "note": "2BLOCK GT(호기 매핑 근사)"},
    "분당화력_CG8": {"capacity_mw": 77.75, "confidence": "근사", "source": "koen_bundang", "note": "2BLOCK GT(호기 매핑 근사)"},
    "분당화력_CS1": {"capacity_mw": 185.0, "confidence": "확실", "source": "koen_bundang", "note": "1BLOCK ST"},
    "분당화력_CS2": {"capacity_mw": 115.0, "confidence": "확실", "source": "koen_bundang", "note": "2BLOCK ST"},
    # 삼천포화력 (1·2호기는 2021.04 폐지)
    "삼천포_3": {"capacity_mw": 560.0, "confidence": "확실", "source": "koen_samcheonpo", "note": "3·4호기 560MW, 1993~94 준공"},
    "삼천포_4": {"capacity_mw": 560.0, "confidence": "확실", "source": "koen_samcheonpo", "note": "3·4호기 560MW"},
    "삼천포_5": {"capacity_mw": 500.0, "confidence": "확실", "source": "koen_samcheonpo", "note": "5·6호기 500MW, 1997~98 준공"},
    "삼천포_6": {"capacity_mw": 500.0, "confidence": "확실", "source": "koen_samcheonpo", "note": "5·6호기 500MW"},
    # 여수화력 (목재펠릿 전소)
    "여수_1": {"capacity_mw": 340.0, "confidence": "확실", "source": "koen_yeosu", "note": "순환유동층, 2017 목재펠릿 전환"},
    "여수_2": {"capacity_mw": 328.6, "confidence": "확실", "source": "koen_yeosu", "note": "시설용량 328.6MW, 2020 목재펠릿 전환"},
    # 영동에코발전 (100% 바이오매스)
    "영동_1": {"capacity_mw": 125.0, "confidence": "확실", "source": "koen_yeongdong", "note": "125MW, 2017 우드펠릿 전환"},
    "영동_2": {"capacity_mw": 200.0, "confidence": "확실", "source": "koen_yeongdong", "note": "200MW, 2020 준공"},
    # 영흥화력 (석탄). 1·2호기 800MW, 3~6호기 870MW. 합계 5,080MW
    "영흥_1": {"capacity_mw": 800.0, "confidence": "확실", "source": "koen_yeongheung", "note": "1·2호기 800MW, 2004 준공"},
    "영흥_2": {"capacity_mw": 800.0, "confidence": "확실", "source": "koen_yeongheung", "note": "1·2호기 800MW"},
    "영흥_3": {"capacity_mw": 870.0, "confidence": "확실", "source": "koen_yeongheung", "note": "3·4호기 870MW, 2008 준공"},
    "영흥_4": {"capacity_mw": 870.0, "confidence": "확실", "source": "koen_yeongheung", "note": "3·4호기 870MW"},
    "영흥_5": {"capacity_mw": 870.0, "confidence": "확실", "source": "koen_yeongheung", "note": "5·6호기 870MW, 2014 준공"},
    "영흥_6": {"capacity_mw": 870.0, "confidence": "확실", "source": "koen_yeongheung", "note": "5·6호기 870MW"},
}


# source 키 -> 출처 URL/문서명
SOURCES: dict[str, str] = {
    "koen_yeongheung": "https://www.koenergy.kr/kosep/hw/br/yh/yhhw04/main.do?menuCd=BR020203 (KOEN 영흥 발전현황)",
    "koen_samcheonpo": "https://www.koenergy.kr/kosep/hw/br/sp/sphw04/main.do?menuCd=BR010203 (KOEN 삼천포 발전현황)",
    "koen_bundang": "https://www.koenergy.kr/kosep/hw/br/bd/bdhw04/main.do?menuCd=BR060203 (KOEN 분당 발전현황)",
    "koen_yeosu": "https://www.koenergy.kr/kosep/hw/br/ys/yshw04/main.do?menuCd=BR050203 (KOEN 여수 발전현황)",
    "koen_yeongdong": "https://www.koenergy.kr/kosep/hw/br/yd/ydhw04/main.do?menuCd=BR040203 (KOEN 영동에코 발전현황)",
    "waterjournal": "https://www.waterjournal.co.kr/news/articleView.html?idxno=13450 (영흥 해양소수력 호기별)",
    "newenergy_yeosu_fc": "https://newenergyinfo.kr/49/?bmode=view&idx=11148107 (여수 연료전지)",
    "energy_news": "http://www.energy-news.co.kr/news/articleView.html?idxno=62984 (분당 연료전지 6단계 SOFC)",
    "edaily_koen_fc": "https://www.edaily.co.kr/news/read?newsId=04414886632233800 (남동발전 연료전지 71.71MW)",
}


def resolve_capacity(plant_name: str) -> Optional[Capacity]:
    """plant_name(발전구분_호기)으로 정격용량 정보를 조회. 미등록이면 None."""
    if not plant_name:
        return None
    return KOEN_PLANT_CAPACITIES.get(plant_name.strip())
