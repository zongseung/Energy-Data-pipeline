"""발전소 좌표 검증.

DB·수집기에 의존하지 않는 순수 함수라 CI(DB 없음)에서도 돈다.
지도에 찍기 전에 좌표가 말이 되는지 기계로 거르는 용도다.
"""

from __future__ import annotations

# 남한 경계. 남단 마라도(33.06), 북단 휴전선 부근(38.6),
# 서단 백령도(124.6), 동단 독도(131.87) 를 여유 있게 감싼다.
LAT_MIN, LAT_MAX = 33.0, 38.7
LON_MIN, LON_MAX = 124.5, 132.0

# error 는 데이터가 틀린 것이고, warning 은 사람이 판단할 일이다.
# 풍력 6기의 좌표 누락과 같은 부지의 좌표 중복은 둘 다 정상 상태라,
# 오류로 세면 검증이 상시 빨간불이 되어 아무도 안 보게 된다.
SEVERITY = {
    "out_of_range": "error",
    "swapped": "error",
    "missing": "warning",
    "duplicate": "warning",
}


def _finding(row: dict, code: str) -> dict:
    return {
        "plant_id": row["plant_id"],
        "plant_name": row["plant_name"],
        "code": code,
        "severity": SEVERITY[code],
        "lat": row["lat"],
        "lon": row["lon"],
    }


def check_coordinates(rows: list[dict]) -> list[dict]:
    """좌표가 수상한 행을 지적 목록으로 돌려준다. 정상이면 빈 리스트."""
    findings = []
    for row in rows:
        lat, lon = row["lat"], row["lon"]
        if lat is None or lon is None:
            findings.append(_finding(row, "missing"))
            continue
        if LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX:
            continue
        # 맞바꾸면 둘 다 제자리로 들어오면 뒤바뀜이다. 고치는 방법이
        # 다르므로(값 교환 한 번) 범위 이탈과 구분해서 알린다.
        swapped = LAT_MIN <= lon <= LAT_MAX and LON_MIN <= lat <= LON_MAX
        findings.append(_finding(row, "swapped" if swapped else "out_of_range"))

    # 좌표가 겹치는 발전소. 같은 부지의 다른 계열이면 정상이므로 오류가 아니라
    # 눈으로 확인할 목록이다 (당진태양광·당진화력수상태양광이 실제로 같은 점).
    flagged = {f["plant_id"] for f in findings}
    seen: dict[tuple, list[dict]] = {}
    for row in rows:
        if row["plant_id"] in flagged or row["lat"] is None or row["lon"] is None:
            continue
        seen.setdefault((row["lat"], row["lon"]), []).append(row)
    for group in seen.values():
        if len(group) < 2:
            continue
        findings.extend(_finding(row, "duplicate") for row in group)
    return findings


def admin_region(address: dict) -> tuple[str | None, str | None]:
    """역지오코딩 주소에서 (시도, 시군구) 를 뽑는다.

    Nominatim 은 지점마다 키가 다르다. 시도가 `province` 로 올 때도 `state` 로
    올 때도 있고, 광역시·특별시는 둘 다 없이 `city` 가 시도 자리에 온다.

    `city` 로 넘어갈 때는 이름이 실제로 시도인지 확인한다. 확인하지 않으면
    전남 일대처럼 시도 키가 비어 있는 지점에서 '여수시'가 시도로 승격된다.
    틀린 값을 넣느니 비워 두는 편이 낫다.
    """
    SIDO_SUFFIXES = ("특별시", "광역시", "특별자치시", "특별자치도", "도")

    sido = address.get("province") or address.get("state")
    if sido:
        below = ("city", "county", "town")
    else:
        city = address.get("city")
        sido = city if city and city.endswith(SIDO_SUFFIXES) else None
        below = ("county", "borough", "city_district", "town")
    if not sido:
        return None, None
    for key in below:
        if address.get(key):
            return sido, address[key]
    return sido, None
