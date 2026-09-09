"""research.plants 좌표 검증.

지도에 찍기 전에 좌표가 말이 되는지 기계로 거른다. 동서발전 원본처럼
경위도가 뒤바뀐 채로 들어오는 소스가 실제로 있었다.
"""

from pipeline.fetch_data.common.coordinates import check_coordinates


def test_정상_좌표는_지적이_없다() -> None:
    rows = [{"plant_id": 1, "plant_name": "부산역 선상 주차장", "lat": 35.11583, "lon": 129.0429}]
    assert check_coordinates(rows) == []


def test_한국_밖_좌표를_잡는다() -> None:
    rows = [{"plant_id": 2, "plant_name": "엉뚱한발전소", "lat": 0.0, "lon": 0.0}]
    (finding,) = check_coordinates(rows)
    assert finding["plant_id"] == 2
    assert finding["code"] == "out_of_range"


def test_경위도가_뒤바뀐_행은_따로_표시한다() -> None:
    """단순 범위 이탈과 구분해야 고치는 방법이 달라진다 — 뒤바뀜은 두 값을 맞바꾸면 끝이다."""
    rows = [{"plant_id": 3, "plant_name": "동해바이오화력본부 태양광", "lat": 129.1453, "lon": 37.48313}]
    (finding,) = check_coordinates(rows)
    assert finding["code"] == "swapped"


def test_좌표가_없으면_누락으로_표시한다() -> None:
    rows = [
        {"plant_id": 4, "plant_name": "좌표없는풍력", "lat": None, "lon": None},
        {"plant_id": 5, "plant_name": "경도만있는풍력", "lat": None, "lon": 127.0},
    ]
    assert [f["code"] for f in check_coordinates(rows)] == ["missing", "missing"]


def test_좌표가_겹치는_발전소를_표시한다() -> None:
    rows = [
        {"plant_id": 6, "plant_name": "당진태양광", "lat": 37.05075, "lon": 126.5103},
        {"plant_id": 7, "plant_name": "당진화력수상태양광", "lat": 37.05075, "lon": 126.5103},
        {"plant_id": 8, "plant_name": "부산역 선상 주차장", "lat": 35.11583, "lon": 129.0429},
    ]
    codes = {f["plant_id"]: f["code"] for f in check_coordinates(rows)}
    assert codes == {6: "duplicate", 7: "duplicate"}


def test_오류와_확인용_지적을_구분한다() -> None:
    """범위 이탈·뒤바뀜은 데이터가 틀린 것이고, 누락·중복은 사람이 판단할 일이다."""
    rows = [
        {"plant_id": 10, "plant_name": "범위밖", "lat": 0.0, "lon": 0.0},
        {"plant_id": 11, "plant_name": "뒤바뀜", "lat": 129.1453, "lon": 37.48313},
        {"plant_id": 12, "plant_name": "누락", "lat": None, "lon": None},
        {"plant_id": 13, "plant_name": "중복A", "lat": 37.05075, "lon": 126.5103},
        {"plant_id": 14, "plant_name": "중복B", "lat": 37.05075, "lon": 126.5103},
    ]
    assert {f["code"]: f["severity"] for f in check_coordinates(rows)} == {
        "out_of_range": "error",
        "swapped": "error",
        "missing": "warning",
        "duplicate": "warning",
    }


def _live_plant_rows():
    """실제 research.plants 를 읽어 온다. DB 가 없으면 None (CI 에서는 건너뛴다)."""
    import psycopg2

    from pipeline.fetch_data.common.db_utils import resolve_db_url

    # resolve_db_url 은 SQLAlchemy 스킴(postgresql+psycopg2://)을 돌려준다.
    # psycopg2 는 그걸 못 읽으므로 드라이버 접미사를 뗀다.
    dsn = resolve_db_url().replace("+psycopg2", "")
    try:
        conn = psycopg2.connect(dsn, connect_timeout=3)
    except psycopg2.OperationalError:
        return None  # DB 가 없을 때만 건너뛴다 — 그 외 오류는 그대로 터뜨린다
    with conn, conn.cursor() as cur:
        cur.execute("SELECT plant_id, plant_name, lat, lon FROM research.plants")
        return [
            {"plant_id": pid, "plant_name": name, "lat": lat, "lon": lon}
            for pid, name, lat, lon in cur.fetchall()
        ]


def test_실제_발전소_좌표에_오류가_없다() -> None:
    """현 상태를 고정하는 회귀 가드. 새 수집기가 좌표를 망치면 여기서 걸린다."""
    rows = _live_plant_rows()
    if rows is None:
        import pytest

        pytest.skip("DB 접속 불가 — CI 에서는 건너뛴다")
    errors = [f for f in check_coordinates(rows) if f["severity"] == "error"]
    assert errors == []


def test_역지오코딩_주소에서_시도_시군구를_뽑는다() -> None:
    """Nominatim 은 지점마다 키가 다르다. 광역시는 province 가 없고 city 가 시도 자리에 온다."""
    from pipeline.fetch_data.common.coordinates import admin_region

    assert admin_region({"province": "경상북도", "city": "구미시"}) == ("경상북도", "구미시")
    assert admin_region({"city": "인천광역시", "county": "옹진군", "town": "영흥면"}) == ("인천광역시", "옹진군")
    assert admin_region({"province": "제주특별자치도", "city": "제주시", "town": "한경면"}) == ("제주특별자치도", "제주시")
    assert admin_region({"city": "부산광역시"}) == ("부산광역시", None)
    assert admin_region({}) == (None, None)


def test_시도가_아닌_시를_시도로_승격하지_않는다() -> None:
    """전남 일대는 province 없이 state 로 온다. state 를 안 보면 '여수시'가 시도가 된다."""
    from pipeline.fetch_data.common.coordinates import admin_region

    assert admin_region({"state": "전남광주통합특별시", "city": "여수시"}) == (
        "전남광주통합특별시", "여수시")
    assert admin_region({"state": "전남광주통합특별시", "county": "영암군", "town": "삼호읍"}) == (
        "전남광주통합특별시", "영암군")
    # 시도로 볼 수 없는 값만 있으면 승격하지 말고 비운다 — 틀린 값보다 빈 값이 낫다
    assert admin_region({"city": "여수시"}) == (None, None)
