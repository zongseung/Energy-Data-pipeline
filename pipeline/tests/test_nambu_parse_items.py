"""남부 API 응답 파싱 — 레코드마다 <items> 태그가 따로 오는 포맷을 다 받는지."""
import xml.etree.ElementTree as ET

from pipeline.fetch_data.pv.nambu_backfill import _parse_items


def test_item_래퍼가_있는_포맷():
    root = ET.fromstring(
        "<response><body><items>"
        "<item><ymd>2026-08-01</ymd><gencd>997D</gencd></item>"
        "<item><ymd>2026-08-02</ymd><gencd>997D</gencd></item>"
        "</items></body></response>"
    )
    assert [r["ymd"] for r in _parse_items(root)] == ["2026-08-01", "2026-08-02"]


def test_item_래퍼가_없으면_items_를_전부_모은다():
    """실제 PwrSunLightInfo 응답 모양. find 를 쓰면 첫 건만 잡혀 나머지를 잃는다."""
    root = ET.fromstring(
        "<response><body>"
        "<items><ymd>2026-08-07</ymd><gencd>997D</gencd></items>"
        "<items><ymd>2026-08-06</ymd><gencd>997D</gencd></items>"
        "<items><ymd>2026-08-05</ymd><gencd>997D</gencd></items>"
        "</body></response>"
    )
    rows = _parse_items(root)
    assert len(rows) == 3
    assert [r["ymd"] for r in rows] == ["2026-08-07", "2026-08-06", "2026-08-05"]


def test_빈_응답():
    assert _parse_items(ET.fromstring("<response><body/></response>")) == []
