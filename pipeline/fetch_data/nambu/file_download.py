"""공공데이터포털 남부발전 파일데이터 내려받기.

포털 파일데이터는 로그인·API키가 필요 없다. 데이터셋 페이지 HTML 에 박힌
`atchFileId` 를 긁어 `fileDownload.do` 로 부르면 CSV 가 그대로 온다.

원본이 CP949 라 저장할 때 UTF-8 로 바꾼다 — 나중에 읽는 쪽이 인코딩을 몰라도 되게.

사용:
    uv run python -m pipeline.fetch_data.nambu.file_download            # 전부
    uv run python -m pipeline.fetch_data.nambu.file_download 15043410   # 일부
"""
from __future__ import annotations

import asyncio
import re
import sys
from pathlib import Path

import aiohttp

from pipeline.fetch_data.common.paths import DATA_DIR
from pipeline.fetch_data.common.logger import get_logger

logger = get_logger(__name__)

PAGE = "https://www.data.go.kr/data/{pk}/fileData.do"
DOWNLOAD = "https://www.data.go.kr/cmm/cmm/fileDownload.do"
UA = {"User-Agent": "Mozilla/5.0"}
OUT_DIR = DATA_DIR / "nambu"
MASTER_DIR = OUT_DIR / "master"

# 발전소별 시간별 실적. 컬럼은 모두 같다:
#   년월일 · 호기(풍력은 단계) · 총량 · 평균 · 최대 · 최소 · 최대(시간별) · 최소(시간별) · 1~24
DATASETS: dict[str, str] = {
    # 태양광 — 우리 generation 에 이미 있는 발전소 (과거 백필·대조용)
    "15043391": "삼척소내", "15043386": "부산본부", "15043388": "부산신항",
    "15043385": "부산복합자재창고", "15043384": "남제주소내", "15043394": "신인천소내",
    "15043387": "부산수처리장", "15043389": "부산운동장", "15043395": "신인천전망대",
    "15043396": "신인천해수구취수구",
    # 태양광 — 우리에게 없는 발전소 (신규)
    "15043407": "하동본부", "15043405": "하동변전소", "15043406": "하동보건소",
    "15043408": "하동정수장", "15043404": "하동공설운동장", "15043409": "하동하수처리장",
    "15043411": "행원소수력", "15043399": "영월본부", "15043402": "인천수산정수장",
    "15043400": "영월철도부지", "15043401": "위미2리", "15043390": "삼척사택",
    "15043397": "안동소내",
    # 풍력 — 한경은 우리 것보다 1년 더 길고 단계가 분리돼 있다. 성산은 신규.
    "15043410": "한경풍력", "15043393": "성산풍력",
}

# 발전소·사업소 마스터. 스키마가 제각각이고 시간축이 없어 폴더를 나눈다
# (시간별 실적을 glob 하는 쪽이 이걸 집어 들면 깨진다).
MASTERS: dict[str, str] = {
    "15030079": "태양광발전기사양",   # 발전소명·주소지·설치용량·설치각
    "15003550": "사업소현황",        # 본부 8곳 — 위치·설비용량·연료·준공일
    "15012779": "풍황계측기",        # 준공년도·발전소명·발전원·용량·위치
    "15127550": "사업소별발전실적",   # 년도·발전원·플랜트·호기·용량·발전량 (2021~2025)
}


def extract_file_id(html: str) -> str | None:
    """데이터셋 페이지에서 첨부파일 id 를 뽑는다."""
    m = re.search(r"atchFileId=(FILE_\d+)", html)
    return m.group(1) if m else None


async def _one(session: aiohttp.ClientSession, pk: str, name: str, out_dir: Path) -> int:
    async with session.get(PAGE.format(pk=pk), headers=UA, timeout=30) as r:
        file_id = extract_file_id(await r.text())
    if not file_id:
        logger.warning(f"[{name}] atchFileId 를 못 찾았다 (pk={pk})")
        return 0

    params = {"atchFileId": file_id, "fileDetailSn": "1"}
    async with session.get(DOWNLOAD, params=params, headers=UA, timeout=120) as r:
        raw = await r.read()

    path = out_dir / f"{pk}_{name}.csv"
    path.write_text(raw.decode("cp949", errors="replace"), encoding="utf-8")
    lines = raw.count(b"\n")
    logger.info(f"[{name}] {lines:,}줄 {len(raw)/1024:,.0f}KB -> {path.name}")
    return lines


async def run(pks: list[str] | None = None, out_dir: Path = OUT_DIR) -> int:
    master_dir = out_dir / "master"
    out_dir.mkdir(parents=True, exist_ok=True)
    master_dir.mkdir(parents=True, exist_ok=True)
    every = {**DATASETS, **MASTERS}
    targets = {k: v for k, v in every.items() if pks is None or k in pks}
    # 포털이 동시 접속에 민감해 6개로 묶는다
    conn = aiohttp.TCPConnector(limit=6)
    async with aiohttp.ClientSession(connector=conn) as s:
        counts = await asyncio.gather(
            *[_one(s, pk, nm, master_dir if pk in MASTERS else out_dir)
              for pk, nm in targets.items()],
            return_exceptions=True,
        )
    ok = [c for c in counts if isinstance(c, int) and c]
    for c in counts:
        if isinstance(c, Exception):
            logger.error(f"내려받기 실패: {c}")
    logger.info(f"완료 — {len(ok)}/{len(targets)}개, 합계 {sum(ok):,}줄")
    return sum(ok)


if __name__ == "__main__":
    asyncio.run(run(sys.argv[1:] or None))
