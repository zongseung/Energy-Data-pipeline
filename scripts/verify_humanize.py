"""윤문 전후 대조 — 건드리면 안 되는 것이 실제로 안 바뀌었는지 기계적으로 확인한다.

    uv run python scripts/verify_humanize.py <원본.md> <윤문본.md>
    uv run python scripts/verify_humanize.py --dir _workspace/2026-08-13-001

윤문 에이전트의 자기 보고("코드는 안 건드렸습니다")를 그대로 믿으면 안 된다.
표 한 칸의 숫자가 바뀌면 문서와 DB 가 어긋나고, 그건 조용히 틀린 답으로 이어진다.
여기서 보는 것은 문체가 아니라 **불변이어야 할 것들**이다.

  코드블록  전체(주석 포함)가 바이트 단위로 같아야 한다
  표        모든 행이 그대로여야 한다 (셀 안 설명문도 계약 문서다)
  수치      본문에 등장하는 숫자 집합이 같아야 한다
  식별자    research.* / 백틱 식별자 / 링크 URL·앵커
  등급명    '정상' '시간별무효' '전면무효' '미검증' '구간시작' 등 규약 용어의 등장 횟수
"""

from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path

# 값이자 규약인 단어들. 동의어로 바뀌면 문서와 DB 가 어긋난다.
PROTECTED_TERMS = [
    "정상", "시간별무효", "전면무효", "미검증", "구간시작", "hour-ending",
    "이중계상", "FDW", "무보정", "읽기전용", "is_aggregate", "hourly_valid_from",
]

# 윤문 에이전트가 본문 끝에 붙이는 메타 블록. 문서의 일부가 아니므로 대조 전에 뗀다
# (안 떼면 그 안의 메트릭 숫자가 '추가된 수치'로 잡혀 전부 위반으로 뜬다).
SUMMARY_RE = re.compile(r"\n*<!--\s*HUMANIZE-SUMMARY.*?-->\s*\Z", re.S)
CODE_RE = re.compile(r"```.*?```", re.S)
TABLE_RE = re.compile(r"^\s*\|.*\|\s*$", re.M)
NUM_RE = re.compile(r"\d[\d,]*(?:\.\d+)?")
IDENT_RE = re.compile(r"`[^`\n]+`|research\.\w+|\bplant_id\b|\w+\.\w+\(\)")
LINK_RE = re.compile(r"\]\(([^)]+)\)")


def body(path: Path) -> str:
    """메타 블록을 뗀 문서 본문."""
    return SUMMARY_RE.sub("", path.read_text())


def facts(text: str) -> dict:
    return {
        "code": [c.strip() for c in CODE_RE.findall(text)],
        # 행 끝 공백·파일 끝 개행 차이는 렌더 결과가 같다. 정규화하지 않으면
        # 마지막 표 행이 매번 '변경'으로 잡혀 진짜 위반이 묻힌다.
        "table": [t.strip() for t in TABLE_RE.findall(text)],
        "num": Counter(NUM_RE.findall(CODE_RE.sub("", text))),
        "ident": Counter(IDENT_RE.findall(text)),
        "link": Counter(LINK_RE.findall(text)),
        "term": Counter({t: text.count(t) for t in PROTECTED_TERMS}),
        "head": re.findall(r"^#{1,6} .*$", text, re.M),
    }


def compare(src: Path, out: Path) -> list[str]:
    a, b = facts(body(src)), facts(body(out))
    bad = []

    for key, label in [("code", "코드블록"), ("table", "표 행"), ("head", "헤딩")]:
        if a[key] != b[key]:
            if len(a[key]) != len(b[key]):
                bad.append(f"{label} 개수 {len(a[key])} → {len(b[key])}")
            else:
                diff = [i for i, (x, y) in enumerate(zip(a[key], b[key])) if x != y]
                first = a[key][diff[0]].strip().replace("\n", " ")[:70]
                bad.append(f"{label} {len(diff)}개 변경 — 첫 건: {first}")

    for key, label in [("num", "수치"), ("ident", "식별자"),
                       ("link", "링크"), ("term", "규약용어")]:
        lost = a[key] - b[key]
        added = b[key] - a[key]
        if lost or added:
            detail = ", ".join(
                [f"-{k}×{v}" for k, v in list(lost.items())[:4]]
                + [f"+{k}×{v}" for k, v in list(added.items())[:4]])
            bad.append(f"{label} 불일치: {detail}")
    return bad


def rate(src: Path, out: Path) -> float:
    """줄 단위 변경률 — 과윤문 감시용."""
    import difflib
    a, b = body(src).splitlines(), body(out).splitlines()
    sm = difflib.SequenceMatcher(None, a, b)
    return (1 - sm.ratio()) * 100


def main() -> int:
    args = sys.argv[1:]
    pairs: list[tuple[Path, Path]] = []
    if args[:1] == ["--dir"]:
        d = Path(args[1])
        for src in sorted(d.glob("01_input_*.md")):
            out = d / src.name.replace("01_input_", "final_")
            if out.exists():
                pairs.append((src, out))
    else:
        pairs.append((Path(args[0]), Path(args[1])))

    if not pairs:
        print("대조할 쌍이 없다 (윤문 결과가 아직 안 나왔을 수 있다)")
        return 0

    failed = 0
    for src, out in pairs:
        bad = compare(src, out)
        name = src.name.replace("01_input_", "")
        if bad:
            failed += 1
            print(f"✗ {name}  (변경률 {rate(src, out):.1f}%)")
            for b in bad:
                print(f"    {b}")
        else:
            print(f"✓ {name}  (변경률 {rate(src, out):.1f}%) — 코드·표·수치·식별자·링크·규약용어 모두 보존")
    print(f"\n{len(pairs)}건 중 {len(pairs)-failed}건 통과, {failed}건 위반")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
