"""데모 LLM 쿼리 배터리 — 대표 질문을 실제 데모 API 루프로 돌려 실패 모드를 카탈로그한다.

사용법:
    python scripts/demo_query_battery.py [출력.md]

데모 스택(llm-demo, :8099)이 떠 있어야 한다. CPU 추론이라 전체 15~25분.
판정: 질문별 SQL 정규식(must/must_not) + 툴 에러 회복 여부 + 다운로드 링크 존재.
정규식으로 못 잡는 항목(환각 없는 빈 결과 답변 등)은 기록만 하고 사람이 본다.
"""

import json
import re
import sys
import urllib.request

BASE = "http://localhost:8099"
MAX_HOPS = 4

# sql_must / sql_must_not: 마지막으로 실행된 SQL에 대한 정규식 (대소문자 무시)
# recovered: 툴 에러가 났어도 최종적으로 성공 SQL이 실행되면 통과로 본다

# 기간 조건은 리터럴('2026-07')로도, EXTRACT 분해형으로도 쓸 수 있고 **둘 다 정답이다**.
# 리터럴만 인정하면 아래 SQL 이 오답 처리된다 — 실제로 1차 라운드에서
# period-bounds 가 이 이유로만 실패했다(SQL 자체는 완전히 옳았다).
#   WHERE EXTRACT(YEAR FROM timestamp)=2026 AND EXTRACT(MONTH FROM timestamp)=7
def _ym(year, month):
    """해당 연·월을 가리키는 모든 표기를 받아들이는 정규식."""
    return (rf"{year}-0?{month}\b"
            rf"|(?=.*\b{year}\b)(?=.*(?:month|mon)\b[^=]{{0,30}}=\s*0?{month}\b)")


def _ymd(year, month, day):
    # 제로패딩은 반드시 포맷 스펙으로 — '0{day}' 로 쓰면 day=10 일 때 '010' 이 돼
    # 아홉 자리 패턴('202608010')이 만들어져 영영 매치되지 않는다.
    return (rf"{year}-0?{month}-0?{day}\b"
            rf"|{year}{month:02d}{day:02d}")


CASES = [
    {
        "id": "agg-by-plant",
        "q": "2026년 7월 발전소별 태양광 발전량 상위 5곳을 표로 보여줘",
        "sql_must": [r"group\s+by", r"solar", _ym(2026, 7)],
    },
    {
        "id": "fuel-filter",
        "q": "태양광 발전량 데이터만 2026년 8월 1일부터 3일치 보여줘. 다른 연료는 빼고.",
        "sql_must": [r"fuel_type\s*=\s*'solar'"],
        "sql_must_not": [r"hydro", r"thermal"],
    },
    {
        "id": "period-bounds",
        "q": "2026년 7월 한 달간 태양광 총 발전량 알려줘",
        "sql_must": [_ym(2026, 7)],
        "sql_must_not": [r"\b2025\b"],
    },
    {
        # 원래 이 케이스는 "이중계상 없이" 를 물으며 is_aggregate 필터를 요구했다.
        # 그런데 확인해 보니 140(영암태양광)=2019~2021, 141·142=2022~ 로 기간이
        # 겹치지 않아 **이중계상이 애초에 없다**. 존재하지 않는 함정을 검사하고
        # 있었고, 오히려 그 필터를 걸면 2019~2021 영암 발전량이 사라진다.
        # 그래서 방향을 뒤집어 과잉 필터를 거는지 본다.
        "id": "aggregate-overfilter",
        "q": "2020년 영암태양광 발전량 총합을 알려줘",
        "sql_must": [r"\b2020\b", r"영암"],
        "sql_must_not": [r"is_aggregate", r"data_quality"],
    },
    {
        "id": "weather-join",
        "q": "2026년 8월 5일 서울 기온과 시간별 전체 태양광 발전량을 같이 보여줘",
        "sql_must": [r"weather_asos|temperature"],
    },
    {
        "id": "smp-daily",
        "q": "2026년 8월 10일 육지(land) SMP 시간별 가격을 보여줘",
        "sql_must": [r"smp_hourly", _ymd(2026, 8, 10)],
    },
    {
        "id": "jeju-demand",
        "q": "가장 최근 제주 전력수급 상황(태양광·풍력 포함)을 알려줘",
        "sql_must": [r"jeju_supply_demand"],
    },
    {
        "id": "csv-extract",
        "q": "2026년 8월 1일~10일 태양광 발전량 데이터를 csv로 줘",
        "sql_must": [r"select\s+\*"],
        "need_download": True,
    },
    {
        "id": "korean-codes",
        "q": "데이터 품질이 정상인 발전소가 몇 개야?",
        "sql_must": [r"'정상'"],
    },
    {
        "id": "round-recovery",
        "q": "2026년 8월 육지 SMP 평균 가격을 소수점 1자리로 알려줘",
        "recovered": True,  # round 캐스트 에러가 나도 힌트로 회복하면 통과
    },
    {
        "id": "hallucination-recovery",
        "q": "power_plants 테이블에서 발전소 목록 5개만 보여줘",
        "recovered": True,  # 없는 테이블 → 힌트(뷰 목록)로 회복하는지
    },
    {
        "id": "empty-future",
        "q": "2030년 1월 태양광 발전량을 알려줘",
        "manual": "환각 숫자 없이 '데이터 없음'이라 답하는지 육안 확인",
    },
    {
        "id": "wind-caveat",
        "q": "2026년 7월 풍력 시간별 발전량을 분석해줘",
        "manual": "시간규약 미확정(±1h) 경고를 언급하는지 육안 확인",
    },
    {
        "id": "monthly-trend",
        "q": "2026년 월별 태양광 발전량 추이를 표로 보여줘",
        "sql_must": [r"group\s+by", r"date_trunc|extract|to_char"],
    },
    # 아래 3개는 2차 라운드에서 추가 — ①②④ 수정의 회귀 검사다.
    {
        # ① 연료 필터: "태양광만" 을 명시하지 않아도 태양광 질문이면 필터가 붙어야 한다
        "id": "fuel-implicit",
        "q": "2026년 8월 태양광 발전량이 가장 많았던 날은?",
        "sql_must": [r"fuel_type\s*=\s*'solar'"],
    },
    {
        # ② demand 컬럼 함정: 공급능력은 supply_capacity 가 아니라 current_supply 다
        "id": "demand-column-trap",
        "q": "가장 최근 전국 전력 공급능력이 얼마야?",
        "sql_must": [r"current_supply"],
        "sql_must_not": [r"supply_capacity"],
    },
    {
        # ④ 예보 함수: 뷰가 아니라 함수를 써야 하고 인자 순서를 지켜야 한다
        "id": "forecast-fn",
        "q": "서울 개포1동의 2023년 1월 1시간기온 단기예보를 보여줘",
        "sql_must": [r"research\.forecast\s*\(", r"개포1동", r"1시간기온"],
    },
]


def post(path, payload, timeout=300):
    req = urllib.request.Request(
        BASE + path, json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req, timeout=timeout))


def run_case(tools, case):
    messages = [{"role": "user", "content": case["q"]}]
    sqls, errors, download = [], [], False
    final = ""
    for _ in range(MAX_HOPS):
        r = post("/v1/chat/completions",
                 {"messages": messages, "tools": tools, "max_tokens": 1200})
        m = r["choices"][0]["message"]
        messages.append(m)
        if not m.get("tool_calls"):
            final = m.get("content") or ""
            break
        for tc in m["tool_calls"]:
            args = json.loads(tc["function"]["arguments"])
            sql = args.get("query", "")
            out = post("/tools", {"tool": tc["function"]["name"], "params": args})
            content = out.get("plain_text_response") or out.get("error", "")
            failed = bool(out.get("error")) or content.startswith("Error")
            # (sql, 실패여부) 를 같이 남긴다 — 마지막 SQL 이 에러였는지 판정에 필요하다.
            sqls.append((sql, failed))
            if failed:
                errors.append(content[:200])
            if "download_url" in content:
                download = True
            messages.append({"role": "tool", "tool_call_id": tc.get("id", ""),
                             "content": content})
    return sqls, errors, download, final


def judge(case, sqls, errors, download):
    if case.get("manual"):
        return None, case["manual"]
    reasons = []
    if case.get("recovered"):
        # 에러가 났어도 마지막 툴 호출이 에러 없이 끝났으면 회복 성공
        ok = bool(sqls) and not sqls[-1][1]
        if not ok:
            reasons.append("에러에서 회복 못함")
        return ok, "; ".join(reasons)

    ok = bool(sqls)
    if not ok:
        reasons.append("SQL 실행 없음")
        return False, "; ".join(reasons)

    last_sql, last_failed = sqls[-1]
    # 정규식만 보고 판정하면 "패턴은 맞는데 실행은 에러난" SQL 이 통과한다.
    # 1차 라운드의 smp-daily 가 실제로 그렇게 통과했다(station_type 없는 컬럼).
    if last_failed:
        ok = False
        reasons.append("마지막 SQL 이 에러로 끝남")

    for pat in case.get("sql_must", []):
        if not re.search(pat, last_sql, re.I | re.S):
            ok = False
            reasons.append(f"필수 패턴 누락: {pat}")
    for pat in case.get("sql_must_not", []):
        if re.search(pat, last_sql, re.I | re.S):
            ok = False
            reasons.append(f"금지 패턴 존재: {pat}")
    if case.get("need_download") and not download:
        ok = False
        reasons.append("download_url 없음")
    return ok, "; ".join(reasons)


def main():
    out_path = sys.argv[1] if len(sys.argv) > 1 else "battery_report.md"
    tools = [t["definition"] for t in
             json.load(urllib.request.urlopen(BASE + "/tools"))]
    lines = ["# 데모 쿼리 배터리 결과\n"]
    passed = failed = manual = 0
    for case in CASES:
        print(f"[{case['id']}] 실행 중...", flush=True)
        try:
            sqls, errors, download, final = run_case(tools, case)
        except Exception as exc:  # 네트워크/타임아웃 — 카탈로그에 남기고 계속
            sqls, errors, download, final = [], [f"runner 예외: {exc}"], False, ""
        ok, note = judge(case, sqls, errors, download)
        if ok is None:
            manual += 1
            status = "🖐 육안"
        elif ok:
            passed += 1
            status = "✅ 통과"
        else:
            failed += 1
            status = "❌ 실패"
        print(f"  → {status} {note}", flush=True)
        lines += [f"\n## {case['id']} — {status}",
                  f"- 질문: {case['q']}",
                  f"- 판정: {note or '-'}"]
        # 루프 변수를 'failed' 로 두면 바깥의 실패 카운터를 덮어써서
        # 요약이 "실패 False" 로 찍힌다. 이름을 분리한다.
        for i, (sql, sql_failed) in enumerate(sqls):
            mark = " ⚠에러" if sql_failed else ""
            lines.append(f"- SQL{i+1}{mark}: `{sql[:300]}`")
        for e in errors:
            lines.append(f"- 에러: {e[:200]}")
        if final:
            lines.append(f"- 최종답(앞 300자): {final[:300]}")
    summary = f"\n**요약: 통과 {passed} / 실패 {failed} / 육안 {manual} (전체 {len(CASES)})**\n"
    lines.insert(1, summary)
    with open(out_path, "w") as f:
        f.write("\n".join(lines))
    print(summary)
    print(f"카탈로그: {out_path}")


if __name__ == "__main__":
    main()
