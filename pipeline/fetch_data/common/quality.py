"""발전량 품질 검사.

파일이든 API 든 DB 든 `(plant, timestamp, gen_kwh)` 긴 형식으로 만든 뒤 여기 넣는다.
적재를 막지 않는다 — **발견 목록만 돌려준다.** 무엇을 버릴지는 부르는 쪽이 정한다.

2026-09-10 남부 파일 25종·중부 parquet 를 훑어 나온 실제 문제들로 항목을 정했다.
"""
from __future__ import annotations

import pandas as pd

__all__ = ["check", "CHECKS"]

CHECKS = (
    "중복",          # 같은 (발전소, 시각) 이 두 번  — 한경풍력 2023-01-30
    "음수",          # 발전량 < 0
    "용량초과",       # 설비용량을 넘는 값        — 중부 스파이크 67행
    "일부시간결손",    # 하루 시간 수 < 24
    "앞날복사",       # 앞날과 24시간이 완전히 같다 — 한경 2023-01-31 = 02-01
    "종일0지속",      # 하루종일 0 이 오래 이어진다 — 행원소수력(정상일 수 있다)
    "총량불일치",     # 원본 일 총량 != 시간별 합계
)


def _finding(name, df, **cols):
    if df.empty:
        return None
    out = df.copy()
    out.insert(0, "검사", name)
    return out.assign(**cols)


def check(
    df: pd.DataFrame,
    *,
    capacity_kw: dict | None = None,
    daily_total: pd.DataFrame | None = None,
    zero_run_days: int = 30,
) -> pd.DataFrame:
    """`df`(plant · timestamp · gen_kwh)를 훑어 발견 목록을 돌려준다.

    capacity_kw : {plant: 설비용량 kW}. 주면 용량초과를 본다.
    daily_total : plant · date · total. 파일의 `총량` 컬럼. 주면 합계를 대조한다.
    zero_run_days : 하루종일 0 이 이만큼 연속되면 잡는다.
    """
    need = {"plant", "timestamp", "gen_kwh"}
    missing = need - set(df.columns)
    if missing:
        raise ValueError(f"컬럼이 없다: {sorted(missing)}")

    d = df.copy()
    d["timestamp"] = pd.to_datetime(d["timestamp"])
    d["date"] = d["timestamp"].dt.date
    found = []

    dup = d[d.duplicated(["plant", "timestamp"], keep=False)]
    found.append(_finding("중복", dup[["plant", "timestamp", "gen_kwh"]]))

    neg = d[d["gen_kwh"] < 0]
    found.append(_finding("음수", neg[["plant", "timestamp", "gen_kwh"]]))

    if capacity_kw:
        cap = d["plant"].map(capacity_kw)
        over = d[cap.notna() & (d["gen_kwh"] > cap)].assign(용량=cap)
        found.append(_finding("용량초과", over[["plant", "timestamp", "gen_kwh", "용량"]]))

    hours = d.groupby(["plant", "date"]).size().rename("시간수").reset_index()
    short = hours[hours["시간수"] < 24]
    found.append(_finding("일부시간결손", short))

    # 앞날복사·종일0지속은 하루를 24칸으로 펴서 본다
    daily = d.pivot_table(index=["plant", "date"], columns=d["timestamp"].dt.hour,
                          values="gen_kwh", aggfunc="first")
    for plant, g in daily.groupby(level=0):
        g = g.sort_index()
        vals = g.fillna(-1).to_numpy()
        if len(vals) > 1:
            same = (vals[1:] == vals[:-1]).all(axis=1)
            live = g.sum(axis=1).to_numpy()[1:] > 0        # 둘 다 0 인 날은 뺀다
            hit = g.index.get_level_values("date")[1:][same & live]
            found.append(_finding("앞날복사", pd.DataFrame({"plant": plant, "date": hit})))

        zero = g.sum(axis=1).fillna(0) == 0
        runs, run, start = [], 0, None
        for day, z in zip(g.index.get_level_values("date"), zero):
            if z:
                run, start = run + 1, start if run else day
            else:
                if run >= zero_run_days:
                    runs.append({"plant": plant, "date": start, "연속일": run})
                run, start = 0, None
        if run >= zero_run_days:
            runs.append({"plant": plant, "date": start, "연속일": run})
        found.append(_finding("종일0지속", pd.DataFrame(runs)))

    if daily_total is not None:
        got = d.groupby(["plant", "date"])["gen_kwh"].sum().rename("합계").reset_index()
        t = daily_total.copy()
        t["date"] = pd.to_datetime(t["date"]).dt.date
        m = t.merge(got, on=["plant", "date"], how="inner")
        bad = m[(m["total"] - m["합계"]).abs() > 1]
        found.append(_finding("총량불일치", bad[["plant", "date", "total", "합계"]]))

    found = [f for f in found if f is not None and not f.empty]
    if not found:
        return pd.DataFrame(columns=["검사", "plant"])
    return pd.concat(found, ignore_index=True)
