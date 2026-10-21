"""발전량 품질 검사 — 실제로 겪은 문제를 그대로 재현해 잡히는지 본다."""
import pandas as pd
import pytest

from pipeline.fetch_data.common.quality import check


def _day(plant, date, values):
    """하루 24시간을 긴 형식으로."""
    ts = pd.date_range(f"{date} 00:00", periods=24, freq="h")
    return pd.DataFrame({"plant": plant, "timestamp": ts, "gen_kwh": values})


def _flat(plant, date, v=1.0):
    return _day(plant, date, [v] * 24)


def test_깨끗하면_아무것도_안_나온다():
    df = pd.concat([_flat("A", "2024-01-01"), _flat("A", "2024-01-02", 2.0)])
    assert check(df).empty


def test_중복을_잡는다():
    """한경풍력 2023-01-30 처럼 같은 (발전소, 시각) 이 두 번."""
    df = pd.concat([_flat("A", "2024-01-01"), _flat("A", "2024-01-01", 9.0)])
    assert (check(df)["검사"] == "중복").sum() == 48


def test_음수를_잡는다():
    df = _day("A", "2024-01-01", [-1.0] + [1.0] * 23)
    assert (check(df)["검사"] == "음수").sum() == 1


def test_용량초과를_잡는다():
    """중부 인천 연료전지 1호기 7,192,739 같은 스파이크."""
    df = _day("A", "2024-01-01", [500.0] + [1.0] * 23)
    r = check(df, capacity_kw={"A": 100})
    assert (r["검사"] == "용량초과").sum() == 1
    assert r.loc[r["검사"] == "용량초과", "gen_kwh"].iloc[0] == 500.0


def test_시간이_모자란_날을_잡는다():
    """DB 남부 결손 1,624일이 이 모양이다."""
    df = _flat("A", "2024-01-01").head(20)
    r = check(df)
    assert r.loc[r["검사"] == "일부시간결손", "시간수"].iloc[0] == 20


def test_앞날복사를_잡는다():
    """한경 2023-01-31 과 02-01 이 24시간 전부 같았다."""
    df = pd.concat([_flat("A", "2024-01-01", 5.0), _flat("A", "2024-01-02", 5.0)])
    r = check(df)
    assert (r["검사"] == "앞날복사").sum() == 1


def test_둘_다_0인_날은_앞날복사로_안_친다():
    df = pd.concat([_flat("A", "2024-01-01", 0.0), _flat("A", "2024-01-02", 0.0)])
    assert "앞날복사" not in set(check(df)["검사"])


def test_종일0이_오래_이어지면_잡는다():
    """행원소수력(60kW 해양소수력)이 2022 년 내내 0 이었다."""
    days = pd.date_range("2024-01-01", periods=40)
    df = pd.concat([_flat("A", d.date(), 0.0) for d in days])
    r = check(df, zero_run_days=30)
    assert r.loc[r["검사"] == "종일0지속", "연속일"].iloc[0] == 40


def test_짧은_0구간은_안_잡는다():
    days = pd.date_range("2024-01-01", periods=5)
    df = pd.concat([_flat("A", d.date(), 0.0) for d in days])
    assert "종일0지속" not in set(check(df, zero_run_days=30)["검사"])


def test_총량불일치를_잡는다():
    """파일의 총량 컬럼과 시간별 합계 대조. 한경풍력 2021-09-26 이 그랬다."""
    df = _flat("A", "2024-01-01", 1.0)          # 합계 24
    tot = pd.DataFrame([{"plant": "A", "date": "2024-01-01", "total": 999.0}])
    r = check(df, daily_total=tot)
    assert (r["검사"] == "총량불일치").sum() == 1


def test_컬럼이_없으면_바로_알려준다():
    with pytest.raises(ValueError, match="컬럼이 없다"):
        check(pd.DataFrame({"plant": ["A"]}))
