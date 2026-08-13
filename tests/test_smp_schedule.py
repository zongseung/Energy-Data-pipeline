"""daily-smp-collection 스케줄 회귀 방지 — KPX 전날 데이터 공표 시각(09:00 KST) 계약."""
from prefect_flows.deploy import DEPLOYMENTS


def test_daily_smp_deployment_runs_at_0900_kst():
    spec = next(s for s in DEPLOYMENTS if s["name"] == "daily-smp-collection")
    assert spec["cron"] == "0 9 * * *"
