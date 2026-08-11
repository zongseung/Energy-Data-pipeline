"""
Prefect Deployment Script

PV 파이프라인 Flow를 배포하고 스케줄을 등록합니다.
- daily-weather-collection-flow: 매일 오전 9시 (기상 데이터)
"""

import asyncio
import os
import time

from prefect.client.orchestration import get_client
from prefect.deployments import Deployment
from prefect.client.schemas.schedules import CronSchedule
from prefect.client.schemas.actions import WorkPoolCreate
from prefect.utilities.importtools import import_object


# =======================================================================
# 환경 설정
# =======================================================================

PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://prefect-server-new:4200/api")
DOCKER_NETWORK = os.getenv("PREFECT_DOCKER_NETWORK", "pv-pipeline-network")
PV_DATABASE_URL = os.getenv(
    "PV_DATABASE_URL",
    "postgresql+psycopg2://pv:pv@pv-db:5432/pv"
)

NAMDONG_WIND_KEY = os.getenv("NAMDONG_WIND_KEY", "")
SERVICE_KEY = os.getenv("SERVICE_KEY") or NAMDONG_WIND_KEY
if not SERVICE_KEY:
    print("[WARN] SERVICE_KEY가 설정되지 않았습니다. 기상 데이터 수집 시 오류가 발생할 수 있습니다.")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
NAMDONG_START_DATE = os.getenv("NAMDONG_START_DATE", "")
NAMDONG_ORG_NO = os.getenv("NAMDONG_ORG_NO", "")
NAMDONG_HOKI_S = os.getenv("NAMDONG_HOKI_S", "")
NAMDONG_HOKI_E = os.getenv("NAMDONG_HOKI_E", "")
NAMDONG_PAGE_INDEX = os.getenv("NAMDONG_PAGE_INDEX", "")
NAMDONG_OUTPUT_DIR = os.getenv("NAMDONG_OUTPUT_DIR", "")
NAMBU_API_KEY = os.getenv("NAMBU_API_KEY", "")
if not NAMBU_API_KEY:
    print("[WARN] NAMBU_API_KEY가 설정되지 않았습니다. 남부발전 PV 수집 시 오류가 발생할 수 있습니다.")

# SMP 개인 DB 백업용(선택). 미설정 시 weekly 백업 flow가 자동 skip.
SMP_LEGACY_DB_URL = os.getenv("SMP_LEGACY_DB_URL", "")
# demand-postgres (제주 수급 → FDW 소비용). 컨테이너 내부 호스트명/포트.
DEMAND_DB_URL = os.getenv("DEMAND_DB_URL", "postgresql+psycopg2://demand:demand@demand-postgres:5432/demand")

os.environ.setdefault("PREFECT_API_URL", PREFECT_API_URL)


# =======================================================================
# 공통 인프라 설정
# =======================================================================

def get_job_variables():
    """Docker work pool job variables를 반환합니다."""
    return {
        "image": "pv-pipeline:latest",
        "image_pull_policy": "Never",
        "auto_remove": True,
        "env": {
            "PREFECT_API_URL": PREFECT_API_URL,
            "SERVICE_KEY": SERVICE_KEY,
            "PV_DATABASE_URL": PV_DATABASE_URL,
            "DB_URL": PV_DATABASE_URL,
            "DEMAND_DB_URL": DEMAND_DB_URL,
            "SLACK_WEBHOOK_URL": SLACK_WEBHOOK_URL,
            "NAMDONG_WIND_KEY": NAMDONG_WIND_KEY,
            "NAMDONG_START_DATE": NAMDONG_START_DATE,
            "NAMDONG_ORG_NO": NAMDONG_ORG_NO,
            "NAMDONG_HOKI_S": NAMDONG_HOKI_S,
            "NAMDONG_HOKI_E": NAMDONG_HOKI_E,
            "NAMDONG_PAGE_INDEX": NAMDONG_PAGE_INDEX,
            "NAMDONG_OUTPUT_DIR": NAMDONG_OUTPUT_DIR,
            "NAMBU_API_KEY": NAMBU_API_KEY,
            "SMP_LEGACY_DB_URL": SMP_LEGACY_DB_URL,
            "TZ": "Asia/Seoul",
        },
        "networks": [DOCKER_NETWORK, "weather-pipeline_prefect-new"],
        "volumes": [
            "/mnt/nvme/Energy-Data-pipeline/data:/app/data",
            "/mnt/iscsi-renewable/jeju_data:/mnt/iscsi-renewable/jeju_data",
        ],
    }


# =======================================================================
# 유틸리티 함수
# =======================================================================

async def wait_for_api(timeout: int = 120) -> None:
    """Prefect API가 살아날 때까지 대기합니다."""
    start = time.time()
    while True:
        try:
            async with get_client() as client:
                await client.api_healthcheck()
            print("Prefect API 연결 성공")
            return
        except Exception as e:
            if time.time() - start > timeout:
                raise RuntimeError("Prefect API 연결 시간 초과") from e
            print(f"Prefect API 대기 중... ({e!r})")
            await asyncio.sleep(5)


async def ensure_work_pool(pool_name: str = "pv-pool") -> None:
    """Docker 타입 work pool을 생성하거나 template을 업데이트합니다."""
    from prefect_docker.worker import DockerWorker

    # 표준 Docker worker template + 기본값 설정
    base_job_template = DockerWorker.get_default_base_job_template()
    base_job_template["variables"]["properties"]["image"]["default"] = "pv-pipeline:latest"
    base_job_template["variables"]["properties"]["image_pull_policy"]["default"] = "Never"

    async with get_client() as client:
        try:
            pool = await client.read_work_pool(work_pool_name=pool_name)
            print(f"Work pool '{pool_name}' 이미 존재 (타입: {pool.type}) - template 업데이트 중...")
            # 기존 풀도 항상 template 업데이트 (image_pull_policy: Never 보장)
            await client._client.patch(
                f"/work_pools/{pool_name}",
                json={"base_job_template": base_job_template},
            )
            print(f"Work pool '{pool_name}' template 업데이트 완료")
        except Exception:
            await client.create_work_pool(
                WorkPoolCreate(
                    name=pool_name,
                    type="docker",
                    description="PV 파이프라인용 Docker 워크 풀",
                    base_job_template=base_job_template,
                )
            )
            print(f"Work pool '{pool_name}' 생성 완료")


# =======================================================================
# 배포 함수
# =======================================================================

async def deploy_weather_flow() -> None:
    """일일 기상 데이터 수집 플로우 배포"""
    flow = import_object(
        "prefect_flows.prefect_pipeline.daily_weather_collection_flow"
    )

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="daily-weather-collection",
        work_pool_name="pv-pool",
        path="/app",
        entrypoint="prefect_flows/prefect_pipeline.py:daily_weather_collection_flow",
        parameters={"target_date": None},
        schedules=[
            CronSchedule(
                cron="0 9 * * *",  # 매일 오전 9시
                timezone="Asia/Seoul",
            )
        ],
        tags=["weather", "daily"],
        description="매일 오전 9시에 전날 기상 데이터를 수집, 처리, 저장",
        job_variables=get_job_variables(),
    )

    await deployment.apply()
    print("Deployment 완료: 'daily-weather-collection' (매일 09:00)")


async def deploy_namdong_flow() -> None:
    """남동발전 PV 수집 플로우 배포"""
    flow = import_object(
        "prefect_flows.namdong_pv_flow.monthly_namdong_pv_flow"
    )

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="monthly-namdong-pv-collection",
        work_pool_name="pv-pool",
        path="/app",
        entrypoint="prefect_flows/namdong_pv_flow.py:monthly_namdong_pv_flow",
        parameters={"target_start": None, "target_end": None, "sleep_sec": 5},
        schedules=[
            CronSchedule(
                cron="0 10 10 * *",  # 매월 10일 오전 10시
                timezone="Asia/Seoul",
            )
        ],
        tags=["pv", "namdong", "monthly"],
        description="매월 10일 오전 10시에 전월 남동발전 PV 데이터를 수집/백필",
        job_variables=get_job_variables(),
    )

    await deployment.apply()
    print("Deployment 완료: 'monthly-namdong-pv-collection' (매월 10일 10:00)")


async def deploy_nambu_flow() -> None:
    """남부발전 PV 수집 플로우 배포"""
    flow = import_object(
        "prefect_flows.nambu_pv_flow.daily_nambu_collection_flow"
    )

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="daily-nambu-pv-collection",
        work_pool_name="pv-pool",
        path="/app",
        entrypoint="prefect_flows/nambu_pv_flow.py:daily_nambu_collection_flow",
        parameters={},
        schedules=[
            CronSchedule(
                cron="30 9 * * *",  # 매일 오전 9시 30분
                timezone="Asia/Seoul",
            )
        ],
        tags=["pv", "nambu", "daily"],
        description="매일 오전 9시 30분에 남부발전 PV 데이터를 수집/백필",
        job_variables=get_job_variables(),
    )

    await deployment.apply()
    print("Deployment 완료: 'daily-nambu-pv-collection' (매일 09:30)")


async def deploy_namdong_wind_flow() -> None:
    """남동발전 풍력 수집 플로우 배포"""
    flow = import_object(
        "prefect_flows.namdong_wind_flow.monthly_namdong_wind_flow"
    )

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="monthly-namdong-wind-collection",
        work_pool_name="pv-pool",
        path="/app",
        entrypoint="prefect_flows/namdong_wind_flow.py:monthly_namdong_wind_flow",
        parameters={"target_start": None, "target_end": None},
        schedules=[
            CronSchedule(
                cron="0 11 10 * *",  # 매월 10일 오전 11시
                timezone="Asia/Seoul",
            )
        ],
        tags=["wind", "namdong", "monthly"],
        description="매월 10일 오전 11시에 전월 남동발전 풍력 데이터를 수집",
        job_variables=get_job_variables(),
    )

    await deployment.apply()
    print("Deployment 완료: 'monthly-namdong-wind-collection' (매월 10일 11:00)")


async def deploy_gen_monthly_flow() -> None:
    """KOEN 비태양광(화력/연료전지/해양소수력) 월별 수집 플로우 배포"""
    flow = import_object("prefect_flows.gen_flow.monthly_gen_flow")

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="monthly-koen-gen-collection",
        work_pool_name="pv-pool",
        path="/app",
        entrypoint="prefect_flows/gen_flow.py:monthly_gen_flow",
        parameters={"gen_keys": None, "mode": "latest"},
        schedules=[
            CronSchedule(
                cron="0 10 10 * *",  # 매월 10일 오전 10시
                timezone="Asia/Seoul",
            )
        ],
        tags=["gen", "koen", "namdong", "monthly"],
        description="매월 10일 10:00에 전월 KOEN 비태양광(화력/연료전지/해양소수력)을 "
                    "수집·변환해 generation 코어에 적재",
        job_variables=get_job_variables(),
    )

    await deployment.apply()
    print("Deployment 완료: 'monthly-koen-gen-collection' (매월 10일 10:00)")


async def deploy_smp_flow() -> None:
    """하루전시장 시간별 SMP 수집 플로우 배포"""
    flow = import_object("prefect_flows.smp_flow.daily_smp_collection_flow")

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="daily-smp-collection",
        work_pool_name="pv-pool",
        path="/app",
        entrypoint="prefect_flows/smp_flow.py:daily_smp_collection_flow",
        parameters={},
        schedules=[CronSchedule(cron="0 9 * * *", timezone="Asia/Seoul")],  # 매일 09:00
        tags=["smp", "daily"],
        description="매일 09:00에 전날 하루전시장 시간별 SMP(육지/제주) + 일별 가중평균 수집",
        job_variables=get_job_variables(),
    )

    await deployment.apply()
    print("Deployment 완료: 'daily-smp-collection' (매일 09:00, 전날 데이터)")


async def deploy_smp_aggregate_flow() -> None:
    """월/연 가중평균 SMP 수집 플로우 배포"""
    flow = import_object("prefect_flows.smp_flow.monthly_smp_aggregate_flow")

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="monthly-smp-aggregate",
        work_pool_name="pv-pool",
        path="/app",
        entrypoint="prefect_flows/smp_flow.py:monthly_smp_aggregate_flow",
        parameters={},
        schedules=[CronSchedule(cron="0 7 2 * *", timezone="Asia/Seoul")],  # 매월 2일 07:00
        tags=["smp", "aggregate", "monthly"],
        description="매월 2일 07:00에 월별/연도별 공식 가중평균 SMP 수집",
        job_variables=get_job_variables(),
    )

    await deployment.apply()
    print("Deployment 완료: 'monthly-smp-aggregate' (매월 2일 07:00)")


async def deploy_smp_realtime_jeju_flow() -> None:
    """제주 실시간 15분 SMP 수집 플로우 배포"""
    flow = import_object("prefect_flows.smp_flow.daily_smp_realtime_jeju_flow")

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="daily-smp-realtime-jeju",
        work_pool_name="pv-pool",
        path="/app",
        entrypoint="prefect_flows/smp_flow.py:daily_smp_realtime_jeju_flow",
        parameters={},
        schedules=[CronSchedule(cron="0 19 * * *", timezone="Asia/Seoul")],  # 매일 19:00
        tags=["smp", "realtime", "jeju", "daily"],
        description="매일 19:00에 제주 실시간시장 15분 SMP(전일 확정) 수집",
        job_variables=get_job_variables(),
    )

    await deployment.apply()
    print("Deployment 완료: 'daily-smp-realtime-jeju' (매일 19:00)")


async def deploy_jeju_realtime_flow() -> None:
    """제주 계통수급 실시간 수집 플로우 배포 (5분마다)"""
    flow = import_object("prefect_flows.jeju_flow.jeju_realtime_flow")

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="jeju-realtime-collection",
        work_pool_name="pv-pool",
        path="/app",
        entrypoint="prefect_flows/jeju_flow.py:jeju_realtime_flow",
        parameters={},
        schedules=[CronSchedule(cron="*/5 * * * *", timezone="Asia/Seoul")],
        tags=["jeju", "realtime"],
        description="5분마다 제주 계통수급 실시간 수집 (공급능력/수요/신재생)",
        job_variables=get_job_variables(),
    )

    await deployment.apply()
    print("Deployment 완료: 'jeju-realtime-collection' (매 5분)")


async def deploy_jeju_sukub_monthly_flow() -> None:
    """제주 수급 월별 백필 플로우 배포"""
    flow = import_object("prefect_flows.jeju_flow.jeju_sukub_monthly_flow")

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="jeju-sukub-monthly-collection",
        work_pool_name="pv-pool",
        path="/app",
        entrypoint="prefect_flows/jeju_flow.py:jeju_sukub_monthly_flow",
        parameters={"target_month": None},
        schedules=[CronSchedule(cron="0 1 1 * *", timezone="Asia/Seoul")],  # 매월 1일 01:00
        tags=["jeju", "sukub", "monthly"],
        description="매월 1일 01:00 전월 제주 계통수급 5분 데이터 백필",
        job_variables=get_job_variables(),
    )

    await deployment.apply()
    print("Deployment 완료: 'jeju-sukub-monthly-collection' (매월 1일 01:00)")


async def deploy_jeju_gen_monthly_flow() -> None:
    """제주 연료원별 거래량 수집 플로우 배포"""
    flow = import_object("prefect_flows.jeju_flow.jeju_gen_monthly_flow")

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="jeju-gen-monthly-collection",
        work_pool_name="pv-pool",
        path="/app",
        entrypoint="prefect_flows/jeju_flow.py:jeju_gen_monthly_flow",
        parameters={},
        schedules=[CronSchedule(cron="0 2 1 * *", timezone="Asia/Seoul")],  # 매월 1일 02:00
        tags=["jeju", "gen", "monthly"],
        description="매월 1일 02:00 제주 연료원별 전력거래량 연간 파일 수집",
        job_variables=get_job_variables(),
    )

    await deployment.apply()
    print("Deployment 완료: 'jeju-gen-monthly-collection' (매월 1일 02:00)")


async def deploy_jeju_demand_flow() -> None:
    """제주 시간별 전력수요 분기 수집 플로우 배포"""
    flow = import_object("prefect_flows.jeju_flow.jeju_demand_flow")

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="jeju-demand-quarterly-collection",
        work_pool_name="pv-pool",
        path="/app",
        entrypoint="prefect_flows/jeju_flow.py:jeju_demand_flow",
        parameters={"from_sukub": False},
        schedules=[CronSchedule(cron="0 3 1 1,4,7,10 *", timezone="Asia/Seoul")],  # 분기 첫날 03:00
        tags=["jeju", "demand", "quarterly"],
        description="분기 첫날 03:00 data.go.kr 15065239 시간별 제주 전력수요 수집",
        job_variables=get_job_variables(),
    )

    await deployment.apply()
    print("Deployment 완료: 'jeju-demand-quarterly-collection' (1/4/7/10월 1일 03:00)")


async def deploy_jeju_supply_demand_db_flow() -> None:
    """제주 수급 → demand DB 동기화 플로우 배포 (10분마다, energy_hub FDW 소비용)"""
    flow = import_object("prefect_flows.jeju_flow.jeju_supply_demand_db_flow")

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="jeju-supply-demand-db-sync",
        work_pool_name="pv-pool",
        path="/app",
        entrypoint="prefect_flows/jeju_flow.py:jeju_supply_demand_db_flow",
        parameters={},
        schedules=[CronSchedule(cron="*/10 * * * *", timezone="Asia/Seoul")],  # 10분마다
        tags=["jeju", "demand", "fdw"],
        description="10분마다 제주 수급 5분 CSV를 demand-postgres로 동기화 (energy_hub가 FDW로 소비)",
        job_variables=get_job_variables(),
    )

    await deployment.apply()
    print("Deployment 완료: 'jeju-supply-demand-db-sync' (매 10분)")


async def deploy_unified_demand_flow() -> None:
    """전국 5분 수요 수집 및 시간별 수요-기상 집계 플로우 배포"""
    flow = import_object("prefect_flows.demand_flow.unified_demand_collection_flow")

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="unified-demand-collection",
        work_pool_name="pv-pool",
        path="/app",
        entrypoint="prefect_flows/demand_flow.py:unified_demand_collection_flow",
        parameters={"force_hourly": False},
        schedules=[
            CronSchedule(cron="*/10 * * * *", timezone="Asia/Seoul")
        ],
        tags=["demand", "weather", "hourly"],
        description="10분마다 전국 5분 수요를 수집하고 매시 수요-기상 집계 및 뷰를 새로고침",
        job_variables=get_job_variables(),
    )

    await deployment.apply()
    print("Deployment 완료: 'unified-demand-collection' (매 10분)")


async def deploy_ekr_pv_flow() -> None:
    """한국농어촌공사 영암/율치 PV 연간 수집 플로우 배포 (odcloud 15005796)"""
    flow = import_object("prefect_flows.ekr_pv_flow.yearly_ekr_pv_flow")

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="yearly-ekr-pv-collection",
        work_pool_name="pv-pool",
        path="/app",
        entrypoint="prefect_flows/ekr_pv_flow.py:yearly_ekr_pv_flow",
        parameters={},
        schedules=[CronSchedule(cron="0 4 8 1 *", timezone="Asia/Seoul")],  # 매년 1월 8일 04:00 (차기 등록 2027-01-06 이후)
        tags=["pv", "ekr", "yearly"],
        description="매년 1월 한국농어촌공사 영암/율치 PV(odcloud 15005796) 전체 연도 멱등 수집 → generation 코어",
        job_variables=get_job_variables(),
    )

    await deployment.apply()
    print("Deployment 완료: 'yearly-ekr-pv-collection' (매년 1월 8일 04:00)")


async def deploy_smp_legacy_sync_flow() -> None:
    """SMP 개인 DB 백업 동기화 플로우 배포"""
    flow = import_object("prefect_flows.smp_flow.weekly_smp_legacy_sync_flow")

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="weekly-smp-legacy-sync",
        work_pool_name="pv-pool",
        path="/app",
        entrypoint="prefect_flows/smp_flow.py:weekly_smp_legacy_sync_flow",
        parameters={},
        schedules=[CronSchedule(cron="0 7 * * 1", timezone="Asia/Seoul")],  # 매주 월 07:00
        tags=["smp", "backup", "weekly"],
        description="매주 월요일 07:00에 공통 DB -> 개인 DB(SMP_LEGACY_DB_URL) 백업 동기화",
        job_variables=get_job_variables(),
    )

    await deployment.apply()
    print("Deployment 완료: 'weekly-smp-legacy-sync' (매주 월 07:00)")


# =======================================================================
# 메인 실행
# =======================================================================

async def create_all_deployments() -> None:
    """모든 배포를 생성합니다."""
    print("\n" + "=" * 60)
    print("Prefect Deployment 시작")
    print("=" * 60 + "\n")

    # API 대기 및 Work Pool 생성
    await wait_for_api()
    await ensure_work_pool("pv-pool")

    print("\n--- Flow 배포 ---\n")

    # 각 Flow 배포
    await deploy_weather_flow()
    await deploy_namdong_flow()
    await deploy_nambu_flow()
    await deploy_namdong_wind_flow()
    await deploy_gen_monthly_flow()
    await deploy_smp_flow()
    await deploy_smp_aggregate_flow()
    await deploy_smp_realtime_jeju_flow()
    await deploy_smp_legacy_sync_flow()
    await deploy_jeju_realtime_flow()
    await deploy_jeju_sukub_monthly_flow()
    await deploy_jeju_gen_monthly_flow()
    await deploy_jeju_demand_flow()
    await deploy_jeju_supply_demand_db_flow()
    await deploy_unified_demand_flow()
    await deploy_ekr_pv_flow()

    print("\n" + "=" * 60)
    print("모든 Deployment 완료!")
    print("=" * 60 + "\n")

    # 배포 요약
    print("배포된 Flow:")
    print("  1. daily-weather-collection        - 매일 09:00 (기상 데이터)")
    print("  3. monthly-namdong-pv-collection   - 매월 10일 10:00 (남동발전 PV)")
    print("  4. daily-nambu-pv-collection       - 매일 09:30 (남부발전 PV)")
    print("  5. monthly-namdong-wind-collection - 매월 10일 11:00 (남동발전 풍력)")
    print("  6. daily-smp-collection            - 매일 09:00 (전날 SMP 시간별+일별)")
    print("  7. monthly-smp-aggregate           - 매월 2일 07:00 (SMP 월/연 가중평균)")
    print("  8. daily-smp-realtime-jeju         - 매일 19:00 (제주 실시간 15분 SMP)")
    print("  9. weekly-smp-legacy-sync          - 매주 월 07:00 (개인 DB 백업)")
    print(" 10. jeju-realtime-collection        - 매 5분 (제주 계통수급 실시간)")
    print(" 11. jeju-sukub-monthly-collection   - 매월 1일 01:00 (제주 수급 백필)")
    print(" 12. jeju-gen-monthly-collection     - 매월 1일 02:00 (제주 연료원별 거래량)")
    print(" 13. jeju-demand-quarterly-collection- 분기 1일 03:00 (제주 시간별 전력수요)")
    print(" 14. monthly-koen-gen-collection     - 매월 10일 10:00 (KOEN 비태양광: 화력/연료전지/소수력)")
    print(" 15. jeju-supply-demand-db-sync      - 매 10분 (제주 수급 → demand DB, energy_hub FDW)")
    print(" 16. yearly-ekr-pv-collection        - 매년 1월 8일 (농어촌공사 영암/율치 PV → generation 코어)")
    print(" 17. unified-demand-collection       - 매 10분 (전국 수요 수집, 매시 수요-기상 집계)")
    print("")


if __name__ == "__main__":
    asyncio.run(create_all_deployments())
