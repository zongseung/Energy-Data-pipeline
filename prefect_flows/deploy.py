"""
Prefect Deployment Script

PV 파이프라인 Flow를 배포하고 스케줄을 등록합니다.
- daily-weather-collection-flow: 매일 오전 9시 (기상 데이터)
- full-etl: 수동 실행 (전체 ETL)
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

SERVICE_KEY = os.getenv("SERVICE_KEY", "")
if not SERVICE_KEY:
    print("[WARN] SERVICE_KEY가 설정되지 않았습니다. 기상 데이터 수집 시 오류가 발생할 수 있습니다.")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
NAMDONG_WIND_KEY = os.getenv("NAMDONG_WIND_KEY", "")
NAMDONG_START_DATE = os.getenv("NAMDONG_START_DATE", "")
NAMDONG_ORG_NO = os.getenv("NAMDONG_ORG_NO", "")
NAMDONG_HOKI_S = os.getenv("NAMDONG_HOKI_S", "")
NAMDONG_HOKI_E = os.getenv("NAMDONG_HOKI_E", "")
NAMDONG_PAGE_INDEX = os.getenv("NAMDONG_PAGE_INDEX", "")
NAMDONG_OUTPUT_DIR = os.getenv("NAMDONG_OUTPUT_DIR", "")
NAMBU_API_KEY = os.getenv("NAMBU_API_KEY", "")
if not NAMBU_API_KEY:
    print("[WARN] NAMBU_API_KEY가 설정되지 않았습니다. 남부발전 PV 수집 시 오류가 발생할 수 있습니다.")

os.environ.setdefault("PREFECT_API_URL", PREFECT_API_URL)


# =======================================================================
# 공통 인프라 설정
# =======================================================================

def get_infra_overrides():
    """Docker 인프라 설정을 반환합니다."""
    return {
        "image": "pv-pipeline:latest",
        "image_pull_policy": "Never",
        "auto_remove": True,
        "env": {
            "PREFECT_API_URL": PREFECT_API_URL,
            "SERVICE_KEY": SERVICE_KEY,
            "PV_DATABASE_URL": PV_DATABASE_URL,
            "DB_URL": PV_DATABASE_URL,
            "SLACK_WEBHOOK_URL": SLACK_WEBHOOK_URL,
            "NAMDONG_WIND_KEY": NAMDONG_WIND_KEY,
            "NAMDONG_START_DATE": NAMDONG_START_DATE,
            "NAMDONG_ORG_NO": NAMDONG_ORG_NO,
            "NAMDONG_HOKI_S": NAMDONG_HOKI_S,
            "NAMDONG_HOKI_E": NAMDONG_HOKI_E,
            "NAMDONG_PAGE_INDEX": NAMDONG_PAGE_INDEX,
            "NAMDONG_OUTPUT_DIR": NAMDONG_OUTPUT_DIR,
            "NAMBU_API_KEY": NAMBU_API_KEY,
            "TZ": "Asia/Seoul",
        },
        "networks": [DOCKER_NETWORK],
        "volumes": ["/mnt/nvme/Energy-Data-pipeline/data:/app/data"],
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
    """Docker 타입 work pool이 없으면 생성합니다."""
    async with get_client() as client:
        try:
            pool = await client.read_work_pool(work_pool_name=pool_name)
            print(f"Work pool '{pool_name}' 이미 존재 (타입: {pool.type})")
        except Exception:
            base_job_template = {
                "job_configuration": {
                    "image": "pv-pipeline:latest",
                    "env": {
                        "PREFECT_API_URL": PREFECT_API_URL,
                        "SERVICE_KEY": SERVICE_KEY,
                        "PV_DATABASE_URL": PV_DATABASE_URL,
                        "TZ": "Asia/Seoul",
                    },
                    "networks": [DOCKER_NETWORK],
                },
                "variables": {
                    "required": ["image"],
                    "properties": {
                        "image": {
                            "title": "Image",
                            "description": "Docker image to use",
                            "type": "string",
                            "default": "pv-pipeline:latest",
                        }
                    },
                },
            }

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
        infra_overrides=get_infra_overrides(),
    )

    await deployment.apply()
    print("Deployment 완료: 'daily-weather-collection' (매일 09:00)")


async def deploy_full_etl_flow() -> None:
    """전체 ETL 플로우 배포 (수동 실행)"""
    flow = import_object(
        "prefect_flows.prefect_pipeline.full_etl_flow"
    )

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="full-etl",
        work_pool_name="pv-pool",
        path="/app",
        entrypoint="prefect_flows/prefect_pipeline.py:full_etl_flow",
        parameters={"target_date": None},
        schedules=[],  # 수동 실행만
        tags=["etl", "full", "manual"],
        description="기상+PV 전체 ETL (수동 실행)",
        infra_overrides=get_infra_overrides(),
    )

    await deployment.apply()
    print("Deployment 완료: 'full-etl' (수동 실행)")


async def deploy_namdong_flow() -> None:
    """남동발전 PV 수집 플로우 배포"""
    flow = import_object(
        "fetch_data.pv.namdong_collect_pv.daily_namdong_collection_flow"
    )

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="monthly-namdong-pv-collection",
        work_pool_name="pv-pool",
        path="/app",
        entrypoint="fetch_data/pv/namdong_collect_pv.py:daily_namdong_collection_flow",
        parameters={"target_start": None, "target_end": None, "sleep_sec": 5},
        schedules=[
            CronSchedule(
                cron="0 10 10 * *",  # 매월 10일 오전 10시
                timezone="Asia/Seoul",
            )
        ],
        tags=["pv", "namdong", "monthly"],
        description="매월 10일 오전 10시에 전월 남동발전 PV 데이터를 수집/백필",
        infra_overrides=get_infra_overrides(),
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
        infra_overrides=get_infra_overrides(),
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
        infra_overrides=get_infra_overrides(),
    )

    await deployment.apply()
    print("Deployment 완료: 'monthly-namdong-wind-collection' (매월 10일 11:00)")


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
    await deploy_full_etl_flow()
    await deploy_namdong_flow()
    await deploy_nambu_flow()
    await deploy_namdong_wind_flow()

    print("\n" + "=" * 60)
    print("모든 Deployment 완료!")
    print("=" * 60 + "\n")

    # 배포 요약
    print("배포된 Flow:")
    print("  1. daily-weather-collection       - 매일 09:00 (기상 데이터)")
    print("  2. full-etl                       - 수동 실행 (전체 ETL)")
    print("  3. monthly-namdong-pv-collection  - 매월 10일 10:00 (남동발전 PV)")
    print("  4. daily-nambu-pv-collection      - 매일 09:30 (남부발전 PV)")
    print("  5. monthly-namdong-wind-collection - 매월 10일 11:00 (남동발전 풍력)")
    print("")


if __name__ == "__main__":
    asyncio.run(create_all_deployments())
