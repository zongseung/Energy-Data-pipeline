"""
Prefect Deployment Script

모든 Flow를 배포하고 스케줄을 등록합니다.
- daily-weather-collection-flow: 매일 오전 9시
- hourly-demand-collection-flow: 매 시간
- backfill-flow: 수동 실행
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
DOCKER_NETWORK = os.getenv("PREFECT_DOCKER_NETWORK", "weather-pipeline_prefect-new")
DEMAND_DATABASE_URL = os.getenv(
    "DEMAND_DATABASE_URL",
    "postgresql+asyncpg://demand:demand@demand-db:5432/demand"
)

SERVICE_KEY = os.getenv("SERVICE_KEY")
if not SERVICE_KEY:
    raise ValueError("SERVI" \
    "CE_KEY 환경변수를 찾을 수 없습니다.")

os.environ.setdefault("PREFECT_API_URL", PREFECT_API_URL)


# =======================================================================
# 공통 인프라 설정
# =======================================================================

def get_infra_overrides():
    """Docker 인프라 설정을 반환합니다."""
    return {
        "image": "weather-pipeline:latest",
        "image_pull_policy": "Never",
        "auto_remove": True,
        "env": {
            "PREFECT_API_URL": PREFECT_API_URL,
            "SERVICE_KEY": SERVICE_KEY,
            "DEMAND_DATABASE_URL": DEMAND_DATABASE_URL,
            "TZ": "Asia/Seoul",
        },
        "networks": [DOCKER_NETWORK],
        "volumes": ["/mnt/nvme/weather-pipeline/data:/app/data"],
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


async def ensure_work_pool(pool_name: str = "weather-new-pool") -> None:
    """Docker 타입 work pool이 없으면 생성합니다."""
    async with get_client() as client:
        try:
            pool = await client.read_work_pool(work_pool_name=pool_name)
            print(f"Work pool '{pool_name}' 이미 존재 (타입: {pool.type})")
        except Exception:
            base_job_template = {
                "job_configuration": {
                    "image": "weather-pipeline:latest",
                    "env": {
                        "PREFECT_API_URL": PREFECT_API_URL,
                        "SERVICE_KEY": SERVICE_KEY,
                        "DEMAND_DATABASE_URL": DEMAND_DATABASE_URL,
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
                            "default": "weather-pipeline:latest",
                        }
                    },
                },
            }

            await client.create_work_pool(
                WorkPoolCreate(
                    name=pool_name,
                    type="docker",
                    description="ETL 파이프라인용 Docker 워크 풀",
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
        work_pool_name="weather-new-pool",
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


async def deploy_demand_flow() -> None:
    """시간별 전력수요 수집 플로우 배포"""
    flow = import_object(
        "prefect_flows.prefect_pipeline.hourly_demand_collection_flow"
    )

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="hourly-demand-collection",
        work_pool_name="weather-new-pool",
        path="/app",
        entrypoint="prefect_flows/prefect_pipeline.py:hourly_demand_collection_flow",
        parameters={"hours": 2},
        schedules=[
            CronSchedule(
                cron="5 * * * *",  # 매 시간 5분
                timezone="Asia/Seoul",
            )
        ],
        tags=["demand", "hourly"],
        description="매 시간 전력수요 데이터를 수집하고 1시간 단위로 통합",
        infra_overrides=get_infra_overrides(),
    )

    await deployment.apply()
    print("Deployment 완료: 'hourly-demand-collection' (매시간 :05)")


async def deploy_backfill_flow() -> None:
    """백필 플로우 배포 (수동 실행)"""
    flow = import_object(
        "prefect_flows.prefect_pipeline.backfill_flow"
    )

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="backfill",
        work_pool_name="weather-new-pool",
        path="/app",
        entrypoint="prefect_flows/prefect_pipeline.py:backfill_flow",
        parameters={},
        schedules=[],  # 수동 실행만
        tags=["backfill", "manual"],
        description="누락된 데이터를 백필 (수동 실행)",
        infra_overrides=get_infra_overrides(),
    )

    await deployment.apply()
    print("Deployment 완료: 'backfill' (수동 실행)")


async def deploy_full_etl_flow() -> None:
    """전체 ETL 플로우 배포 (수동 실행)"""
    flow = import_object(
        "prefect_flows.prefect_pipeline.full_etl_flow"
    )

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="full-etl",
        work_pool_name="weather-new-pool",
        path="/app",
        entrypoint="prefect_flows/prefect_pipeline.py:full_etl_flow",
        parameters={"target_date": None},
        schedules=[],  # 수동 실행만
        tags=["etl", "full", "manual"],
        description="기상+전력수요 전체 ETL (수동 실행)",
        infra_overrides=get_infra_overrides(),
    )

    await deployment.apply()
    print("Deployment 완료: 'full-etl' (수동 실행)")


# =======================================================================
# 백필 트리거
# =======================================================================

async def trigger_backfill() -> None:
    """백필 플로우를 트리거합니다."""
    async with get_client() as client:
        try:
            # 배포 조회
            deployment = await client.read_deployment_by_name(
                "backfill-flow/backfill"
            )

            # Flow Run 생성
            flow_run = await client.create_flow_run_from_deployment(
                deployment.id,
                parameters={"default_start": "20240101"},
                tags=["auto-backfill", "startup"],
            )

            print(f"백필 Flow 트리거 완료: {flow_run.id}")
            return flow_run.id

        except Exception as e:
            print(f"백필 트리거 실패: {e}")
            return None


# =======================================================================
# 메인 실행
# =======================================================================

async def create_all_deployments(auto_backfill: bool = True) -> None:
    """모든 배포를 생성합니다."""
    print("\n" + "=" * 60)
    print("Prefect Deployment 시작")
    print("=" * 60 + "\n")

    # API 대기 및 Work Pool 생성
    await wait_for_api()
    await ensure_work_pool("weather-new-pool")

    print("\n--- Flow 배포 ---\n")

    # 각 Flow 배포
    await deploy_weather_flow()
    await deploy_demand_flow()
    await deploy_backfill_flow()
    await deploy_full_etl_flow()

    print("\n" + "=" * 60)
    print("모든 Deployment 완료!")
    print("=" * 60 + "\n")

    # 배포 요약
    print("배포된 Flow:")
    print("  1. daily-weather-collection    - 매일 09:00 (기상 데이터)")
    print("  2. hourly-demand-collection    - 매시간 :05 (전력수요)")
    print("  3. backfill                    - 수동 실행 (백필)")
    print("  4. full-etl                    - 수동 실행 (전체 ETL)")
    print("")

    # 자동 백필 실행
    if auto_backfill:
        print("\n" + "=" * 60)
        print("자동 백필 시작...")
        print("=" * 60 + "\n")

        # Worker가 준비될 때까지 잠시 대기
        await asyncio.sleep(10)

        await trigger_backfill()
        print("백필 작업이 백그라운드에서 실행됩니다.")
        print("Prefect UI에서 진행 상황을 확인하세요: http://localhost:4300")


if __name__ == "__main__":
    # AUTO_BACKFILL 환경변수로 자동 백필 제어 (기본값: False - 수동 실행 권장)
    auto_backfill = os.getenv("AUTO_BACKFILL", "false").lower() == "true"
    asyncio.run(create_all_deployments(auto_backfill=auto_backfill))
