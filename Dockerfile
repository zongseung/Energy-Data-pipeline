# ============================================================
# 주의: 이 파일은 현재 미사용 상태입니다.
# 실제 운영 이미지: docker/Dockerfile
# 실행: make up  또는  docker compose -f docker/docker-compose.yml up -d
# ============================================================
FROM python:3.11-slim

WORKDIR /app

# uv 설치
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# 1. 의존성 설치 (빌드 속도 최적화)
COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-install-project

# 2. 코드 및 설정 파일 전체 복사
COPY . .

# 3. 환경 변수 설정
# 하드코딩된 URL을 제거하고, 파이썬 로그가 즉시 출력되도록 설정합니다.
ENV PYTHONUNBUFFERED=1

# 4. 실행 명령
# Prefect 워커(Worker)를 실행하여 배포된 플로우(Flow)를 대기합니다.
CMD ["uv", "run", "prefect", "worker", "start", "--pool", "default-agent-pool"]