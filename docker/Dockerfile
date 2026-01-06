FROM prefecthq/prefect:2-python3.10

WORKDIR /app

# 1) 프로젝트 메타 복사
COPY pyproject.toml uv.lock ./

# 2) uv 설치 후 시스템 Python에 직접 설치
RUN pip install --no-cache-dir uv
RUN uv pip install --system --no-cache -r pyproject.toml

# 3) 나머지 코드 복사
COPY . .

# 4) 배포 스크립트 실행
CMD ["python", "-m", "prefect_flows.deploy"]
 