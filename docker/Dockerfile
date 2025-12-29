FROM prefecthq/prefect:2-python3.10

WORKDIR /app

# 1) uv 프로젝트 메타 복사
COPY pyproject.toml uv.lock ./

# 2) uv 설치
RUN pip install --no-cache-dir uv

# 3) uv lock → requirements.txt로 내리되, prefect는 제거
RUN uv export --no-dev --frozen --format requirements-txt \
    | grep -v '^prefect==' > requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

# 4) 나머지 코드 복사
COPY . .

# 5) 배포 스크립트 실행
CMD ["python", "-m", "prefect_flows.deploy"]
 