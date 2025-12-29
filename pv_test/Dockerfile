FROM python:3.11-slim

WORKDIR /app

# 시스템 패키지
RUN apt-get update && apt-get install -y --no-install-recommends \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Python 의존성
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 스크립트 복사
COPY init_db.py .

# 데이터 파일 복사 (빌드 시 이미지에 포함)
COPY input/ /app/input/

# 데이터 폴더 생성
RUN mkdir -p /app/data

CMD ["python", "init_db.py"]
