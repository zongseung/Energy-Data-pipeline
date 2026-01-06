#!/bin/bash
# PV Database Restore Script
# NAS 백업 -> 로컬 PostgreSQL 복원

set -e

# 설정
CONTAINER_NAME="pv-postgres"
DB_USER="pv"
DB_NAME="pv"
BACKUP_DIR="/mnt/nvme/weather-data-download-center/nas-weather-data/db_backup/pv_db"

# 백업 파일 선택
if [ -z "$1" ]; then
    echo "사용법: $0 <백업파일명>"
    echo ""
    echo "사용 가능한 백업 파일:"
    ls -lh "${BACKUP_DIR}"/pv_db_*.sql.gz 2>/dev/null || echo "  (백업 파일 없음)"
    exit 1
fi

BACKUP_FILE="$1"

# 전체 경로가 아니면 BACKUP_DIR 추가
if [[ ! "$BACKUP_FILE" == /* ]]; then
    BACKUP_FILE="${BACKUP_DIR}/${BACKUP_FILE}"
fi

# 파일 존재 확인
if [ ! -f "${BACKUP_FILE}" ]; then
    echo "[ERROR] 백업 파일을 찾을 수 없습니다: ${BACKUP_FILE}"
    exit 1
fi

echo "[$(date)] PV DB 복원 시작..."
echo "백업 파일: ${BACKUP_FILE}"

# 컨테이너 실행 확인
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "[ERROR] ${CONTAINER_NAME} 컨테이너가 실행 중이 아닙니다."
    exit 1
fi

# 확인
read -p "기존 데이터가 덮어씌워집니다. 계속하시겠습니까? (y/N): " confirm
if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
    echo "취소되었습니다."
    exit 0
fi

# 복원 실행
echo "[$(date)] 복원 중..."
gunzip -c "${BACKUP_FILE}" | docker exec -i ${CONTAINER_NAME} psql -U ${DB_USER} -d ${DB_NAME}

echo "[$(date)] 복원 완료!"
