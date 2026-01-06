#!/bin/bash
# PV Database Backup Script
# 로컬 PostgreSQL -> NAS 백업

set -e

# 설정
CONTAINER_NAME="pv-postgres"
DB_USER="pv"
DB_NAME="pv"
BACKUP_DIR="/mnt/nvme/weather-data-download-center/nas-weather-data/db_backup/pv_db"
RETENTION_DAYS=7

# 타임스탬프
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_FILE="${BACKUP_DIR}/pv_db_${TIMESTAMP}.sql.gz"

echo "[$(date)] PV DB 백업 시작..."

# 컨테이너 실행 확인
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "[ERROR] ${CONTAINER_NAME} 컨테이너가 실행 중이 아닙니다."
    exit 1
fi

# pg_dump 실행 (압축)
docker exec ${CONTAINER_NAME} pg_dump -U ${DB_USER} ${DB_NAME} | gzip > "${BACKUP_FILE}"

# 백업 파일 확인
if [ -f "${BACKUP_FILE}" ] && [ -s "${BACKUP_FILE}" ]; then
    SIZE=$(du -h "${BACKUP_FILE}" | cut -f1)
    echo "[$(date)] 백업 완료: ${BACKUP_FILE} (${SIZE})"
else
    echo "[ERROR] 백업 파일 생성 실패"
    exit 1
fi

# 오래된 백업 삭제 (7일 이상)
echo "[$(date)] ${RETENTION_DAYS}일 이상 된 백업 정리..."
find "${BACKUP_DIR}" -name "pv_db_*.sql.gz" -mtime +${RETENTION_DAYS} -delete

# 현재 백업 목록
echo "[$(date)] 현재 백업 목록:"
ls -lh "${BACKUP_DIR}"/pv_db_*.sql.gz 2>/dev/null || echo "  (백업 파일 없음)"

echo "[$(date)] 완료!"
