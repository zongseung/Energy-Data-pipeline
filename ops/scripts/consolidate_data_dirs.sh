#!/usr/bin/env bash
# data/ 를 유일한 보존 경로로 통일한다. data/ 가 root 소유라 sudo 가 필요하다.
#
#   sudo bash ops/scripts/consolidate_data_dirs.sh
#
# data/ 소유권을 호스트 사용자에게 넘기고(컨테이너는 root 라 계속 쓸 수 있다),
# 흩어져 있던 보존 대상을 data/ 밑으로 옮긴다.
# ASOS 일별 CSV 는 이미 data/ 에 있어 건드리지 않는다.
set -euo pipefail
cd "$(dirname "$0")/../.."

OWNER="${SUDO_USER:-$(id -un)}"

echo "== data/ 소유권 -> $OWNER"
chown -R "$OWNER:$OWNER" data

if [ -d komipo_data_raw ]; then
  echo "== komipo_data_raw/ -> data/komipo/"
  mkdir -p data/komipo
  mv komipo_data_raw/*.parquet data/komipo/
  rmdir komipo_data_raw
fi

if [ -d backups ]; then
  echo "== backups/ -> data/backups/"
  mv backups data/backups
fi

chown -R "$OWNER:$OWNER" data
echo "== 완료"
ls -la data/
