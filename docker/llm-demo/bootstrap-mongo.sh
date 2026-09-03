#!/bin/sh
set -eu

# 기존 mongo-data 마이그레이션 전용이다. Mongo만 --auth로 먼저 시작하면 사용자
# 없는 DB의 localhost exception을 컨테이너 내부 mongosh만 사용할 수 있다. root를
# 만든 순간 예외가 닫히며, 마지막 파일 기반 healthcheck가 인증 강제를 검증한다.
: "${LLM_SECRET_DIR:?LLM_SECRET_DIR를 설정하세요}"
: "${LLM_APPROVAL_PUBLIC_ORIGIN:?LLM_APPROVAL_PUBLIC_ORIGIN을 설정하세요}"
: "${LLM_EXPORT_PUBLIC_ORIGIN:?LLM_EXPORT_PUBLIC_ORIGIN을 설정하세요}"

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
compose_file=$script_dir/compose.yml

docker compose --env-file /dev/null -f "$compose_file" up -d --no-deps mongodb
attempt=0
until docker compose --env-file /dev/null -f "$compose_file" exec -T mongodb \
  mongosh --quiet --file /usr/local/share/mongo-healthcheck.js >/dev/null 2>&1 || \
  docker compose --env-file /dev/null -f "$compose_file" exec -T mongodb \
  mongosh --quiet --eval 'quit(db.runCommand({ping:1}).ok ? 0 : 2)' >/dev/null 2>&1
do
  attempt=$((attempt + 1))
  [ "$attempt" -lt 30 ] || { echo "Mongo 시작을 확인할 수 없습니다." >&2; exit 1; }
  sleep 1
done
docker compose --env-file /dev/null -f "$compose_file" exec -T mongodb \
  mongosh --quiet --file /docker-entrypoint-initdb.d/init-mongo-users.js
docker compose --env-file /dev/null -f "$compose_file" exec -T mongodb \
  mongosh --quiet --file /usr/local/share/mongo-healthcheck.js
