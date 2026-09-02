#!/usr/bin/env bash
set -eu
umask 077

[ "$#" -eq 1 ] || { echo "사용법: provision-secrets.sh /absolute/secret/dir" >&2; exit 1; }
secret_dir=$1
case "$secret_dir" in /*) ;; *) echo "절대경로가 필요합니다." >&2; exit 1 ;; esac

install -d -m 700 "$secret_dir"
openssl rand -hex 32 > "$secret_dir/jwt_secret"
openssl rand -hex 32 > "$secret_dir/jwt_refresh_secret"
openssl rand -hex 32 > "$secret_dir/creds_key"
openssl rand -hex 16 > "$secret_dir/creds_iv"
openssl rand -hex 32 > "$secret_dir/mongo_root_password"
openssl rand -hex 32 > "$secret_dir/librechat_mongo_password"
openssl rand -hex 32 > "$secret_dir/energy_mcp_mongo_password"

printf "새 OpenAI API key: " >&2
IFS= read -r -s openai_key
printf '\n' >&2
printf '%s' "$openai_key" > "$secret_dir/openai_api_key"
unset openai_key

printf "읽기전용 PostgreSQL 비밀번호: " >&2
IFS= read -r -s postgres_password
printf '\n' >&2
printf '%s' "$postgres_password" > "$secret_dir/postgres_readonly_password"
encoded_password=$(printf '%s' "$postgres_password" | python3 -c \
  'import sys, urllib.parse; print(urllib.parse.quote(sys.stdin.read(), safe=""), end="")')
printf 'postgresql://demo_ro:%s@pgbouncer:5432/pv' "$encoded_password" \
  > "$secret_dir/energy_mcp_dsn"
unset postgres_password encoded_password

chmod 600 \
  "$secret_dir/jwt_secret" \
  "$secret_dir/jwt_refresh_secret" \
  "$secret_dir/creds_key" \
  "$secret_dir/creds_iv" \
  "$secret_dir/mongo_root_password" \
  "$secret_dir/librechat_mongo_password" \
  "$secret_dir/energy_mcp_mongo_password" \
  "$secret_dir/openai_api_key" \
  "$secret_dir/postgres_readonly_password" \
  "$secret_dir/energy_mcp_dsn"
