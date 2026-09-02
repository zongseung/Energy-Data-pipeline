#!/usr/bin/env bash
set -eu
umask 077

[ "$#" -eq 1 ] || { echo "사용법: provision-secrets.sh /absolute/secret/dir" >&2; exit 1; }
secret_dir=$1
case "$secret_dir" in /*) ;; *) echo "절대경로가 필요합니다." >&2; exit 1 ;; esac
case "$secret_dir" in /|*/) echo "secret 디렉터리 경로가 잘못되었습니다." >&2; exit 1 ;; esac
[ ! -e "$secret_dir" ] && [ ! -L "$secret_dir" ] || {
  echo "이미 존재하는 경로는 사용할 수 없습니다." >&2
  exit 1
}

parent=${secret_dir%/*}
[ -n "$parent" ] || parent=/
base=${secret_dir##*/}
mkdir -p "$parent"
staging_dir=$(mktemp -d "$parent/.${base}.tmp.XXXXXX")
cleanup() {
  [ -z "${staging_dir:-}" ] || rm -rf -- "$staging_dir"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

openssl rand -hex 32 > "$staging_dir/jwt_secret"
openssl rand -hex 32 > "$staging_dir/jwt_refresh_secret"
openssl rand -hex 32 > "$staging_dir/creds_key"
openssl rand -hex 16 > "$staging_dir/creds_iv"
openssl rand -hex 32 > "$staging_dir/mongo_root_password"
openssl rand -hex 32 > "$staging_dir/librechat_mongo_password"
openssl rand -hex 32 > "$staging_dir/energy_mcp_mongo_password"

printf "새 OpenAI API key: " >&2
IFS= read -r -s openai_key
printf '\n' >&2
[ -n "$openai_key" ] || { echo "OpenAI API key는 비어 있을 수 없습니다." >&2; exit 1; }
printf '%s' "$openai_key" > "$staging_dir/openai_api_key"
unset openai_key

printf "읽기전용 PostgreSQL 비밀번호: " >&2
IFS= read -r -s postgres_password
printf '\n' >&2
[ -n "$postgres_password" ] || { echo "PostgreSQL 비밀번호는 비어 있을 수 없습니다." >&2; exit 1; }
case "$postgres_password" in
  *\"*|*\\*|*$'\r'*)
    echo "PostgreSQL 비밀번호에는 큰따옴표, 역슬래시, CR을 사용할 수 없습니다." >&2
    exit 1
    ;;
esac
printf '%s' "$postgres_password" > "$staging_dir/postgres_readonly_password"
encoded_password=$(printf '%s' "$postgres_password" | python3 -c \
  'import sys, urllib.parse; print(urllib.parse.quote(sys.stdin.read(), safe=""), end="")')
printf 'postgresql://demo_ro:%s@pgbouncer:5432/pv' "$encoded_password" \
  > "$staging_dir/energy_mcp_dsn"
unset postgres_password encoded_password

chmod 600 \
  "$staging_dir/jwt_secret" \
  "$staging_dir/jwt_refresh_secret" \
  "$staging_dir/creds_key" \
  "$staging_dir/creds_iv" \
  "$staging_dir/mongo_root_password" \
  "$staging_dir/librechat_mongo_password" \
  "$staging_dir/energy_mcp_mongo_password" \
  "$staging_dir/openai_api_key" \
  "$staging_dir/postgres_readonly_password" \
  "$staging_dir/energy_mcp_dsn"
mv -T -- "$staging_dir" "$secret_dir"
staging_dir=
