#!/bin/sh
set -eu

secret_dir=${LOAD_SECRETS_DIR:-/run/secrets}

read_secret() {
    variable=$1
    file=$secret_dir/$2
    [ -r "$file" ] || { echo "필수 secret 파일이 없습니다: $2" >&2; exit 1; }
    value=$(cat "$file")
    export "$variable=$value"
    unset value
}

[ "$#" -ge 2 ] || { echo "사용법: load-secrets <profile> <명령...>" >&2; exit 1; }
profile=$1
shift

case "$profile" in
    librechat)
        read_secret OPENAI_API_KEY openai_api_key
        read_secret JWT_SECRET jwt_secret
        read_secret JWT_REFRESH_SECRET jwt_refresh_secret
        read_secret CREDS_KEY creds_key
        read_secret CREDS_IV creds_iv
        read_secret MONGO_PASSWORD librechat_mongo_password
        export MONGO_URI="mongodb://librechat_app:${MONGO_PASSWORD}@mongodb:27017/LibreChat?authSource=LibreChat"
        unset MONGO_PASSWORD
        ;;
    energy-mcp)
        read_secret OPENAI_API_KEY openai_api_key
        read_secret ENERGY_MCP_DSN energy_mcp_dsn
        read_secret MONGO_PASSWORD energy_mcp_mongo_password
        export ENERGY_MCP_MONGO_URI="mongodb://energy_mcp_app:${MONGO_PASSWORD}@mongodb:27017/energy_mcp?authSource=energy_mcp"
        unset MONGO_PASSWORD
        ;;
    pgbouncer)
        read_secret DB_PASSWORD postgres_readonly_password
        ;;
    *) echo "알 수 없는 secret profile: $profile" >&2; exit 1 ;;
esac

exec "$@"
