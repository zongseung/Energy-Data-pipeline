#!/bin/sh
set -eu

secret_dir=${LOAD_SECRETS_DIR:-/run/secrets}

read_secret() {
    secret_name=$1
    file=$secret_dir/$secret_name
    [ -r "$file" ] || { echo "필수 secret 파일이 없습니다: $secret_name" >&2; exit 1; }
    secret_value=$(cat "$file")
    [ -n "$secret_value" ] || {
        echo "필수 secret 파일이 비어 있습니다: $secret_name" >&2
        exit 1
    }
}

[ "$#" -ge 2 ] || { echo "사용법: load-secrets <profile> <명령...>" >&2; exit 1; }
profile=$1
shift

case "$profile" in
    librechat)
        read_secret openai_api_key; openai_api_key=$secret_value
        read_secret jwt_secret; jwt_secret=$secret_value
        read_secret jwt_refresh_secret; jwt_refresh_secret=$secret_value
        read_secret creds_key; creds_key=$secret_value
        read_secret creds_iv; creds_iv=$secret_value
        read_secret librechat_mongo_password; mongo_password=$secret_value
        export OPENAI_API_KEY="$openai_api_key"
        export JWT_SECRET="$jwt_secret"
        export JWT_REFRESH_SECRET="$jwt_refresh_secret"
        export CREDS_KEY="$creds_key"
        export CREDS_IV="$creds_iv"
        export MONGO_URI="mongodb://librechat_app:${mongo_password}@mongodb:27017/LibreChat?authSource=LibreChat"
        unset openai_api_key jwt_secret jwt_refresh_secret creds_key creds_iv mongo_password
        ;;
    energy-mcp)
        read_secret openai_api_key; openai_api_key=$secret_value
        read_secret energy_mcp_dsn; energy_mcp_dsn=$secret_value
        read_secret energy_mcp_mongo_password; mongo_password=$secret_value
        export OPENAI_API_KEY="$openai_api_key"
        export ENERGY_MCP_DSN="$energy_mcp_dsn"
        export ENERGY_MCP_MONGO_URI="mongodb://energy_mcp_app:${mongo_password}@mongodb:27017/energy_mcp?authSource=energy_mcp"
        unset openai_api_key energy_mcp_dsn mongo_password
        ;;
    pgbouncer)
        read_secret postgres_readonly_password; postgres_password=$secret_value
        export DB_PASSWORD="$postgres_password"
        unset postgres_password
        ;;
    *) echo "알 수 없는 secret profile: $profile" >&2; exit 1 ;;
esac

unset secret_value secret_name file
exec "$@"
