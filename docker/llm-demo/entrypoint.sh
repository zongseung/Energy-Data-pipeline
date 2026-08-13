#!/bin/sh
# MCP 설정을 환경변수(ENERGY_MCP_DSN)로부터 생성 후 llama-server 기동.
# llama.cpp의 --mcp-servers-config 는 환경변수 치환을 안 하므로 기동 시점에 파일을 만든다.
set -e
: "${ENERGY_MCP_DSN:?ENERGY_MCP_DSN 환경변수가 필요합니다}"

cat > /tmp/mcp-servers.json <<EOF
{
  "mcpServers": {
    "energy-db": {
      "command": "/opt/venv/bin/energy-mcp",
      "args": [],
      "env": { "ENERGY_MCP_DSN": "${ENERGY_MCP_DSN}" }
    }
  }
}
EOF

# CSV 다운로드 서빙 — energy-mcp 가 /exports 에 떨군 파일을 stdlib 서버로 노출
mkdir -p /exports
python3 -m http.server 8098 --directory /exports &

# Qwen3 thinking 모드 비활성 — SQL 생성에 긴 추론이 불필요하고 CPU에서 응답을 수 배 늦춘다
exec /opt/llama/llama-server \
    -m /models/model.gguf \
    --host 0.0.0.0 --port 8080 -c 16384 --jinja \
    --chat-template-kwargs '{"enable_thinking": false}' \
    --mcp-servers-config /tmp/mcp-servers.json \
    "$@"
