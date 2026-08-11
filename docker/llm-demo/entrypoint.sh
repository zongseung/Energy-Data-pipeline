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

# Qwen3 thinking 모드 비활성 — SQL 생성에 긴 추론이 불필요하고 CPU에서 응답을 수 배 늦춘다
exec /opt/llama/llama-server \
    -m /models/model.gguf \
    --host 0.0.0.0 --port 8080 -c 8192 --jinja \
    --chat-template-kwargs '{"enable_thinking": false}' \
    --mcp-servers-config /tmp/mcp-servers.json \
    "$@"
