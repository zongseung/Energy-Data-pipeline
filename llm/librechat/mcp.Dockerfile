# energy-mcp 단독 컨테이너 — LibreChat 데모용.
# streamable-http 로 MCP 를 서빙하고(8000), /exports 의 CSV 를 8098 로 노출한다.
# 빌드는 저장소 루트 컨텍스트에서: (llm/mcp-server/ 를 COPY 하기 때문)
FROM ghcr.io/astral-sh/uv:0.8.17 AS uv
FROM python:3.11-slim

# uv.lock 대로 고정 설치한다 — workflow 모드가 pymongo 등 새 의존성을 쓴다.
COPY --from=uv /uv /uvx /bin/
COPY llm/mcp-server /opt/mcp-server
WORKDIR /opt/mcp-server
RUN uv sync --frozen --no-dev
ENV PATH="/opt/mcp-server/.venv/bin:$PATH"
COPY llm/librechat/serve_exports.py /serve_exports.py

EXPOSE 8000 8098
CMD ["sh", "-c", "mkdir -p /exports && python /serve_exports.py & exec energy-mcp"]
