# energy-mcp 단독 컨테이너 — LibreChat 데모용.
# streamable-http 로 MCP 를 서빙하고(8000), /exports 의 CSV 를 8098 로 노출한다.
# 빌드는 저장소 루트 컨텍스트에서: (mcp-server/ 를 COPY 하기 때문)
FROM python:3.11-slim

COPY mcp-server /opt/mcp-server
RUN pip install --no-cache-dir /opt/mcp-server
COPY docker/llm-demo/serve_exports.py /serve_exports.py

EXPOSE 8000 8098
CMD ["sh", "-c", "mkdir -p /exports && python /serve_exports.py & exec energy-mcp"]
