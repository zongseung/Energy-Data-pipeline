# 시범용 로컬 LLM — llama.cpp(CPU) + Qwen3-4B GGUF + energy-mcp
#
# 빌드는 저장소 루트 컨텍스트에서: (mcp-server/ 를 COPY 하기 때문)
#   docker compose -f docker/llm-demo/compose.yml build
#
# ubuntu 24.04 = GLIBC 2.39 — llama.cpp 프리빌트 바이너리 요구사항.
# (호스트 22.04(GLIBC 2.35)에서는 바이너리가 안 돌아 컨테이너로 감싼다)
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates python3 python3-venv libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# llama.cpp CPU 빌드 (검증된 버전 고정 — 2026-08-11 실측)
ARG LLAMA_BUILD=b10359
RUN curl -sL -o /tmp/llama.tar.gz \
        "https://github.com/ggml-org/llama.cpp/releases/download/${LLAMA_BUILD}/llama-${LLAMA_BUILD}-bin-ubuntu-x64.tar.gz" \
    && mkdir -p /opt/llama \
    && tar xzf /tmp/llama.tar.gz -C /opt/llama --strip-components=1 \
    && rm /tmp/llama.tar.gz

# energy-mcp (읽기전용 run_sql MCP 서버) — 독립 패키지라 저장소 다른 코드 미포함
COPY mcp-server /opt/mcp-server
RUN python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir /opt/mcp-server

COPY docker/llm-demo/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV LD_LIBRARY_PATH=/opt/llama
EXPOSE 8080
ENTRYPOINT ["/entrypoint.sh"]
