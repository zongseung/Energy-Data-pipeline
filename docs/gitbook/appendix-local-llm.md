# 부록: 로컬 LLM으로 MCP 서버 쓰기 (선택)

[LLM·MCP 가이드](03-llm-mcp.md)의 MCP 서버(`energy-mcp`)는 Claude Desktop 같은
외부 LLM 클라이언트와 함께 쓰는 것이 기본 경로다. 조회 결과(SQL 응답)가
외부 LLM 서버로 나가는 것 자체가 꺼려지는 연구원을 위해 완전히 로컬에서
도는 경로도 남겨뒀습니다. **이 경로는 선택이며, 없어도 psql/pandas/R로 연구에
지장이 없습니다.**

아래는 이 서버에서 직접 실측해 검증한 llama.cpp 경로입니다. **여기 적힌 명령·
플래그만 쓰시고, 검증 안 된 다른 조합은 시도하지 마세요.**

## 설치

Rust 컴파일도, GLIBC 업그레이드도 필요 없습니다 — 미리 빌드된 바이너리를
받기만 하면 됩니다:

```bash
curl -sL -o llama.tar.gz \
  https://github.com/ggml-org/llama.cpp/releases/download/b10359/llama-b10359-bin-ubuntu-x64.tar.gz
mkdir -p llama && tar xzf llama.tar.gz -C llama    # 16.5MB
```

압축을 풀면 `llama-b10359/llama-server`와 `libggml-cpu-*.so`(CPU 세대별)가
나옵니다. 실행할 때 이 중 맞는 라이브러리를 자동으로 골라 씁니다. 대신
`LD_LIBRARY_PATH`는 이 디렉터리로 잡아주셔야 합니다.

## 모델

**Qwen3-4B, Q4_K_M 양자화(GGUF)**를 씁니다 — `Qwen/Qwen3-4B-GGUF` 리포지터리의
`Qwen3-4B-Q4_K_M.gguf` (약 2.5GB).

> **Qwen3.5 GGUF는 피하세요.** 아키텍처를 인식하지 못하는 이슈가 있습니다.
> Qwen3 계열을 쓰시면 됩니다.

## MCP 서버 연결

Cursor 호환 형식의 JSON 파일(`mcp-servers.json`)로 저장합니다:

```json
{
  "mcpServers": {
    "energy-db": {
      "command": "uvx",
      "args": ["energy-mcp"],
      "env": {
        "ENERGY_MCP_DSN": "postgresql://<본인_role>:<발급받은_비밀번호>@<tailnet-host>:5436/pv"
      }
    }
  }
}
```

`ENERGY_MCP_DSN` 값은 [직접 SQL 가이드](02-direct-sql.md)에서 안내한 본인의
role 자격증명으로 바꿔 쓰세요. **실제 비밀번호·tailnet 주소는 절대 이 파일이나
저장소, 채팅에 남기지 마시고 플레이스홀더로 두세요.**

## 기동

```bash
LD_LIBRARY_PATH=llama/llama-b10359 llama/llama-b10359/llama-server \
  -m Qwen3-4B-Q4_K_M.gguf --host 127.0.0.1 --port 8080 -c 8192 --jinja \
  --mcp-servers-config mcp-servers.json
```

아래처럼 로그가 나오면 MCP 연결이 정상입니다:

```
srv start: MCP warmup: 'energy-db' discovered 1 tools
srv setup: Added 1 MCP tools
srv llama_server: listening on http://127.0.0.1:8080
```

## 사용

브라우저로 `http://127.0.0.1:8080`에 접속하세요. 웹 UI가 MCP 호스트 역할을 겸해
툴 호출과 결과 되먹임 루프를 처리하는 구조입니다(구성요소는 API 경로로 실측
확인했으나 브라우저 UI 자체의 동작은 미확인) — 별도 클라이언트가 필요 없습니다.

## 실측 성능

64코어 Xeon E5-2697A v4(Broadwell, AVX-512 없음) 기준:

| 단계 | 시간 |
|---|---|
| 서버 기동 + 모델 로드 | 약 6초 |
| 질의 → 툴 호출 생성 | 약 46초 |
| 툴 결과 → 최종 답변 | 약 26초 |
| **질의당 총합** | **약 1~2분** |

검증한 질의: "`research.plants` 뷰에서 `data_quality`가 정상인 태양광
발전소가 몇 기인지 세어줘" → 모델이
`SELECT COUNT(*) FROM research.plants WHERE data_quality = '정상';`을
정확히 짜서 **정답(33기)**을 답했습니다.

## 한계

- **질의당 1~2분** 걸립니다. 대화형으로 쓰기엔 답답한 속도입니다.
- 4B급 모델은 **다단계·중첩 쿼리에서 정확도가 떨어집니다.** 조인 3개 이상,
  서브쿼리, 집계 위에 조건을 또 거는 쿼리 등에서 SQL을 자주 틀립니다. 이 경로는
  **스키마 탐색과 간단한 조회용**입니다. 논문·보고서에 들어갈 최종 분석 쿼리는
  반드시 사람이 직접 확인하세요.
- llama.cpp의 MCP 지원은 아직 **experimental**입니다(`--mcp-servers-config`
  도움말 기준).
- 이 경로가 필요한 경우는 하나뿐입니다 — 조회 결과가 외부 LLM 서버로 나가는
  것이 곤란할 때. **그렇지 않으면 본문의 psql/pandas나 Claude Desktop 같은
  클라이언트가 훨씬 빠르고 정확합니다.**

[데이터 카탈로그](04-data-catalog.md)의 함정(단위, 시간 규약, 품질 등급)은 모델이
자동으로 챙겨주지 않습니다 — 결과를 그대로 믿지 마시고 직접 검증하는 습관이
필요합니다.

## 쓰지 마세요

- **`pip install mistralrs`(mistral.rs) 경로**: 이 서버에서 실측한 결과
  0.9.0 설치는 되지만 CLI 바이너리(`mistralrs serve`)가 아예 오지 않았습니다.
  Python SDK로 MCP를 붙이면 `Tool 'run_sql' execution failed: A Tokio 1.x
  context was found, but it is being shutdown.` 라이브러리 버그로 툴
  실행이 깨졌습니다. 프리빌트 CLI 바이너리는 GLIBC 2.38/2.39를 요구해 이
  서버(Ubuntu 22.04, GLIBC 2.35)에서 실행되지 않았습니다.
- **Continue + Ollama 조합**: "MCP tools don't work with Ollama models"
  이슈(`continuedev/continue#7828`)가 열려 있어 지금은 신뢰할 수 없습니다.
- 검증 안 한 다른 명령·플래그. 위에 적힌 것만 쓰세요.
