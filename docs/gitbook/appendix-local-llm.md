# 부록: 로컬 LLM으로 MCP 서버 쓰기 (선택)

[02-access.md](02-access.md)의 MCP 서버(`energy-mcp`)는 Claude Desktop 같은
외부 LLM 클라이언트와 함께 쓰는 것이 기본 경로다. 조회 결과(SQL 응답)가
외부 LLM 서버로 나가는 것 자체가 꺼려지는 연구원을 위해, 완전히 로컬에서
도는 경로도 남겨둔다. **이 경로는 선택이며, 없어도 psql/pandas/R로 연구에
지장이 없다.**

## 설치

```bash
pip install mistralrs
```

CPU 전용 prebuilt wheel이 배포되므로 GPU나 Rust 툴체인이 필요 없다.

## 모델

**Qwen3 4B, Q4_K_M 양자화(GGUF)**를 쓴다. 메모리 사용량은 약 3GB 수준이라
일반 노트북에서도 돌아간다.

> **Qwen3.5 GGUF는 피할 것.** 일부 런타임이 아직 해당 아키텍처를 인식하지
> 못하는 이슈가 있다. Qwen3 계열을 쓴다.

정확한 모델 다운로드/양자화 지정 플래그는 `mistralrs` 버전마다 바뀔 수
있으니 `mistralrs serve --help`와 [mistral.rs 공식 문서](https://github.com/EricLBuehler/mistral.rs)를
확인해 현재 버전에 맞는 값을 쓴다.

## MCP 서버 연결 (`mcp.json`)

`mistralrs serve`는 내장 MCP 클라이언트를 갖고 있어, `energy-mcp`를
서브프로세스(Process transport)로 띄워 그대로 도구로 연결할 수 있다.

`mcp.json`:

```json
{
  "servers": [
    {
      "name": "energy-pv",
      "source": {
        "type": "Process",
        "command": "uvx",
        "args": ["energy-mcp"],
        "env": {
          "ENERGY_MCP_DSN": "postgresql://<본인_role>:<비밀번호>@<tailnet-host>:5436/pv"
        }
      }
    }
  ],
  "auto_register_tools": true,
  "tool_timeout_secs": 30
}
```

실행:

```bash
mistralrs serve --mcp-config mcp.json -m <Qwen3-4B 모델 경로/ID>
```

`ENERGY_MCP_DSN`의 값은 [02-access.md](02-access.md)에서 안내한 본인의
role 자격증명으로 바꿔 쓴다. 실제 비밀번호를 이 파일이나 저장소에 남기지
않는다.

## 한계

4B급 모델은 **다단계·중첩 쿼리에서 정확도가 떨어진다.** 조인 3개 이상,
서브쿼리, 집계 위에 조건을 또 거는 쿼리 등에서 SQL을 잘못 짜는 경우가
흔하다. 이 경로는 **스키마 탐색과 간단한 조회용**으로 쓰고, 논문·보고서에
들어갈 최종 분석 쿼리는 반드시 사람이 직접 확인하라. [01-data.md](01-data.md)의
함정(단위, 시간 규약, 품질 등급)은 모델이 자동으로 챙겨주지 않는다 —
결과를 그대로 믿지 말고 직접 검증하는 습관이 필요하다.
