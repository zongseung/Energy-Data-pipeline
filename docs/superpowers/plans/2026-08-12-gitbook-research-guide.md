# 연구 데이터 GitBook 안내서 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 연구원이 공개 GitBook에서 직접 SQL과 LLM·MCP 두 조회 방법을 쉽게 구분하고, Tailscale과 개인별 읽기전용 계정으로 실제 데이터를 안전하게 조회하도록 안내한다.

**Architecture:** 저장소의 `docs/gitbook/` Markdown을 단일 진실 원천으로 두고 GitBook Free Space와 Git Sync한다. 문서는 짧은 시작 페이지, 공통 데이터 흐름, 방법별 독립 가이드, 데이터 카탈로그, 이용조건으로 나누며 두 조회 방법 모두 Tailscale과 개인 PostgreSQL role을 공통 보안 경계로 사용한다. GitBook API는 기존 항목을 조회한 뒤 정확히 같은 제목이 없을 때 전용 Space와 공개 Site를 생성하는 데만 사용한다.

**Tech Stack:** GitBook Free, Git Sync, Markdown, Mermaid, Python 3 표준 라이브러리, pytest, GitBook REST API

## Global Constraints

- 한국어를 기본으로 하고 처음 나오는 기술어에는 한 줄 설명을 붙인다.
- GitBook은 공개 문서이며 실제 데이터는 Tailscale과 개인별 읽기전용 PostgreSQL role로 보호한다.
- 직접 SQL과 LLM·MCP는 독립된 공개 서비스가 아니라 같은 Tailscale 보안망 위의 두 사용 방식이다.
- 실제 비밀번호, 완성된 DSN, Tailnet IP, Tailscale 초대 링크, GitBook API 토큰을 저장소에 기록하지 않는다.
- GitBook 문서에는 운영 파이프라인 배포·장애 대응 절차와 수집기 내부 구현을 넣지 않는다.
- 기존 데이터 카탈로그의 수치·품질 경고·단위·시간 규약은 삭제하거나 의미를 바꾸지 않는다.
- 새 서드파티 의존성을 추가하지 않는다.
- 저장소 Markdown을 기준본으로 사용하고 GitBook 웹 편집은 긴급 수정 외 사용하지 않는다.
- 기존 GitBook 기본 Space·Site는 삭제하거나 덮어쓰지 않는다.
- API 생성은 정확한 제목으로 사전 조회한 뒤 수행하며, 무료 플랜 제한으로 실패하면 기존 항목을 임의 변경하지 않는다.

---

## 파일 구조

### 최종 GitBook 문서

| 파일 | 책임 |
|---|---|
| `docs/gitbook/README.md` | 1분 안에 대상·보안 경계·두 조회 방식·다음 행동을 이해시키는 시작 페이지 |
| `docs/gitbook/01-architecture.md` | Mermaid와 번호 목록으로 공통 요청·응답 흐름과 보호 장치를 설명 |
| `docs/gitbook/02-direct-sql.md` | Tailscale 연결부터 psql·pandas·R 첫 조회와 검증된 예제 쿼리 제공 |
| `docs/gitbook/03-llm-mcp.md` | 로컬 `energy-mcp` 설치·클라이언트 설정·질문·결과 검증·오류 해결 제공 |
| `docs/gitbook/04-data-catalog.md` | 기존 데이터 카탈로그와 스키마 사전의 정본 |
| `docs/gitbook/05-terms.md` | 이용조건, 재배포 금지, 감사 로그, 계정 회수, 서약 |
| `docs/gitbook/appendix-local-llm.md` | 외부 LLM에 결과를 보내지 않는 로컬 LLM 선택 경로 |
| `docs/gitbook/SUMMARY.md` | 시작하기·데이터 조회·데이터 이해·정책과 참고의 4개 목차 그룹 |

### 검증 파일

| 파일 | 책임 |
|---|---|
| `tests/test_gitbook_docs.py` | 필수 페이지, 공통 보안 설명, 내부 링크, 레거시 파일 제거, 비밀정보 미포함을 검증 |

### 제거되는 레거시 경로

- `docs/gitbook/01-data.md` → `04-data-catalog.md`로 이동
- `docs/gitbook/02-access.md` → 직접 SQL과 LLM·MCP 두 페이지로 분리 후 제거
- `docs/gitbook/03-terms.md` → `05-terms.md`로 이동

---

### Task 1: 문서 계약 테스트와 시작 페이지·아키텍처 작성

**Files:**
- Create: `tests/test_gitbook_docs.py`
- Modify: `docs/gitbook/README.md`
- Create: `docs/gitbook/01-architecture.md`

**Interfaces:**
- Consumes: 승인된 설계 `docs/superpowers/specs/2026-08-12-gitbook-research-guide-design.md`
- Produces: 두 조회 방식과 공통 Tailscale 경계를 정의하는 문서 정본, 이후 태스크가 확장할 pytest 계약

- [ ] **Step 1: 시작 페이지와 아키텍처의 실패 테스트 작성**

`tests/test_gitbook_docs.py`를 다음 내용으로 시작한다.

```python
from pathlib import Path


DOCS = Path("docs/gitbook")


def read(name: str) -> str:
    return (DOCS / name).read_text(encoding="utf-8")


def test_landing_explains_the_two_access_methods() -> None:
    text = read("README.md")
    assert "직접 SQL" in text
    assert "LLM·MCP" in text
    assert "Tailscale" in text
    assert "개인별" in text
    assert "공개" in text


def test_architecture_shows_the_shared_security_boundary() -> None:
    text = read("01-architecture.md")
    assert "```mermaid" in text
    assert "flowchart TB" in text
    assert "psql" in text
    assert "energy-mcp" in text
    assert "Tailscale 폐쇄망" in text
    assert "개인별 읽기전용" in text
    assert "research 스키마" in text
```

- [ ] **Step 2: 테스트가 예상대로 실패하는지 확인**

Run:

```bash
uv run pytest tests/test_gitbook_docs.py -q
```

Expected: `01-architecture.md`가 없으므로 `FileNotFoundError`로 실패.

- [ ] **Step 3: `README.md`를 짧은 시작 페이지로 다시 작성**

아래 순서를 그대로 사용한다.

```markdown
# 에너지 연구 데이터 이용 안내

발전량·SMP·기상·전력수요 데이터를 연구 목적으로 안전하게 조회하는 방법을 안내합니다.

> 이 GitBook은 누구나 읽을 수 있는 공개 안내서입니다. 실제 데이터는 Tailscale 폐쇄망과 연구원별 읽기전용 PostgreSQL 계정으로 보호됩니다.

## 처음 한 번만 준비하세요

1. 이용조건을 읽고 서약합니다.
2. 관리자가 보낸 초대로 Tailscale에 가입합니다.
3. 개인별 DB 계정을 별도 채널로 받습니다.
4. 직접 SQL 또는 LLM·MCP 중 조회 방법을 선택합니다.

## 어떤 방법을 사용할까요?
```

이어지는 비교표에는 다음 행을 넣는다.

| 구분 | 직접 SQL | LLM·MCP |
|---|---|---|
| 추천 대상 | 재현 가능한 분석·통계·그래프 | 스키마 탐색·간단한 자연어 조회 |
| 사용하는 도구 | psql, pandas, R, DBeaver | Claude Desktop 등 LLM 클라이언트와 로컬 `energy-mcp` |
| 공통 조건 | Tailscale + 개인 DB 계정 | Tailscale + 같은 개인 DB 계정 |
| 결과 확인 | SQL과 원자료를 직접 확인 | 생성된 SQL과 데이터 규칙을 반드시 재확인 |

마지막에는 `01-architecture.md`, `02-direct-sql.md`, `03-llm-mcp.md`,
`04-data-catalog.md`, `05-terms.md`로 가는 명시적 링크를 둔다. 설치 명령과
긴 스키마 표는 첫 화면에 넣지 않는다.

- [ ] **Step 4: 공통 흐름 문서 작성**

`docs/gitbook/01-architecture.md`에 다음 절을 순서대로 작성한다.

```markdown
# 데이터 제공 구조

직접 SQL과 LLM·MCP가 어디에서 갈라지고, 어디에서 다시 같은 보안 경계를 사용하는지 설명합니다.

## 한눈에 보는 전체 흐름

## 최초 1회 준비

## 방법 1: 직접 SQL

## 방법 2: LLM·MCP

## 두 방법에 공통인 보호 장치

## GitBook에 공개하지 않는 정보
```

`한눈에 보는 전체 흐름`에는 설계 문서의 Mermaid를 그대로 사용한다. Mermaid
바로 아래에는 다음 네 가지를 한 줄씩 설명한다.

- 네트워크: Tailscale 밖에서는 PostgreSQL에 연결할 수 없음
- 권한: 운영 테이블은 숨기고 `research` 스키마 뷰만 `SELECT` 허용
- 식별: 연구원마다 서로 다른 PostgreSQL role 사용
- 감사: 누가 어떤 SQL을 실행했는지 DB 로그에 기록

`GitBook에 공개하지 않는 정보`에는 실제 호스트, 비밀번호, Tailscale 초대 링크,
개인별 DSN을 별도 채널로 전달한다고 명시한다.

- [ ] **Step 5: 집중 테스트 실행**

Run:

```bash
uv run pytest tests/test_gitbook_docs.py -q
```

Expected: `2 passed`.

- [ ] **Step 6: Task 1 변경만 커밋**

새 `docs/` 파일은 저장소 ignore 규칙 때문에 정확한 경로만 강제 추가한다.

```bash
git add tests/test_gitbook_docs.py docs/gitbook/README.md
git add -f docs/gitbook/01-architecture.md
git commit -m "docs: GitBook 시작 페이지와 데이터 흐름 추가"
```

---

### Task 2: 직접 SQL과 LLM·MCP 가이드 분리

**Files:**
- Modify: `tests/test_gitbook_docs.py`
- Create: `docs/gitbook/02-direct-sql.md`
- Create: `docs/gitbook/03-llm-mcp.md`
- Source: `docs/gitbook/02-access.md`
- Source: `mcp-server/README.md`

**Interfaces:**
- Consumes: Task 1의 공통 Tailscale·개인 role 정의와 기존 검증된 접속·쿼리 예시
- Produces: 연구원이 위에서 아래로 따라 할 수 있는 두 개의 독립된 조회 가이드

- [ ] **Step 1: 방법별 페이지 계약 테스트 추가**

`tests/test_gitbook_docs.py`에 다음 테스트를 추가한다.

```python
def test_direct_sql_guide_is_complete() -> None:
    text = read("02-direct-sql.md")
    for required in (
        "tailscale status",
        "psql",
        "pandas",
        "RPostgres",
        "statement_timeout",
        "LIMIT 100",
        "문제 해결",
    ):
        assert required in text


def test_mcp_guide_uses_the_same_personal_database_role() -> None:
    text = read("03-llm-mcp.md")
    for required in (
        "로컬",
        "stdio",
        "energy-mcp",
        "ENERGY_MCP_DSN",
        "Tailscale",
        "개인",
        "run_sql",
        "실행 SQL",
        "직접 SQL로 전환",
    ):
        assert required in text
```

- [ ] **Step 2: 새 페이지가 없어 실패하는지 확인**

Run:

```bash
uv run pytest tests/test_gitbook_docs.py -q
```

Expected: `02-direct-sql.md` 또는 `03-llm-mcp.md`의 `FileNotFoundError`.

- [ ] **Step 3: 직접 SQL 가이드 작성**

`docs/gitbook/02-direct-sql.md`의 절 순서는 다음과 같다.

```markdown
# 직접 SQL로 조회

## 시작하기 전에
## 1. Tailscale 설치와 연결 확인
## 2. 개인 접속정보 이해하기
## 3. psql로 첫 조회
## 4. Python·pandas로 조회
## 5. R로 조회
## 6. 자주 쓰는 검증된 쿼리
## 문제 해결
```

`02-access.md`의 Tailscale, 포트 표, role 제약, psql, pandas, R, 예제 쿼리
8개를 의미 변경 없이 옮긴다. 첫 psql 쿼리는 기간과 `LIMIT 100`이 있는 작은
조회로 시작한다. 모든 DSN은 `<발급받은_ID>`, `<발급받은_비밀번호>`,
`<tailnet-host>` 플레이스홀더를 유지한다.

`문제 해결`에는 다음을 넣는다.

| 증상 | 먼저 확인할 것 | 해결 |
|---|---|---|
| 연결 시간 초과 | `tailscale status` | Tailscale 재연결 후 관리자에게 호스트 확인 |
| 인증 실패 | 개인 role·비밀번호 | 문서나 채팅에 비밀번호를 붙이지 말고 재발급 요청 |
| 60초 후 쿼리 종료 | 조회 기간 | 기간·발전소·지점 조건과 `LIMIT` 추가 |
| 뷰를 찾을 수 없음 | 접속 DB와 `search_path` | pv/demand 포트와 발급 계정 재확인 |

- [ ] **Step 4: LLM·MCP 가이드 작성**

`docs/gitbook/03-llm-mcp.md`의 첫 문단에 다음 문장을 포함한다.

```markdown
`energy-mcp`는 인터넷에 공개된 서버가 아니라 연구원 PC에서 실행되는 로컬 stdio 프로그램입니다. LLM·MCP 방식도 먼저 Tailscale에 연결하며 직접 SQL 방식과 같은 개인 DB 계정을 사용합니다.
```

절 순서는 다음과 같다.

```markdown
# LLM·MCP로 조회

## 이 방법이 적합한 경우
## 요청이 처리되는 순서
## 시작하기 전에
## 1. 로컬 energy-mcp 실행
## 2. pv와 demand 서버 등록
## 3. 첫 질문 보내기
## 4. 실행 SQL과 결과 검증
## 좋은 질문과 피해야 할 질문
## 문제 해결
## 직접 SQL로 전환
```

현재 PyPI 미배포 상태를 숨기지 말고 다음 실행법만 제공한다.

```bash
uvx --from /path/to/Energy-Data-pipeline/mcp-server energy-mcp
```

Claude Desktop 설정에는 pv와 demand 두 서버를 각각 등록하고 DSN에는
플레이스홀더만 사용한다. `run_sql` 읽기전용, 기본 60초 제한, 기본 10,000행
제한, 잘림 표시를 설명한다. 첫 질문은 다음 문장을 사용한다.

```text
2025년 구미태양광의 월별 발전량 합계를 조회해 줘. 실행한 SQL도 함께 보여줘.
```

최종 분석은 실행 SQL, `04-data-catalog.md`의 단위·시간 규약·품질 등급을
연구원이 직접 확인해야 한다고 명시한다.

- [ ] **Step 5: 방법별 테스트 실행**

Run:

```bash
uv run pytest tests/test_gitbook_docs.py -q
```

Expected: `4 passed`.

- [ ] **Step 6: Task 2 변경만 커밋**

```bash
git add tests/test_gitbook_docs.py
git add -f docs/gitbook/02-direct-sql.md docs/gitbook/03-llm-mcp.md
git commit -m "docs: 직접 SQL과 LLM MCP 가이드 분리"
```

---

### Task 3: 카탈로그·약관 이동과 최종 목차·링크 정리

**Files:**
- Modify: `tests/test_gitbook_docs.py`
- Move: `docs/gitbook/01-data.md` → `docs/gitbook/04-data-catalog.md`
- Move: `docs/gitbook/03-terms.md` → `docs/gitbook/05-terms.md`
- Modify: `docs/gitbook/README.md`
- Modify: `docs/gitbook/01-architecture.md`
- Modify: `docs/gitbook/02-direct-sql.md`
- Modify: `docs/gitbook/03-llm-mcp.md`
- Modify: `docs/gitbook/appendix-local-llm.md`
- Modify: `docs/gitbook/SUMMARY.md`
- Delete: `docs/gitbook/02-access.md`

**Interfaces:**
- Consumes: Task 1·2의 새 페이지와 기존 카탈로그·약관·로컬 LLM 내용
- Produces: 깨진 링크와 중복 페이지가 없는 최종 GitBook 트리

- [ ] **Step 1: 최종 문서 트리·링크·비밀정보 테스트 추가**

`tests/test_gitbook_docs.py` 상단에 다음 import를 추가한다.

```python
import re
from urllib.parse import unquote, urlparse
```

이어 다음 테스트를 추가한다.

```python
FINAL_PAGES = {
    "README.md",
    "01-architecture.md",
    "02-direct-sql.md",
    "03-llm-mcp.md",
    "04-data-catalog.md",
    "05-terms.md",
    "appendix-local-llm.md",
    "SUMMARY.md",
}


def test_final_page_tree() -> None:
    assert FINAL_PAGES == {path.name for path in DOCS.glob("*.md")}
    for stale in ("01-data.md", "02-access.md", "03-terms.md"):
        assert not (DOCS / stale).exists()


def test_internal_markdown_links_exist() -> None:
    link_pattern = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
    for page in DOCS.glob("*.md"):
        for raw_target in link_pattern.findall(page.read_text(encoding="utf-8")):
            parsed = urlparse(raw_target)
            if parsed.scheme or raw_target.startswith("#"):
                continue
            target = unquote(raw_target.split("#", 1)[0])
            if target:
                assert (page.parent / target).exists(), f"{page}: {raw_target}"


def test_public_docs_contain_no_live_credentials() -> None:
    combined = "\n".join(read(name) for name in FINAL_PAGES)
    assert "GITBOOK_API_TOKEN" not in combined
    assert not re.search(
        r"postgresql(?:\+\w+)?://(?!<)[^\s:/]+:[^<\s@]+@",
        combined,
    )
    assert not re.search(r"\b100\.(?:\d{1,3}\.){2}\d{1,3}\b", combined)
```

- [ ] **Step 2: 레거시 파일이 남아 있어 실패하는지 확인**

Run:

```bash
uv run pytest tests/test_gitbook_docs.py -q
```

Expected: `test_final_page_tree`가 레거시 파일 존재와 새 파일 부재로 실패.

- [ ] **Step 3: 카탈로그와 약관을 새 이름으로 이동**

`apply_patch`의 Move를 사용해 파일 내용을 그대로 보존한다.

```text
docs/gitbook/01-data.md  → docs/gitbook/04-data-catalog.md
docs/gitbook/03-terms.md → docs/gitbook/05-terms.md
```

카탈로그 제목은 `# 데이터 카탈로그 · 스키마 사전`, 약관 제목은
`# 이용조건 · 보안 서약`으로 바꾼다. 데이터 수치와 경고 본문은 수정하지 않는다.

- [ ] **Step 4: 모든 상대 링크를 새 경로로 갱신**

다음 치환을 `docs/gitbook/*.md` 전체에 적용한다.

```text
01-data.md  → 04-data-catalog.md
02-access.md → 문맥에 따라 02-direct-sql.md 또는 03-llm-mcp.md
03-terms.md → 05-terms.md
```

`appendix-local-llm.md` 첫 링크는 `03-llm-mcp.md`, DSN 안내 링크는
`02-direct-sql.md`, 데이터 함정 링크는 `04-data-catalog.md`를 가리키게 한다.

- [ ] **Step 5: `SUMMARY.md`를 네 그룹으로 정리**

최종 내용은 다음과 같다.

```markdown
# Summary

## 시작하기

* [한눈에 보기](README.md)
* [데이터 제공 구조](01-architecture.md)

## 데이터 조회

* [직접 SQL로 조회](02-direct-sql.md)
* [LLM·MCP로 조회](03-llm-mcp.md)

## 데이터 이해

* [데이터 카탈로그 · 스키마 사전](04-data-catalog.md)

## 정책과 참고

* [이용조건 · 보안 서약](05-terms.md)
* [부록: 로컬 LLM](appendix-local-llm.md)
```

- [ ] **Step 6: 레거시 `02-access.md` 제거**

직접 SQL 예제 8개와 MCP 핵심 설명이 새 페이지에 모두 존재하는지 대조한 뒤
`docs/gitbook/02-access.md`만 삭제한다.

- [ ] **Step 7: 최종 문서 계약 테스트 실행**

Run:

```bash
uv run pytest tests/test_gitbook_docs.py -q
```

Expected: 모든 테스트 통과.

- [ ] **Step 8: Task 3 변경만 커밋**

새 경로가 ignore되므로 정확한 문서 경로를 강제 추가하고 삭제도 함께 스테이징한다.

```bash
git add tests/test_gitbook_docs.py docs/gitbook/README.md docs/gitbook/SUMMARY.md docs/gitbook/appendix-local-llm.md
git add -u docs/gitbook
git add -f docs/gitbook/01-architecture.md docs/gitbook/02-direct-sql.md docs/gitbook/03-llm-mcp.md docs/gitbook/04-data-catalog.md docs/gitbook/05-terms.md
git commit -m "docs: 연구 데이터 GitBook 목차와 링크 정리"
```

---

### Task 4: 문서 전체 회귀 검증과 가독성 검토

**Files:**
- Review: `docs/gitbook/*.md`
- Review: `tests/test_gitbook_docs.py`
- Modify only if needed: 위 검토에서 발견된 문서 또는 테스트

**Interfaces:**
- Consumes: Task 1~3의 최종 문서 트리
- Produces: GitBook에 연결해도 되는 검증 완료 Markdown

- [ ] **Step 1: GitBook 문서 계약 테스트 실행**

Run:

```bash
uv run pytest tests/test_gitbook_docs.py -q
```

Expected: 모든 테스트 통과.

- [ ] **Step 2: 전체 저장소 테스트 실행**

Run:

```bash
uv run pytest -q
```

Expected: 기존 테스트를 포함해 모두 통과. 환경 의존 테스트는 기존 skip 조건에 따라 skip.

- [ ] **Step 3: Markdown과 Git diff 검사**

Run:

```bash
git diff --check HEAD~3..HEAD
rg -n "01-data\.md|02-access\.md|03-terms\.md" docs/gitbook tests/test_gitbook_docs.py
```

Expected: 공백 오류 없음. 두 번째 명령은 테스트의 레거시 파일 목록 외 문서 본문에서 결과 없음.

- [ ] **Step 4: 내용 보존과 가독성 수동 검토**

다음을 한 항목씩 확인한다.

- 첫 화면에서 1분 안에 직접 SQL과 LLM·MCP의 차이를 설명할 수 있음
- 모든 페이지 첫 문단이 독자와 목적을 설명함
- 직접 SQL 절차가 Tailscale → 개인 계정 → 첫 제한 쿼리 순서임
- MCP 절차가 로컬 stdio → 같은 개인 role → 실행 SQL 검증 순서임
- 기존 카탈로그의 행수·기간·단위·품질·시간 규약이 유지됨
- 긴 명령과 SQL은 복사 가능한 코드 블록에 있음
- 실제 비밀정보와 운영자 전용 명령이 없음

- [ ] **Step 5: 검토 수정이 있으면 별도 커밋**

변경이 있을 때만 실행한다.

```bash
git add tests/test_gitbook_docs.py
git add -f docs/gitbook/*.md
git commit -m "docs: GitBook 가독성과 검증 오류 보완"
```

---

### Task 5: GitBook API로 전용 Space와 공개 Site 생성

**Files:**
- Read only: `.env`
- External state: GitBook organization, Space, Docs Site
- No repository file changes

**Interfaces:**
- Consumes: `.env`의 `GITBOOK_API_TOKEN`, Task 4의 검증 완료 문서
- Produces: `에너지 연구 데이터 안내`라는 전용 Space와 `basic`·`public` Docs Site

- [ ] **Step 1: 토큰을 노출하지 않고 인증 확인**

`.env` 전체를 `source`하지 않는다. `GITBOOK_API_TOKEN` 값만 읽어 Bearer 헤더로
사용하고 `GET https://api.gitbook.com/v1/user`를 호출한다. 응답 본문은 임시 파일에
저장하며 화면에는 HTTP 상태 코드만 출력한다.

Expected: `200`.

- [ ] **Step 2: 조직·Space·Site를 읽기 전용으로 재조회**

아래 endpoint를 순서대로 호출한다.

```text
GET /v1/orgs?limit=100
GET /v1/orgs/$organization_id/spaces?limit=100
GET /v1/orgs/$organization_id/sites?limit=100
```

조직이 정확히 하나가 아니면 생성하지 않고 사용자에게 조직 선택을 요청한다.
Space와 Site에서는 제목이 정확히 `에너지 연구 데이터 안내`인 항목만 재사용한다.
응답 JSON에서 선택한 조직 ID를 `organization_id`, 생성 또는 재사용한 Space ID를
`space_id`, Site ID를 `site_id` 셸 변수로 보관한다. 이 값은 저장소 파일에 쓰지 않는다.

- [ ] **Step 3: 전용 Space가 없을 때만 생성**

Request:

```http
POST /v1/orgs/$organization_id/spaces
Authorization: Bearer $GITBOOK_API_TOKEN
Content-Type: application/json

{
  "title": "에너지 연구 데이터 안내",
  "emoji": "⚡",
  "language": "ko",
  "editMode": "live"
}
```

Expected: 새 생성은 `201`; 기존 정확한 제목이 있으면 POST하지 않고 기존 ID 사용.

- [ ] **Step 4: 전용 Site가 없을 때만 생성**

Request:

```http
POST /v1/orgs/$organization_id/sites
Authorization: Bearer $GITBOOK_API_TOKEN
Content-Type: application/json

{
  "type": "basic",
  "title": "에너지 연구 데이터 안내",
  "visibility": "public",
  "spaces": ["$space_id"]
}
```

Expected: 새 생성은 `201`; 기존 정확한 제목이 있으면 POST하지 않고 기존 Site 사용.
무료 플랜 제한 등으로 `4xx`가 오면 기존 `API guide` Site를 변경하지 말고 중단해
응답 상태와 오류 메시지만 보고한다.

- [ ] **Step 5: 생성 결과 검증**

아래 GET 요청으로 결과를 확인한다.

```text
GET /v1/spaces/$space_id
GET /v1/orgs/$organization_id/sites/$site_id
GET /v1/orgs/$organization_id/sites/$site_id/site-spaces
```

Expected: 제목 `에너지 연구 데이터 안내`, Site type `basic`, visibility `public`,
대상 Space가 Site에 연결됨. ID와 공개 URL은 최종 보고에만 사용하고 저장소에 기록하지 않는다.

- [ ] **Step 6: 임시 API 응답 파일 삭제**

토큰 자체는 응답에 없지만 사용자 프로필과 조직 정보가 남지 않도록 이 태스크에서
만든 `/tmp/gitbook-*.json` 파일만 정확히 삭제한다.

---

### Task 6: GitHub App·Git Sync·Mermaid UI 연결과 공개 검증

**Files:**
- External state: GitBook Space integrations and Git Sync settings
- No repository file changes

**Interfaces:**
- Consumes: Task 5의 Space·Site, GitHub의 현재 저장소, `docs/gitbook/` 최종 문서
- Produces: 기본 브랜치 변경이 자동 반영되고 Mermaid가 렌더링되는 공개 GitBook URL

- [ ] **Step 1: 사용자가 GitBook GitHub App 권한을 승인**

GitBook Space에서 `Set up Git Sync`를 열고 GitHub App에 현재 저장소만 접근 권한을
준다. 이 단계는 GitBook API 토큰으로 대체하지 않는다.

- [ ] **Step 2: Git Sync 범위 설정**

다음 값을 설정한다.

```text
Repository: zongseung/Energy-Data-pipeline
Branch: main
Project directory: docs/gitbook
Initial sync direction: GitHub → GitBook
```

최초 동기화 전에 GitBook 웹 편집기의 기본 콘텐츠가 중요한 사용자 문서가 아닌지
확인한다. 새 전용 Space이므로 GitHub 내용을 기준으로 동기화한다. 문서 변경이
`main`에 합쳐지고 GitHub에 push된 뒤 최초 동기화를 실행한다.

- [ ] **Step 3: Mermaid 연동 활성화**

GitBook 조직의 Integrations에서 Mermaid를 설치하고, `에너지 연구 데이터 안내`
Space에 활성화한다. `01-architecture.md`의 Mermaid가 일반 코드 블록이 아니라
다이어그램으로 보이는지 확인한다.

- [ ] **Step 4: 공개 사이트 검증**

비로그인 시크릿 브라우저에서 다음을 확인한다.

- 시작 페이지와 7개 읽기 페이지가 모두 열림
- 왼쪽 목차가 네 그룹으로 보임
- 내부 링크와 이전/다음 이동이 정상
- Mermaid 다이어그램이 렌더링됨
- 실제 호스트·비밀번호·초대 링크가 없음
- 모바일 폭에서 표와 코드 블록을 읽을 수 있음

- [ ] **Step 5: Git Sync 메타데이터와 최초 동기화 검증**

`GET /v1/spaces/$space_id/git/info`를 호출해 `installationProvider`가 `github`,
저장소가 `zongseung/Energy-Data-pipeline`인지 확인한다. GitBook의 동기화 이력에서
`main`의 최신 문서 커밋을 가져왔는지 확인하고, 공개 사이트의 `README.md` 제목과
`01-architecture.md` Mermaid가 해당 커밋 내용과 일치하면 연동 완료로 판정한다.

---

## 최종 완료 조건

- `tests/test_gitbook_docs.py`와 전체 pytest가 통과한다.
- `docs/gitbook/`에는 최종 8개 Markdown 파일만 남고 레거시 3개 파일은 없다.
- 직접 SQL과 LLM·MCP 모두 Tailscale과 개인 role을 공통으로 사용한다고 설명한다.
- GitBook API 토큰과 실제 연구원 인증정보가 Git에 들어가지 않는다.
- 기존 GitBook 기본 Space·Site는 삭제하거나 덮어쓰지 않는다.
- 전용 Space와 공개 Site가 중복 없이 존재한다.
- Git Sync가 `docs/gitbook`만 가져오고 Mermaid가 실제 다이어그램으로 보인다.
- 비로그인 사용자가 공개 URL에서 전체 안내서를 읽을 수 있다.
