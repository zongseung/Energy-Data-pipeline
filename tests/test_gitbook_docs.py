import re
from pathlib import Path
from urllib.parse import unquote, urlparse


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


# 카탈로그는 뷰가 11개라 한 페이지에 다 넣으면 좌측 목차 항목이 하나뿐이라
# 원하는 뷰로 갈 방법이 없다. 도메인별로 쪼개 하위 폴더에 뒀다.
CATALOG_PAGES = {
    "generation.md",
    "smp.md",
    "weather.md",
    "demand.md",
    "oil.md",
    "grid.md",
}


def test_final_page_tree() -> None:
    assert FINAL_PAGES == {path.name for path in DOCS.glob("*.md")}
    assert CATALOG_PAGES == {path.name for path in (DOCS / "catalog").glob("*.md")}
    for stale in ("01-data.md", "02-access.md", "03-terms.md"):
        assert not (DOCS / stale).exists()


LINK_RE = re.compile(r"\[[^\]]+\]\(([^)]+)\)")


def _pages():
    return sorted(DOCS.glob("*.md")) + sorted((DOCS / "catalog").glob("*.md"))


def _anchor(heading: str) -> str:
    """GitBook·GitHub 방식 앵커 슬러그.

    소문자화 → 구두점 **삭제**(공백으로 치환하지 않는다) → 공백 하나당 하이픈 하나.

    '## 함정 — is_aggregate (지금은 빼면 안 된다)' 에서 em dash 가 사라지면
    양옆 공백이 그대로 남아 하이픈이 둘 이어진다
    ('함정--is_aggregate-지금은-빼면-안-된다'). 구두점을 공백으로 바꾸거나
    연속 공백을 하나로 접으면 하이픈이 하나가 돼 실제 앵커와 어긋난다.
    """
    text = heading.lstrip("#").strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)      # \w 는 한글을 포함한다
    return text.strip().replace(" ", "-")


def test_internal_markdown_links_exist() -> None:
    for page in _pages():
        for raw_target in LINK_RE.findall(page.read_text(encoding="utf-8")):
            parsed = urlparse(raw_target)
            if parsed.scheme or raw_target.startswith("#"):
                continue
            target = unquote(raw_target.split("#", 1)[0])
            if target:
                assert (page.parent / target).exists(), f"{page}: {raw_target}"


def test_internal_link_anchors_exist() -> None:
    """앵커까지 지정한 링크가 실제 헤딩을 가리키는지 확인한다.

    파일 존재만 검사하면 페이지를 쪼갤 때 앵커가 조용히 깨진다. 실제로
    카탈로그를 4개로 분리하면서 `#함정--is_aggregate-...` 링크 2건이
    다른 파일로 옮겨갔다.
    """
    for page in _pages():
        for raw_target in LINK_RE.findall(page.read_text(encoding="utf-8")):
            parsed = urlparse(raw_target)
            if parsed.scheme or "#" not in raw_target:
                continue
            file_part, anchor = raw_target.split("#", 1)
            anchor = unquote(anchor)
            target = (page.parent / unquote(file_part)) if file_part else page
            headings = re.findall(r"^#{1,6} .*$", target.read_text(encoding="utf-8"), re.M)
            slugs = {_anchor(h) for h in headings}
            assert anchor in slugs, (
                f"{page}: '#{anchor}' 에 해당하는 헤딩이 {target.name} 에 없다. "
                f"후보: {sorted(s for s in slugs if 'aggregate' in s or anchor[:6] in s)}"
            )


def test_public_docs_contain_no_live_credentials() -> None:
    # 카탈로그 하위 페이지까지 전부 훑는다 — 최상위만 보던 시절 03-llm-mcp 에
    # 데모 서버의 Tailscale·LAN 주소가 그대로 실려 공개된 적이 있다.
    combined = "\n".join(page.read_text(encoding="utf-8") for page in _pages())
    assert "GITBOOK_API_TOKEN" not in combined
    assert not re.search(
        r"postgresql(?:\+\w+)?://(?!<)[^\s:/]+:[^<\s@]+@",
        combined,
    )
    assert not re.search(r"\b100\.(?:\d{1,3}\.){2}\d{1,3}\b", combined)
    # 접속 주소는 플레이스홀더로만 적는다. 루프백만 예외 — 연구원 PC 에서
    # 자기 로컬 서버를 가리키는 주소라 유출될 내부 정보가 없다.
    literal_hosts = [
        host for host in re.findall(r"https?://(\d{1,3}(?:\.\d{1,3}){3})", combined)
        if host != "127.0.0.1"
    ]
    assert not literal_hosts, f"공개 문서에 실주소가 있다: {literal_hosts}"


# GitBook 전용 블록은 여닫이가 안 맞으면 에러가 아니라 '{% endhint %}' 같은
# 글자가 그대로 렌더링된다. 조용히 깨지는 종류라 기계로 센다.
BLOCK_PAIRS = [
    ("hint", "endhint"),
    ("tabs", "endtabs"),
    ("tab", "endtab"),
    ("stepper", "endstepper"),
    ("step", "endstep"),
]


def test_gitbook_blocks_are_balanced() -> None:
    for page in _pages():
        text = page.read_text(encoding="utf-8")
        for opener, closer in BLOCK_PAIRS:
            opens = len(re.findall(r"\{%\s*" + opener + r"\b", text))
            closes = len(re.findall(r"\{%\s*" + closer + r"\s*%\}", text))
            # 'tab' 은 'tabs' 에도, 'step' 은 'stepper' 에도 걸리므로 뺀다.
            if opener == "tab":
                opens -= len(re.findall(r"\{%\s*tabs\b", text))
                closes -= len(re.findall(r"\{%\s*endtabs\s*%\}", text))
            if opener == "step":
                opens -= len(re.findall(r"\{%\s*stepper\b", text))
                closes -= len(re.findall(r"\{%\s*endstepper\s*%\}", text))
            assert opens == closes, f"{page}: {opener} 여는 {opens}개 / 닫는 {closes}개"
        assert text.count("<details") == text.count("</details>"), f"{page}: details 불일치"


def test_hint_styles_are_valid() -> None:
    """style 값을 잘못 쓰면 GitBook 이 블록을 통째로 무시한다."""
    valid = {"info", "success", "warning", "danger"}
    for page in _pages():
        for style in re.findall(r'\{%\s*hint\s+style="([^"]+)"', page.read_text(encoding="utf-8")):
            assert style in valid, f"{page}: 알 수 없는 hint style '{style}'"
