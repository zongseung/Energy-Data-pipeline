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
