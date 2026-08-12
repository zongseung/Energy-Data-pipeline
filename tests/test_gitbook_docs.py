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
