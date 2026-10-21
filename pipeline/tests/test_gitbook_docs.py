import re
from pathlib import Path
from urllib.parse import unquote, urlparse


DOCS = Path("docs/gitbook")


def read(name: str) -> str:
    return (DOCS / name).read_text(encoding="utf-8")


def test_landing_answers_how_to_use_what_data_and_how_security_works() -> None:
    text = read("README.md")
    # 처음 온 사람의 세 질문: 어떻게 쓰나 / 어떤 데이터가 있나 / 보안은 어떻게 되나
    for heading in ("## 사용방법", "## 어떤 데이터가 들어 있나요", "## 보안설명"):
        assert heading in text
    for required in ("승인", "읽기", "OpenAI", "감사", "공개"):
        assert required in text
    assert "start/access.md" in text and "start/first-query.md" in text
    assert "데이터 처리와 투명성" in text
    # 직접 SQL 안내는 공개 문서에서 뺐다 — 개인 role 이 발급되지 않는다.
    assert "직접 SQL" not in text


def test_start_pages_describe_the_authkey_flow() -> None:
    access = read("start/access.md")
    for required in ("tskey-auth-", "--auth-key", "--hostname", "Log in", "서약", "PowerShell", "Mac", "status", "logout"):
        assert required in access
    assert "초대 링크" not in access  # 이메일 초대 방식은 쓰지 않는다
    assert "join-researcher.bat" not in access  # .bat 은 배포하지 않는다 — 명령어 한 줄을 그대로 전달한다
    assert "powershell-example.png" in access

    first = read("start/first-query.md")
    for required in ("energy-db", "CSV", "로그인"):
        assert required in first
    # 가입은 열어 둔다. 대신 이름·이메일을 서약 폼과 같게 쓰게 해 대장과 맞춘다.
    assert "회원가입" in first and "서약 폼에 적은" in first

    ask = read("start/ask.md")
    for required in ("질문에 넣을 것", "되묻", "결과를 쓰기 전에", "CSV", "30만", "공유"):
        assert required in ask
    # 직접 SQL·다른 도구 안내는 공개 문서에서 뺐다 — 문의로만 받는다.
    for page in ("start/access.md", "start/ask.md", "start/first-query.md"):
        assert "02-direct-sql" not in read(page)


def test_summary_groups_pages_by_what_the_reader_wants_to_do() -> None:
    summary = read("SUMMARY.md")
    for group in ("시작하기", "데이터 구성", "보안과 이용조건"):
        assert f"## {group}" in summary
    for listed in (
        "README.md", "start/access.md", "start/first-query.md", "start/ask.md",
        "04-data-catalog.md", "catalog/forecast-collect.md", "06-llm-transparency.md",
    ):
        assert listed in summary
    # 조회 방법은 '시작하기'에 합쳤다 — 따로 그룹을 두지 않는다.
    for gone in ("데이터 조회 방법", "03-llm-mcp.md", "appendix-local-llm.md"):
        assert gone not in summary
    # 데이터 제공 구조는 독립 그룹이 아니라 '데이터 구성' 아래 목록 항목이다.
    assert summary.index("## 데이터 구성") < summary.index("01-architecture.md") < summary.index("## 보안과 이용조건")
    assert "02-direct-sql.md" not in summary  # 직접 SQL 은 비공개로 돌렸다


def test_architecture_shows_the_user_flow_diagram_and_hosted_lookup() -> None:
    text = read("01-architecture.md")
    assert "](images/user-architecture.png)" in text
    assert (DOCS / "images/user-architecture.png").is_file()
    assert "직접 SQL" not in text
    assert "Tailscale" in text
    assert "LibreChat" in text
    assert "`demo_ro`" in text
    for tool in ("describe_data", "run_sql", "collect_forecast", "forecast_collection_status"):
        assert f"`{tool}`" in text
    assert "PgBouncer" in text
    assert "research" in text


def test_root_readme_describes_the_hosted_lookup_without_an_approval_step() -> None:
    text = Path("README.md").read_text(encoding="utf-8")

    assert "LibreChat" in text
    assert "공용 읽기전용 role `demo_ro`" in text
    assert "승인" not in text
    assert "레거시 `run_sql`" not in text


def test_forecast_collection_page_explains_requests_and_states() -> None:
    text = read("catalog/forecast-collect.md")
    for required in ("NAS 기상 조회", "수집을 요청", "completed", "partial", "unavailable", "failed", "UTC", "운영 검증 기록"):
        assert required in text
    # 데스크톱 클라이언트 설정 안내는 두지 않는다.
    assert "Claude Desktop" not in text
    assert "claude_desktop_config" not in text


FINAL_PAGES = {
    "README.md",
    "01-architecture.md",
    "04-data-catalog.md",
    "05-terms.md",
    "06-llm-transparency.md",
    "SUMMARY.md",
}


def test_llm_transparency_page_documents_real_storage_and_retention() -> None:
    text = read("06-llm-transparency.md")
    for required in (
        "LibreChat",
        "`energy_mcp.weather_collection_jobs`",
        "대화 간 장기 기억",
        "질문 원문",
        "조회 결과 행",
        "7일",
        "비동기",
        "24시간",
        "OpenAI",
        "학습에 사용하지",
        "Zero Data Retention",
        "API 키",
        "JWT",
    ):
        assert required in text

    llm_pages = "\n".join(
        read(name)
        for name in ("README.md", "01-architecture.md", "start/ask.md", "06-llm-transparency.md")
    )
    for excluded in (
        "IP 허용 목록",
        "LLM_APPROVAL_PUBLIC_ORIGIN",
        "LLM_EXPORT_PUBLIC_ORIGIN",
    ):
        assert excluded not in llm_pages


# 카탈로그는 뷰가 11개라 한 페이지에 다 넣으면 좌측 목차 항목이 하나뿐이라
# 원하는 뷰로 갈 방법이 없다. 도메인별로 쪼개 하위 폴더에 뒀다.
CATALOG_PAGES = {
    "generation.md",
    "smp.md",
    "weather.md",
    "forecast.md",
    "forecast-collect.md",
    "demand.md",
    "oil.md",
    "grid.md",
}


START_PAGES = {"access.md", "first-query.md", "ask.md"}


def test_final_page_tree() -> None:
    assert FINAL_PAGES == {path.name for path in DOCS.glob("*.md")}
    assert CATALOG_PAGES == {path.name for path in (DOCS / "catalog").glob("*.md")}
    assert START_PAGES == {path.name for path in (DOCS / "start").glob("*.md")}
    for stale in ("01-data.md", "02-access.md", "03-terms.md"):
        assert not (DOCS / stale).exists()


LINK_RE = re.compile(r"\[[^\]]+\]\(([^)]+)\)")


def _pages():
    return (
        sorted(DOCS.glob("*.md"))
        + sorted((DOCS / "start").glob("*.md"))
        + sorted((DOCS / "catalog").glob("*.md"))
    )


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
    # 메일 주소를 공개 문서에 두면 수집 봇의 표적이 된다. 제출·문의 창구는
    # 구글 폼이나 '관리자에게 문의' 로만 적는다.
    emails = re.findall(r"[\w.+-]+@[\w-]+\.[\w.]+", combined)
    assert not emails, f"공개 문서에 메일 주소가 있다: {emails}"


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


def test_backslash_line_breaks_have_no_trailing_space() -> None:
    """줄 끝 `\\` 는 줄바꿈이지만 뒤에 공백이 있으면 글자 `\\` 가 그대로 보인다.

    코드 블록 안(셸 줄 이음)은 문서 렌더링과 무관하므로 제외한다.
    """
    for page in _pages():
        text = re.sub(r"```.*?```", "", page.read_text(encoding="utf-8"), flags=re.S)
        bad = [n for n, line in enumerate(text.splitlines(), 1) if re.search(r"\\[ \t]+$", line)]
        assert not bad, f"{page}: 역슬래시 뒤 공백 (본문 {bad}번째 줄 부근)"


def test_no_accidental_strikethrough_tildes() -> None:
    """한 줄에 물결표가 둘 이상이고 글자에 붙어 있으면 GitBook 이 취소선으로 해석한다.

    '1~24시 표기를 0~23시' 가 '~24시 표기를 0~' 에 줄이 그어진 채 올라간 적이 있다.
    공백으로 띄운 ' ~ ' 와 코드(백틱) 안의 물결표는 괜찮다.
    """
    for page in _pages():
        text = re.sub(r"```.*?```", "", page.read_text(encoding="utf-8"), flags=re.S)
        for n, line in enumerate(text.splitlines(), 1):
            plain = re.sub(r"`[^`]*`", "", line).replace("\\~", "")
            tildes = [m.start() for m in re.finditer("~", plain)]
            attached = [i for i in tildes if (i > 0 and not plain[i - 1].isspace()) or (i + 1 < len(plain) and not plain[i + 1].isspace())]
            assert not (len(tildes) >= 2 and attached), f"{page}: 물결표가 붙어 있다 (본문 {n}번째 줄): {line[:60]}"


def test_hint_styles_are_valid() -> None:
    """style 값을 잘못 쓰면 GitBook 이 블록을 통째로 무시한다."""
    valid = {"info", "success", "warning", "danger"}
    for page in _pages():
        for style in re.findall(r'\{%\s*hint\s+style="([^"]+)"', page.read_text(encoding="utf-8")):
            assert style in valid, f"{page}: 알 수 없는 hint style '{style}'"


# 질문마다 SQL 을 사람이 승인하던 방식(2026-10-06~21)은 없앴다. 공개 문서에 그 절차가 다시 들어오면 안 된다.
APPROVAL_TERMS = ("승인 링크", "승인 화면", "승인 workflow", "plan_query", "execute_query", "query_workflows", "SQL 계획")


def test_no_page_describes_a_sql_approval_step() -> None:
    for page in _pages():
        text = page.read_text(encoding="utf-8")
        for term in APPROVAL_TERMS:
            assert term not in text, f"{page.name}: {term}"
