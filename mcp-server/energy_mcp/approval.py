from collections.abc import Callable
from datetime import timezone
from hmac import compare_digest
from html import escape
from re import search
from urllib.parse import parse_qsl

from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import HTMLResponse, PlainTextResponse

from energy_mcp.workflow import _hash, approve_workflow, decline_workflow, issue_csrf, utcnow

SECURITY_HEADERS = {
    "Cache-Control": "no-store",
    "Content-Security-Policy": (
        "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; "
        "base-uri 'none'; frame-ancestors 'none'"
    ),
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
}

APPROVAL_HTML = """<!doctype html>
<html lang="ko">
<head><meta charset="utf-8"><title>조회 조건 승인</title></head>
<body>
<h1>조회 조건을 확인하세요</h1>
<p>{summary}</p>
<pre>{sql}</pre>
<p>만료 시각: {expires_at}</p>
<form method="post" action="{form_action}">
<input type="hidden" name="token" value="{token}">
<input type="hidden" name="csrf" value="{csrf}">
<button name="action" value="confirm" type="submit">승인</button>
<button name="action" value="decline" type="submit">거절</button>
</form>
</body>
</html>"""


def _text(message: str, status_code: int) -> PlainTextResponse:
    return PlainTextResponse(message, status_code=status_code, headers=SECURITY_HEADERS)


def _form(body: bytes) -> dict[str, str] | None:
    try:
        raw = body.decode()
        if search(r"%(?![0-9A-Fa-f]{2})", raw):
            return None
        pairs = parse_qsl(raw, keep_blank_values=True, strict_parsing=True,
                          encoding="utf-8", errors="strict", max_num_fields=3)
    except (UnicodeDecodeError, ValueError):
        return None
    if len(pairs) != 3 or {key for key, _ in pairs} != {"action", "token", "csrf"}:
        return None
    form = dict(pairs)
    return form if all(form.values()) else None


async def approval_get(request: Request, collection, now=utcnow):
    workflow_id = request.path_params["workflow_id"]
    token = request.query_params.get("token", "")
    current = now()
    doc = collection.find_one({
        "_id": workflow_id,
        "status": "awaiting_confirmation",
        "expires_at": {"$gt": current},
        "approval_token_hash": _hash(token),
    })
    if doc is None:
        return _text("승인 요청이 없거나 만료됐습니다.", 404)
    sql = doc.get("sql")
    sql_sha256 = doc.get("sql_sha256")
    if not isinstance(sql, str) or not isinstance(sql_sha256, str) or not compare_digest(
        _hash(sql), sql_sha256
    ):
        return _text("승인 SQL이 변경됐습니다. 새 workflow를 만드세요.", 409)
    try:
        csrf = issue_csrf(collection, workflow_id, sql, sql_sha256, current)
    except RuntimeError:
        return _text("승인 요청이 없거나 만료됐습니다.", 404)
    body = APPROVAL_HTML.format(
        summary=escape(doc["summary"]),
        sql=escape(sql),
        expires_at=escape(
            doc["expires_at"].astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        ),
        form_action=escape(request.url.path, quote=True),
        token=escape(token, quote=True),
        csrf=escape(csrf, quote=True),
    )
    return HTMLResponse(body, headers=SECURITY_HEADERS)


async def approval_post(request: Request, collection, now=utcnow):
    if request.headers.get("content-type", "").partition(";")[0].lower() != (
        "application/x-www-form-urlencoded"
    ):
        return _text("알 수 없는 처리입니다.", 400)
    form = _form(await request.body())
    if form is None:
        return _text("알 수 없는 처리입니다.", 400)
    action = form["action"]
    if action not in {"confirm", "decline"}:
        return _text("알 수 없는 처리입니다.", 400)
    token = form["token"]
    csrf = form["csrf"]
    transition = approve_workflow if action == "confirm" else decline_workflow
    if not transition(collection, request.path_params["workflow_id"], token, csrf, now()):
        return _text("승인 요청이 없거나 이미 처리됐습니다.", 409)
    if action == "confirm":
        return _text("승인했습니다. 채팅으로 돌아가 승인했다고 알려주세요.", 200)
    return _text("거절했습니다. 채팅으로 돌아가 거절했다고 알려주세요.", 200)


def register_approval_routes(mcp: FastMCP, collection_factory: Callable) -> None:
    @mcp.custom_route(
        "/approval/{workflow_id}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD", "TRACE", "CONNECT"],
    )
    async def approval(request: Request):
        if request.method == "GET":
            return await approval_get(request, collection_factory())
        if request.method == "POST":
            return await approval_post(request, collection_factory())
        return _text("허용되지 않은 HTTP 메서드입니다.", 405)
