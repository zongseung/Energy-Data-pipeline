from collections.abc import Callable
from html import escape
from urllib.parse import parse_qs

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
    try:
        csrf = issue_csrf(collection, workflow_id, current)
    except RuntimeError:
        return _text("승인 요청이 없거나 만료됐습니다.", 404)
    body = APPROVAL_HTML.format(
        summary=escape(doc["summary"]),
        sql=escape(doc["sql"]),
        form_action=escape(request.url.path, quote=True),
        token=escape(token, quote=True),
        csrf=escape(csrf, quote=True),
    )
    return HTMLResponse(body, headers=SECURITY_HEADERS)


async def approval_post(request: Request, collection, now=utcnow):
    try:
        form = parse_qs((await request.body()).decode(), max_num_fields=4)
    except (UnicodeDecodeError, ValueError):
        return _text("알 수 없는 처리입니다.", 400)
    action = form.get("action", [""])[0]
    if action not in {"confirm", "decline"}:
        return _text("알 수 없는 처리입니다.", 400)
    token = form.get("token", [""])[0]
    csrf = form.get("csrf", [""])[0]
    transition = approve_workflow if action == "confirm" else decline_workflow
    if not transition(collection, request.path_params["workflow_id"], token, csrf, now()):
        return _text("승인 요청이 없거나 이미 처리됐습니다.", 409)
    return _text("조회 조건을 처리했습니다. 채팅으로 돌아가세요.", 200)


def register_approval_routes(mcp: FastMCP, collection_factory: Callable) -> None:
    @mcp.custom_route("/approval/{workflow_id}", methods=["GET"])
    async def get_approval(request: Request):
        return await approval_get(request, collection_factory())

    @mcp.custom_route("/approval/{workflow_id}", methods=["POST"])
    async def post_approval(request: Request):
        return await approval_post(request, collection_factory())
