import asyncio
import re
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import httpx
from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette

from energy_mcp.approval import approval_get, approval_post, register_approval_routes
from energy_mcp.workflow import _hash

NOW = datetime(2026, 9, 2, 10, 0, tzinfo=timezone.utc)


def request(app, method, url, **kwargs):
    async def send():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            return await client.request(method, url, **kwargs)

    return asyncio.run(send())


def csrf_from(response):
    return re.search(r'name="csrf" value="([^"]+)"', response.text).group(1)


def assert_security_headers(response):
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["content-security-policy"].startswith("default-src 'none'")
    assert response.headers["referrer-policy"] == "no-referrer"
    assert response.headers["x-content-type-options"] == "nosniff"


def app_for(collection):
    app = Starlette()

    async def get_approval(request):
        return await approval_get(request, collection, now=lambda: NOW)

    async def post_approval(request):
        return await approval_post(request, collection, now=lambda: NOW)

    app.add_route("/approval/{workflow_id}", get_approval, methods=["GET"])
    app.add_route("/approval/{workflow_id}", post_approval, methods=["POST"])
    return app


class Collection:
    def __init__(self, doc):
        self.doc = doc
        self.transitions = 0

    def _matches(self, query):
        for key, expected in query.items():
            value = self.doc.get(key)
            if isinstance(expected, dict):
                if not all(value > limit for op, limit in expected.items() if op == "$gt"):
                    return False
            elif value != expected:
                return False
        return True

    def find_one(self, query):
        return self.doc if self._matches(query) else None

    def update_one(self, query, update):
        if not self._matches(query):
            return type("Result", (), {"matched_count": 0})()
        self.doc.update(update.get("$set", {}))
        for key in update.get("$unset", {}):
            self.doc.pop(key, None)
        if "status" in update.get("$set", {}):
            self.transitions += 1
        return type("Result", (), {"matched_count": 1})()


def test_get_rejects_bad_token_without_showing_sql():
    collection = MagicMock()
    collection.find_one.return_value = None

    response = request(app_for(collection), "GET", "/approval/wf?token=bad")

    assert response.status_code == 404
    assert "SELECT" not in response.text
    assert_security_headers(response)


def test_get_escapes_values_and_replaces_csrf_nonce():
    collection = MagicMock()
    collection.find_one.return_value = {
        "_id": "wf",
        "status": "awaiting_confirmation",
        "summary": "<script>alert(1)</script>",
        "sql": "SELECT '<tag>'",
        "expires_at": NOW + timedelta(minutes=10),
        "approval_token_hash": _hash("secret"),
    }
    collection.update_one.return_value.matched_count = 1

    response = request(app_for(collection), "GET", "/approval/wf?token=secret")

    assert response.status_code == 200
    assert "<script>" not in response.text
    assert "&lt;script&gt;" in response.text
    assert "SELECT &#x27;&lt;tag&gt;&#x27;" in response.text
    query, update = collection.update_one.call_args.args
    assert query["status"] == "awaiting_confirmation"
    assert update["$set"]["approval_csrf_hash"]
    assert (NOW + timedelta(minutes=10)).isoformat() in response.text
    assert_security_headers(response)


def test_post_confirm_uses_atomic_workflow_transition():
    collection = MagicMock()
    collection.update_one.return_value.matched_count = 1

    response = request(app_for(collection), "POST", "/approval/wf",
        data={"action": "confirm", "token": "secret", "csrf": "nonce"},
    )

    assert response.status_code == 200
    query, update = collection.update_one.call_args.args
    assert query["status"] == "awaiting_confirmation"
    assert query["approval_token_hash"] == _hash("secret")
    assert query["approval_csrf_hash"] == _hash("nonce")
    assert update["$set"]["status"] == "confirmed"
    assert update["$unset"] == {"approval_csrf_hash": ""}
    assert_security_headers(response)


def test_post_rejects_unknown_action_without_transition():
    collection = MagicMock()

    response = request(app_for(collection), "POST", "/approval/wf",
        data={"action": "execute", "token": "secret", "csrf": "nonce"}
    )

    assert response.status_code == 400
    collection.update_one.assert_not_called()
    assert_security_headers(response)


def test_get_refreshes_csrf_and_rejects_the_replaced_nonce():
    collection = Collection({
        "_id": "wf",
        "status": "awaiting_confirmation",
        "summary": "summary",
        "sql": "SELECT 1",
        "expires_at": NOW + timedelta(minutes=10),
        "approval_token_hash": _hash("secret"),
    })
    app = app_for(collection)

    first = request(app, "GET", "/approval/wf?token=secret")
    second = request(app, "GET", "/approval/wf?token=secret")
    expired_csrf = csrf_from(first)
    current_csrf = csrf_from(second)
    reused = request(app, "POST", "/approval/wf", data={
        "action": "confirm", "token": "secret", "csrf": expired_csrf,
    })
    confirmed = request(app, "POST", "/approval/wf", data={
        "action": "confirm", "token": "secret", "csrf": current_csrf,
    })

    assert expired_csrf != current_csrf
    assert reused.status_code == 409
    assert confirmed.status_code == 200
    assert collection.doc["status"] == "confirmed"
    assert "approval_csrf_hash" not in collection.doc
    assert collection.transitions == 1
    assert_security_headers(reused)
    assert_security_headers(confirmed)


def test_post_declines_once_then_conflicts():
    collection = Collection({
        "_id": "wf",
        "status": "awaiting_confirmation",
        "expires_at": NOW + timedelta(minutes=10),
        "approval_token_hash": _hash("secret"),
        "approval_csrf_hash": _hash("nonce"),
    })
    app = app_for(collection)
    data = {"action": "decline", "token": "secret", "csrf": "nonce"}

    declined = request(app, "POST", "/approval/wf", data=data)
    repeated = request(app, "POST", "/approval/wf", data=data)

    assert declined.status_code == 200
    assert repeated.status_code == 409
    assert collection.doc["status"] == "declined"
    assert collection.transitions == 1
    assert_security_headers(declined)
    assert_security_headers(repeated)


def test_post_rejects_malformed_forms_without_a_transition():
    for body in (
        b"action=confirm&token=secret&csrf=nonce&extra=value",
        b"action=confirm&action=decline&token=secret&csrf=nonce",
        b"action=confirm&token=secret",
        b"action=confirm&token=secret&csrf=nonce&",
        b"action=confirm&token=secret&csrf=%ZZ",
        b"action=confirm&token=secret&csrf=\xff",
    ):
        collection = MagicMock()
        response = request(app_for(collection), "POST", "/approval/wf", content=body,
                           headers={"content-type": "application/x-www-form-urlencoded"})

        assert response.status_code == 400
        collection.update_one.assert_not_called()
        assert_security_headers(response)


def test_post_rejects_non_form_content_without_a_transition():
    collection = MagicMock()

    response = request(app_for(collection), "POST", "/approval/wf", content=b"{}",
                       headers={"content-type": "application/json"})

    assert response.status_code == 400
    collection.update_one.assert_not_called()
    assert_security_headers(response)


def test_expired_get_request_hides_sql_and_keeps_security_headers():
    collection = Collection({
        "_id": "wf",
        "status": "awaiting_confirmation",
        "summary": "summary",
        "sql": "SELECT secret FROM research.data",
        "expires_at": NOW - timedelta(seconds=1),
        "approval_token_hash": _hash("secret"),
    })

    response = request(app_for(collection), "GET", "/approval/wf?token=secret")

    assert response.status_code == 404
    assert "SELECT" not in response.text
    assert_security_headers(response)


def test_registered_route_secures_unsupported_methods():
    mcp = FastMCP("test")
    register_approval_routes(mcp, MagicMock())

    response = request(mcp.streamable_http_app(), "PUT", "/approval/wf")

    assert response.status_code == 405
    assert_security_headers(response)
