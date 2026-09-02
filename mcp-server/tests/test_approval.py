from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from starlette.applications import Starlette
from starlette.testclient import TestClient

from energy_mcp.approval import approval_get, approval_post, register_approval_routes
from energy_mcp.workflow import _hash

NOW = datetime(2026, 9, 2, 10, 0, tzinfo=timezone.utc)


def app_for(collection):
    app = Starlette()

    async def get_approval(request):
        return await approval_get(request, collection, now=lambda: NOW)

    async def post_approval(request):
        return await approval_post(request, collection, now=lambda: NOW)

    app.add_route("/approval/{workflow_id}", get_approval, methods=["GET"])
    app.add_route("/approval/{workflow_id}", post_approval, methods=["POST"])
    return app


def test_get_rejects_bad_token_without_showing_sql():
    collection = MagicMock()
    collection.find_one.return_value = None

    response = TestClient(app_for(collection)).get("/approval/wf?token=bad")

    assert response.status_code == 404
    assert "SELECT" not in response.text
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["referrer-policy"] == "no-referrer"
    assert response.headers["x-content-type-options"] == "nosniff"


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

    response = TestClient(app_for(collection)).get("/approval/wf?token=secret")

    assert response.status_code == 200
    assert "<script>" not in response.text
    assert "&lt;script&gt;" in response.text
    assert "SELECT &#x27;&lt;tag&gt;&#x27;" in response.text
    query, update = collection.update_one.call_args.args
    assert query["status"] == "awaiting_confirmation"
    assert update["$set"]["approval_csrf_hash"]
    assert response.headers["content-security-policy"].startswith("default-src 'none'")


def test_post_confirm_uses_atomic_workflow_transition():
    collection = MagicMock()
    collection.update_one.return_value.matched_count = 1

    response = TestClient(app_for(collection)).post(
        "/approval/wf",
        data={"action": "confirm", "token": "secret", "csrf": "nonce"},
    )

    assert response.status_code == 200
    query, update = collection.update_one.call_args.args
    assert query["status"] == "awaiting_confirmation"
    assert query["approval_token_hash"] == _hash("secret")
    assert query["approval_csrf_hash"] == _hash("nonce")
    assert update["$set"]["status"] == "confirmed"
    assert update["$unset"] == {"approval_csrf_hash": ""}


def test_post_rejects_unknown_action_without_transition():
    collection = MagicMock()

    response = TestClient(app_for(collection)).post(
        "/approval/wf", data={"action": "execute", "token": "secret", "csrf": "nonce"}
    )

    assert response.status_code == 400
    collection.update_one.assert_not_called()


def test_registers_get_and_post_approval_routes():
    class FakeMCP:
        def __init__(self):
            self.routes = []

        def custom_route(self, path, methods):
            def register(handler):
                self.routes.append((path, methods, handler))
                return handler

            return register

    mcp = FakeMCP()

    register_approval_routes(mcp, MagicMock())

    assert [(path, methods) for path, methods, _ in mcp.routes] == [
        ("/approval/{workflow_id}", ["GET"]),
        ("/approval/{workflow_id}", ["POST"]),
    ]
