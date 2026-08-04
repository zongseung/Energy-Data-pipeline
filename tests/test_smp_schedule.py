import ast
from pathlib import Path


def test_daily_smp_deployment_runs_at_0900_kst():
    source = Path("prefect_flows/deploy.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    deploy = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "deploy_smp_flow"
    )
    crons = [
        keyword.value.value
        for node in ast.walk(deploy)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "CronSchedule"
        for keyword in node.keywords
        if keyword.arg == "cron" and isinstance(keyword.value, ast.Constant)
    ]
    timezones = [
        keyword.value.value
        for node in ast.walk(deploy)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "CronSchedule"
        for keyword in node.keywords
        if keyword.arg == "timezone" and isinstance(keyword.value, ast.Constant)
    ]

    assert crons == ["0 9 * * *"]
    assert timezones == ["Asia/Seoul"]
