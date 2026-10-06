"""Bounded jobs for the existing weather-data collector; no credentials in jobs."""
from __future__ import annotations

import calendar
import csv
import fcntl
import importlib.util
import json
import math
import os
import re
import shutil
import subprocess
import sys
import threading
import uuid
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path

from pymongo import MongoClient

from energy_mcp.planner import ForecastRequest
from energy_mcp.workflow import utcnow

WEATHER_ROOT = Path('/mnt/nvme/weather-data')
NAS_ROOT = WEATHER_ROOT / 'nas-weather'
REGION_CSV = WEATHER_ROOT / '지역코드 copy.csv'
JOB_TIMEOUT_SECONDS = 1200
_running: set[str] = set()
_owner = uuid.uuid4().hex


def is_cifs_mount(root=NAS_ROOT, mountinfo=Path('/proc/self/mountinfo')):
    target = str(Path(root).resolve())
    try:
        for line in Path(mountinfo).read_text().splitlines():
            before, after = line.split(' - ', 1)
            mounted = re.sub(r'\\([0-7]{3})', lambda m: chr(int(m[1], 8)), before.split()[4])
            if mounted == target:
                return after.split()[0] == 'cifs'
    except (OSError, ValueError, IndexError):
        pass
    return False


def requested_months(forecast: ForecastRequest) -> list[str]:
    current = datetime.strptime(forecast.from_ym, '%Y%m')
    end = datetime.strptime(forecast.to_ym, '%Y%m')
    months = []
    while current <= end:
        months.append(current.strftime('%Y%m'))
        if len(months) > 12:
            raise ValueError('수집 요청은 한 읍면동·한 요소·최대 12개월입니다.')
        current = datetime(current.year + (current.month == 12), current.month % 12 + 1, 1)
    return months


def month_interval(month: str, mode: str) -> tuple[datetime, datetime]:
    start = datetime.strptime(month, '%Y%m')
    if mode == 'monthly':
        return start, start.replace(day=calendar.monthrange(start.year, start.month)[1])
    return start, datetime(start.year + (start.month == 12), start.month % 12 + 1, 1)


@lru_cache(maxsize=1)
def weather_config():
    path = WEATHER_ROOT / 'src/weather_downloader/config.py'
    spec = importlib.util.spec_from_file_location('_weather_config', path)
    if spec is None or spec.loader is None:
        raise RuntimeError('기상 수집 코드가 연결되지 않았습니다.')
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.WeatherConfig


def validate_scope(forecast, catalog, mapping):
    requested_months(forecast)
    if forecast.element not in mapping.get(forecast.forecast_type, {}):
        raise ValueError('해당 예보종에서 지원하지 않는 요소입니다.')
    with Path(catalog).open(encoding='utf-8-sig', newline='') as handle:
        rows = [r for r in csv.DictReader(handle) if
                (r['Level1'], r['Level2'], r['Level3']) ==
                (forecast.sido, forecast.sigungu, forecast.dong)]
    if len(rows) != 1 or not re.fullmatch(r'\d+_\d+', rows[0]['ReqList_Last']):
        raise ValueError('지역코드에 정확히 일치하는 시도·시군구·읍면동이 없습니다.')
    for value in (forecast.sido, forecast.sigungu, forecast.dong, forecast.element):
        if '/' in value or '\\' in value or value in {'.', '..'}:
            raise ValueError('잘못된 지역 또는 요소입니다.')
    return rows[0]


def validate_runtime_scope(forecast):
    return validate_scope(forecast, REGION_CSV, weather_config().VARIABLE_MAPPINGS)


def valid_csv_rows(path: Path, month: str, grid: str, forecast_type: str) -> int:
    """Require a header and rows the existing PostgreSQL parser can actually read."""
    try:
        with path.open(encoding='euc-kr', newline='') as handle:
            header = handle.readline()
            if not re.search(r'location:\s*' + re.escape(grid) + r'(?:\s|$)', header):
                return 0
            if not re.search(r'Start\s*:\s*' + month + r'\d{2}', header):
                return 0
            if 'format:' not in header:
                return 0
            rows = 0
            expected = 3 if forecast_type == '초단기실황' else 4
            last_day = calendar.monthrange(int(month[:4]), int(month[4:]))[1]
            for cells in csv.reader(handle):
                cells = [c.strip() for c in cells]
                if not any(cells):
                    continue
                if len(cells) == 1 and re.fullmatch(r'Start\s*:\s*\d{8}', cells[0]):
                    continue
                if len(cells) != expected:
                    return 0
                if re.fullmatch(r'-\d+', cells[0]) or (cells[0].isdigit() and not 1 <= int(cells[0]) <= last_day):
                    continue  # Match PostgreSQL's exclusion of invalid source day labels.
                if not cells[0].isdigit():
                    return 0
                hour = cells[1].zfill(4)
                if not re.fullmatch(r'\d{4}', hour) or int(hour[:2]) > 23:
                    return 0
                if expected == 4 and not re.fullmatch(r'[+-]?\d+', cells[2]):
                    return 0
                if not re.fullmatch(r'[+-]?\d+(\.\d+)?', cells[-1]) or not math.isfinite(float(cells[-1])):
                    return 0
                rows += 1
            return rows
    except (OSError, UnicodeError, ValueError):
        return 0


def publish_csv(source, target, month, grid, forecast_type):
    count = valid_csv_rows(source, month, grid, forecast_type)
    if not count:
        raise ValueError('수집 CSV에 유효한 자료가 없습니다.')
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f'.{target.name}.{uuid.uuid4().hex}.tmp')
    try:
        shutil.copyfile(source, temporary)
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return count


def scope_files(forecast):
    folder = NAS_ROOT / forecast.forecast_type / forecast.sido / forecast.sigungu / forecast.dong / forecast.element
    return sorted(folder.glob('*.csv'))


def verified_months(forecast, available):
    grid = validate_runtime_scope(forecast)['ReqList_Last']
    requested = set(requested_months(forecast))
    files = scope_files(forecast)
    valid = set(available)
    for month in requested & valid:
        monthly = [f for f in files if re.search(r'_(\d{6})', f.name) and re.search(r'_(\d{6})', f.name)[1] == month]
        if not monthly or not all(valid_csv_rows(f, month, grid, forecast.forecast_type) for f in monthly):
            valid.discard(month)
    return valid


@lru_cache(maxsize=1)
def jobs():
    uri = os.environ.get('ENERGY_MCP_MONGO_URI')
    if not uri:
        raise RuntimeError('수집 작업 저장소가 설정되지 않았습니다.')
    collection = MongoClient(uri, tz_aware=True).get_default_database()['weather_collection_jobs']
    collection.create_index('expires_at', expireAfterSeconds=0)
    return collection


def start_collection(forecast, months):
    if not os.environ.get('KMA_ID') or not os.environ.get('KMA_PW'):
        return {'status': 'credentials_required', 'message': '관리자가 서버에 기상청 계정을 등록해야 합니다.'}
    if not is_cifs_mount():
        return {'status': 'nas_unavailable', 'message': 'NAS 마운트가 연결되지 않았습니다.'}
    # ponytail: one shared writer for both deployments; split locks if throughput matters.
    lock = Path('/weather-state/collection.lock').open('a')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock.close()
        return {'status': 'busy', 'message': '다른 기상 수집 작업이 실행 중입니다. 잠시 후 확인해 주세요.'}
    job_id = uuid.uuid4().hex
    request = forecast.model_dump()
    try:
        jobs().insert_one({'_id': job_id, 'status': 'running', 'owner': _owner, 'forecast': request,
                           'requested_months': months, 'created_at': utcnow(),
                           'expires_at': utcnow() + timedelta(days=7)})
        _running.add(job_id)
        threading.Thread(target=_run_job, args=(job_id, request, months, lock), daemon=True).start()
    except Exception:
        _running.discard(job_id)
        lock.close()
        raise
    return {'status': 'running', 'job_id': job_id, 'forecast': request, 'requested_months': months}


def _run_job(job_id, request, months, lock):
    try:
        env = {**os.environ, 'SLACK_WEBHOOK_URL': ''}
        completed = subprocess.run(
            [sys.executable, '-m', 'energy_mcp.weather_runner'],
            input=json.dumps({'forecast': request, 'months': months}), text=True,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            timeout=JOB_TIMEOUT_SECONDS, check=True, env=env,
        )
        result = json.loads(completed.stdout)
        if result.get('status') not in {'completed', 'partial', 'unavailable', 'failed'}:
            raise ValueError('잘못된 수집 결과')
        jobs().update_one({'_id': job_id}, {'$set': {**result, 'finished_at': utcnow()}})
    except subprocess.TimeoutExpired:
        jobs().update_one({'_id': job_id}, {'$set': {'status': 'failed', 'error_code': 'collection_timeout', 'finished_at': utcnow()}})
    except Exception:
        # Never forward collector stderr, which may include upstream response bodies.
        jobs().update_one({'_id': job_id}, {'$set': {'status': 'failed', 'error_code': 'collector_error', 'finished_at': utcnow()}})
    finally:
        _running.discard(job_id)
        lock.close()


def collection_status(job_id):
    if not re.fullmatch(r'[a-f0-9]{32}', job_id):
        raise ValueError('잘못된 작업 ID입니다.')
    document = jobs().find_one({'_id': job_id})
    if not document:
        raise ValueError('작업이 없거나 만료됐습니다.')
    if document['status'] == 'running' and document.get('owner') == _owner and job_id not in _running:
        document.update(status='failed', error_code='collection_interrupted')
        jobs().update_one({'_id': job_id}, {'$set': {'status': 'failed', 'error_code': 'collection_interrupted'}})
    if document['status'] == 'running' and (utcnow() - document['created_at']).total_seconds() > JOB_TIMEOUT_SECONDS + 60:
        document.update(status='failed', error_code='collection_interrupted')
        jobs().update_one({'_id': job_id}, {'$set': {'status': 'failed', 'error_code': 'collection_interrupted'}})
    return {'job_id': job_id, **{key: value for key, value in document.items() if key not in {'_id', 'expires_at', 'owner'}}}
