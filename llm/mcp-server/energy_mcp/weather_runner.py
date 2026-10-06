"""Isolated process reusing weather-data's downloader with a bounded request."""
from __future__ import annotations

import asyncio
import csv
import json
import os
import re
import sys
import tempfile
from pathlib import Path

from energy_mcp.planner import ForecastRequest
from energy_mcp.weather import NAS_ROOT, WEATHER_ROOT, is_cifs_mount, month_interval, publish_csv, scope_files, valid_csv_rows, validate_runtime_scope


async def collect(forecast, months):
    if not is_cifs_mount():
        raise RuntimeError('NAS is not mounted')
    region = validate_runtime_scope(forecast)
    sys.path.insert(0, str(WEATHER_ROOT / 'src'))
    from weather_downloader import downloader as native
    from weather_downloader.config import DownloadConfig, WeatherConfig

    class Downloader(native.WeatherDownloader):
        def _send_slack_alert(self, *_):
            pass

        def _post(self, url, headers, data, stream=False):
            response = self.session.post(url, headers=headers, data=data, stream=stream, timeout=(10, 120))
            return response.status_code, response.content

        def get_cookie(self, login_id, password):
            response = self.session.post('https://data.kma.go.kr/login/loginAjax.do',
                                         data={'loginId': login_id, 'passwordNo': password}, timeout=(10, 60))
            response.raise_for_status()
            return '; '.join(f'{k}={v}' for k, v in self.session.cookies.get_dict().items())

        async def _try_once(self, cfg, region, variable, start, end, base_dir, retry_count, file_callback):
            # Reuse native download/unpack; expose the final NAS filename only after validation.
            with tempfile.TemporaryDirectory(prefix='weather-download-') as stage:
                kind = await super()._try_once(cfg, region, variable, start, end, stage, retry_count, lambda *_: None)
                if kind != 'done':
                    return kind
                source = Path(native._csv_paths(stage, region, variable['name'], start, end)[3])
                if not valid_csv_rows(source, start[:6], region['code'], forecast.forecast_type):
                    return 'empty'
                if not is_cifs_mount():
                    raise RuntimeError('NAS is not mounted')
                target = Path(native._csv_paths(base_dir, region, variable['name'], start, end)[3])
                publish_csv(source, target, start[:6], region['code'], forecast.forecast_type)
                file_callback(str(target))
                return 'done'

    original_load = native._load_ledger

    def actual_ledger(path):
        # A stale success entry must not suppress recovery of a missing file.
        return {key: record for key, record in original_load(path).items()
                if record.get('status') != 'done' or
                (Path(path).parent / record.get('csv', '')).is_file()}

    native._load_ledger = actual_ledger  # private to this short-lived subprocess
    succeeded, failed, errors, counts = [], [], [], {}
    with tempfile.TemporaryDirectory(prefix='weather-job-') as temp:
        region_file = Path(temp) / 'region.csv'
        with region_file.open('w', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=['Level1', 'Level2', 'Level3', 'ReqList_Last'])
            writer.writeheader()
            writer.writerow(region)
        for month in months:
            for path in scope_files(forecast):
                matched = re.search(r'_(\d{6})', path.name)
                if matched and matched[1] == month and not valid_csv_rows(path, month, region['ReqList_Last'], forecast.forecast_type):
                    # Preserve existing bad content for diagnosis without treating it as queryable CSV.
                    path.rename(path.with_name(path.name + '.invalid-' + os.urandom(4).hex()))
            mode = WeatherConfig.CONFIGS[forecast.forecast_type]['mode']
            start, end = month_interval(month, mode)
            config = DownloadConfig(os.environ['KMA_ID'], os.environ['KMA_PW'],
                                    forecast.forecast_type, start, end, [forecast.element], concurrency=1)
            collector = Downloader(out_dir=str(NAS_ROOT))
            await collector.download(config, lambda *_: None, lambda *_: None, csv_file=str(region_file))
            expected_start, expected_end = collector.generate_intervals(start, end, mode)[0]
            folder = NAS_ROOT / forecast.forecast_type / forecast.sido / forecast.sigungu / forecast.dong / forecast.element
            filename = f'{forecast.dong}_{forecast.element}_{expected_start}_{expected_end}.csv'
            count = valid_csv_rows(folder / filename, month, region['ReqList_Last'], forecast.forecast_type)
            (succeeded if count else failed).append(month)
            if count:
                counts[month] = count
            else:
                ledger = native._load_ledger(str(NAS_ROOT / forecast.forecast_type / '.download_ledger.jsonl'))
                key = native._task_key(forecast.forecast_type, {'level1': forecast.sido, 'level2': forecast.sigungu, 'level3': forecast.dong}, forecast.element, expected_start, expected_end)
                if ledger.get(key, {}).get('reason') == 'error':
                    errors.append(month)
    return {'status': 'partial' if succeeded and failed else 'completed' if succeeded else 'failed' if errors else 'unavailable',
            'collected_months': succeeded, 'missing_months': failed, 'error_months': errors, 'row_counts': counts}


if __name__ == '__main__':
    payload = json.load(sys.stdin)
    forecast = ForecastRequest.model_validate(payload['forecast'])
    # Only months within the caller's validated range may be executed.
    from energy_mcp.weather import requested_months
    months = payload['months']
    if not months or not set(months).issubset(requested_months(forecast)):
        raise ValueError('잘못된 수집 월')
    print(json.dumps(asyncio.run(collect(forecast, months))))
