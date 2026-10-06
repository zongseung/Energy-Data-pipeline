import csv
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from energy_mcp.planner import ForecastRequest
from energy_mcp import weather


def request(**changes):
    return ForecastRequest(**{
        'forecast_type': '단기예보', 'sido': '경기도', 'sigungu': '성남시수정구',
        'dong': '복정동', 'element': '1시간기온', 'from_ym': '202301', 'to_ym': '202301',
        **changes,
    })


def test_month_bounds_and_collection_limit():
    assert weather.month_interval('202402', 'range') == (datetime(2024, 2, 1), datetime(2024, 3, 1))
    assert weather.month_interval('202402', 'monthly') == (datetime(2024, 2, 1), datetime(2024, 2, 29))
    assert weather.requested_months(request(from_ym='202312', to_ym='202402')) == ['202312', '202401', '202402']
    with pytest.raises(ValueError, match='12'):
        weather.requested_months(request(from_ym='202301', to_ym='202402'))


def test_scope_uses_exact_catalog_region_and_element(tmp_path):
    catalog = tmp_path / 'regions.csv'
    catalog.write_text('Level1,Level2,Level3,ReqList_Last\n경기도,성남시수정구,복정동,62_124\n')
    mapping = {'단기예보': {'1시간기온': 'TMP'}}
    assert weather.validate_scope(request(), catalog, mapping)['ReqList_Last'] == '62_124'
    assert weather.validate_scope(request(sigungu='성남시 수정구'), catalog, mapping)['Level2'] == '성남시수정구'
    for changed in [{'sigungu': '성남시분당구'}, {'dong': '../../etc'}, {'element': '../기온'}]:
        with pytest.raises(ValueError):
            weather.validate_scope(request(**changed), catalog, mapping)


def test_csv_validation_requires_matching_grid_month_and_numeric_rows(tmp_path):
    path = tmp_path / 'file.csv'
    header = 'format: day,hour,forecast,value location:62_124 Start : 20230101\n'
    path.write_text(header + '1,0200,+6,-7.000000\n2,0300,+12,1.000000\n')
    assert weather.valid_csv_rows(path, '202301', '62_124', '단기예보') == 2
    for body in [header, header + '1,0200,+6,nan\n', header.replace('62_124', '60_127') + '1,0200,+6,1\n', header.replace('20230101', '20230201') + '1,0200,+6,1\n']:
        path.write_text(body)
        assert weather.valid_csv_rows(path, '202301', '62_124', '단기예보') == 0


def test_native_csv_daily_start_markers_are_not_data_rows(tmp_path):
    path = tmp_path / 'native.csv'
    path.write_text('format: day,hour,forecast,value location:62_124 Start : 20230101\n1,0200,+6,1.000000\nStart : 20230102\n2,0200,+6,2.000000\nStart : 20230201\n\n \n')
    assert weather.valid_csv_rows(path, '202301', '62_124', '단기예보') == 2


def test_native_invalid_days_are_filtered_like_postgres(tmp_path):
    path = tmp_path / 'native.csv'
    path.write_text('format: day,hour,forecast,value location:62_124 Start : 20230101\n1,0200,+6,1.000000\n-971917344,0200,+6,0.000000\n32,0200,+6,0.000000\n2,0200,+6,2.000000\n')
    assert weather.valid_csv_rows(path, '202301', '62_124', '단기예보') == 2


def test_check_does_not_start_collection_without_explicit_confirmation(monkeypatch):
    from energy_mcp import server
    monkeypatch.setattr(weather, 'validate_runtime_scope', lambda *_: {'ReqList_Last': '62_124', 'Level2': '성남시수정구'})
    monkeypatch.setattr(server, '_collection_available_months', lambda *_: ['202301'])
    start = MagicMock()
    monkeypatch.setattr(weather, 'start_collection', start)
    result = server.collect_forecast(request(from_ym='202301', to_ym='202302'))
    assert result['status'] == 'needs_collection_confirmation'
    assert result['missing_months'] == ['202302']
    start.assert_not_called()
    assert server.collect_forecast(request())['status'] == 'available'
    start.assert_not_called()


def test_missing_only_is_sent_to_worker(monkeypatch):
    from energy_mcp import server
    monkeypatch.setattr(weather, 'validate_runtime_scope', lambda *_: {'ReqList_Last': '62_124', 'Level2': '성남시수정구'})
    monkeypatch.setattr(server, '_collection_available_months', lambda *_: ['202301'])
    start = MagicMock(return_value={'status': 'running', 'job_id': 'f' * 32})
    monkeypatch.setattr(weather, 'start_collection', start)
    result = server.collect_forecast(request(sigungu='성남시 수정구', from_ym='202301', to_ym='202302'), confirmed=True)
    assert result['job_id'] == 'f' * 32
    assert start.call_args.args[0].sigungu == '성남시수정구'
    assert start.call_args.args[1] == ['202302']


def test_job_status_rejects_invalid_identifiers():
    with pytest.raises(ValueError):
        weather.collection_status('../arbitrary-file')


def test_mount_verification_rejects_plain_bind_mount(tmp_path):
    info = tmp_path / 'mountinfo'
    root = str(tmp_path / 'nas')
    info.write_text(f'24 1 8:1 / {root} rw - ext4 /dev/sda rw\n')
    assert not weather.is_cifs_mount(root, info)
    info.write_text(f'24 1 0:1 / {root} rw - cifs //nas/share rw\n')
    assert weather.is_cifs_mount(root, info)


def test_atomic_publish_preserves_existing_file_on_failure(tmp_path, monkeypatch):
    source = tmp_path / 'source.csv'
    target = tmp_path / 'target.csv'
    header = 'format: day,hour,forecast,value location:62_124 Start : 20230101\n'
    source.write_text(header + '1,0200,+6,1.000000\n')
    target.write_text('existing valid data')
    def interrupted_copy(src, dst):
        Path(dst).write_text('partial')
        raise OSError('copy interrupted')
    monkeypatch.setattr(weather.shutil, 'copyfile', interrupted_copy)
    with pytest.raises(OSError):
        weather.publish_csv(source, target, '202301', '62_124', '단기예보')
    assert target.read_text() == 'existing valid data'
    assert not list(tmp_path.glob('*.tmp'))


def test_atomic_publish_exposes_only_valid_csv(tmp_path):
    source = tmp_path / 'source.csv'
    target = tmp_path / 'target.csv'
    source.write_text('format: day,hour,forecast,value location:62_124 Start : 20230101\n')
    with pytest.raises(ValueError):
        weather.publish_csv(source, target, '202301', '62_124', '단기예보')
    assert not target.exists()
    source.write_text(source.read_text() + '1,0200,+6,1.000000\n')
    assert weather.publish_csv(source, target, '202301', '62_124', '단기예보') == 1
    assert weather.valid_csv_rows(target, '202301', '62_124', '단기예보') == 1
