"""
리팩토링 검증 테스트

DB / API / 환경변수 없이 실행 가능한 순수 함수 위주 테스트.
리팩토링 전후로 동작이 동일한지 확인하는 용도.

NOTE: prefect_flows, daily_pv_automation 등 모듈 레벨 부수효과가 있는
모듈은 직접 import하지 않고, 순수 함수만 별도로 로드하여 테스트한다.
"""

import importlib.util
import re
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd
import pytest

# 프로젝트 루트를 sys.path에 추가
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ============================================================
# Helper: 부수효과 없이 특정 함수만 소스에서 추출
# ============================================================
def _load_function_from_source(filepath: str, func_name: str):
    """모듈 전체를 import하지 않고, 소스 코드에서 순수 함수를 추출·실행한다."""
    source = (PROJECT_ROOT / filepath).read_text(encoding="utf-8")
    # 함수 정의 블록 추출
    pattern = rf"(^def {func_name}\(.*?\n(?:(?:    .+|)\n)*)"
    match = re.search(pattern, source, re.MULTILINE)
    if not match:
        raise RuntimeError(f"{func_name}을 {filepath}에서 찾을 수 없습니다")
    func_source = match.group(1)
    ns = {}
    exec(func_source, ns)
    return ns[func_name]


# normalize_date_format: prefect import 없이 소스에서 직접 로드
normalize_date_format = _load_function_from_source(
    "prefect_flows/prefect_pipeline.py", "normalize_date_format"
)


# ============================================================
# 1. normalize_date_format
# ============================================================
class TestNormalizeDateFormat:
    def test_yyyymmdd_passthrough(self):
        assert normalize_date_format("20260101") == "20260101"

    def test_dash_separated(self):
        assert normalize_date_format("2026-01-01") == "20260101"

    def test_slash_separated(self):
        assert normalize_date_format("2026/01/01") == "20260101"

    def test_invalid_length_raises(self):
        with pytest.raises(ValueError, match="날짜 형식"):
            normalize_date_format("202601")

    def test_non_digit_raises(self):
        with pytest.raises(ValueError, match="날짜 형식"):
            normalize_date_format("abcdefgh")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="날짜 형식"):
            normalize_date_format("")


# ============================================================
# 2. find_consecutive_missing_groups
# ============================================================
from fetch_data.common.impute_missing import find_consecutive_missing_groups


class TestFindConsecutiveMissingGroups:
    def test_no_missing(self):
        s = pd.Series([1.0, 2.0, 3.0, 4.0])
        assert find_consecutive_missing_groups(s) == []

    def test_single_missing(self):
        s = pd.Series([1.0, np.nan, 3.0])
        groups = find_consecutive_missing_groups(s)
        assert groups == [(1, 1)]

    def test_consecutive_missing(self):
        s = pd.Series([1.0, np.nan, np.nan, np.nan, 5.0])
        groups = find_consecutive_missing_groups(s)
        assert groups == [(1, 3)]

    def test_multiple_groups(self):
        s = pd.Series([np.nan, 2.0, np.nan, np.nan, 5.0, np.nan])
        groups = find_consecutive_missing_groups(s)
        assert groups == [(0, 1), (2, 2), (5, 1)]

    def test_all_missing(self):
        s = pd.Series([np.nan, np.nan, np.nan])
        groups = find_consecutive_missing_groups(s)
        assert groups == [(0, 3)]

    def test_empty_series(self):
        s = pd.Series([], dtype=float)
        assert find_consecutive_missing_groups(s) == []


# ============================================================
# 3. spline_impute
# ============================================================
from fetch_data.common.impute_missing import spline_impute


class TestSplineImpute:
    def test_single_gap_interpolated(self):
        s = pd.Series([1.0, np.nan, 3.0])
        result = spline_impute(s.copy(), start_idx=1, length=1)
        assert not np.isnan(result.iloc[1])
        assert pytest.approx(result.iloc[1], abs=0.5) == 2.0

    def test_boundary_gap_start(self):
        """첫 번째 값이 결측인 경우 — numpy np.interp 기반으로 해결됨"""
        s = pd.Series([np.nan, 2.0, 3.0, 4.0])
        result = spline_impute(s.copy(), start_idx=0, length=1)
        assert not np.isnan(result.iloc[0])

    def test_preserves_existing_values(self):
        s = pd.Series([1.0, np.nan, np.nan, 4.0, 5.0])
        result = spline_impute(s.copy(), start_idx=1, length=2)
        assert result.iloc[0] == 1.0
        assert result.iloc[3] == 4.0
        assert result.iloc[4] == 5.0


# ============================================================
# 4. impute_missing_values 통합 테스트
# ============================================================
from fetch_data.common.impute_missing import impute_missing_values


class TestImputeMissingValues:
    @pytest.fixture
    def sample_weather_df(self):
        """간단한 기상 데이터 DataFrame"""
        dates = pd.date_range("2025-01-01", periods=24, freq="h")
        data = {
            "tm": dates.strftime("%Y-%m-%d %H:%M"),
            "stnNm": ["서울"] * 24,
            "ta": [float(i) for i in range(24)],
            "hm": [50.0 + i for i in range(24)],
        }
        df = pd.DataFrame(data)
        # 일부 결측 삽입
        df.loc[5, "ta"] = np.nan
        df.loc[10, "hm"] = np.nan
        df.loc[11, "hm"] = np.nan
        return df

    def test_returns_tuple_when_debug(self, sample_weather_df):
        result = impute_missing_values(sample_weather_df, debug=True)
        assert isinstance(result, tuple)
        df_out, info = result
        assert isinstance(df_out, pd.DataFrame)
        assert isinstance(info, dict)

    def test_returns_dataframe_when_no_debug(self, sample_weather_df):
        result = impute_missing_values(sample_weather_df, debug=False)
        assert isinstance(result, pd.DataFrame)

    def test_missing_values_reduced(self, sample_weather_df):
        before_ta = sample_weather_df["ta"].isna().sum()
        before_hm = sample_weather_df["hm"].isna().sum()
        df_out, _ = impute_missing_values(sample_weather_df, debug=True)
        after_ta = df_out["ta"].isna().sum()
        after_hm = df_out["hm"].isna().sum()
        assert after_ta <= before_ta
        assert after_hm <= before_hm

    def test_shape_preserved(self, sample_weather_df):
        n_rows = len(sample_weather_df)
        df_out, _ = impute_missing_values(sample_weather_df, debug=True)
        assert len(df_out) == n_rows

    def test_no_temp_columns_remain(self, sample_weather_df):
        df_out, _ = impute_missing_values(sample_weather_df, debug=True)
        underscored = [c for c in df_out.columns if c.startswith("_")]
        assert underscored == [], f"임시 컬럼이 남아있음: {underscored}"

    def test_missing_date_col_raises(self):
        df = pd.DataFrame({"x": [1, 2], "stnNm": ["A", "A"]})
        with pytest.raises(ValueError, match="날짜 컬럼"):
            impute_missing_values(df, columns=["x"], debug=False)

    def test_missing_station_col_raises(self):
        df = pd.DataFrame({"tm": ["2025-01-01", "2025-01-02"], "ta": [1.0, 2.0]})
        with pytest.raises(ValueError, match="지역 컬럼"):
            impute_missing_values(df, columns=["ta"], station_col="없는컬럼", debug=False)


# ============================================================
# 4-1. impute 최적화 동등성 테스트 (multi-station, historical 경로 포함)
# ============================================================


class TestImputeOptimizationEquivalence:
    """최적화 후 동등성 검증 — historical_average 경로까지 커버"""

    @pytest.fixture
    def multi_station_df(self):
        """다중 station, 다양한 결측 패턴 데이터"""
        rng = np.random.default_rng(123)
        rows = []
        for station in ["서울", "부산"]:
            dates = pd.date_range("2023-01-01", periods=72, freq="h")
            ta = 10.0 + rng.normal(0, 3, 72)
            hm = 55.0 + rng.normal(0, 5, 72)
            # 짧은 결측 (스플라인 경로): 2개 연속
            ta[10:12] = np.nan
            hm[20:22] = np.nan
            # 긴 결측 (historical 경로): 5개 연속
            ta[40:45] = np.nan
            hm[50:55] = np.nan
            df_s = pd.DataFrame({
                "tm": dates.strftime("%Y-%m-%d %H:%M"),
                "stnNm": station,
                "ta": ta,
                "hm": hm,
            })
            rows.append(df_s)
        return pd.concat(rows, ignore_index=True)

    def test_all_missing_reduced(self, multi_station_df):
        before_ta = multi_station_df["ta"].isna().sum()
        before_hm = multi_station_df["hm"].isna().sum()
        df_out, info = impute_missing_values(multi_station_df, debug=True)
        after_ta = df_out["ta"].isna().sum()
        after_hm = df_out["hm"].isna().sum()
        assert after_ta < before_ta, "ta 결측치가 줄어들어야 함"
        assert after_hm < before_hm, "hm 결측치가 줄어들어야 함"

    def test_shape_unchanged(self, multi_station_df):
        n = len(multi_station_df)
        df_out = impute_missing_values(multi_station_df, debug=False)
        assert len(df_out) == n

    def test_historical_path_triggered(self, multi_station_df):
        """연속 5개 결측 그룹이 historical 경로를 타는지 확인"""
        _, info = impute_missing_values(multi_station_df, debug=True)
        # 각 컬럼마다 historical 처리가 1회 이상 발생해야 함
        for col in ["ta", "hm"]:
            assert info["processing_stats"][col]["historical"] >= 1, \
                f"{col}: historical 경로가 호출되지 않음"

    def test_no_nan_in_filled_positions(self, multi_station_df):
        """보간 대상 위치가 실제로 채워졌는지 확인"""
        df_out = impute_missing_values(multi_station_df, debug=False)
        # 서울 station의 ta[10:12] 위치 (스플라인으로 처리됨)
        assert not df_out.loc[10:11, "ta"].isna().any(), "스플라인 결측 미처리"


# ============================================================
# 5. send_slack_message (notify/slack_notifier.py)
# ============================================================
from notify.slack_notifier import send_slack_message


class TestSendSlackMessage:
    @patch("notify.slack_notifier.requests.post")
    def test_sends_post_request(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200)
        send_slack_message("테스트 메시지", webhook_url="https://hooks.slack.com/test")
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert call_args[0][0] == "https://hooks.slack.com/test"

    @patch("notify.slack_notifier.requests.post")
    def test_skips_when_no_url(self, mock_post, capsys):
        with patch.dict("os.environ", {}, clear=True):
            send_slack_message("테스트", webhook_url=None)
        mock_post.assert_not_called()
        captured = capsys.readouterr()
        assert "스킵" in captured.out or "SLACK_WEBHOOK_URL" in captured.out

    @patch("notify.slack_notifier.requests.post")
    def test_handles_http_error(self, mock_post, capsys):
        mock_post.return_value = MagicMock(status_code=500, text="Internal Error")
        send_slack_message("테스트", webhook_url="https://hooks.slack.com/test")
        captured = capsys.readouterr()
        assert "실패" in captured.out

    @patch("notify.slack_notifier.requests.post", side_effect=ConnectionError("timeout"))
    def test_handles_exception(self, mock_post, capsys):
        send_slack_message("테스트", webhook_url="https://hooks.slack.com/test")
        captured = capsys.readouterr()
        assert "예외" in captured.out


# ============================================================
# 6. 중복 코드 존재 검증 (리팩토링 후 삭제 확인용)
# ============================================================
class TestDuplicateCodeDetection:
    """리팩토링 후 중복이 제거되었는지 확인하는 테스트."""

    @staticmethod
    def _count_pattern_in_file(filepath: str, pattern: str) -> int:
        p = PROJECT_ROOT / filepath
        if not p.exists():
            return 0
        return p.read_text(encoding="utf-8").count(pattern)

    def test_resolve_db_url_only_in_common(self):
        """_resolve_db_url은 fetch_data/common/db_utils.py에만 존재해야 함"""
        duplicated_files = [
            "fetch_data/pv/nambu_backfill.py",
            "fetch_data/pv/namdong_collect_pv.py",
            "fetch_data/wind/namdong_wind_collect.py",
            "fetch_data/wind/seobu_wind_load.py",
            "fetch_data/wind/hangyoung_wind_load.py",
        ]
        for f in duplicated_files:
            count = self._count_pattern_in_file(f, "def _resolve_db_url")
            assert count == 0, f"{f}에 _resolve_db_url 정의가 남아있음"

    def test_send_slack_only_in_notify(self):
        """send_slack_message는 notify/slack_notifier.py에만 정의되어야 함"""
        duplicated_files = [
            "fetch_data/pv/namdong_collect_pv.py",
            "fetch_data/wind/namdong_wind_collect.py",
            "prefect_flows/prefect_pipeline.py",
        ]
        for f in duplicated_files:
            count = self._count_pattern_in_file(f, "def send_slack_message")
            assert count == 0, f"{f}에 send_slack_message 정의가 남아있음"

    def test_no_duplicate_import_os_in_slack_notifier(self):
        """notify/slack_notifier.py에서 import os 중복 없어야 함"""
        count = self._count_pattern_in_file("notify/slack_notifier.py", "import os")
        assert count <= 1, f"import os가 {count}번 존재"


# ============================================================
# 7. 레거시 파일 존재 검증 (리팩토링 후 삭제 확인용)
# ============================================================
class TestLegacyFileCleanup:
    """리팩토링 후 삭제 대상 파일이 제거되었는지 확인."""

    def test_main_py_removed(self):
        assert not (PROJECT_ROOT / "main.py").exists(), "main.py가 아직 존재함"

    def test_readme1md_removed(self):
        assert not (PROJECT_ROOT / "readme.1md").exists(), "readme.1md가 아직 존재함"
