# 리팩토링 테스트 결과

실행 일시: 2026-02-17
테스트 파일: `tests/test_refactoring.py`
실행 명령: `uv run python -m pytest tests/test_refactoring.py -v`

## 결과 요약

| 구분 | 개수 |
|------|------|
| PASSED | 25 |
| XFAIL (예상된 실패) | 6 |
| FAILED | 0 |
| **합계** | **31** |

## 테스트 상세

### 1. normalize_date_format (6/6 PASSED)

| 테스트 | 상태 |
|--------|------|
| `test_yyyymmdd_passthrough` — "20260101" 그대로 통과 | PASSED |
| `test_dash_separated` — "2026-01-01" → "20260101" | PASSED |
| `test_slash_separated` — "2026/01/01" → "20260101" | PASSED |
| `test_invalid_length_raises` — "202601" → ValueError | PASSED |
| `test_non_digit_raises` — "abcdefgh" → ValueError | PASSED |
| `test_empty_string_raises` — "" → ValueError | PASSED |

### 2. find_consecutive_missing_groups (6/6 PASSED)

| 테스트 | 상태 |
|--------|------|
| `test_no_missing` — 결측 없는 시리즈 | PASSED |
| `test_single_missing` — 단일 결측 | PASSED |
| `test_consecutive_missing` — 연속 3개 결측 | PASSED |
| `test_multiple_groups` — 복수 그룹 | PASSED |
| `test_all_missing` — 전체 결측 | PASSED |
| `test_empty_series` — 빈 시리즈 | PASSED |

### 3. spline_impute (2 PASSED, 1 XFAIL)

| 테스트 | 상태 |
|--------|------|
| `test_single_gap_interpolated` — 중간 결측 보간 | PASSED |
| `test_boundary_gap_start` — 맨 앞 결측 보간 | **XFAIL** (버그) |
| `test_preserves_existing_values` — 기존값 보존 | PASSED |

### 4. impute_missing_values 통합 (7/7 PASSED)

| 테스트 | 상태 |
|--------|------|
| `test_returns_tuple_when_debug` — debug=True → (df, info) | PASSED |
| `test_returns_dataframe_when_no_debug` — debug=False → df | PASSED |
| `test_missing_values_reduced` — 결측치 감소 확인 | PASSED |
| `test_shape_preserved` — 행 수 유지 | PASSED |
| `test_no_temp_columns_remain` — `_` 접두사 임시 컬럼 제거 확인 | PASSED |
| `test_missing_date_col_raises` — 날짜 컬럼 없으면 ValueError | PASSED |
| `test_missing_station_col_raises` — 지역 컬럼 없으면 ValueError | PASSED |

### 5. send_slack_message (4/4 PASSED)

| 테스트 | 상태 |
|--------|------|
| `test_sends_post_request` — webhook URL로 POST 호출 확인 | PASSED |
| `test_skips_when_no_url` — URL 없으면 스킵 | PASSED |
| `test_handles_http_error` — HTTP 500 처리 | PASSED |
| `test_handles_exception` — ConnectionError 처리 | PASSED |

### 6. 중복 코드 검증 (3 XFAIL — 리팩토링 후 통과 예정)

| 테스트 | 상태 | 설명 |
|--------|------|------|
| `test_resolve_db_url_only_in_common` | XFAIL | 5개 파일에 `_resolve_db_url` 정의 중복 존재 |
| `test_send_slack_only_in_notify` | XFAIL | 3개 파일에 `send_slack_message` 정의 중복 존재 |
| `test_no_duplicate_import_os_in_slack_notifier` | XFAIL | `notify/slack_notifier.py`에 `import os` 2회 |

### 7. 레거시 파일 검증 (2 XFAIL — 리팩토링 후 통과 예정)

| 테스트 | 상태 | 설명 |
|--------|------|------|
| `test_main_py_removed` | XFAIL | `main.py` 아직 존재 |
| `test_readme1md_removed` | XFAIL | `readme.1md` 아직 존재 |

## 발견된 버그

### `spline_impute()` 시리즈 맨 앞 결측치 보간 실패

- **파일**: `fetch_data/common/impute_missing.py:30` (`spline_impute`)
- **현상**: `start_idx=0`일 때 `valid_before`가 빈 시리즈 → `interpolate(method='linear')` fallback이 앞에 유효값이 없어 NaN 반환
- **영향**: 첫 번째 시간대의 기상 데이터가 결측이면 보간되지 않음
- **수정 방안**: `bfill` (backward fill) fallback 추가

```python
# 현재 (문제)
series.iloc[start_idx:start_idx + length] = series.interpolate(method='linear').iloc[start_idx:start_idx + length]

# 개선안
interpolated = series.interpolate(method='linear')
still_missing = interpolated.iloc[start_idx:start_idx + length].isna()
if still_missing.any():
    interpolated = interpolated.bfill()
series.iloc[start_idx:start_idx + length] = interpolated.iloc[start_idx:start_idx + length]
```

## XFAIL 테스트 활용 방법

리팩토링 진행 시:

1. 해당 작업 완료 후 `@pytest.mark.xfail` 데코레이터 제거
2. `uv run python -m pytest tests/test_refactoring.py -v` 실행
3. PASSED로 전환되면 리팩토링 정상 완료 확인
