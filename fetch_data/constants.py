"""프로젝트 전체에서 사용하는 API 및 설정 상수."""


class NamebuAPI:
    ENDPOINT = "https://apis.data.go.kr/B552520/PwrSunLightInfo/getDataService"
    MAX_ROWS = 100


class NamdongAPI:
    BASE_URL = "https://www.koenergy.kr"
    MENU_CD = "FN0912020217"
    CSV_URL = f"{BASE_URL}/kosep/gv/nf/dt/nfdt21/csvDown.do"


class NamdongGenAPI:
    """남동발전(KOEN) 시간대별 발전실적 - 비태양광 발전원.

    각 발전원은 koenergy.kr의 별도 메뉴 페이지(nfdtNN)를 가진다.
    수집 패턴은 태양광(NamdongAPI)과 동일:
        1) main.do GET 으로 세션 쿠키(JSESSIONID) 확보
        2) csvDown.do POST 로 기간별 CSV 다운로드 (Referer = main.do)

    엔드포인트는 2026-05 기준 koenergy.kr 네비게이션에서 확인.
    """

    BASE_URL = "https://www.koenergy.kr"

    # 발전원 key -> (한글명, 페이지 경로, menuCd)
    GEN_TYPES = {
        "ocean_hydro": {"label": "해양소수력", "page": "nfdt24", "menu_cd": "FN0912020219"},
        "fuel_cell": {"label": "연료전지", "page": "nfdt25", "menu_cd": "FN0912020220"},
        "thermal": {"label": "화력", "page": "nfdt26", "menu_cd": "FN0912020221"},
    }

    @classmethod
    def main_url(cls, page: str) -> str:
        return f"{cls.BASE_URL}/kosep/gv/nf/dt/{page}/main.do"

    @classmethod
    def csv_url(cls, page: str) -> str:
        return f"{cls.BASE_URL}/kosep/gv/nf/dt/{page}/csvDown.do"


class WeatherAPI:
    ENDPOINT = "https://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList"


class SMPAPI:
    """KPX 전력거래소 SMP(계통한계가격) 스크래핑 엔드포인트.

    KPX는 데이터 종류별로 도메인/엔드포인트가 다르다 (2026-06 라이브 확인):
      - 일별 시간별, 제주 실시간 : 구도메인 www.kpx.or.kr (구 mid)
      - 월별/연도별 가중평균      : 신도메인 new.kpx.or.kr (신 mid, GET-only)

    시간 표기는 hour-ending 규약(1~24시 = 구간 종료). 적재 시 구간시작으로 변환:
      시간별  라벨 N    -> (N-1)시
      실시간  N h K구간 -> (N-1)시 + (K-1)*15분  (1일 96구간)
    """

    # --- 일별 시간별 (GET -> CSRF -> POST) ---
    DAILY = {
        "land": ("https://www.kpx.or.kr/smpInland.es", "a10404080100"),
        "jeju": ("https://www.kpx.or.kr/smpJeju.es", "a10404080200"),
    }
    EXPECTED_VALUE_COUNT = 27  # 1~24시 + 최대/최소/가중평균

    # --- 제주 실시간 15분 (GET) ---
    REALTIME_JEJU_URL = "https://www.kpx.or.kr/bidSmpLfdDataRt.es"
    REALTIME_JEJU_MID = "a10406020200"
    REALTIME_JEJU_PARAMS = {"device": "pc", "division": "smpDataRt", "gubun": "today"}
    REALTIME_SLOTS_PER_DAY = 96  # 24시간 * 4구간(15분)

    # --- 월별/연도별 가중평균 (EPSIS, 공식 가중평균 + BLMP) ---
    # 페이지 GET으로 세션쿠키 확보 후 *.ajax POST 호출.
    # 응답은 JS 코드 문자열: c1=통합SMP, c2=육지SMP, c3=제주SMP, c4=BLMP (textFormmat 출현순).
    #   - 통합/제주: 2010~ 존재 (그 이전 0)
    #   - BLMP: 2001~2006 존재 (그 이후 0). 0은 '데이터 없음'으로 skip.
    # selYear: 연='Y'(beginDate/endDate=YYYY), 월='N'(=YYYYMM).
    EPSIS_PAGE = "https://epsis.kpx.or.kr/epsisnew/selectEkmaSmpSmpChart.do?menuId=040201"
    EPSIS_AJAX = "https://epsis.kpx.or.kr/epsisnew/selectEkmaSmpSmp.ajax"
    # c-컬럼 순서 -> (region, price_type)
    EPSIS_COLS = [
        ("unified", "smp"),  # c1
        ("land", "smp"),     # c2
        ("jeju", "smp"),     # c3
        ("land", "blmp"),    # c4 (BLMP는 육지 기준, 2001~2006)
    ]

    # (구) KPX 월/연 페이지 — EPSIS로 대체됨. 폴백용 보존.
    MONTHLY_URL = "https://new.kpx.or.kr/smpMonthly.es"
    MONTHLY_MID = "a10606080300"
    YEARLY_URL = "https://new.kpx.or.kr/smpYearly.es"
    YEARLY_MID = "a10606080400"

    # 표의 '구분' 라벨 -> region 코드 (KPX 폴백 파서용)
    REGION_LABELS = {
        "육지": "land",
        "제주": "jeju",
        "통합": "unified",
    }

    # --- 라이브 조회 하한(외부 레포 교차검증) ---
    LIVE_FLOOR = "2013-02-03"  # 이 이전은 master CSV 백필로만 채움

    # --- 차단 대응 ---
    MAX_RETRIES = 3
    COOL_OFF_SEC = 30
    CONCURRENT_LIMIT = 4
    WAIT_RANGE = (1.5, 2.5)
    TIMEOUT_SEC = 30
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
