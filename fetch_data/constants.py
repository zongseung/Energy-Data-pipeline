"""프로젝트 전체에서 사용하는 API 및 설정 상수."""


class NamebuAPI:
    ENDPOINT = "https://apis.data.go.kr/B552520/PwrSunLightInfo/getDataService"
    MAX_ROWS = 100


class NamdongAPI:
    BASE_URL = "https://www.koenergy.kr"
    MENU_CD = "FN0912020217"
    CSV_URL = f"{BASE_URL}/kosep/gv/nf/dt/nfdt21/csvDown.do"


class WeatherAPI:
    ENDPOINT = "https://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList"
