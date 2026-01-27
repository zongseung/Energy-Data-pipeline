"""
한경풍력 데이터 CSV 로드 모듈

Hangyoung_wind_power.csv를 읽어 wind_hangyoung 테이블에 적재합니다.
CSV 컬럼: timestamp, generation (plant_name 없음, 한 timestamp에 여러 터빈)
"""

import os
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, urlunparse

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


def _running_in_docker() -> bool:
    return Path("/.dockerenv").exists() or os.getenv("RUNNING_IN_DOCKER") == "1"


def _resolve_db_url(cli_db_url: Optional[str] = None) -> str:
    if cli_db_url:
        return cli_db_url

    db_url = os.getenv("DB_URL")
    if db_url:
        return db_url

    pv_db_url = os.getenv("PV_DATABASE_URL") or ""
    local_db_url = os.getenv("LOCAL_DB_URL") or ""

    if not _running_in_docker() and pv_db_url:
        try:
            u = urlparse(pv_db_url)
            if u.hostname == "pv-db" and local_db_url:
                return local_db_url
            if u.hostname == "pv-db":
                host_port = int(os.getenv("PV_DB_PORT_FORWARD", "5435"))
                if u.username and u.password:
                    netloc = f"{u.username}:{u.password}@localhost:{host_port}"
                elif u.username:
                    netloc = f"{u.username}@localhost:{host_port}"
                else:
                    netloc = f"localhost:{host_port}"
                return urlunparse(u._replace(netloc=netloc))
        except Exception:
            pass

    return pv_db_url or local_db_url


def load_hangyoung_wind_csv(csv_path: Optional[str] = None) -> pd.DataFrame:
    """
    Hangyoung_wind_power.csv를 읽어 DB 적재용 DataFrame을 반환합니다.

    Returns:
        DataFrame[timestamp, plant_name, generation]
    """
    if csv_path is None:
        csv_path = PROJECT_ROOT / "Hangyoung_wind_power.csv"
    else:
        csv_path = Path(csv_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV 파일을 찾을 수 없습니다: {csv_path}")

    df = pd.read_csv(csv_path, index_col=0)

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["generation"] = pd.to_numeric(df["generation"], errors="coerce")
    df["plant_name"] = "Hangyoung"

    result = df[["timestamp", "plant_name", "generation"]].dropna(subset=["timestamp"])
    print(f"[CSV] 한경풍력 CSV 로드: {len(result)}행")
    return result.reset_index(drop=True)


def load_hangyoung_to_db(csv_path: Optional[str] = None, db_url: Optional[str] = None) -> int:
    """
    wind_hangyoung 테이블에 bulk insert합니다.
    한경풍력은 unique constraint가 없으므로 truncate + insert 방식.
    """
    df = load_hangyoung_wind_csv(csv_path)

    if df.empty:
        print("[DB] 적재할 데이터가 없습니다.")
        return 0

    resolved_url = _resolve_db_url(db_url)
    if not resolved_url:
        raise RuntimeError("DB_URL이 설정되지 않았습니다.")

    engine = create_engine(resolved_url)

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE wind_hangyoung"))
        df.to_sql("wind_hangyoung", con=conn, if_exists="append", index=False)

    print(f"[DB] wind_hangyoung 적재 완료: {len(df)}행")
    return len(df)


if __name__ == "__main__":
    load_hangyoung_to_db()
