"""
Data fetching module for Demand-MLops.

Organized into submodules:
- common: Database and utility functions
- weather: ASOS weather data collection
- demand: Power demand data collection
- pv: PV generation data collection
"""

# Re-exports for backward compatibility
from fetch_data.common.database import (
    Base,
    Demand5Min,
    DemandWeather1H,
    get_async_session,
    get_engine,
    init_db,
    get_last_timestamp_5min,
    get_last_timestamp_1h,
)
from fetch_data.common.impute_missing import impute_missing_values

__all__ = [
    # Submodules
    "common",
    "weather",
    "demand",
    "pv",
    # Database (backward compatibility)
    "Base",
    "Demand5Min",
    "DemandWeather1H",
    "get_async_session",
    "get_engine",
    "init_db",
    "get_last_timestamp_5min",
    "get_last_timestamp_1h",
    # Utilities (backward compatibility)
    "impute_missing_values",
]
