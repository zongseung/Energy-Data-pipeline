"""
Data fetching module for Energy-Data-pipeline.

Organized into submodules:
- common: Database and utility functions
- weather: ASOS weather data collection
- pv: PV generation data collection
"""

from fetch_data.common.impute_missing import impute_missing_values

__all__ = [
    # Submodules
    "common",
    "weather",
    "pv",
    # Utilities (backward compatibility)
    "impute_missing_values",
]
