"""데이터 경로 한 곳.

살아남는 경로는 `data/` 하나뿐이다 — 컨테이너에 마운트되는 게 그것뿐이라
(`ops/docker/.../deploy.py` 볼륨), 다른 데 쓴 파일은 flow 가 끝나면
`auto_remove` 로 컨테이너와 함께 사라진다.

  DATA_DIR    : 보존해야 하는 것 (DB 에 없거나, 의도한 export)
  SCRATCH_DIR : 버려도 되는 중간 산출물. 수집 커서는 DB 라 원본 CSV 는 안 쌓아도 된다
                ([[collector-cursor-local-file-bug]] 해소)
"""
from __future__ import annotations

import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = PROJECT_ROOT / "data"
SCRATCH_DIR = Path(tempfile.gettempdir()) / "energy-pipeline"
