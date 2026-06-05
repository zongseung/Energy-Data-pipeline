# intake/ — EM_planning 스펙 핸드오프 착지점

EM_planning(에너지 마켓 리스크/보험/FM 프로젝트)이 "어떤 소스를 어떻게 수집·전처리할지" 결정한 **SourceSpec**을 이 디렉토리로 push한다. 이 프로젝트(Energy-Data-pipeline)가 픽업해 실제 수집·적재 파이프라인을 구현한다.

## 규약
- **한 소스 = 한 파일**: `<source>.spec.md` (양식: `docs/SOURCE_SPEC_TEMPLATE.md`)
- **전달 경로**: EM_planning의 `nas_exec.py` 브리지가 **이 경로(`/mnt/nvme/Energy-Data-pipeline/intake/`)에만** 쓰기 허용(하드닝됨, 2026-06-05 E2E 검증). 두 Claude 인스턴스는 직접 대화 못 하므로 이 파일이 유일한 핸드오프 채널.
  - 브리지 내용 전달 패턴(생산자 측): `<내용> | nas_exec "tee <intake경로>"` (stdin→원격 tee). `>` redirect·intake 밖 쓰기·컨테이너 제어·`.env` read는 가드가 차단. 소비자(이 프로젝트)는 떨어진 파일을 읽기만 하면 됨.
- **임시 착지점**: 구현이 끝나면 스펙의 영구 위치는 구현체 옆 `fetch_data/<source>/SPEC.md`로 이동한다(출처가 코드에 붙어다니도록). intake는 처리 대기 큐.
- **self-contained**: 스펙이 EM_planning의 다른 문서(regime 정의·리스크 매트릭스 등)를 참조하면 그 문서도 함께 전달되어야 한다.

## 구현 절차
스펙을 받으면 `docs/NEW_SOURCE_PLAYBOOK.md`를 따라 수집기·테이블·flow를 스캐폴딩하고 라이브 검증한다.
