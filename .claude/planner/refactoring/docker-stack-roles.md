# Docker 스택 역할 정리 (Step 4)

기준일: 2026-02-17

## 유지 기준

- 루트 `docker-compose.yml`: 현재 기본 실행 스택
- `docker/`: 구 Prefect/Grafana 분리 스택 및 대시보드/프로비저닝 자산 보관
- `pv_test/`: 로컬 PV DB 초기화/검증용 테스트 스택

## 디렉토리별 역할

### `docker/`
- `docker/docker-compose.yml`, `docker/Dockerfile`은 레거시/대체 실행 스택
- 루트 compose에서 Grafana 자산을 `./docker/grafana/*` 경로로 참조하므로
  자산 디렉토리는 즉시 삭제하지 않고 유지

### `pv_test/`
- `pv_test/docker-compose.yml`, `pv_test/Dockerfile`, `pv_test/init_db.py`는
  테스트/초기 적재 검증 전용 스택
- 운영 기본 스택과 분리된 실험/검증 용도로 유지

## 후속 정리 원칙

1. 운영 표준은 루트 compose를 기준으로 유지
2. `docker/`, `pv_test/`는 문서화된 보조 스택으로만 사용
3. 보조 스택 폐기 시, 루트 compose가 참조 중인 `docker/grafana/*` 자산 마이그레이션을 선행
