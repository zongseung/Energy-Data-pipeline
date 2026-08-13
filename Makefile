# Energy-Data-pipeline Makefile
# 실제 운영: docker/docker-compose.yml 사용

COMPOSE = docker compose -f docker/docker-compose.yml

.PHONY: up down logs logs-worker rebuild deploy ps ui db

## 스택 시작
up:
	$(COMPOSE) up -d

## 스택 종료
down:
	$(COMPOSE) down

## 전체 로그 확인
logs:
	$(COMPOSE) logs -f

## 워커 로그만 확인
logs-worker:
	$(COMPOSE) logs -f pv-worker

## pv-pipeline 이미지 재빌드 + 배포 재등록
rebuild:
	$(COMPOSE) build pv-deployer
	$(COMPOSE) up pv-deployer --no-deps

## 배포만 재등록 (이미지 재빌드 없이)
deploy:
	$(COMPOSE) up pv-deployer --no-deps

## 컨테이너 상태 확인
ps:
	$(COMPOSE) ps

## Prefect UI / Grafana 접속 주소 출력
ui:
	@echo "Prefect UI: http://localhost:4400"
	@echo "Grafana:    http://localhost:3006"

## DB 접속 (psql)
db:
	$(COMPOSE) exec pv-db psql -U pv -d pv
