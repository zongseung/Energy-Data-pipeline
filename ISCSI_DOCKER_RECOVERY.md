# iSCSI DB 및 Docker 복구 런북

기준일: 2026-08-04

## 현재 구성

| 항목 | 값 |
|---|---|
| NAS 관리 화면 | `http://192.9.65.61:5000` |
| iSCSI portal | `192.9.65.61:3260` |
| 서버 주소/인터페이스 | `192.9.65.58` / `enp4s0` |
| PV DB 마운트 | `/mnt/iscsi-renewable` |
| PV DB 파일시스템 UUID | `c8b2ccb1-0d59-43f7-b0cc-18fa6d591dd8` |
| PV DB iSCSI 경로 | `default-target`, LUN 2 |
| PV DB LUN serial | `c2629f0b-dc4a-4ec1-8eee-d40e7011c36e` |
| PV DB LUN WWN | `6001405c2629f0bddc4ad4ec1d8eeedd` |
| 운영 compose | `docker/docker-compose.yml` |
| 운영 DB 컨테이너 | `pv-data-postgres` (`pv-db`) |
| 운영 DB 데이터 | `/mnt/iscsi-renewable/postgres/pv-data-postgres` |
| 수급 DB 마운트 | `/mnt/iscsi` |
| 수급 DB 파일시스템 UUID | `a85a78d5-555f-4783-bf3a-e93088a03b55` |
| 수급 DB compose | `/mnt/nvme/weather-pipeline/docker/docker-compose.yml` |
| 수급 DB 컨테이너 | `demand-postgres` (`demand-db`) |
| 수급 DB 데이터 | `/mnt/iscsi/postgres/demand-postgres` |

루트의 `docker-compose.yml`은 과거 스택이다. `pv-main-db`를 사용하는 이
파일은 복구에 사용하지 않는다.

## 2026-08-04 진단 결과

1. NAS `192.9.65.61`은 ping에 응답한다.
2. `5000/tcp`는 Synology DSM 웹 포트이고 `3260/tcp`는 iSCSI 포트이며 둘 다
   연결된다.
3. 서버에는 활성 iSCSI 세션과 위 UUID의 블록 장치가 없다.
4. `iscsid`와 `open-iscsi` 서비스는 활성/자동 시작 상태다.
5. 부팅 로그에는 `iscsiadm: No records found`가 기록되어 자동 로그인할
   target이 없거나 `node.startup=automatic`이 아닌 상태다.
6. 장애 직전에는 이전 portal `192.9.66.137:3260`으로 재접속하다
   `Network is unreachable`이 반복됐다.
7. PostgreSQL은 2026-08-01부터 `Input/output error`와 `Read-only file system`을
   기록했고, 운영 스택은 `Exited (255)` 상태다.

따라서 복구 순서는 **새 portal 로그인 -> 블록 장치 확인 -> 오프라인
파일시스템 점검 -> 마운트 확인 -> 운영 compose 시작**이다.

> 중요: 이 LUN을 다른 서버에서 동시에 마운트하지 않는다. 일반 ext4는
> 클러스터 파일시스템이 아니므로 동시 마운트하면 DB가 손상될 수 있다.

## 2026-08-04 복구 결과

1. 새 portal `192.9.65.61:3260`에서 target을 검색하고 자동 로그인으로
   설정했다.
2. DB LUN은 `/dev/sdc`, `default-target`의 LUN 2로 확인됐다.
3. `e2fsck -pf`가 ext4 journal을 복구했다. 선택적 extent tree 최적화는
   건너뛰었으며 심각한 파일·디렉터리 손상은 보고되지 않았다.
4. UUID가 일치하는 `/dev/sdc`를 `/mnt/iscsi-renewable`에 ext4 `rw`로
   마운트했다. 마운트 이후 커널 I/O 오류나 read-only 전환은 없었다.
5. `pv-data-postgres`는 `healthy`, `pg_is_in_recovery()`는 `false`, SQL 조회와
   주요 테이블(`generation`, `plants`, `smp_hourly`)의 데이터 존재 여부를
   확인했다.
6. 운영 compose 전체를 시작했다. PostgreSQL 2개와 Prefect server가 healthy,
   Grafana와 worker가 running 상태이며 Prefect/Grafana health API가 정상이다.
7. 최신 Prefect deployment 17개를 다시 등록했고 deployer는 exit 0으로
   종료했다.
8. `demand-postgres`도 같은 iSCSI 장애로 WAL 쓰기 중 `Input/output error`와
   `Read-only file system`이 발생해 exit 139로 종료됐다. OOM은 아니었고
   restart policy가 `no`라 자동 재시작되지 않았다.
9. 별도 LUN `/dev/sdb`를 unmount한 뒤 `e2fsck -pf`로 검사했다. 오류는
   보고되지 않았고 `/mnt/iscsi`에 ext4 `rw`로 다시 마운트했다.
10. `demand-postgres`는 `healthy`이고 SQL 접속 및 사용자 테이블 7개를
    확인했다. 15:20의 `jeju-supply-demand-db-sync`가 `Completed`됐으며
    `jeju_supply_demand`에는 586,051행, 최신 시각 `2026-08-04 15:10:00`이
    저장돼 있다.
11. 남부발전 daily/backfill의 상태 조회를 삭제된 레거시 테이블 대신 core
    `plants`/`generation`으로 전환했다. 수동 검증 run
    `1ba9e032-c726-43d7-9bb6-63f644c82eae`는 약 150초 후 `Completed`됐고,
    활성 설비 15개가 `2026-08-03 23:00:00`까지 갱신됐다.
12. 삭제된 entrypoint를 가리키던 `daily-namdong-pv-collection` deployment는
    제거했다. 현재 코드의 `monthly-namdong-pv-collection`은 유지했다.

13. 기상 API 키는 `SERVICE_KEY`를 우선 사용하고, 값이 없으면
    `NAMDONG_WIND_KEY`를 사용하도록 통합했다. 현재 deployment에는 fallback으로
    해석된 키가 전달되며 키 원문은 로그나 이 문서에 남기지 않는다.
14. 전국 전력수요 수집, 시간별 수요-기상 결합, materialized view 갱신을
    `Unified Demand Collection Flow/unified-demand-collection` 하나로 통합했다.
    `pv-pool`에서 10분마다 실행한다.

## 자동 수집 점검 결과

2026-08-04 기준 코드에는 Prefect flow 17개가 모두 deployment로 정의돼 있다.
16개는 cron 자동 실행이고 `full-etl`만 의도적으로 수동 실행이다. `pv-pool`은
`READY`이고 PV Prefect server, worker, 두 운영 DB는 실행 중이다.

실시간 핵심 경로는 정상이다.

| Deployment | 주기 | 확인 결과 |
|---|---|---|
| `unified-demand-collection` | 10분 | 전국 5분 수요를 이어서 수집하고 매시 첫 실행에 수요-기상 시간 집계와 두 materialized view를 갱신. 18:50~19:20 예약 실행 연속 `Completed` |
| `jeju-realtime-collection` | 5분 | 15:10~15:30 최근 5회 연속 `Completed` |
| `jeju-supply-demand-db-sync` | 10분 | DB 복구 후 15:20, 15:30 연속 `Completed` |
| `daily-weather-collection` | 매일 09:00 | `SERVICE_KEY` -> `NAMDONG_WIND_KEY` 순서로 키를 해석. 8월 1~3일 수동 복구 run 모두 `Completed` |
| `daily-smp-collection` | 매일 09:00 | 실행일 기준 전날 하루전시장 SMP 수집. Prefect `READY` 및 `Asia/Seoul` 확인 |
| `daily-nambu-pv-collection` | 매일 09:30 | core `plants`/`generation` 기준 수집. 수동 실행 `Completed`, 활성 15개 설비가 전날 23시까지 갱신됨 |
| `monthly-namdong-pv-collection` | 매월 10일 10:00 | 현재 코드 entrypoint를 사용하는 정상 deployment. `READY` 및 active 유지 |

의도적으로 수동인 항목은 다음과 같다.

| 항목 | 상태 | 원인/조치 |
|---|---|---|
| `full-etl` | 수동 | 의도된 수동 실행이며 누락이 아님 |

호스트 재부팅 자동화도 아직 완성되지 않았다.

1. Docker, `open-iscsi`, `iscsid` 자체는 enable 상태다.
2. `energy-data-pipeline.service`는 아직 설치되지 않았다.
3. 운영 compose와 `demand-postgres`의 restart policy는 기본값 `no`다.
4. `/mnt/iscsi-renewable`의 fstab 항목에는 `_netdev`가 빠져 있다.

따라서 현재 프로세스가 살아 있는 동안에는 Prefect가 스케줄을 실행하지만,
서버 재부팅 뒤에는 이 문서의 systemd unit과 fstab 보정을 적용하기 전까지
자동 복구가 보장되지 않는다. 과거 `weather-pipeline`의 Prefect server/worker는
중복 수집을 막기 위해 시작하지 않고 `demand-db`만 사용한다.

## 2026-08-04 데이터 누락 복구 결과

1. 기존 ASOS CSV를 현재 운영 데이터 경로로 옮긴 뒤 실제 누락일인
   `2026-08-01`~`2026-08-03`을 날짜순으로 수집했다. 각 날짜는 95개 관측소,
   2,280개 시간 행을 가지며 세 run 모두 `Completed`됐다.
2. 전국 수요 강제 복구 run `92f98475-356c-4fca-a484-61724ea9c6f6`은
   `Completed`됐다. `demand_weather_1h`은 6,103,911행, 최신
   `2026-08-03 23:00:00`까지 복구됐다.
3. 수리 가능한 `UNKNOWN` placeholder 4,911행을 실측 관측소 행으로 교체했다.
   남은 88행은 5분 수요가 한 시간에 12건 미만이거나 해당 시간의 ASOS 원본이
   전혀 없어 보존했다. 값을 추정하거나 임의로 만들지 않았다.
4. `mv_latest_weather`는 95행, `mv_hourly_national`은 66,489행이며 둘 다
   `2026-08-03 23:00:00`까지 갱신됐다.
5. Jeju 월 수집기가 실시간 수집기가 만든 현재 월 부분 파일을 그대로
   건너뛰는 문제를 수정했다. 이제 요청 기간을 다시 받아 기존 파일과 병합하고,
   공통 파일 잠금과 atomic replace로 실시간 writer와의 충돌을 막는다.
6. Jeju 재수집 run `78b214bb-71a3-4c4f-9e5b-6b863f310980`과 DB sync run
   `ae876f19-f055-4e03-a185-8f3bd53b39ba`은 `Completed`됐다.
   `2026-08-01`~`2026-08-03`은 날짜별 288행으로 완전하며 오늘 데이터도
   계속 증가한다.
7. 확정 하루전 SMP는 `smp_hourly`에 `2026-08-03 23:00:00`까지 있다.
   실시간 Jeju SMP 원천이 응답 자체를 주지 않으면 flow를 실패/retry 처리한다.
   원천 표가 정상이나 아직 확정된 행이 없는 경우에는 0건으로 끝내되 가짜 가격을
   넣지 않는다.

## 수집 데이터 복구 명령

### ASOS 특정 날짜

날짜 파라미터는 Prefect 스키마가 문자열로 받도록 JSON 문자열로 넘긴다.

```bash
PREFECT_API_URL=http://127.0.0.1:4400/api uv run prefect deployment run \
  'daily-weather-collection-flow/daily-weather-collection' \
  --param target_date='"20260803"' --watch
```

### 전국 수요와 시간 집계

`force_hourly=true`는 최근 48시간만 보는 일반 실행과 달리, 가장 오래된 복구
가능 `UNKNOWN` 또는 현재 집계 끝 다음 시각부터 다시 결합한다.

```bash
PREFECT_API_URL=http://127.0.0.1:4400/api uv run prefect deployment run \
  'Unified Demand Collection Flow/unified-demand-collection' \
  --param force_hourly=true --watch
```

### Jeju 현재 월

월 수집을 먼저 완료한 뒤 DB sync를 실행한다. 기존 현재 월 CSV가 있어도 삭제하지
않고 원천 데이터와 병합한다.

```bash
PREFECT_API_URL=http://127.0.0.1:4400/api uv run prefect deployment run \
  'jeju-sukub-monthly-collection/jeju-sukub-monthly-collection' \
  --param target_month='"2026-08"' --watch
PREFECT_API_URL=http://127.0.0.1:4400/api uv run prefect deployment run \
  'jeju-supply-demand-db-sync/jeju-supply-demand-db-sync' --watch
```

### 적재 상태 SQL

```bash
docker exec demand-postgres psql -U demand -d demand -c "
SELECT 'demand_5min' AS source, count(*), max(timestamp) FROM demand_5min
UNION ALL
SELECT 'demand_weather_1h', count(*), max(timestamp) FROM demand_weather_1h;
SELECT count(*) AS unknown_rows, min(timestamp), max(timestamp)
FROM demand_weather_1h WHERE station_name = 'UNKNOWN';
SELECT 'mv_latest_weather' AS source, count(*), max(timestamp) FROM mv_latest_weather
UNION ALL
SELECT 'mv_hourly_national', count(*), max(timestamp) FROM mv_hourly_national;
SELECT date(ts), count(*), min(ts), max(ts)
FROM jeju_supply_demand
WHERE ts >= current_date - interval '7 days'
GROUP BY date(ts) ORDER BY date(ts);"
```

Prefect에서는 다음 네 조건을 같이 확인한다.

1. `unified-demand-collection`이 `READY`, unpaused, `*/10 * * * *`,
   `Asia/Seoul`인지 확인한다.
2. 최근 예약 run이 `Completed`인지 확인한다.
3. `pv-pool` worker가 online인지 확인한다.
4. 옛 `prefect-server-new`, `weather-worker-new`는 stopped 상태인지 확인한다.

## 즉시 복구

### 1. DB를 정지 상태로 유지

현재는 이미 종료 상태지만 파일시스템을 확인하기 전에는 DB를 시작하지 않는다.

```bash
cd /mnt/nvme/Energy-Data-pipeline
docker stop pv-data-postgres pv-main-db 2>/dev/null || true
```

### 2. 새 portal 검색 및 로그인

```bash
sudo systemctl enable --now iscsid.service open-iscsi.service
sudo iscsiadm -m discovery -t sendtargets -p 192.9.65.61:3260
sudo iscsiadm -m node -p 192.9.65.61:3260 \
  --op update -n node.startup -v automatic
sudo iscsiadm -m node -p 192.9.65.61:3260 --login
sudo udevadm settle
```

CHAP 인증 오류가 나면 DSM의 SAN Manager에서 이 서버의 initiator IQN, target
ACL, CHAP 설정을 확인한다. 비밀번호를 명령행이나 이 문서에 기록하지 않는다.

로그인 결과를 확인한다.

```bash
sudo iscsiadm -m session -P 1
lsblk -f
ls -l /dev/disk/by-uuid/c8b2ccb1-0d59-43f7-b0cc-18fa6d591dd8
```

마지막 UUID가 보이지 않으면 마운트나 Docker를 시작하지 말고 DSM에서 해당
LUN이 검색된 target과 이 initiator에 매핑됐는지 확인한다.

### 3. 파일시스템을 읽기 전용으로 검사

장애 직전 I/O 오류가 있었으므로 바로 마운트하지 않는다.

```bash
findmnt -S UUID=c8b2ccb1-0d59-43f7-b0cc-18fa6d591dd8
sudo e2fsck -fn /dev/disk/by-uuid/c8b2ccb1-0d59-43f7-b0cc-18fa6d591dd8
```

`findmnt`가 실제 ext4 마운트를 출력하면 먼저 DB가 정지했는지 확인하고
마운트를 해제한 뒤 검사한다. `e2fsck -fn`이 수정 필요를 보고하면 DSM에서 LUN
스냅샷/백업을 먼저 만든 다음, 마운트되지 않은 상태에서 다음 명령으로 복구한다.

```bash
sudo e2fsck -f /dev/disk/by-uuid/c8b2ccb1-0d59-43f7-b0cc-18fa6d591dd8
```

### 4. 마운트와 실제 DB 디렉터리 확인

```bash
sudo mount /mnt/iscsi-renewable
findmnt -T /mnt/iscsi-renewable
test -f /mnt/iscsi-renewable/postgres/pv-data-postgres/PG_VERSION
```

`findmnt`의 source가 위 UUID 장치인지, 옵션에 `rw`가 있는지 확인한다.
`PG_VERSION` 검사가 실패하면 빈 디렉터리를 만들거나 PostgreSQL을 시작하지
말고 올바른 LUN/경로인지 다시 확인한다.

### 5. 운영 Docker 스택 시작 및 검증

```bash
cd /mnt/nvme/Energy-Data-pipeline
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml ps
docker compose -f docker/docker-compose.yml exec -T pv-db \
  pg_isready -U pv -d pv
docker compose -f docker/docker-compose.yml exec -T pv-db \
  psql -U pv -d pv -c 'SELECT current_timestamp;'
```

DB가 healthy가 된 뒤 Prefect와 Grafana 상태를 확인한다. 장애 원인 확인은 다음
명령으로 충분하다.

```bash
docker compose -f docker/docker-compose.yml logs --tail=100 pv-db
docker compose -f docker/docker-compose.yml ps
```

새 portal과 DB가 정상임을 확인한 뒤에만 옛 portal 레코드를 삭제한다.

```bash
sudo iscsiadm -m node
sudo iscsiadm -m node -p 192.9.66.137:3260 --op delete
```

### 6. 수급 DB 복구

`demand-postgres`는 다른 LUN을 사용하므로 PV DB와 별도로 검사한다.

```bash
docker stop demand-postgres 2>/dev/null || true
sudo umount /mnt/iscsi
findmnt -rn -S /dev/sdb
sudo e2fsck -pf /dev/disk/by-uuid/a85a78d5-555f-4783-bf3a-e93088a03b55
sudo mount /mnt/iscsi
docker compose -f /mnt/nvme/weather-pipeline/docker/docker-compose.yml up -d demand-db
docker inspect demand-postgres --format '{{.State.Status}} {{.State.Health.Status}}'
docker exec demand-postgres pg_isready -U demand -d demand
```

`findmnt`가 출력되면 아직 마운트된 상태이므로 `e2fsck`를 실행하지 않는다.
이 compose와 PV compose의 프로젝트명이 모두 `docker`로 표시되므로
`--remove-orphans`는 사용하지 않는다. 과거 weather Prefect 스택 전체가 아니라
`demand-db`만 시작해야 중복 수집을 피할 수 있다.

## 재부팅 후 자동 복구 설정

### 1. iSCSI 자동 로그인

다음 두 조건이 모두 필요하다.

```bash
sudo iscsiadm -m node -p 192.9.65.61:3260 \
  --op update -n node.startup -v automatic
sudo systemctl enable iscsid.service open-iscsi.service
```

### 2. `/etc/fstab` 보정

현재 `/mnt/iscsi-renewable` 항목에는 `_netdev`가 빠져 있다. 다음처럼 네트워크
파일시스템임을 명시한다.

```fstab
UUID=a85a78d5-555f-4783-bf3a-e93088a03b55 /mnt/iscsi ext4 defaults,_netdev,nofail,x-systemd.automount,x-systemd.device-timeout=30s,x-systemd.mount-timeout=30s 0 2
UUID=c8b2ccb1-0d59-43f7-b0cc-18fa6d591dd8 /mnt/iscsi-renewable ext4 defaults,_netdev,nofail,x-systemd.automount,x-systemd.device-timeout=30s,x-systemd.mount-timeout=30s 0 2
```

수정 후 설정 오류만 검사한다. 실제 재부팅은 운영 점검 시간에 수행한다.

```bash
sudo systemctl daemon-reload
sudo findmnt --verify --verbose
```

### 3. Docker가 마운트 뒤에 시작되도록 설정

`/etc/systemd/system/energy-data-pipeline.service`를 다음 내용으로 만든다.

```ini
[Unit]
Description=Energy data pipeline Docker stack
Wants=network-online.target
After=network-online.target open-iscsi.service docker.service
Requires=docker.service
RequiresMountsFor=/mnt/iscsi /mnt/iscsi-renewable

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/mnt/nvme/Energy-Data-pipeline
ExecStart=/usr/bin/docker compose -f /mnt/nvme/weather-pipeline/docker/docker-compose.yml up -d demand-db
ExecStart=/usr/bin/docker compose -f docker/docker-compose.yml up -d
ExecStop=/usr/bin/docker compose -f docker/docker-compose.yml stop
ExecStop=/usr/bin/docker compose -f /mnt/nvme/weather-pipeline/docker/docker-compose.yml stop demand-db
TimeoutStartSec=180
TimeoutStopSec=180

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now energy-data-pipeline.service
systemctl status energy-data-pipeline.service --no-pager
```

이 unit은 정상 종료 시 Docker 스택을 먼저 정지하고, 재부팅 시 iSCSI 마운트가
준비된 뒤 운영 compose를 시작한다. NAS가 부팅 시점에 응답하지 않아 unit이
실패했다면 NAS 복구 후 다음 한 줄로 다시 시작한다.

```bash
sudo systemctl restart energy-data-pipeline.service
```

## 고장 진단 순서

아래에서 처음 실패한 단계가 원인 계층이다.

| 단계 | 명령 | 실패 시 조치 |
|---|---|---|
| NAS 네트워크 | `ping -c 3 192.9.65.61` | NAS 전원, 케이블, VLAN, 서버 라우팅 확인 |
| DSM 관리 | `nc -zvw3 192.9.65.61 5000` | DSM/NAS 서비스 상태 확인 |
| iSCSI 포트 | `nc -zvw3 192.9.65.61 3260` | SAN Manager와 방화벽 확인 |
| iSCSI 세션 | `sudo iscsiadm -m session` | discovery, ACL, CHAP, `node.startup` 확인 |
| LUN/UUID | `lsblk -f` | DSM LUN 매핑과 udev 확인 |
| 파일시스템 | `journalctl -k -b | grep -Ei 'iscsi|I/O error|EXT4-fs|read-only'` | DB 정지, unmount, 스냅샷 후 `e2fsck` |
| 마운트 | `findmnt -T /mnt/iscsi-renewable` | `/etc/fstab`, UUID, mount unit 확인 |
| DB 디렉터리 | `test -f /mnt/iscsi-renewable/postgres/pv-data-postgres/PG_VERSION` | 잘못된 LUN/빈 bind 경로 여부 확인 |
| Docker DB | `docker compose -f docker/docker-compose.yml ps` | `logs --tail=100 pv-db` 확인 |
| PostgreSQL | `docker compose -f docker/docker-compose.yml exec -T pv-db pg_isready -U pv -d pv` | PostgreSQL 로그와 파일 권한 확인 |
| 수급 DB 마운트 | `findmnt -T /mnt/iscsi` | UUID `a85a78d5-...`와 `rw` 여부 확인 |
| 수급 DB | `docker inspect demand-postgres` | `demand-db` 시작 후 health/log 확인 |

자주 보이는 오류의 의미는 다음과 같다.

| 오류 | 의미 |
|---|---|
| `No active sessions` | iSCSI 로그인 안 됨 |
| `No records found` | 저장된 자동 로그인 target이 없음 |
| `Network is unreachable` | 잘못된 portal/라우팅; 이 장애에서는 옛 주소 사용 |
| `no such device` | Docker bind source 아래의 iSCSI 마운트가 없음 |
| `Input/output error` | 세션/LUN I/O 단절; DB 즉시 정지 필요 |
| `Read-only file system` | ext4가 보호를 위해 읽기 전용 전환; 오프라인 점검 필요 |

## 계획 정전 또는 서버 종료

systemd unit을 설치한 경우 다음 순서를 사용한다.

```bash
sudo systemctl stop energy-data-pipeline.service
sudo umount /mnt/iscsi-renewable
sudo umount /mnt/iscsi
sudo iscsiadm -m node -p 192.9.65.61:3260 --logout
sudo poweroff
```

강제 전원 차단은 ext4 저널과 PostgreSQL crash recovery만으로 완전히 보호되지
않는다. 서버와 NAS에 UPS를 사용하고, NAS보다 서버를 먼저 종료한다.
