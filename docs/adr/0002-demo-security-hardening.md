# 데모·연구원 접근 경로의 보안 정리와 저장소 감사 (2026-08-25)

이용조건 문서(`docs/gitbook/05-terms.md`)를 공개하고 나서 실제 DB·데모 상태를 대조한
결과, **문서가 약속한 것과 실제가 어긋나는 지점 두 개**를 찾았다. 여기에 저장소 감사에서
나온 정리 항목을 합쳐 처리 순서를 정한다.

## 사실 (2026-08-25 확인)

| 항목 | 확인 방법 | 결과 |
|---|---|---|
| 로그인 가능한 DB role | `pg_roles where rolcanlogin` | `pv`(슈퍼유저), `repl` 둘뿐 |
| `research_ro` | 같은 쿼리 | 로그인 불가 그룹, 소속 멤버 0명 |
| 데모 MCP 접속 계정 | `compose.yml` `ENERGY_MCP_DSN` | `pv` — `rolsuper = t` |
| LibreChat 가입 | `printenv ALLOW_REGISTRATION` | `true` |
| 데모 노출 | `docker port` | 8099·8098 이 `0.0.0.0` |
| exports 누적 | `du -sh /exports` | 42파일 53MB, 정리 로직 없음 |

## 문제

**S1 — 개인별 감사 로그가 물리적으로 불가능하다.** 05-terms 4항은 "발급받은 role 로 실행하는
모든 SQL 이 감사 로그로 기록된다"고 약속하고 03-llm-mcp 는 "발급받은 개인 role"로 접속하라고
안내하지만, 발급된 개인 role 이 하나도 없다. 지금 상태로 연구원을 받으면 전원이 같은 계정을
쓰게 되고 4항은 지킬 수 없는 약속이 된다.

**S2 — 데모가 슈퍼유저로 DB 에 붙는다.** `pv` 는 슈퍼유저라 읽기전용 세션이 쓰기만 막을 뿐
`research` 스키마 한정이 아니다. 여기에 가입 개방 + LAN 바인드가 겹쳐, LAN 에 닿는 누구나
가입해 슈퍼유저 권한으로 조회할 수 있는 경로가 완성돼 있다. `compose.yml` 주석의
"시연 배포 후 false 로 잠글 것"이 아직 안 지켜졌다.

**S3 — CSV 익스포트에 수명도 접근 제어도 없다.** 모든 쿼리가 파일을 남기고(잘림 여부와 무관),
파일명 8자리가 유일한 방어선이며, 링크는 nginx 액세스 로그에 그대로 남는다.

## 처리 순서

우선순위는 "약속과 실제의 간극"이 큰 것부터다.

1. **S2-a 데모 가입 잠그기** — `ALLOW_REGISTRATION: "false"`. 한 줄이고 되돌리기 쉽다.
2. **S2-b 데모 전용 읽기전용 role** — `research_ro` 를 상속받는 로그인 role 을 만들어
   `ENERGY_MCP_DSN` 을 교체한다. 슈퍼유저 경로를 끊는다.
3. **S1 개인 role 발급** — 연구원별 로그인 role 을 `research_ro` 소속으로 만든다.
   이게 끝나야 05-terms 4항이 참이 되고, 8명분 원격 MCP(`compose.mcp-users.yml`)도 뜬다.
4. **S3 익스포트 수명** — 오래된 파일 정리. 잘린 결과에만 파일을 만들도록 조건을 좁히는 것도
   같이 검토한다.

## 저장소 감사 (별건, P1)

- `.worktrees/gitbook-research-guide` 812MB — main 이 이미 앞서 있다(branch→main diff 가 -2299줄).
- 코드가 읽지만 아무도 세팅하지 않는 환경변수 14개. `NAMDONG_*` 6개는 CLAUDE.md 에
  "Key Environment Variables" 로 문서화까지 돼 있는데 실체가 없다.
- `scripts/verify_humanize.py` 124줄 — 미참조 1회성.

자르지 않기로 한 것: `scripts/migrations/`(README 가 삭제 금지를 명시하고 라이브 트리거
정의를 포함한다), `fetch_data/wind/`(휴면이지 죽은 코드가 아니다),
`fetch_data/common/logger.py`(17줄 줄이자고 10개 파일을 건드릴 값어치가 없다).

## 이 문서가 다루지 않는 것

발전소 지도(`scripts/build_plant_map.py`)와 MCP 지도 지시문은 보안과 무관해 별도로 다룬다.
