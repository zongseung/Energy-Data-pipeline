# 발전소 지도 GitBook Integration

GitBook 페이지 안에 발전소 지도를 **박아 넣기** 위한 integration이다.

## 왜 integration이 필요한가

GitBook은 마크다운에 `<iframe>`을 쓰는 것을 CSP로 막는다.

> GitBook doesn't support direct embedding of external content using an HTML iframe due to its content security policy.
> — [GitBook 문서](https://gitbook.com/docs/help-center/editing-content/assets-and-files/can-i-embed-an-iframe-in-gitbook)

Embed 블록은 공개 URL을 카드로 보여줄 뿐 상호작용이 안 된다. 페이지 안에서 실제로
클릭·검색이 되게 하려면 integration이 `webframe` 블록을 그려 주는 이 경로뿐이다.

## 구조

```
지도 HTML (정적, 데이터 내장)          이 integration
scripts/build_plant_map.py             gitbook-manifest.yaml
   ↓ 생성                                 + src/index.tsx
docs/gitbook/assets/plant-map.html          ↓ 배포
   ↓ 업로드                              GitBook 페이지의 webframe 블록
어디든 공개 호스팅
```

지도는 **데이터를 페이지에 통째로 구운 단일 HTML**이다. 백엔드도 API도 없다.
발전소 마스터가 91행뿐이라 API를 세울 이유가 없고, 그 덕에 CORS·인증·가동시간
걱정이 전부 사라진다.

## 배포 순서

### 1. 지도 생성

```bash
uv run python scripts/build_plant_map.py
# → docs/gitbook/assets/plant-map.html (발전소 91기 / 좌표 84기 / 지점 25곳)
```

발전소가 추가되거나 좌표·품질등급이 바뀌면 이 명령만 다시 돌린다.

### 2. 조직 ID 확인

GitBook 관리 화면 URL의 `/o/<ORG_ID>/` 부분이다. `gitbook-manifest.yaml`의
`organization:`에 넣는다.

### 3. 지도 호스팅

`plant-map.html`을 공개 URL로 올린다. 정적 파일 하나라 어디든 된다
(GitHub Pages, S3, Cloudflare Pages 등).

올린 주소를 **세 곳**에 같이 넣어야 한다. 하나라도 어긋나면 블록이 빈 칸으로 뜬다.

| 파일 | 항목 |
|---|---|
| `gitbook-manifest.yaml` | `contentSecurityPolicy.frame-src` |
| `gitbook-manifest.yaml` | `blocks[].urlUnfurl` |
| `src/index.tsx` | `MAP_URL` |

### 4. 배포

```bash
npm i -g @gitbook/cli
gitbook auth        # 토큰 입력
gitbook publish .
```

### 5. 설치와 사용

GitBook 조직 설정 → Integrations에서 **발전소 지도**를 스페이스에 설치한다.
페이지 편집 중 `/발전소 지도`를 입력하면 블록이 삽입된다.

## 주의

- **공개 범위.** 매니페스트는 `visibility: private`(조직 내부)로 시작한다.
  지도에는 발전소 좌표가 들어 있으므로, 공개 GitBook에 싣기 전에 좌표를 공개해도
  되는지 먼저 확인할 것.
- **좌표 정확도.** 태양광 44기만 검증됐고 그마저 부지 POI·행정구역 중심
  근사(±2km)다. 비태양광 40기는 검증 대상이 아니었다. 지도에도 그렇게 적혀 있다.
- **좌표 없는 7기**(풍력 6, 태양광 1)는 지도에 찍히지 않는다. 페이지 우측 안내에
  개수와 연료가 표시된다.

## 로컬 확인

integration 배포 없이 지도만 보려면 브라우저로 직접 열면 된다.

```bash
xdg-open docs/gitbook/assets/plant-map.html
```
