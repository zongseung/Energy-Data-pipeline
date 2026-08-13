"""발전소 지도 webframe 페이지 생성 — research.plants 를 읽어 단일 HTML 로 굽는다.

    uv run python scripts/build_plant_map.py [출력경로]
    (기본 출력: docs/gitbook/assets/plant-map.html)

왜 정적 HTML 인가
    발전소 마스터는 91행이다. 이 정도를 위해 API 를 세우고 CORS·인증을 관리하는
    것은 낭비다. 데이터를 페이지에 통째로 굽으면 백엔드가 없어지고, GitBook
    webframe 이 어디에 호스팅되든 그대로 뜬다. 발전소가 추가되면 이 스크립트를
    다시 돌리면 된다.

왜 해안선이 아니라 격자인가
    research.plants.lat/lon 은 태양광 44기만 검증됐고 그마저 부지 POI·행정구역
    중심 근사(±2km)다. 정밀한 해안선 위에 찍으면 없는 정밀도를 주장하게 된다.
    위경도 격자를 바탕으로 쓰고 해안선은 개략 윤곽으로만 깐다.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text  # noqa: E402

from fetch_data.common.db_utils import resolve_db_url  # noqa: E402

OUT_DEFAULT = Path("docs/gitbook/assets/plant-map.html")

QUERY = """
SELECT plant_id, plant_name, unit_no, operator, fuel_type, region,
       capacity_mw, lat, lon, data_quality, is_aggregate,
       hourly_valid_from::text AS hourly_valid_from
FROM research.plants
ORDER BY plant_id
"""

# 남한 개략 윤곽 (경도, 위도). 측량용이 아니라 방향 감각용이다 — 페이지에도 그렇게 적는다.
COAST = [
    # 서해안 (인천 -> 목포)
    (126.62, 37.45), (126.32, 36.92), (126.15, 36.70), (126.52, 36.32),
    (126.38, 36.02), (126.52, 35.62), (126.32, 35.20), (126.12, 34.92),
    # 남해안 (해남 -> 부산)
    (126.32, 34.58), (126.52, 34.32), (127.02, 34.42), (127.48, 34.72),
    (128.02, 34.86), (128.60, 34.76), (129.00, 35.10),
    # 동해안 (울산 -> 고성)
    (129.35, 35.52), (129.45, 36.05), (129.40, 36.52), (129.35, 37.00),
    (129.10, 37.42), (128.90, 37.80), (128.60, 38.22), (128.35, 38.55),
    # 휴전선 (개략)
    (127.50, 38.30), (126.90, 38.00), (126.68, 37.78), (126.62, 37.45),
]
JEJU = [
    (126.16, 33.35), (126.35, 33.55), (126.75, 33.55), (126.95, 33.42),
    (126.88, 33.22), (126.50, 33.18), (126.22, 33.24), (126.16, 33.35),
]
# 방향 감각용 기준 도시 (실제 좌표)
CITIES = [
    ("서울", 37.5665, 126.9780), ("부산", 35.1796, 129.0756),
    ("대구", 35.8714, 128.6014), ("광주", 35.1595, 126.8526),
    ("대전", 36.3504, 127.3845), ("강릉", 37.7519, 128.8761),
    ("제주", 33.4996, 126.5312),
]

FUEL_LABEL = {
    "solar": "태양광", "wind": "풍력", "hydro": "수력",
    "thermal": "화력", "fuel_cell": "연료전지",
}


def fetch_plants() -> list[dict]:
    engine = create_engine(resolve_db_url(None))
    with engine.connect() as conn:
        rows = conn.execute(text(QUERY)).mappings().all()
    out = []
    for r in rows:
        d = dict(r)
        for k in ("capacity_mw", "lat", "lon"):
            if d[k] is not None:
                d[k] = float(d[k])
        out.append(d)
    return out


def build(plants: list[dict]) -> str:
    payload = json.dumps(
        {
            "plants": plants,
            "coast": COAST,
            "jeju": JEJU,
            "cities": [{"n": n, "lat": la, "lon": lo} for n, la, lo in CITIES],
            "fuelLabel": FUEL_LABEL,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return TEMPLATE.replace("/*__DATA__*/null", payload)


TEMPLATE = r"""<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>발전소 지도</title>
<style>
/* ── 팔레트 ────────────────────────────────────────────────────────────────
   전기 도면(청사진)에서 가져온 차가운 종이색. 계통 도면의 어휘를 쓰되
   네온-온-블랙 터미널 클리셰는 피한다. */
:root{
  --paper:#E6EBF1; --panel:#F2F5F8; --ink:#16202B; --ink-2:#4A5A6B;
  --rule:#B4C2CE; --rule-soft:#CFD9E2; --focus:#0B5FA5;
  --solar:#C4820A; --wind:#2E7D91; --hydro:#3F63A8; --thermal:#A8462F; --fuel_cell:#5F5199;
  --bad:#A8462F; --warn:#9A6B00;
  --sp:clamp(12px,2vw,20px);
}
:root:not([data-theme="light"]){}
@media (prefers-color-scheme: dark){
  :root:not([data-theme="light"]){
    --paper:#101820; --panel:#18232E; --ink:#DFE8F0; --ink-2:#93A5B5;
    --rule:#33465A; --rule-soft:#243343; --focus:#6FB4EC;
    --solar:#E6A92C; --wind:#5FB6CB; --hydro:#7C9BE0; --thermal:#DC7A5F; --fuel_cell:#9C8BD8;
    --bad:#DC7A5F; --warn:#D7A22B;
  }
}
:root[data-theme="dark"]{
  --paper:#101820; --panel:#18232E; --ink:#DFE8F0; --ink-2:#93A5B5;
  --rule:#33465A; --rule-soft:#243343; --focus:#6FB4EC;
  --solar:#E6A92C; --wind:#5FB6CB; --hydro:#7C9BE0; --thermal:#DC7A5F; --fuel_cell:#9C8BD8;
  --bad:#DC7A5F; --warn:#D7A22B;
}

*{box-sizing:border-box}
html,body{margin:0;height:100%}
body{
  background:var(--paper); color:var(--ink);
  font-family:-apple-system,BlinkMacSystemFont,"Pretendard","Apple SD Gothic Neo",
              "Malgun Gothic","Noto Sans KR",sans-serif;
  font-size:14px; line-height:1.5;
  display:flex; flex-direction:column;
}
/* 계측기 눈금 글자 — 숫자·코드·라벨은 전부 등폭으로. 계기 판독의 어휘. */
.mono{font-family:ui-monospace,"SF Mono","Cascadia Mono",Menlo,Consolas,monospace;
      font-variant-numeric:tabular-nums}

/* ── 상단 제어 레일 ─────────────────────────────────────────────────── */
header{
  border-bottom:1px solid var(--rule); padding:var(--sp);
  display:flex; flex-wrap:wrap; gap:12px 18px; align-items:flex-end;
}
h1{
  margin:0; font-size:clamp(15px,2.4vw,19px); font-weight:640;
  letter-spacing:-.02em; white-space:nowrap;
}
h1 small{
  display:block; font-weight:400; font-size:11px; letter-spacing:.08em;
  color:var(--ink-2); text-transform:uppercase; margin-bottom:3px;
  font-family:ui-monospace,Menlo,monospace;
}
.search{flex:1 1 200px; min-width:160px}
.search input{
  width:100%; padding:7px 10px; background:var(--panel); color:var(--ink);
  border:1px solid var(--rule); border-radius:2px; font:inherit;
}
.search input:focus-visible{outline:2px solid var(--focus); outline-offset:1px}

.fuels{display:flex; flex-wrap:wrap; gap:6px}
.chip{
  display:inline-flex; align-items:center; gap:6px; cursor:pointer;
  padding:5px 10px 5px 7px; border:1px solid var(--rule); border-radius:2px;
  background:transparent; color:var(--ink-2); font:inherit; font-size:12px;
  transition:color .12s, border-color .12s, background .12s;
}
.chip:focus-visible{outline:2px solid var(--focus); outline-offset:1px}
.chip .sw{width:9px; height:9px; border-radius:1px; background:var(--c);
          box-shadow:inset 0 0 0 1px rgba(0,0,0,.18)}
.chip[aria-pressed="true"]{color:var(--ink); border-color:var(--c); background:var(--panel)}
.chip[aria-pressed="false"] .sw{background:transparent; box-shadow:inset 0 0 0 1px var(--rule)}
.chip b{font-family:ui-monospace,Menlo,monospace; font-weight:600; font-size:11px}

/* ── 본문 ───────────────────────────────────────────────────────────── */
main{flex:1; display:grid; grid-template-columns:1fr 300px; min-height:0}
@media (max-width:760px){ main{grid-template-columns:1fr; grid-template-rows:1fr auto} }

#map{width:100%; height:100%; display:block; touch-action:manipulation}
.gridline{stroke:var(--rule-soft); stroke-width:.5}
.gridlabel{fill:var(--ink-2); font-size:8px; font-family:ui-monospace,Menlo,monospace}
.coast{fill:none; stroke:var(--rule); stroke-width:1; opacity:.55}
.city{fill:var(--ink-2)}
.citytick{stroke:var(--ink-2); stroke-width:.7}

/* 부지 노드 — 호기 구성 막대. 이 페이지의 시그니처. */
.site{cursor:pointer}
.site .seg{stroke:var(--paper); stroke-width:.5}
.site .halo{fill:none; stroke:var(--focus); stroke-width:2; opacity:0}
.site[data-on="1"] .halo{opacity:1}
.site:focus-visible{outline:none}
.site:focus-visible .halo{opacity:1}
.site .cnt{fill:var(--ink); font-size:8.5px; font-weight:600;
           font-family:ui-monospace,Menlo,monospace; pointer-events:none}
.site.dim{opacity:.16}
/* 시간별 신뢰 불가 호기는 빗금 — 색으로만 구분하면 색각 이상에서 사라진다 */
.hatch{stroke:var(--paper); stroke-width:1}

/* ── 상세 패널 ──────────────────────────────────────────────────────── */
aside{
  border-left:1px solid var(--rule); background:var(--panel);
  padding:var(--sp); overflow-y:auto; min-height:0;
}
@media (max-width:760px){ aside{border-left:0; border-top:1px solid var(--rule); max-height:44vh} }
aside h2{margin:0 0 2px; font-size:15px; letter-spacing:-.01em}
.coord{font-size:11px; color:var(--ink-2); margin-bottom:12px}
.units{list-style:none; margin:0 0 14px; padding:0}
.units li{
  display:grid; grid-template-columns:auto 1fr auto; gap:8px; align-items:baseline;
  padding:6px 0; border-bottom:1px solid var(--rule-soft); font-size:12.5px;
}
.units .id{font-family:ui-monospace,Menlo,monospace; font-size:11px; color:var(--ink-2)}
.units .nm{overflow-wrap:anywhere}
.units .q{font-size:10.5px; white-space:nowrap; font-family:ui-monospace,Menlo,monospace}
.q.ok{color:var(--ink-2)} .q.warn{color:var(--warn)} .q.bad{color:var(--bad)}
.dot{display:inline-block; width:7px; height:7px; border-radius:1px; margin-right:5px}

.sqlbox{margin-top:4px}
.sqlbox label{font-size:10px; letter-spacing:.09em; text-transform:uppercase;
  color:var(--ink-2); font-family:ui-monospace,Menlo,monospace}
pre{
  margin:5px 0 0; padding:9px; background:var(--paper); color:var(--ink);
  border:1px solid var(--rule); border-radius:2px; font-size:11px; line-height:1.45;
  /* 호기가 많은 부지는 IN 목록이 길다(영흥 11기). pre 로 두면 옆으로 잘려 보이고
     읽으려면 가로 스크롤을 해야 한다. 줄 구조는 지키되 긴 줄만 접는다. */
  white-space:pre-wrap; overflow-wrap:anywhere; font-family:ui-monospace,Menlo,monospace;
}
button.copy{
  margin-top:6px; padding:5px 9px; font:inherit; font-size:11.5px; cursor:pointer;
  background:transparent; color:var(--ink); border:1px solid var(--rule); border-radius:2px;
}
button.copy:hover{border-color:var(--focus)}
button.copy:focus-visible{outline:2px solid var(--focus); outline-offset:1px}
.empty{color:var(--ink-2); font-size:12.5px}
.empty b{color:var(--ink); font-weight:600}

footer{
  border-top:1px solid var(--rule); padding:8px var(--sp);
  font-size:11px; color:var(--ink-2); display:flex; flex-wrap:wrap; gap:4px 16px;
}
@media (prefers-reduced-motion:reduce){ *{transition:none !important; animation:none !important} }
</style>
</head>
<body>
<header>
  <h1><small>research.plants</small>발전소 지도</h1>
  <div class="search">
    <label class="visually-hidden" for="q" style="position:absolute;left:-9999px">발전소 이름 검색</label>
    <input id="q" type="search" placeholder="발전소 이름으로 찾기 — 예: 영흥, 삼천포" autocomplete="off">
  </div>
  <div class="fuels" id="fuels" role="group" aria-label="연료 필터"></div>
</header>

<main>
  <svg id="map" role="img" aria-label="발전소 위치 지도"></svg>
  <aside id="panel" aria-live="polite"></aside>
</main>

<footer>
  <span id="stat" class="mono"></span>
  <span>좌표는 부지 근사값(±2km). 해안선은 방향 감각용 개략선이며 측량 자료가 아니다.</span>
</footer>

<script>
const DATA = /*__DATA__*/null;
const FUELS = ["solar","wind","hydro","thermal","fuel_cell"];
const active = new Set(FUELS);
let query = "", selected = null;

const withCoord = DATA.plants.filter(p => p.lat != null && p.lon != null);
// 같은 부지에 여러 호기가 있다 — 분당 16기, 영흥 11기. 지점 단위로 묶어야
// 핀이 겹쳐 읽히지 않는다. 소수 4자리(≈10m)면 같은 부지가 정확히 뭉친다.
const sites = [...withCoord.reduce((m, p) => {
  const k = p.lat.toFixed(4) + "," + p.lon.toFixed(4);
  (m.get(k) || m.set(k, {lat:p.lat, lon:p.lon, units:[]}).get(k)).units.push(p);
  return m;
}, new Map()).values()];
sites.forEach(s => s.units.sort((a,b) => a.plant_id - b.plant_id));

const noCoord = DATA.plants.filter(p => p.lat == null);
const qual = p =>
  p.data_quality === "전면무효" ? ["bad","전면무효"] :
  p.data_quality === "시간별무효" ? ["warn", p.hourly_valid_from ? "시간별무효 · " + p.hourly_valid_from + "~ 유효" : "시간별무효"] :
  p.data_quality === "미검증" ? ["warn","미검증"] : ["ok","정상"];

/* ── 투영 ──────────────────────────────────────────────────────────────
   등장방형(경도·위도를 그대로 x·y). 위도 35도 부근에서 가로를 cos(35°)만큼
   눌러 종횡비를 맞춘다. 측량 정확도를 주장하지 않으므로 이 정도로 충분하다. */
const BOUNDS = {lon0:125.9, lon1:129.7, lat0:32.9, lat1:38.7};
const KX = Math.cos(35.8 * Math.PI / 180);
let VW = 800, VH = 600, pad = 34;
const svg = document.getElementById("map");

function project(lat, lon){
  const w = (BOUNDS.lon1 - BOUNDS.lon0) * KX, h = BOUNDS.lat1 - BOUNDS.lat0;
  const s = Math.min((VW - pad*2) / w, (VH - pad*2) / h);
  return [
    (VW - w*s)/2 + (lon - BOUNDS.lon0) * KX * s,
    (VH - h*s)/2 + (BOUNDS.lat1 - lat) * s,
  ];
}
const esc = s => String(s).replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));

function draw(){
  const r = svg.getBoundingClientRect();
  VW = Math.max(320, r.width); VH = Math.max(280, r.height);
  svg.setAttribute("viewBox", `0 0 ${VW} ${VH}`);
  const out = [];

  // 위경도 격자 — 이 페이지의 바탕. 계측기 눈금처럼 읽힌다.
  for(let lon = 126; lon <= 129.5; lon += 0.5){
    const [x] = project(BOUNDS.lat0, lon), [,y0] = project(BOUNDS.lat1, lon), [,y1] = project(BOUNDS.lat0, lon);
    out.push(`<line class="gridline" x1="${x}" y1="${y0}" x2="${x}" y2="${y1}"/>`);
    if(lon % 1 === 0) out.push(`<text class="gridlabel" x="${x+2}" y="${y1-3}">${lon}°E</text>`);
  }
  for(let lat = 33; lat <= 38.5; lat += 0.5){
    const [,y] = project(lat, 126), [x0] = project(lat, BOUNDS.lon0), [x1] = project(lat, BOUNDS.lon1);
    out.push(`<line class="gridline" x1="${x0}" y1="${y}" x2="${x1}" y2="${y}"/>`);
    if(lat % 1 === 0) out.push(`<text class="gridlabel" x="${x0+2}" y="${y-2}">${lat}°N</text>`);
  }
  const path = pts => pts.map(([lo,la],i) => (i?"L":"M") + project(la,lo).map(v=>v.toFixed(1)).join(" ")).join("");
  out.push(`<path class="coast" d="${path(DATA.coast)}"/>`, `<path class="coast" d="${path(DATA.jeju)}"/>`);

  DATA.cities.forEach(c => {
    const [x,y] = project(c.lat, c.lon);
    out.push(`<line class="citytick" x1="${x-3}" y1="${y}" x2="${x+3}" y2="${y}"/>`,
             `<line class="citytick" x1="${x}" y1="${y-3}" x2="${x}" y2="${y+3}"/>`,
             `<text class="city gridlabel" x="${x+5}" y="${y+3}">${c.n}</text>`);
  });

  // 부지 노드 — 호기를 연료별로 쌓은 막대. 핀 모양이 곧 구성표다.
  sites.forEach((s, i) => {
    const vis = s.units.filter(u => active.has(u.fuel_type) && matches(u));
    const [x,y] = project(s.lat, s.lon);
    const n = s.units.length, H = Math.min(34, 8 + n*1.9), W = 9;
    let acc = 0;
    const segs = FUELS.map(f => {
      const c = s.units.filter(u => u.fuel_type === f).length;
      if(!c) return "";
      const h = H * c / n, yy = y - H/2 + acc; acc += h;
      const broken = s.units.some(u => u.fuel_type === f && u.data_quality !== "정상");
      return `<rect class="seg" x="${x-W/2}" y="${yy}" width="${W}" height="${h}" fill="var(--${f})"`
           + (broken ? ` stroke-dasharray="2 2"` : ``) + `/>`;
    }).join("");
    // 작은 핀은 누르기 어렵다 — 최소 24px 짜리 투명 판을 깔아 터치 대상을 넓힌다.
    const hw = Math.max(24, W + 10), hh = Math.max(24, H + 10);
    out.push(
      `<g class="site${vis.length?"":" dim"}" data-i="${i}" data-on="${selected===i?1:0}" `
      + `tabindex="0" role="button" aria-label="${esc(label(s))} ${n}기">`
      + `<rect fill="transparent" x="${x-hw/2}" y="${y-hh/2}" width="${hw}" height="${hh}"/>`
      + `<rect class="halo" x="${x-W/2-3}" y="${y-H/2-3}" width="${W+6}" height="${H+6}" rx="2"/>`
      + segs
      + (n>1 ? `<text class="cnt" x="${x+W/2+3}" y="${y+3}">${n}</text>` : ``)
      + `</g>`);
  });
  svg.innerHTML = out.join("");
  svg.querySelectorAll(".site").forEach(g => {
    const pick = () => select(+g.dataset.i);
    g.addEventListener("click", pick);
    g.addEventListener("keydown", e => { if(e.key==="Enter"||e.key===" "){ e.preventDefault(); pick(); } });
  });
}

/* 부지 이름 — 발전소명은 회사 접두어가 붙어 있어서(‘한국남부발전(주)_신인천소내
   태양광발전실적’) 앞에서 자르면 회사명만 남는다. 접두어를 떼고 그 부지 호기들의
   **최장 공통 접두**를 쓴다. 그러면 신인천 6기 -> ‘신인천’, 분당 16기 -> ‘분당’,
   영흥 11기 -> ‘영흥’ 처럼 사람이 부르는 이름이 그대로 나온다. */
const OP_PREFIX = /^[^_]*(?:발전\(주\)|공사|발전)_/;
const clean = n => n.replace(OP_PREFIX, "").trim();
function label(s){
  const names = s.units.map(u => clean(u.plant_name));
  let p = names[0];
  for(const n of names.slice(1)){
    let i = 0; while(i < p.length && i < n.length && p[i] === n[i]) i++;
    p = p.slice(0, i);
  }
  // 한 부지의 호기 이름이 완전히 같으면(삼척소내 3기) 공통 접두가 이름 전체가 된다.
  // 원천 표기의 꼬리말을 떼어 부지명만 남긴다.
  p = p.replace(/\s*(태양광)?발전실적$/, "").replace(/[\s_#·,\-]*\d*$/, "").trim();
  return p || names[0];
}
const matches = u => !query || u.plant_name.toLowerCase().includes(query);

function select(i){ selected = i; draw(); renderPanel(); }

function renderPanel(){
  const el = document.getElementById("panel");
  if(selected === null){
    const shown = sites.reduce((a,s)=>a+s.units.filter(u=>active.has(u.fuel_type)&&matches(u)).length,0);
    el.innerHTML = `<p class="empty">지도에서 <b>부지</b>를 고르면 호기 목록과 바로 쓸 SQL 이 나온다.`
      + `<br><br>막대 한 칸이 호기 하나다. 점선 칸은 <b>시간별 데이터를 믿을 수 없는</b> 호기다.`
      + (noCoord.length ? `<br><br>좌표가 없어 지도에 못 찍은 발전소가 <b>${noCoord.length}기</b> 있다 `
          + `(${esc([...new Set(noCoord.map(p=>DATA.fuelLabel[p.fuel_type]||p.fuel_type))].join(", "))}).` : ``)
      + `</p>`;
    document.getElementById("stat").textContent =
      `지점 ${sites.length} · 표시 호기 ${shown}/${DATA.plants.length}`;
    return;
  }
  const s = sites[selected];
  const us = s.units.filter(u => active.has(u.fuel_type));
  const ids = us.map(u => u.plant_id).join(", ");
  const sql = `SELECT timestamp, plant_name, gen_kwh\nFROM research.generation\nWHERE plant_id IN (${ids})\n  AND timestamp >= '2026-01-01'\nORDER BY timestamp;`;

  // 필터가 이 부지의 호기를 전부 걸러낸 상태. 빈 화면만 보여 주면 사용자는
  // 데이터가 없는 줄 안다 — 이 부지에 실제로 뭐가 있는지 말해 주고 되돌릴 길을 준다.
  if(!us.length){
    const have = [...new Set(s.units.map(u => DATA.fuelLabel[u.fuel_type] || u.fuel_type))];
    el.innerHTML =
      `<h2>${esc(label(s))}</h2>`
      + `<div class="coord mono">${s.lat.toFixed(4)}°N ${s.lon.toFixed(4)}°E · ${s.units.length}기</div>`
      + `<p class="empty">지금 켜 둔 연료 필터에 맞는 호기가 이 부지에 없다.<br><br>`
      + `이 부지는 <b>${esc(have.join(" · "))}</b> ${s.units.length}기다. `
      + `위에서 해당 연료를 다시 켜면 목록과 SQL 이 나온다.</p>`;
    document.getElementById("stat").textContent = `${label(s)} · 호기 0/${s.units.length}`;
    return;
  }

  el.innerHTML =
    `<h2>${esc(label(s))}</h2>`
    + `<div class="coord mono">${s.lat.toFixed(4)}°N ${s.lon.toFixed(4)}°E · ${s.units.length}기</div>`
    + `<ul class="units">` + us.map(u => {
        const [cls, txt] = qual(u);
        return `<li><span class="id">#${u.plant_id}</span>`
          + `<span class="nm"><span class="dot" style="background:var(--${u.fuel_type})"></span>${esc(u.plant_name)}`
          + (u.capacity_mw != null ? ` <span class="mono" style="color:var(--ink-2)">${u.capacity_mw}MW</span>` : ``)
          + `</span><span class="q ${cls}">${esc(txt)}</span></li>`;
      }).join("") + `</ul>`
    + `<div class="sqlbox"><label for="sql">이 부지 발전량 조회</label>`
    + `<pre id="sql">${esc(sql)}</pre>`
    + `<button class="copy" type="button">SQL 복사</button></div>`;
  el.querySelector(".copy").addEventListener("click", e => {
    navigator.clipboard.writeText(sql).then(() => {
      e.target.textContent = "복사됨"; setTimeout(() => e.target.textContent = "SQL 복사", 1400);
    }).catch(() => e.target.textContent = "복사 실패 — 직접 선택하세요");
  });
  document.getElementById("stat").textContent =
    `${label(s)} · 호기 ${us.length}/${s.units.length}`;
}

// 연료 필터 — 개수를 함께 보여 준다. 필터가 무엇을 지우는지 미리 알 수 있어야 한다.
const fuelBar = document.getElementById("fuels");
FUELS.forEach(f => {
  const n = DATA.plants.filter(p => p.fuel_type === f).length;
  const b = document.createElement("button");
  b.className = "chip"; b.type = "button"; b.style.setProperty("--c", `var(--${f})`);
  b.setAttribute("aria-pressed", "true");
  b.innerHTML = `<span class="sw"></span>${DATA.fuelLabel[f] || f} <b>${n}</b>`;
  b.addEventListener("click", () => {
    active.has(f) ? active.delete(f) : active.add(f);
    b.setAttribute("aria-pressed", active.has(f));
    draw(); renderPanel();
  });
  fuelBar.appendChild(b);
});

document.getElementById("q").addEventListener("input", e => {
  query = e.target.value.trim().toLowerCase();
  selected = null; draw(); renderPanel();
});

addEventListener("resize", () => { draw(); renderPanel(); });
draw(); renderPanel();
</script>
</body>
</html>
"""


def main() -> int:
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else OUT_DEFAULT
    plants = fetch_plants()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(build(plants), encoding="utf-8")
    with_coord = sum(1 for p in plants if p["lat"] is not None)
    sites = len({(round(p["lat"], 4), round(p["lon"], 4)) for p in plants if p["lat"]})
    print(f"{out} 생성 — 발전소 {len(plants)}기 / 좌표 {with_coord}기 / 지점 {sites}곳")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
